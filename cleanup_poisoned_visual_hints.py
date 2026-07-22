"""
Cleanup Poisoned Visual Hints (one-off)
------------------------------------------
Purges visual_hint/canonical_show data poisoned by identify_show.py's
vision-layer bug (fixed 2026-07-23): qwen/qwen3.6-27b's <think>...</think>
reasoning block getting truncated by too-tight max_tokens and saved
verbatim as visual_hint instead of a real title. See identify_show.py's
module docstring for the full explanation.

Finds:
  1. posts.visual_hint or posts.canonical_show containing '<think>'
     (the direct poison signature), OR longer than a real title should
     ever be (heuristic backstop in case tags were partially stripped
     by something upstream but garbage text remains)
  2. canonical_shows rows whose canonical_name or any alias contains
     '<think>' — franchise-level rows that got created FROM a poisoned
     visual_hint via show_canon.canonicalize()

For each poisoned post: clears visual_hint, canonical_show, and
identified_at, so identify_show.py picks it up again on the next run and
reprocesses it with the fixed vision call.

For each poisoned canonical_shows row: deletes it entirely, and clears
canonical_show on ANY post referencing it (not just the ones caught by
the posts-table scan above) so nothing is left pointing at a deleted row.

USAGE:
  python cleanup_poisoned_visual_hints.py           # dry run — lists what would change
  python cleanup_poisoned_visual_hints.py --apply   # actually applies the cleanup
"""

import sys
import json

import db

MAX_PLAUSIBLE_TITLE_LEN = 150  # a real anime title is never this long


def find_poisoned_posts():
    conn = db.get_connection()
    rows = conn.execute(
        """SELECT key, channel, post_id, visual_hint, canonical_show
           FROM posts
           WHERE (visual_hint IS NOT NULL AND (
                    visual_hint LIKE '%<think>%'
                    OR length(visual_hint) > ?
                 ))
              OR (canonical_show IS NOT NULL AND (
                    canonical_show LIKE '%<think>%'
                    OR length(canonical_show) > ?
                 ))""",
        (MAX_PLAUSIBLE_TITLE_LEN, MAX_PLAUSIBLE_TITLE_LEN),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def find_poisoned_canonical_shows():
    conn = db.get_connection()
    rows = conn.execute("SELECT * FROM canonical_shows").fetchall()
    conn.close()
    poisoned = []
    for row in rows:
        aliases = json.loads(row["aliases"])
        name_bad = "<think>" in row["canonical_name"].lower() or len(row["canonical_name"]) > MAX_PLAUSIBLE_TITLE_LEN
        alias_bad = any("<think>" in a.lower() or len(a) > MAX_PLAUSIBLE_TITLE_LEN for a in aliases)
        if name_bad or alias_bad:
            poisoned.append(dict(row))
    return poisoned


def main():
    apply_changes = "--apply" in sys.argv
    db.init_db()

    poisoned_posts = find_poisoned_posts()
    poisoned_shows = find_poisoned_canonical_shows()

    if not poisoned_posts and not poisoned_shows:
        print("No poisoned visual_hint/canonical_show data found — clean.")
        return

    if poisoned_posts:
        print(f"{len(poisoned_posts)} poisoned post(s):\n")
        for p in poisoned_posts:
            preview = (p["visual_hint"] or p["canonical_show"] or "")[:80].replace("\n", " ")
            print(f"  {p['key']}: {preview!r}...")

    if poisoned_shows:
        print(f"\n{len(poisoned_shows)} poisoned canonical_shows row(s):\n")
        for s in poisoned_shows:
            print(f"  id={s['id']}  canonical_name={s['canonical_name'][:80]!r}...")

    if not apply_changes:
        print(f"\nDry run — nothing changed. Re-run with --apply to actually "
              f"clear {len(poisoned_posts)} post(s) and delete "
              f"{len(poisoned_shows)} canonical_shows row(s).")
        return

    conn = db.get_connection()

    for p in poisoned_posts:
        conn.execute(
            "UPDATE posts SET visual_hint = NULL, canonical_show = NULL, "
            "identified_at = NULL WHERE key = ?",
            (p["key"],),
        )

    bad_names = [s["canonical_name"] for s in poisoned_shows]
    for name in bad_names:
        # Clear ANY post referencing this canonical name, not just the
        # ones the posts-table scan above already caught — a clean post
        # could have fuzzy-matched onto a poisoned canonical_shows row.
        conn.execute(
            "UPDATE posts SET canonical_show = NULL, identified_at = NULL "
            "WHERE canonical_show = ?", (name,),
        )
        conn.execute("DELETE FROM canonical_shows WHERE canonical_name = ?", (name,))

    conn.commit()
    conn.close()

    print(f"\nCleared {len(poisoned_posts)} post(s) and deleted "
          f"{len(poisoned_shows)} canonical_shows row(s). All affected posts "
          f"had identified_at cleared too — identify_show.py will "
          f"reprocess them (with the fixed vision call) on its next run.")


if __name__ == "__main__":
    main()