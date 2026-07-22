"""
Cleanup Orphan Digest Records (one-off)
------------------------------------------
Purges every EXISTING phantom row in the `digests` table — rows with
built_at IS NULL, meaning a digest build that started (created a folder
under finished/news/<digest_id>/) but never actually finished (no
video.mp4). These accumulate when build_news_digest.py gets SIGKILLed
mid-build (screen lock / backgrounded without a wake lock — the same
un-catchable failure mode documented on run_summary.py's
close_stale_runs()), which skips its own except-block cleanup
(shutil.rmtree(out_dir)), and an older version of
db.sync_digests_from_filesystem() would then insert a `digests` row for
that leftover folder regardless of whether it ever produced a video.

This is a ONE-TIME cleanup for rows that already exist from before that
function was fixed (see db.py's sync_digests_from_filesystem() docstring
— it now refuses to create a row until video.mp4 exists, so this
situation shouldn't recur going forward).

Does NOT touch any digest with built_at set (a real, finished digest) —
those are left completely alone regardless of publish status.

Does NOT delete anything on disk automatically — it only reports whether
each phantom digest's folder still exists under finished/news/, so you
can decide whether to rm -rf it yourself (some may already be gone if a
partial cleanup did run before the kill).

USAGE:
  python cleanup_orphan_digests.py           # dry run — lists what it would remove
  python cleanup_orphan_digests.py --apply   # actually deletes the phantom rows
"""

import os
import sys

import db

FINISHED_DIR = "finished"


def find_orphans():
    conn = db.get_connection()
    rows = conn.execute(
        "SELECT digest_id, built_at FROM digests WHERE built_at IS NULL"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def main():
    apply_changes = "--apply" in sys.argv

    db.init_db()
    orphans = find_orphans()

    if not orphans:
        print("No orphan digest records found (nothing with built_at IS NULL).")
        return

    print(f"{len(orphans)} orphan digest record(s) found "
          f"(built_at IS NULL — never finished building):\n")

    for o in orphans:
        digest_id = o["digest_id"]
        folder = os.path.join(FINISHED_DIR, "news", digest_id)
        exists_on_disk = os.path.isdir(folder)
        status = f"YES — {folder}" if exists_on_disk else "no, already gone"
        print(f"  {digest_id}  (folder on disk: {status})")

    if not apply_changes:
        print(f"\nDry run — nothing changed. Re-run with --apply to actually "
              f"delete these {len(orphans)} phantom record(s) from pipeline.db.")
        print("(Any leftover folder listed above is NOT deleted automatically — "
              "remove those manually with rm -rf if you don't want them.)")
        return

    conn = db.get_connection()
    for o in orphans:
        digest_id = o["digest_id"]
        conn.execute("DELETE FROM digests WHERE digest_id = ?", (digest_id,))
        conn.execute("DELETE FROM digest_posts WHERE digest_id = ?", (digest_id,))
        conn.execute("DELETE FROM publish_log WHERE content_key = ?", (digest_id,))
    conn.commit()
    conn.close()

    print(f"\nRemoved {len(orphans)} phantom digest record(s) from pipeline.db.")
    print("Leftover folders on disk (if any, listed above) were NOT deleted — "
          "remove manually if you want a full clean slate.")


if __name__ == "__main__":
    main()
