"""
Pipeline Stage Reset
----------------------
When a stage produced a bad result — a wrong caption, a failed publish
that needs retrying, a digest you want rebuilt — this clears the right
record(s) in pipeline.db so the normal pipeline scripts pick that item
back up on their next run, same as if that stage had never completed.

FOUR COMMANDS:

  1. STAGE — clear one pipeline stage on a clip.
       python reset_stage.py stage <channel>/<post_id> <stage_name>
       e.g. python reset_stage.py stage otaku_usa/4341df27f7104bffd46f captioned

     IMPORTANT — not every stage is a simple timestamp clear:
       - captioned: also clears the saved caption_tiktok/caption_youtube
         text. generate_captions.py's skip-check looks at whether that
         TEXT exists, not the timestamp — clearing only the timestamp
         would leave the old (bad) caption in place and get skipped again.
       - identified: also clears visual_hint/canonical_show, so stale
         data doesn't linger even though identify_show.py's own skip-check
         is timestamp-based (this is just cleanliness, not required).
       - thumbnailed: THIS SCRIPT CANNOT FORCE A RETHUMBNAIL BY ITSELF.
         generate_thumbnails.py decides what needs a thumbnail by checking
         whether finished/<channel>/<post_id>/thumbnail.jpg exists on
         disk — not the database. Clearing thumbnailed_at without also
         deleting that file does nothing; the very next sync will just
         re-mark it done because the file's still there. Delete the file
         yourself first:
           rm finished/<channel>/<post_id>/thumbnail.jpg
         then run this command to clear the DB timestamp too.
       - hooked: BE CAREFUL. add_hook_overlay.py burns text directly INTO
         finished/<channel>/<post_id>/video.mp4 — it doesn't work from a
         separate untouched source. If you reset this stage and re-run
         add_hook_overlay.py, it will burn a SECOND hook overlay on top of
         the first one, on the same file. Only reset this if you still
         have a pre-hook backup of that video to restore first, or are
         prepared to also reset+rerun 'subtitled' (which rebuilds
         finished/.../video.mp4 from edited/, wiping the old hook) so the
         hook step then starts from a clean file.

  2. PUBLISH — clear a failed (or successful, if you really mean to
     re-publish) publish attempt. A FAILED publish_log row is terminal by
     design (won't auto-retry — see publish_youtube.py/publish_tiktok.py
     docstrings), specifically to avoid an infinite retry loop on a
     permanently broken asset. Use this once you've confirmed the actual
     problem is fixed.
       python reset_stage.py publish <content_key> <platform>
       e.g. python reset_stage.py publish otaku_usa/4341df27f7104bffd46f youtube
       e.g. python reset_stage.py publish digest_20260717_195731 youtube

  3. DIGEST — undo an entire built digest: reverts every post that went
     into it back to status='downloaded' (eligible for a fresh digest),
     deletes the digest's database record, and clears any publish
     attempts logged for it. Does NOT delete the video files on disk —
     the message this prints tells you the exact folder to remove
     yourself if you want a full clean slate.

     Also handles a PHANTOM digest — one that exists as a row in the
     `digests` table but has zero linked posts in digest_posts (e.g. a
     build that got SIGKILLed mid-way and never actually produced a
     video.mp4, but still ended up with a row via sync_digests_from_
     filesystem() before that function was fixed to require video.mp4
     first — see db.py's sync_digests_from_filesystem() docstring). A
     previous version of this command bailed out before ever touching the
     digests row in that case, printing "no digest found" while the
     phantom row sat there forever. Now it just deletes the row (there
     are no posts to revert, since none were ever linked).
       python reset_stage.py digest <digest_id>
       e.g. python reset_stage.py digest digest_20260717_195731

  4. QUALITY — clear a quality-gate verdict so quality_gate.py re-examines
     the item on its next run. quality_status is a terminal verdict (same
     convention as publish_log) — quality_gate.py will never re-check
     something it's already gated unless you clear it here first. Use this
     after fixing whatever caused a 'rejected' or 'held' verdict (e.g. you
     re-ran subtitled/hooked to produce a clean file, or you just adjusted
     QUALITY_APPROVE_THRESHOLD/QUALITY_REJECT_THRESHOLD in .env and want a
     held item re-scored against the new bar) — NOT as a way to force
     through an item whose underlying content is still actually broken.
       python reset_stage.py quality <content_key> <clip|digest>
       e.g. python reset_stage.py quality otaku_usa/4341df27f7104bffd46f clip
       e.g. python reset_stage.py quality digest_20260717_195731 digest

Nothing here touches files on disk except where explicitly noted above —
by default this only modifies pipeline.db.

USAGE:
  python reset_stage.py stage <channel>/<post_id> <stage_name>
  python reset_stage.py publish <content_key> <platform>
  python reset_stage.py digest <digest_id>
  python reset_stage.py quality <content_key> <clip|digest>
  python reset_stage.py --list-stages
"""

import sys
import db

VALID_STAGES = db.STAGES  # downloaded, edited, subtitled, identified, captioned, thumbnailed, hooked, published

# Stages whose downstream script decides "needs reprocessing" by checking
# saved CONTENT, not the stage timestamp — clearing just the timestamp for
# these does nothing useful, the content field(s) must be cleared too.
_CONTENT_FIELDS_BY_STAGE = {
    "captioned": ["caption_tiktok", "caption_youtube", "caption_hook"],
    "identified": ["visual_hint", "canonical_show"],
}

# Add this near the top of reset_stage.py with the other constants:
_STAGE_REVERT_STATUS = {
    "downloaded": None,           # nothing to revert to
    "edited":     "downloaded",
    "subtitled":  "edited",
    "identified": "subtitled",
    "captioned":  "subtitled",    # or "identified" — either is fine
    "thumbnailed": "captioned",
    "hooked":     "thumbnailed",
    "published":  "hooked",
}

_FILE_DRIVEN_STAGES = {
    "thumbnailed": "generate_thumbnails.py decides this by checking whether "
                    "finished/<channel>/<post_id>/thumbnail.jpg exists on disk, "
                    "NOT the database. Delete that file yourself first — see "
                    "this script's docstring for the exact command — or clearing "
                    "the timestamp here will have no visible effect.",
}

_IN_PLACE_MUTATION_STAGES = {
    "hooked": "add_hook_overlay.py burns text directly into "
              "finished/<channel>/<post_id>/video.mp4 in place. Resetting this "
              "and re-running will burn a SECOND overlay on top of the first "
              "unless you restore a pre-hook backup, or also reset 'subtitled' "
              "so the video gets rebuilt clean first. See this script's "
              "docstring for details.",
}


def reset_stage(content_key, stage):
    if stage not in VALID_STAGES:
        print(f"Unknown stage {stage!r}. Valid stages: {', '.join(VALID_STAGES)}")
        return

    if "/" not in content_key:
        print(f"'{content_key}' doesn't look like a <channel>/<post_id> key.")
        return
    channel, post_id = content_key.split("/", 1)

    post = db.get_post(channel, post_id)
    if post is None:
        print(f"No post found for key '{content_key}'.")
        return

    if stage in _FILE_DRIVEN_STAGES:
        print(f"NOTE: {_FILE_DRIVEN_STAGES[stage]}\n")
    if stage in _IN_PLACE_MUTATION_STAGES:
        print(f"WARNING: {_IN_PLACE_MUTATION_STAGES[stage]}\n")
        confirm = input("Continue anyway? (y/N): ").strip().lower()
        if confirm != "y":
            print("Cancelled — nothing changed.")
            return

    conn = db.get_connection()
    set_clauses = [f"{stage}_at = NULL"]
    params = []

    for field in _CONTENT_FIELDS_BY_STAGE.get(stage, []):
        set_clauses.append(f"{field} = NULL")

    if post["status"] == "failed":
        set_clauses.append("status = 'downloaded'")
        set_clauses.append("fail_reason = NULL")
    elif stage in _STAGE_REVERT_STATUS and _STAGE_REVERT_STATUS[stage]:
        revert_to = _STAGE_REVERT_STATUS[stage]
        set_clauses.append("status = ?")
        params.insert(0, revert_to)

    params.append(content_key)
    conn.execute(f"UPDATE posts SET {', '.join(set_clauses)} WHERE key = ?", params)
    conn.commit()
    conn.close()

    print(f"Reset '{stage}' for {content_key} — it will be reprocessed on the next relevant run.")


def reset_publish(content_key, platform):
    conn = db.get_connection()
    cur = conn.execute(
        "DELETE FROM publish_log WHERE content_key = ? AND platform = ?",
        (content_key, platform),
    )
    conn.commit()
    deleted = cur.rowcount
    conn.close()

    if deleted:
        print(f"Cleared the {platform} publish record for '{content_key}' — "
              f"it's eligible for another publish attempt on the next run.")
    else:
        print(f"No {platform} publish_log entry found for '{content_key}' — "
              f"nothing to clear (it may never have been attempted, or you "
              f"already cleared it).")


def reset_digest(digest_id):
    conn = db.get_connection()

    digest_row = conn.execute(
        "SELECT digest_id, built_at FROM digests WHERE digest_id = ?", (digest_id,)
    ).fetchone()

    if digest_row is None:
        print(f"No digest found with id '{digest_id}'.")
        conn.close()
        return

    rows = conn.execute(
        "SELECT post_id FROM digest_posts WHERE digest_id = ?", (digest_id,)
    ).fetchall()
    post_ids = [r["post_id"] for r in rows]

    if post_ids:
        placeholders = ",".join("?" * len(post_ids))
        conn.execute(
            f"UPDATE posts SET status = 'downloaded' WHERE id IN ({placeholders})",
            post_ids,
        )
        conn.execute("DELETE FROM digest_posts WHERE digest_id = ?", (digest_id,))

    conn.execute("DELETE FROM digests WHERE digest_id = ?", (digest_id,))
    conn.execute("DELETE FROM publish_log WHERE content_key = ?", (digest_id,))
    conn.commit()
    conn.close()

    if post_ids:
        print(f"Reset digest '{digest_id}': {len(post_ids)} post(s) reverted to "
              f"'downloaded' (eligible for a fresh digest build), digest record "
              f"and any publish attempts cleared.")
        print(f"\nNOTE: video files on disk were NOT deleted. For a full clean "
              f"slate, also run:\n  rm -rf finished/news/{digest_id}")
    else:
        # A phantom digest — a row that existed with no linked posts (see
        # this script's docstring and db.py's sync_digests_from_filesystem()
        # docstring for how these happen). Nothing to revert since no post
        # was ever attached to it; just the row itself gets cleaned up.
        print(f"Removed phantom digest record '{digest_id}' (built_at="
              f"{digest_row['built_at']!r}, zero linked posts — most likely a "
              f"build that was interrupted before finishing). No posts needed "
              f"reverting.")
        print(f"\nNOTE: if finished/news/{digest_id}/ still exists on disk with "
              f"leftover partial files, remove it manually:\n"
              f"  rm -rf finished/news/{digest_id}")


def reset_quality(content_key, content_type):
    if content_type not in ("clip", "digest"):
        print(f"content_type must be 'clip' or 'digest', got {content_type!r}.")
        return

    conn = db.get_connection()
    table = "posts" if content_type == "clip" else "digests"
    key_col = "key" if content_type == "clip" else "digest_id"
    row = conn.execute(
        f"SELECT quality_status, quality_score FROM {table} WHERE {key_col} = ?",
        (content_key,),
    ).fetchone()
    conn.close()

    if row is None:
        print(f"No {content_type} found for key '{content_key}'.")
        return
    if row["quality_status"] is None:
        print(f"'{content_key}' has no quality verdict yet — nothing to clear "
              f"(it'll be picked up by quality_gate.py's normal run anyway).")
        return

    db.reset_quality(content_key, content_type)
    print(f"Cleared quality verdict for '{content_key}' (was '{row['quality_status']}', "
          f"score {row['quality_score']}) — quality_gate.py will re-check it on its next run.")


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        return

    if sys.argv[1] == "--list-stages":
        print("Valid stage names:", ", ".join(VALID_STAGES))
        return

    db.init_db()
    command = sys.argv[1]

    if command == "stage":
        if len(sys.argv) != 4:
            print("Usage: python reset_stage.py stage <channel>/<post_id> <stage_name>")
            return
        reset_stage(sys.argv[2], sys.argv[3])

    elif command == "publish":
        if len(sys.argv) != 4:
            print("Usage: python reset_stage.py publish <content_key> <platform>")
            return
        reset_publish(sys.argv[2], sys.argv[3])

    elif command == "digest":
        if len(sys.argv) != 3:
            print("Usage: python reset_stage.py digest <digest_id>")
            return
        reset_digest(sys.argv[2])

    elif command == "quality":
        if len(sys.argv) != 4:
            print("Usage: python reset_stage.py quality <content_key> <clip|digest>")
            return
        reset_quality(sys.argv[2], sys.argv[3])

    else:
        print(f"Unknown command '{command}'. Use: stage | publish | digest | quality | --list-stages")


if __name__ == "__main__":
    main()
