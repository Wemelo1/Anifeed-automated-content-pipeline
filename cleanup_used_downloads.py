"""
Cleanup Used Downloads
-------------------------
Frees disk space by deleting raw_media_path files for posts that are
FULLY DONE with their pipeline run — never touches anything still a live
candidate or mid-pipeline. Exists specifically because manually deleting
files under downloads/ (or running clear_missing.py as a "make room"
tool) destroys real, not-yet-used content: 395 AniList posts got marked
file_missing_on_disk after their files were deleted by hand while still
sitting as valid, not-yet-used digest candidates — a real, permanent
content loss caused by conflating "hasn't built into a digest yet" with
"safe to delete."

SAFE-TO-DELETE DEFINITION (deliberately conservative — when genuinely
unsure, this script does NOT delete):
  - Image posts: status = 'used_in_digest' — build_news_digest.py has
    already rendered this image into a stitched digest video; the raw
    source file is never read again after that point.
  - Video posts (clips): status = 'published' — edit_clips.py already
    made its own derivative copy in edited/ before ANY later stage
    (burn_subtitles.py, identify_show.py, generate_captions.py,
    generate_thumbnails.py, add_hook_overlay.py) ever touches disk
    again; none of them read raw_media_path. By the time a clip is fully
    published, the original raw download has had zero readers for the
    rest of its trip through the pipeline.
  - Old failures: status = 'failed' AND downloaded more than
    FAILED_GRACE_DAYS ago AND fail_reason isn't already
    'file_missing_on_disk' (nothing to delete there — the file's already
    gone). The grace period exists so a FRESH failure stays on disk long
    enough to be manually inspected before this script clears it.

NEVER TOUCHES: status='downloaded' (still a live candidate — this is
EXACTLY the category the original incident destroyed) or any
mid-pipeline status (edited/subtitled/identified/captioned/thumbnailed/
hooked). A clip's raw file is technically unread past the 'edited'
stage, but the bar here is "fully terminal," not "technically
unreferenced" — given this project's history with premature deletion,
erring toward not deleting costs nothing; erring the other way already
cost 395 posts once.

Dry-run by default, same convention as this project's other maintenance
scripts (cleanup_orphan_digests.py, cleanup_poisoned_visual_hints.py) —
lists what WOULD be deleted and the total size, changes nothing until
you pass --apply. pipeline.db rows are NEVER touched by this script,
under any flag — only files on disk.

USAGE:
  python cleanup_used_downloads.py           # dry run
  python cleanup_used_downloads.py --apply   # actually deletes the files

.env overrides:
  CLEANUP_FAILED_GRACE_DAYS=14   # how old a 'failed' post must be before
                                    its raw file is cleared
"""

import os
import sys
from datetime import datetime, timezone, timedelta

import db

FAILED_GRACE_DAYS = int(os.getenv("CLEANUP_FAILED_GRACE_DAYS", "14"))


def find_deletable():
    conn = db.get_connection()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=FAILED_GRACE_DAYS)).isoformat()

    rows = conn.execute(
        """SELECT key, status, media_type, raw_media_path, fail_reason, downloaded_at
           FROM posts
           WHERE raw_media_path IS NOT NULL
             AND (
                  status = 'used_in_digest'
               OR status = 'published'
               OR (status = 'failed' AND downloaded_at < ?
                   AND (fail_reason IS NULL OR fail_reason != 'file_missing_on_disk'))
             )""",
        (cutoff,),
    ).fetchall()
    conn.close()

    deletable = []
    for r in rows:
        path = r["raw_media_path"]
        if path and os.path.isfile(path):
            deletable.append(dict(r))
    return deletable


def main():
    apply_changes = "--apply" in sys.argv
    db.init_db()

    items = find_deletable()
    if not items:
        print("Nothing eligible for cleanup right now — every downloaded "
              "file still on disk belongs to a live candidate or a "
              "mid-pipeline item.")
        return

    total_bytes = 0
    print(f"{len(items)} file(s) eligible for cleanup:\n")
    for it in items:
        try:
            size = os.path.getsize(it["raw_media_path"])
        except OSError:
            size = 0
        total_bytes += size
        print(f"  {it['key']}  ({it['status']}, {size/1024:.0f} KB)  {it['raw_media_path']}")

    print(f"\nTotal: {total_bytes/1024/1024:.1f} MB")

    if not apply_changes:
        print("\nDry run — nothing deleted. Re-run with --apply to actually "
              "delete these files. pipeline.db rows are NEVER touched — "
              "only the raw files on disk.")
        return

    deleted = 0
    for it in items:
        try:
            os.remove(it["raw_media_path"])
            deleted += 1
        except OSError as e:
            print(f"  Could not delete {it['raw_media_path']}: {e}")

    print(f"\nDeleted {deleted} file(s), freed ~{total_bytes/1024/1024:.1f} MB.")


if __name__ == "__main__":
    main()
