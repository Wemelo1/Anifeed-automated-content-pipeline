"""
Check Missing Finished Files
--------------------------------
clear_missing.py already does this for the ACQUIRE stage (an image post
whose raw_media_path vanished before ever being used). This is the same
concept extended to every stage AFTER burn_subtitles.py — and it's a
bigger gap than clear_missing.py's, because of HOW those later stages
discover their work.

THE ROOT ISSUE: identify_show.py, generate_captions.py,
generate_thumbnails.py, and add_hook_overlay.py all find their work the
SAME way — by os.listdir()-ing directories under finished/<channel>/.
None of them query the database for "what SHOULD exist." That means if
a finished/<channel>/<post_id>/ directory is ever deleted — manual
cleanup, a storage mishap, anything — the clip doesn't just stall at
whatever stage it was at; it becomes INVISIBLE to every single
subsequent stage simultaneously, with zero error and zero log line,
because there's nothing in finished/ for any of those scripts to even
see. A DB-driven check is the only way to catch this — this script
queries pipeline.db directly for every video post that's subtitled but
not yet published, then checks disk, instead of asking disk what's
there and hoping the database agrees.

CONFIRMED INCIDENT: 12 clips sitting 'thumbnailed' with no 'hooked_at'
for 3 months, surviving the wake-lock fix untouched, because their
finished/ directories were gone — add_hook_overlay.py's directory scan
never had anything to find. See this script's SCOPE note below for why
the fix generalizes past just the hook stage.

SCOPE: covers every video post with subtitled_at set (the point from
which every later stage reads finished/, not edited/) and published_at
NOT set (anything already published is done, a missing file there is
irrelevant). Does not touch image posts — that's clear_missing.py's
job, different stage, different check (raw_media_path, not finished/).

Dry-run by default — lists what it would mark, with WHICH stage each
clip was stuck at (so you know whether it died before captioning,
before thumbnailing, etc.) and the reason, changes nothing until
--apply. Marks status='failed', fail_reason='file_missing_on_disk' —
same exact fail_reason string clear_missing.py already uses, so
performance_insights.py's failure-rate reporting and any future
'file_missing_on_disk' check treats both consistently.

USAGE:
  python check_missing_finished.py           # dry run — lists what's affected
  python check_missing_finished.py --apply   # marks them failed
"""

import os
import sys

import db

FINISHED_DIR = "finished"

# Stage timestamps checked in order, latest-reached first — used only to
# report which stage a clip got stuck at, for a human reading the
# output; has no effect on which clips are selected.
_STAGE_ORDER_DESC = ["hooked_at", "thumbnailed_at", "captioned_at",
                     "identified_at", "subtitled_at"]


def _latest_stage_reached(post):
    for field in _STAGE_ORDER_DESC:
        if post.get(field):
            return field.replace("_at", "")
    return "subtitled"  # the floor this query's WHERE clause guarantees


def find_missing():
    conn = db.get_connection()
    rows = conn.execute(
        """SELECT channel, post_id, key, subtitled_at, identified_at,
                  captioned_at, thumbnailed_at, hooked_at
           FROM posts
           WHERE media_type = 'video'
             AND subtitled_at IS NOT NULL
             AND published_at IS NULL
             AND status != 'failed'"""
    ).fetchall()
    conn.close()

    missing = []
    for r in rows:
        post = dict(r)
        video_path = os.path.join(FINISHED_DIR, post["channel"], post["post_id"], "video.mp4")
        if not os.path.isfile(video_path):
            post["stuck_at_stage"] = _latest_stage_reached(post)
            missing.append(post)
    return missing


def main():
    apply_changes = "--apply" in sys.argv
    db.init_db()

    missing = find_missing()
    if not missing:
        print("No video posts are missing their finished/ file right now — "
              "every in-flight clip (subtitled, not yet published) has its "
              "directory on disk.")
        return

    print(f"{len(missing)} video post(s) have no finished/.../video.mp4 on "
          f"disk despite being mid-pipeline:\n")
    for p in missing:
        print(f"  {p['key']}  (stuck at: {p['stuck_at_stage']})")

    if not apply_changes:
        print(f"\nDry run — nothing changed. These rows will keep silently "
              f"stalling forever (invisible to every later stage's "
              f"directory-based discovery) until marked failed. Re-run with "
              f"--apply to mark all {len(missing)} as failed "
              f"(fail_reason='file_missing_on_disk') so they stop polluting "
              f"stuck-clip counts and get correctly counted in failure-rate "
              f"reporting instead.")
        return

    conn = db.get_connection()
    for p in missing:
        conn.execute(
            "UPDATE posts SET status='failed', fail_reason='file_missing_on_disk' WHERE key=?",
            (p["key"],),
        )
    conn.commit()
    conn.close()

    print(f"\nMarked {len(missing)} post(s) as failed.")


if __name__ == "__main__":
    main()
