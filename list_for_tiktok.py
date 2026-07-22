"""
List for Manual TikTok Posting
--------------------------------
TikTok auto-publishing is on hold pending the app audit (see run_all.sh).
This lists every finished clip and digest that's ready to post — reached
the 'thumbnailed' stage, has both video.mp4 and caption_final.txt on disk —
and hasn't already been logged as posted to TikTok in pipeline.db. Uses the
exact same db.get_publishable_posts/get_publishable_digests("tiktok") calls
publish_tiktok.py itself uses, so this queue always matches what auto-
publish would attempt once it's switched back on.

After you post something manually in the TikTok app, mark it here so it
drops off the list next time — this writes into the same publish_log table
as a real API-driven publish, so nothing gets double-counted or re-shown
once TikTok publishing is switched back on later.

USAGE:
  python list_for_tiktok.py
"""

import os
import db

FINISHED_DIR = "finished"


def gather_pending():
    """Returns a list of dicts: {key, content_type, video_path, caption_path,
    caption_text}, filtered to items that still have their files on disk."""
    items = []

    for post in db.get_publishable_posts("tiktok", min_stage="thumbnailed"):
        post_dir = os.path.join(FINISHED_DIR, post["channel"], post["post_id"])
        video_path = os.path.join(post_dir, "video.mp4")
        caption_path = os.path.join(post_dir, "caption_final.txt")
        if not (os.path.isfile(video_path) and os.path.isfile(caption_path)):
            print(f"NOTE: {post['key']} is pending but its files are gone from "
                  f"disk (deleted after an earlier YouTube publish?) — skipping.")
            continue
        with open(caption_path, "r", encoding="utf-8") as f:
            caption_text = f.read().strip()
        items.append({
            "key": post["key"],
            "content_type": "clip",
            "video_path": video_path,
            "caption_path": caption_path,
            "caption_text": caption_text,
        })

    for digest in db.get_publishable_digests("tiktok", min_stage="thumbnailed"):
        digest_dir = os.path.join(FINISHED_DIR, "news", digest["digest_id"])
        video_path = os.path.join(digest_dir, "video.mp4")
        caption_path = os.path.join(digest_dir, "caption_final.txt")
        if not (os.path.isfile(video_path) and os.path.isfile(caption_path)):
            print(f"NOTE: {digest['digest_id']} is pending but its files are "
                  f"gone from disk — skipping.")
            continue
        with open(caption_path, "r", encoding="utf-8") as f:
            caption_text = f.read().strip()
        items.append({
            "key": digest["digest_id"],
            "content_type": "digest",
            "video_path": video_path,
            "caption_path": caption_path,
            "caption_text": caption_text,
        })

    return items


def main():
    db.init_db()
    items = gather_pending()

    if not items:
        print("Nothing pending — everything's either already marked posted "
              "or hasn't reached the thumbnailed stage yet.")
        return

    print(f"{len(items)} item(s) ready to post manually to TikTok:\n")
    print("=" * 70)
    for i, item in enumerate(items, 1):
        print(f"\n[{i}] {item['content_type'].upper()} — {item['key']}")
        schedule = db.get_schedule_entry(item['key'], "tiktok")
        if schedule and schedule["status"] != "cancelled":
            print(f"    Best time to post: {schedule['scheduled_at']} — {schedule['reasoning']}")
        print(f"    Video:   {item['video_path']}")
        print(f"    Caption:\n")
        for line in item["caption_text"].splitlines():
            print(f"      {line}")
        print()
        print("-" * 70)

    print(f"\n{len(items)} item(s) listed above.")
    print("After posting any of these manually in the TikTok app, mark them")
    print("here so they don't show up again next time.\n")
    choice = input(
        "Enter number(s) to mark as posted (e.g. 1,3,5), 'all', or press "
        "Enter to skip: "
    ).strip()

    if not choice:
        print("Nothing marked. Run this again any time to see the same list.")
        return

    if choice.lower() == "all":
        to_mark = list(range(1, len(items) + 1))
    else:
        try:
            to_mark = [int(x.strip()) for x in choice.split(",") if x.strip()]
        except ValueError:
            print(f"Couldn't parse '{choice}' — expected numbers like '1,3,5' or 'all'. Nothing marked.")
            return

    marked = 0
    for n in to_mark:
        if not (1 <= n <= len(items)):
            print(f"  Skipping #{n} — out of range.")
            continue
        item = items[n - 1]
        db.log_publish(
            item["key"], item["content_type"], "tiktok", "success",
            platform_content_id=None, privacy_level="MANUAL",
        )
        print(f"  Marked #{n} ({item['key']}) as posted.")
        marked += 1

    print(f"\nDone. {marked} item(s) marked posted — they won't show up here "
          f"or get double-posted once TikTok auto-publish is switched back on.")


if __name__ == "__main__":
    main()
