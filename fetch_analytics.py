"""
Analytics Collector
---------------------
Fetches current stats (views/likes/comments/shares) for every clip and
digest that's been successfully published, and records them as an
append-only snapshot via db.record_metrics() — never overwriting, so
growth over time is queryable (db.metrics_history()) instead of only ever
showing the latest number.

This is the feedback loop the pipeline didn't have before: db.top_performers()
can now answer "what's actually landing with viewers", which is the real
signal for improving prompts (narration style, caption hooks, thumbnail
choices) — as opposed to assuming what should work.

PLATFORM COVERAGE:
  - YouTube: works for every successful publish — YouTube always returns a
    real video_id at publish time (see publish_youtube.py).
  - TikTok: only works for posts that have a platform_content_id recorded.
    TikTok only hands back a real post id once a PUBLIC post clears
    moderation — private (SELF_ONLY) posts, which is effectively all of
    them until the app passes TikTok's audit, never get one. Posts without
    an id are skipped with a single summary note, not a per-item warning.

CONTENT REMOVAL TRACKING:
  When a video previously had a stats snapshot and then stops appearing in
  the API response at all, that's a confirmed removal (Content ID claim,
  manual takedown, or self-deletion — the YouTube Data API v3 does NOT
  expose which one; that detail only exists in YouTube Studio, not via any
  API available to a normal creator). Confirmed removals are recorded via
  db.mark_removed() rather than re-queried every run forever — that would
  burn API quota for zero new signal on what's almost always permanent.
  A removed item gets one recheck attempt after RECHECK_AFTER_DAYS (claims
  do occasionally get reversed on dispute); if it's back, it's cleared from
  tracking automatically. A video that has NEVER had a snapshot (brand new,
  not yet indexed) is deliberately NOT marked removed on a single miss —
  only a regression (had stats, now doesn't) counts as confirmed.

  db.removal_rate_by_show() surfaces which canonical shows get pulled most
  often — a real content-strategy signal (a show can perform well per-view
  and still not be worth the churn if most of its clips get taken down
  before they earn those views), separate from performance_insights.py's
  pure view-based ranking.

Reuses the same OAuth/token handling as the publishers rather than
duplicating credential logic: publish_youtube.get_authenticated_service()
and publish_tiktok.get_access_token().

NOTE (fixed 2026-07-21): this used to call yt.get_youtube_service(), which
never existed under that name in publish_youtube.py — that module's actual
function is get_authenticated_service(). Looks like this file's docstring
and call site were written against an assumed name that was never actually
matched to the real implementation, so this crashed the very first time
collect_youtube() ran end-to-end rather than being a regression from a
rename. Fixed at the call site here rather than renaming the working
function in publish_youtube.py, since that module's own main() already
depends on the current name.

SETUP: same as publish_youtube.py / publish_tiktok.py — this script piggybacks
on those two scripts' existing auth.

USAGE:
  python fetch_analytics.py   (run any time — e.g. daily, via cron/Termux:Boot)
"""

import os
import time
import requests
from dotenv import load_dotenv

from db import PipelineDB
import publish_youtube as yt
import publish_tiktok as tt

load_dotenv()

YOUTUBE_BATCH_SIZE = 50   # videos.list allows up to 50 ids per call
TIKTOK_BATCH_SIZE = 20    # video/query allows up to 20 ids per call
DELAY_BETWEEN_BATCHES_SECONDS = float(os.getenv("ANALYTICS_BATCH_DELAY", "1"))


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ── YouTube ──────────────────────────────────────────────────────────────────

def fetch_youtube_stats(youtube, video_ids):
    """Returns {video_id: statistics_dict}."""
    resp = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
    return {item["id"]: item["statistics"] for item in resp.get("items", [])}


def collect_youtube(db):
    publishes = db.successful_publishes("youtube")
    publishes = [p for p in publishes if p.get("platform_content_id")]

    if not publishes:
        print("YouTube: nothing published yet.")
        return 0

    # Skip anything already confirmed removed, UNLESS it's due for a
    # periodic recheck (see db.mark_removed's docstring — Content ID
    # claims/blocks are occasionally reversed on dispute, so this isn't
    # necessarily permanent). This is what stops the pipeline from
    # burning API quota re-querying the same known-gone videos every
    # single day forever for zero new signal.
    removed_map = db.removed_content_map("youtube")
    to_query, skipped_removed = [], 0
    for p in publishes:
        removal = removed_map.get(p["target_id"])
        if removal and not db.is_due_for_recheck(removal):
            skipped_removed += 1
            continue
        to_query.append(p)

    if not to_query:
        print(f"YouTube: nothing to query — all {len(publishes)} published item(s) "
              f"are already confirmed removed and not yet due for a recheck.")
        return 0

    print(f"YouTube: fetching stats for {len(to_query)} published item(s)"
          + (f" ({skipped_removed} already-confirmed-removed skipped)" if skipped_removed else "")
          + "...")
    # publish_youtube.py's actual auth entry point is get_authenticated_service()
    # — see this module's NOTE above.
    youtube = yt.get_authenticated_service()

    by_video_id = {p["platform_content_id"]: p for p in to_query}
    collected, newly_removed, came_back = 0, 0, 0

    for batch in chunked(list(by_video_id.keys()), YOUTUBE_BATCH_SIZE):
        try:
            stats = fetch_youtube_stats(youtube, batch)
        except Exception as e:
            print(f"  FAILED batch of {len(batch)}: {e}")
            continue

        for video_id in batch:
            row = by_video_id[video_id]
            key, ttype = row["target_id"], row["target_type"]
            was_removed = key in removed_map
            s = stats.get(video_id)

            if s is None:
                if was_removed:
                    # Recheck confirms it's still gone — just refresh the
                    # recheck timer, no need to shout about it again.
                    db.mark_removed(key, ttype, "youtube",
                                     last_known_views=removed_map[key]["last_known_views"])
                    print(f"  {video_id}: still removed (rechecked)")
                    continue

                history = db.metrics_history(ttype, key, "youtube")
                if history:
                    # Had a real snapshot before, now gone — a confirmed
                    # removal, not just "not indexed yet".
                    last_views = history[-1]["views"]
                    db.mark_removed(key, ttype, "youtube", last_known_views=last_views)
                    print(f"  REMOVED: {video_id} (had {last_views} views as of its "
                          f"last snapshot — likely Content ID claim or takedown)")
                    newly_removed += 1
                else:
                    # Never had a snapshot at all — most likely just not
                    # indexed yet (very recently published), not a
                    # confirmed removal. Deliberately NOT marked removed,
                    # to avoid a false positive on a brand-new upload.
                    print(f"  NOTE: {video_id} returned no stats yet (too new to be "
                          f"indexed, or a genuine removal — will recheck next run)")
                continue

            if was_removed:
                # It's back! (dispute resolved, block lifted, etc.)
                db.clear_removed(key, "youtube")
                came_back += 1
                print(f"  BACK UP: {video_id} is reachable again — cleared from "
                      f"removed tracking.")

            db.record_metrics(
                ttype, key, "youtube",
                views=int(s.get("viewCount", 0)),
                likes=int(s.get("likeCount", 0)) if "likeCount" in s else None,
                comments=int(s.get("commentCount", 0)) if "commentCount" in s else None,
                shares=None,  # not exposed by the Data API
                raw=s,
            )
            collected += 1

        time.sleep(DELAY_BETWEEN_BATCHES_SECONDS)

    print(f"YouTube: recorded {collected} snapshot(s)"
          + (f", {newly_removed} newly confirmed removed" if newly_removed else "")
          + (f", {came_back} came back up" if came_back else "")
          + ".")
    return collected


# ── TikTok ─────────────────────────────────────────────────────────────────────

def fetch_tiktok_stats(access_token, video_ids):
    """Returns {video_id: video_dict}."""
    resp = requests.post(
        f"{tt.API_BASE}/v2/video/query/",
        params={"fields": "id,like_count,comment_count,share_count,view_count"},
        headers={"Authorization": f"Bearer {access_token}",
                 "Content-Type": "application/json; charset=UTF-8"},
        json={"filters": {"video_ids": video_ids}},
        timeout=15,
    )
    resp.raise_for_status()
    videos = resp.json()["data"]["videos"]
    return {v["id"]: v for v in videos}


def collect_tiktok(db):
    publishes = db.successful_publishes("tiktok")
    with_id = [p for p in publishes if p.get("platform_content_id")]
    without_id = len(publishes) - len(with_id)

    if without_id:
        print(f"TikTok: {without_id} published item(s) have no post id yet "
              f"(expected for private/SELF_ONLY posts — see this script's "
              f"docstring) and will be skipped until they're publicly visible.")

    if not with_id:
        print("TikTok: nothing with a fetchable post id right now.")
        return 0

    print(f"TikTok: fetching stats for {len(with_id)} published item(s)...")
    access_token = tt.get_access_token()

    by_video_id = {p["platform_content_id"]: p for p in with_id}
    collected = 0

    for batch in chunked(list(by_video_id.keys()), TIKTOK_BATCH_SIZE):
        try:
            stats = fetch_tiktok_stats(access_token, batch)
        except Exception as e:
            print(f"  FAILED batch of {len(batch)}: {e}")
            continue

        for video_id in batch:
            row = by_video_id[video_id]
            v = stats.get(video_id)
            if v is None:
                print(f"  NOTE: {video_id} returned no stats (video may have been removed)")
                continue
            db.record_metrics(
                row["target_type"], row["target_id"], "tiktok",
                views=v.get("view_count"),
                likes=v.get("like_count"),
                comments=v.get("comment_count"),
                shares=v.get("share_count"),
                raw=v,
            )
            collected += 1

        time.sleep(DELAY_BETWEEN_BATCHES_SECONDS)

    print(f"TikTok: recorded {collected} snapshot(s).")
    return collected


def main():
    db = PipelineDB()

    yt_count = collect_youtube(db)
    print()
    tt_count = collect_tiktok(db)

    print(f"\nDone. {yt_count + tt_count} total snapshot(s) recorded this run.")

    top = db.top_performers(metric="views", limit=5)
    if top:
        print("\nTop 5 by views (latest snapshot per item):")
        for row in top:
            print(f"  [{row['platform']}] {row['target_type']} #{row['target_id']}: "
                  f"{row['views']} views, {row['likes']} likes, {row['comments']} comments")

    removal_rates = db.removal_rate_by_show("youtube")
    if removal_rates:
        print("\nRemoval rate by show (youtube, clips only — content-strategy signal, "
              "not just bookkeeping):")
        for r in removal_rates[:5]:
            print(f"  {r['canonical_show']}: {r['removed_count']}/{r['published_count']} "
                  f"clips removed ({r['removal_rate_pct']:.0f}%)")


if __name__ == "__main__":
    main()
