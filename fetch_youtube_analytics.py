"""
YouTube Retention Signal — Roadmap Item K
--------------------------------------------
fetch_analytics.py already collects views/likes/comments/shares — surface
metrics that say WHETHER a video got clicked, not whether it actually held
anyone once they clicked. This module adds the retention signal:
YouTube's Analytics API (distinct from the Data API v3 used everywhere
else in this pipeline) exposes averageViewPercentage and
estimatedMinutesWatched per video, plus a per-country audience breakdown.
Recorded into the SAME metrics table fetch_analytics.py already writes
to (retention_pct/watch_time_seconds/audience_country_top columns — see
db.py's schema), so it's just another dimension on the existing
append-only snapshot history, not a parallel data store.

WHY A SEPARATE SCRIPT, NOT FOLDED INTO fetch_analytics.py: the YouTube
Analytics API is a genuinely different Google API (youtubeAnalytics v2,
not youtube v3) with its own quota, its own query shape (a reporting
query against a date range + dimensions/metrics, not a simple videos.list
by id), and its own auth scope
(https://www.googleapis.com/auth/yt-analytics.readonly — see
publish_youtube.py's SCOPES). Keeping this separate means a quota/auth
problem on the Analytics side degrades independently of the Data API
stats fetch_analytics.py already does reliably — one failing never blocks
the other (same per-source-of-truth isolation PROJECT_RULES.md's Rule 2
already asks for).

WHAT IT PULLS, PER PUBLISHED YOUTUBE CLIP:
  averageViewPercentage  -> retention_pct (0-100, "what fraction of the
                             video does the average viewer watch")
  estimatedMinutesWatched -> watch_time_seconds (converted from minutes)
  A second query, dimensioned by country, sorted by estimatedMinutesWatched
  descending, top row only -> audience_country_top (a 2-letter code, e.g.
  "US") — enough for a coarse "who's actually watching this" signal
  without pulling a full per-country breakdown nobody downstream reads
  yet.

SCOPE: YouTube only. TikTok's public Content Posting API doesn't expose
an equivalent retention metric — see fetch_analytics.py's own docstring
for the TikTok-side limitation (private/SELF_ONLY posts don't even get a
queryable id most of the time). If that changes, a TikTok-side retention
puller is a natural follow-up, not a reason to block this one.

DATE RANGE: YouTube Analytics reports are queried over
[published_date, published_date + RETENTION_LOOKBACK_DAYS] — a video's
retention shape stabilizes within the first couple weeks; querying the
full lifetime-to-date range every run would slowly change the number for
old videos in a way that's hard to correlate against a specific
render_params_json snapshot. RETENTION_LOOKBACK_DAYS caps the window so
a comparison stays apples-to-apples across videos published at different
times.

RE-QUERY POLICY: unlike fetch_analytics.py's views/likes/comments (which
genuinely keep changing and are worth re-snapshotting daily), retention
over a FIXED lookback window stabilizes — re-querying a video whose
window has already fully elapsed wastes Analytics API quota for
(usually) an unchanged number. A video only gets re-queried if its
lookback window hasn't fully elapsed yet, or it's never been queried at
all — tracked via the existing metrics table itself (this module's own
prior snapshot for that video, if any).

SETUP:
  1. Enable "YouTube Analytics API" in the same Google Cloud project as
     your existing YouTube Data API v3 OAuth client (APIs & Services ->
     Library -> search "YouTube Analytics API" -> Enable).
  2. Delete YOUTUBE_TOKEN_FILE once (publish_youtube.py's SCOPES now
     includes yt-analytics.readonly — an old token minted before that
     doesn't have it) and run `python publish_youtube.py` once to
     re-consent.

USAGE:
  python fetch_youtube_analytics.py
  Run daily, alongside fetch_analytics.py (see run_analytics.sh) — after
  it, since this reads db.successful_publishes("youtube") the same way.

.env overrides (all optional):
  RETENTION_LOOKBACK_DAYS=14      # window (from publish date) retention
                                    is measured over
  RETENTION_BATCH_DELAY_SECONDS=1 # pause between per-video Analytics
                                    API calls (no documented batch
                                    endpoint for multiple video ids the
                                    way Data API's videos.list has)
"""

import os
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from db import PipelineDB
import publish_youtube as yt

load_dotenv()

RETENTION_LOOKBACK_DAYS = int(os.getenv("RETENTION_LOOKBACK_DAYS", "14"))
RETENTION_BATCH_DELAY_SECONDS = float(os.getenv("RETENTION_BATCH_DELAY_SECONDS", "1"))

# YouTube's own channel-level "all videos" pseudo-id, required by the
# Analytics API's `ids` query parameter — the video itself is selected
# via the `filters` parameter (video==<id>), not `ids`.
_CHANNEL_IDS = "channel==MINE"


def _already_measured_this_window(db, target_id, published_at):
    """True if a retention snapshot for this video already exists AND
    its lookback window has fully elapsed — meaning re-querying would
    just burn quota for an unchanged number. False (needs querying)
    covers both 'never measured' and 'measured before the window
    finished'."""
    history = db.metrics_history("clip", target_id, "youtube")
    retention_snapshots = [h for h in history if h.get("retention_pct") is not None]
    if not retention_snapshots:
        return False
    try:
        published_dt = datetime.fromisoformat(published_at)
    except (TypeError, ValueError):
        return False
    window_end = published_dt + timedelta(days=RETENTION_LOOKBACK_DAYS)
    return datetime.now(timezone.utc) >= window_end


def _query_retention(analytics, video_id, start_date, end_date):
    """Returns (retention_pct, watch_time_seconds) or (None, None) if the
    API returned no rows (can happen for a very new/low-view video that
    Analytics hasn't indexed yet)."""
    resp = analytics.reports().query(
        ids=_CHANNEL_IDS,
        startDate=start_date,
        endDate=end_date,
        metrics="averageViewPercentage,estimatedMinutesWatched",
        filters=f"video=={video_id}",
    ).execute()
    rows = resp.get("rows") or []
    if not rows:
        return None, None
    avg_view_pct, minutes_watched = rows[0][0], rows[0][1]
    return round(avg_view_pct, 2), int(round(minutes_watched * 60))


def _query_top_country(analytics, video_id, start_date, end_date):
    """Returns the 2-letter country code with the most watch time for
    this video in the window, or None if no rows / the call fails —
    never blocks the retention_pct/watch_time_seconds values above, which
    are the primary signal."""
    try:
        resp = analytics.reports().query(
            ids=_CHANNEL_IDS,
            startDate=start_date,
            endDate=end_date,
            metrics="estimatedMinutesWatched",
            dimensions="country",
            filters=f"video=={video_id}",
            sort="-estimatedMinutesWatched",
            maxResults=1,
        ).execute()
        rows = resp.get("rows") or []
        return rows[0][0] if rows else None
    except Exception as e:
        print(f"      (country breakdown unavailable, non-fatal: {e})")
        return None


def collect(db):
    publishes = db.successful_publishes("youtube")
    publishes = [p for p in publishes if p.get("target_type") == "clip"]

    to_query = []
    for p in publishes:
        post = db_module_get_post_for(p["target_id"])
        published_at = post.get("published_at") if post else None
        if not published_at:
            continue
        if _already_measured_this_window(db, p["target_id"], published_at):
            continue
        to_query.append((p, published_at))

    if not to_query:
        print("YouTube retention: nothing due for measurement this run "
              "(every published clip is either already fully measured for "
              "its lookback window, or has no publish timestamp yet).")
        return 0

    print(f"YouTube retention: measuring {len(to_query)} clip(s) "
          f"(lookback window {RETENTION_LOOKBACK_DAYS}d)...")

    try:
        analytics = yt.get_authenticated_analytics_service()
    except Exception as e:
        print(f"FAILED to authenticate against YouTube Analytics API: {e}")
        print("If this mentions insufficient permissions/scope, delete "
              "YOUTUBE_TOKEN_FILE and run `python publish_youtube.py` once "
              "to re-consent with the yt-analytics.readonly scope.")
        return 0

    collected = 0
    for p, published_at in to_query:
        video_id = p.get("platform_content_id")
        if not video_id:
            continue

        try:
            published_dt = datetime.fromisoformat(published_at)
        except ValueError:
            continue
        start_date = published_dt.strftime("%Y-%m-%d")
        end_date = min(
            published_dt + timedelta(days=RETENTION_LOOKBACK_DAYS),
            datetime.now(timezone.utc),
        ).strftime("%Y-%m-%d")

        try:
            retention_pct, watch_time_seconds = _query_retention(
                analytics, video_id, start_date, end_date)
        except Exception as e:
            print(f"  {p['target_id']}: FAILED ({e})")
            continue

        if retention_pct is None:
            print(f"  {p['target_id']}: no Analytics data yet (too new/low "
                  f"views to be indexed) — will retry next run")
            continue

        top_country = _query_top_country(analytics, video_id, start_date, end_date)

        db.record_metrics(
            "clip", p["target_id"], "youtube",
            retention_pct=retention_pct,
            watch_time_seconds=watch_time_seconds,
            audience_country_top=top_country,
        )
        collected += 1
        print(f"  {p['target_id']}: retention={retention_pct:.1f}%  "
              f"watch_time={watch_time_seconds}s"
              + (f"  top_country={top_country}" if top_country else ""))

        time.sleep(RETENTION_BATCH_DELAY_SECONDS)

    print(f"YouTube retention: recorded {collected} snapshot(s).")
    return collected


# Small helper: successful_publishes() doesn't carry published_at, only
# publish_log's own logged_at — the actual publish timestamp lives on
# posts.published_at (COALESCE'd to the first successful publish, see
# db.log_publish()). A tiny direct lookup rather than widening
# successful_publishes()'s own return shape for one caller.
def db_module_get_post_for(content_key):
    import db as db_module
    if "/" in content_key:
        channel, post_id = content_key.split("/", 1)
        return db_module.get_post(channel, post_id)
    return None


def main():
    db = PipelineDB()
    collect(db)


if __name__ == "__main__":
    main()
