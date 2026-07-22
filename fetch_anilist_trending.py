"""
AniList Trending Anime Pull
------------------------------
Secondary content source, independent of Telegram — feeds the exact same
pipeline.db `posts` table that telegram_pull.py populates, so
build_news_digest.py doesn't know or care where a candidate image post
came from. Exists because the pipeline was previously 100% dependent on
a handful of Telegram channels actually posting that day; if all of them
go quiet, this keeps the digest queue fed from AniList's public trending
API — no API key needed, so a Telegram dry spell no longer risks breaking
the daily posting streak.

WHAT IT PULLS:
  AniList's GraphQL API, sorted by TRENDING_DESC (their own real-time
  trending signal — recent airing activity, list/forum activity, etc,
  not just "currently airing"). For each of the top N anime this builds a
  short, genuinely time-sensitive news-style blurb (trending rank, airing
  status, next episode date if known, synopsis) and downloads the show's
  banner/cover art as the image asset — same shape find_candidate_posts()
  in build_news_digest.py already expects from a Telegram image post.

DEDUP:
  Two layers, mirroring telegram_pull.py's own convention:
    1. media_id = "anilist_<anime_id>_<iso_year>-W<iso_week>" — the same
       show CAN resurface in a later week (new arc, new episode, renewed
       relevance) but never gets inserted twice inside the same week, even
       if this script runs more than once in a day. Checked via the same
       db.media_id_seen() telegram_pull.py already uses.
    2. build_news_digest.py's own is_duplicate_topic() cross-digest
       caption-similarity check still applies on top of this at
       digest-build time — this script doesn't need to duplicate that
       logic, just avoid inserting the literal same row twice.

IMAGE VALIDATION — TWO LAYERS (mirrors rss_news_pull.py):
  1. is_valid_image() — ffprobe confirms the downloaded bytes decode to
     SOME image stream with real width/height.
  2. normalize_image() (video_utils.py) — re-encodes the file in place to
     a clean baseline JPEG. A file can pass layer 1 (readable width/
     height) and still be a format ffmpeg's image2 demuxer won't accept
     later when build_news_digest.py -loop's it as a background image
     ("Option loop not found") — this closes that gap at the source.
     AniList's own CDN is generally reliable, but this keeps both
     external image sources defended the same way rather than only
     hardening the specific feed that happened to trigger the bug in
     production (an RSS-sourced image, not this script — see
     rss_news_pull.py's docstring for the incident).

CONNECTIVITY RETRY (added — see incident note below):
  fetch_trending() and download_image() are now wrapped in _with_retry(),
  the same pattern rss_news_pull.py just got and identify_show.py /
  build_news_digest.py already use for their own network calls. Before
  this, a single transient DNS/connection blip killed this entire script
  for the run. Seen in production: graphql.anilist.co, feedburner.com,
  and randomc.net all failed with NameResolutionError within the same
  few seconds — a network-layer outage, not AniList being down. This
  does NOT fix a connection that's down for the whole run (nothing can)
  — it only absorbs short blips, which is what production actually showed.

WHY GRAPHQL / NO KEY: AniList's public API is a single GraphQL POST
endpoint, no auth required, with a generous unauthenticated rate limit
for the one-query-per-run volume this script uses.

SETUP:
  pip install requests python-dotenv
  Nothing else — no API key, no account needed.

USAGE:
  python fetch_anilist_trending.py
  Run any time. Recommended as its own stage in run_all.sh, right after
  telegram_pull.py and before build_news_digest.py, so freshly pulled
  AniList items are eligible the same run they're fetched.

.env overrides (all optional):
  ANILIST_TRENDING_COUNT=20          # how many trending anime to fetch per run
  ANILIST_MIN_DESCRIPTION_CHARS=40   # skip anime too thin to narrate well
                                      # (unless there's a hard next-episode date)
  ANILIST_MAX_RETRIES=3              # connectivity-blip retries per network call
  ANILIST_RETRY_BACKOFF_SECONDS=2    # linear backoff base (2s, 4s, 6s, ...)
"""

import os
import re
import html
import socket
import subprocess
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

from db import PipelineDB
from video_utils import normalize_image

load_dotenv()

ANILIST_URL = "https://graphql.anilist.co"
PLATFORM = "anilist"
SOURCE_HANDLE = "trending"

TRENDING_COUNT = int(os.getenv("ANILIST_TRENDING_COUNT", "20"))
MIN_DESCRIPTION_CHARS = int(os.getenv("ANILIST_MIN_DESCRIPTION_CHARS", "40"))

DOWNLOAD_DIR = os.path.join("downloads", "anilist")

QUERY = """
query ($perPage: Int) {
  Page(page: 1, perPage: $perPage) {
    media(type: ANIME, sort: TRENDING_DESC) {
      id
      title { romaji english }
      description
      format
      status
      episodes
      genres
      season
      seasonYear
      trending
      nextAiringEpisode { episode airingAt }
      coverImage { extraLarge }
      bannerImage
      siteUrl
    }
  }
}
"""

# ── Connectivity retry ───────────────────────────────────────────────────
# Same pattern rss_news_pull.py just got — see that module's docstring and
# this module's own CONNECTIVITY RETRY note above for the production
# incident that motivated it.
ANILIST_MAX_RETRIES = int(os.getenv("ANILIST_MAX_RETRIES", "3"))
ANILIST_RETRY_BACKOFF_SECONDS = float(os.getenv("ANILIST_RETRY_BACKOFF_SECONDS", "2"))

_CONNECTIVITY_EXCEPTION_TYPES = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    socket.gaierror,
    socket.timeout,
    ConnectionError,
    TimeoutError,
)
_CONNECTIVITY_KEYWORDS = (
    "connection", "network", "getaddrinfo", "name resolution",
    "name or service not known", "timed out", "timeout",
    "unreachable", "temporary failure in name resolution",
    "failed to resolve", "max retries exceeded", "reset by peer",
)


def _is_connectivity_error(e):
    if isinstance(e, _CONNECTIVITY_EXCEPTION_TYPES):
        return True
    text = str(e).lower()
    return any(kw in text for kw in _CONNECTIVITY_KEYWORDS)


def _with_retry(fn, *args, **kwargs):
    """Runs fn(*args, **kwargs), retrying on connectivity-type errors with
    linear backoff. Re-raises immediately on a non-connectivity error, and
    re-raises the last error if every attempt is exhausted — callers keep
    their existing try/except around this exactly as before."""
    last_error = None
    for attempt in range(1, ANILIST_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if not _is_connectivity_error(e):
                raise
            last_error = e
            if attempt < ANILIST_MAX_RETRIES:
                wait = ANILIST_RETRY_BACKOFF_SECONDS * attempt
                print(f"    Connectivity blip ({e}); retrying in {wait:.0f}s "
                      f"({attempt}/{ANILIST_MAX_RETRIES})...")
                time.sleep(wait)
    raise last_error
# -----------------------------


def _clean_description(raw):
    """Strips AniList's HTML-ish description markup (mainly <br> and
    <i>) and unescapes entities, down to plain narratable text."""
    if not raw:
        return ""
    text = re.sub(r"<br\s*/?>", " ", raw)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _iso_week_key(dt=None):
    dt = dt or datetime.now(timezone.utc)
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _title_for(media):
    title = media.get("title") or {}
    return title.get("english") or title.get("romaji") or "Unknown Title"


def _format_airing_note(media):
    """Prefers the concrete next-episode date when AniList has one — that's
    the genuinely time-sensitive fact worth leading a news blurb with.
    Falls back to a plain status line otherwise."""
    nae = media.get("nextAiringEpisode")
    if nae:
        airing_at = nae.get("airingAt")
        ep = nae.get("episode")
        if airing_at:
            when = datetime.fromtimestamp(airing_at, tz=timezone.utc)
            return f"Episode {ep} airs {when.strftime('%B %d, %Y')}."
        return f"Episode {ep} airing soon."
    status = (media.get("status") or "").replace("_", " ").title()
    return f"Status: {status}." if status else ""


def build_caption(media, rank):
    """Builds a news-style blurb from raw AniList fields — deliberately
    framed as a trending/status report (rank + airing facts first, synopsis
    second) so build_news_digest.py's is_anime_content() classifier and
    generate_narration() both read this the same way they'd read a real
    Telegram news post, not a plain wiki summary."""
    title = _title_for(media)
    airing_note = _format_airing_note(media)
    desc = _clean_description(media.get("description"))
    genres = media.get("genres") or []
    genre_note = f"Genres: {', '.join(genres[:3])}." if genres else ""

    parts = [f"{title} is currently the #{rank} trending anime on AniList."]
    if airing_note:
        parts.append(airing_note)
    if desc:
        parts.append(desc[:400])
    if genre_note:
        parts.append(genre_note)
    return " ".join(parts).strip()


def fetch_trending():
    resp = requests.post(
        ANILIST_URL,
        json={"query": QUERY, "variables": {"perPage": TRENDING_COUNT}},
        headers={"Content-Type": "application/json"},
        timeout=20,
    )
    if resp.status_code != 200:
        # raise_for_status() alone throws away the response BODY, which is
        # exactly where AniList puts the actual reason for a 400 (bad
        # query syntax, invalid argument, etc.) — surfacing it here is the
        # difference between "400 Bad Request" (useless) and the real
        # cause (actionable).
        try:
            detail = resp.json()
        except ValueError:
            detail = resp.text[:500]
        raise RuntimeError(f"AniList API returned HTTP {resp.status_code}: {detail}")
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"AniList API errors: {data['errors']}")
    return data["data"]["Page"]["media"]


def download_image(url, out_path):
    resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(resp.content)


def is_valid_image(path):
    """Same first-layer validation as rss_news_pull.py's — see that
    module for the full rationale. AniList's own CDN is generally
    reliable, but this keeps both external-image sources defended the
    same way rather than only fixing the specific feed that happened to
    trigger the bug."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=15,
        )
    except Exception:
        return False
    if r.returncode != 0 or not r.stdout.strip():
        return False
    try:
        w, h = r.stdout.strip().split(",")[:2]
        return int(w) > 0 and int(h) > 0
    except (ValueError, IndexError):
        return False


def main():
    db = PipelineDB()
    source_id = db.get_or_create_source(PLATFORM, SOURCE_HANDLE)

    print(f"Fetching top {TRENDING_COUNT} trending anime from AniList...")
    try:
        media_list = _with_retry(fetch_trending)
    except Exception as e:
        print(f"FAILED to fetch AniList trending list: {e}")
        return

    week_key = _iso_week_key()
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    inserted = 0
    skipped_dupe = 0
    skipped_thin = 0
    skipped_no_image = 0
    skipped_bad_image = 0

    for rank, media in enumerate(media_list, 1):
        anime_id = media.get("id")
        if anime_id is None:
            continue

        media_id = f"anilist_{anime_id}_{week_key}"
        if db.media_id_seen(media_id):
            skipped_dupe += 1
            continue

        desc = _clean_description(media.get("description"))
        if len(desc) < MIN_DESCRIPTION_CHARS and not media.get("nextAiringEpisode"):
            # Too thin to narrate well AND nothing time-sensitive to report
            # — skip rather than force a weak digest story.
            skipped_thin += 1
            continue

        image_url = (media.get("bannerImage")
                     or (media.get("coverImage") or {}).get("extraLarge"))
        if not image_url:
            skipped_no_image += 1
            continue

        caption = build_caption(media, rank)
        post_dir = os.path.join(DOWNLOAD_DIR, str(anime_id))
        os.makedirs(post_dir, exist_ok=True)
        image_path = os.path.join(post_dir, "image.jpg")

        try:
            _with_retry(download_image, image_url, image_path)
        except Exception as e:
            print(f"  #{rank} {_title_for(media)}: image download failed, skipping ({e})")
            continue

        if not is_valid_image(image_path):
            print(f"  #{rank} {_title_for(media)}: downloaded file isn't a valid "
                  f"image, skipping")
            try:
                os.remove(image_path)
            except OSError:
                pass
            skipped_bad_image += 1
            continue

        if not normalize_image(image_path):
            # Passed ffprobe's width/height check but ffmpeg couldn't
            # re-encode it — the exact class of file that later breaks
            # build_news_digest.py's -loop background build with
            # "Option loop not found". Treated the same as a bad image.
            print(f"  #{rank} {_title_for(media)}: image failed normalization "
                  f"(would likely break downstream ffmpeg -loop usage), skipping")
            try:
                os.remove(image_path)
            except OSError:
                pass
            skipped_bad_image += 1
            continue

        with open(os.path.join(post_dir, "caption_raw.txt"), "w", encoding="utf-8") as f:
            f.write(caption)
        with open(os.path.join(post_dir, "type.txt"), "w") as f:
            f.write("image")

        post_id = db.insert_post(
            source_id=source_id,
            external_id=f"{anime_id}_{week_key}",
            media_type="image",
            caption_raw=caption,
            media_id=media_id,
            thumb_hash=None,
            raw_media_path=image_path,
        )
        if post_id is None:
            print(f"  #{rank} {_title_for(media)}: already recorded (race/rerun) — skipping")
            continue

        inserted += 1
        print(f"  #{rank} {_title_for(media)}: added.")

    print(f"\nDone. Added {inserted} new AniList post(s). "
          f"Skipped {skipped_dupe} already-seen this week, "
          f"{skipped_thin} too thin to narrate, "
          f"{skipped_no_image} with no usable image, "
          f"{skipped_bad_image} with a corrupt/invalid image.")


if __name__ == "__main__":
    main()
