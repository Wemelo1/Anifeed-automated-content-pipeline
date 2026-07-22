"""
Reddit Source
---------------
Fourth Acquire-stage content source, same redundancy pattern as
rss_news_pull.py / fetch_anilist_trending.py / fetch_youtube_trailers.py:
one more independent feed into the same pipeline.db `posts` table, so a
dry spell (or a block/outage) on any ONE source doesn't starve
build_news_digest.py's candidate pool.

AUTH — CHANGED: this used to hit Reddit's public per-subreddit JSON
listing (any URL + ".json") with a plain GET, no key/OAuth needed. On
May 28, 2026 Reddit deprecated unauthenticated .json access — those
requests now return 403 for essentially everyone, regardless of
User-Agent (Reddit is filtering on TLS fingerprint / IP reputation, not
headers). This module now goes through PRAW using a read-only OAuth
"script" app instead — see SETUP below. No Reddit username/password is
needed for read-only listing access, only a client_id/client_secret pair
tied to a free Reddit developer app.

WHAT IT PULLS:
  Hot posts from a configurable list of anime-adjacent subreddits
  (r/anime by default). Only posts that are (a) not NSFW, (b) not
  stickied/mod-pinned, (c) not text-only self-posts, and (d) carry a
  usable image URL are kept — this pipeline's image-post shape (see
  build_news_digest.py's find_candidate_posts()) needs a real image
  asset, so a text discussion thread or a video-only post has nothing
  for it to render. A minimum score (upvotes) floor filters out
  low-signal/spam posts before they ever reach the caption-similarity
  and is_anime_content() classifiers downstream — those still get the
  final say on relevance, same as every other source; this floor is
  just a cheap pre-filter so obvious noise doesn't burn an LLM call.

NSFW: over_18 posts are hard-skipped, no override — see REDDIT_MIN_SCORE
note above. This is a content-safety floor, not a tunable.

IMAGE SOURCE PRIORITY: i.redd.it direct image links first (already a
final CDN URL), then the post's own preview.images[0].source.url as a
fallback (needs HTML-entity unescaping — Reddit's JSON API returns that
field HTML-escaped, e.g. "&amp;" instead of "&"). Gallery posts and
video posts are skipped outright — no single "the" image to grab, and
guessing which gallery frame matters isn't worth the complexity for a
source whose whole value proposition is being cheap to add.

DEDUP: permanent, keyed on media_id = "reddit_<subreddit>_<post_id>" via
the same db.media_id_seen() check every other source uses — a given
Reddit post is only ever pulled once, period (no "same week" grace
period; unlike AniList trending, a Reddit thread doesn't resurface).

IMAGE VALIDATION — TWO LAYERS, same convention as rss_news_pull.py /
fetch_anilist_trending.py:
  1. is_valid_image() — ffprobe confirms the downloaded bytes decode to
     SOME image stream with real width/height (catches Reddit's own
     "this content isn't available" placeholder images, which return
     HTTP 200).
  2. normalize_image() (video_utils.py) — re-encodes to a clean baseline
     JPEG, closing the same "passes ffprobe, still breaks ffmpeg's
     -loop demuxer later" gap documented in those modules.

CONNECTIVITY RETRY: fetch_subreddit() and download_image() are wrapped in
_with_retry(), the exact same linear-backoff pattern rss_news_pull.py /
fetch_anilist_trending.py / fetch_youtube_trailers.py already use — a
transient DNS/connection blip costs this one subreddit's contribution for
the run, not the whole script.

SETUP:
  pip install requests praw python-dotenv
  Requires a free Reddit "script" app (client_id + client_secret) — see
  the setup walkthrough in the project README / chat history. Read-only
  listing access needs no Reddit account login at request time, just
  those two values in .env.

USAGE:
  python reddit_pull.py
  Run any time — recommended alongside rss_news_pull.py /
  fetch_anilist_trending.py at the start of run_all.sh, so freshly pulled
  Reddit items are eligible the same run they're fetched.

.env required:
  REDDIT_CLIENT_ID=...
  REDDIT_CLIENT_SECRET=...

.env overrides (all optional):
  REDDIT_SUBREDDITS="anime,manga"       # comma-separated, no "r/" prefix
  REDDIT_LISTING=hot                    # hot | new | top
  REDDIT_MAX_ITEMS_PER_SUB=25
  REDDIT_MIN_SCORE=50                   # upvote floor — pre-filter only,
                                          # NOT a substitute for the
                                          # downstream is_anime_content()
                                          # relevance check
  REDDIT_MAX_RETRIES=3
  REDDIT_RETRY_BACKOFF_SECONDS=2
"""

import os
import re
import html
import socket
import subprocess
import time
import requests
import praw
import prawcore
from dotenv import load_dotenv

from db import PipelineDB
from video_utils import normalize_image

load_dotenv()

PLATFORM = "reddit"
DOWNLOAD_DIR = os.path.join("downloads", "reddit")

REDDIT_LISTING = os.getenv("REDDIT_LISTING", "hot").strip().lower()
if REDDIT_LISTING not in ("hot", "new", "top"):
    REDDIT_LISTING = "hot"

MAX_ITEMS_PER_SUB = int(os.getenv("REDDIT_MAX_ITEMS_PER_SUB", "25"))
REDDIT_MIN_SCORE = int(os.getenv("REDDIT_MIN_SCORE", "50"))

_DEFAULT_SUBREDDITS = "anime"
SUBREDDITS = [
    s.strip() for s in os.getenv("REDDIT_SUBREDDITS", _DEFAULT_SUBREDDITS).split(",")
    if s.strip()
]

# Still used for the plain-GET image *download* step (i.redd.it / preview
# CDN URLs, not the listing API) — those aren't behind the OAuth wall.
USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "AniFeedStudio/1.0 (personal anime-news content pipeline; "
    "single-instance, low-volume)",
)
HEADERS = {"User-Agent": USER_AGENT}

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp")

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "").strip()
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "").strip()

_reddit_client = None


def _get_reddit_client():
    """Lazily builds a single read-only PRAW client. Read-only mode needs
    only client_id/client_secret (no username/password) — it authenticates
    as the app itself via OAuth client-credentials, which is enough for
    public listing reads. Fails loudly at call time (not import time) so
    a missing .env value gives a clear error instead of crashing before
    argv/help handling."""
    global _reddit_client
    if _reddit_client is None:
        if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
            raise RuntimeError(
                "REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set in .env — "
                "see module docstring SETUP section."
            )
        _reddit_client = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=USER_AGENT,
        )
        _reddit_client.read_only = True
    return _reddit_client

# ── Connectivity retry ───────────────────────────────────────────────────
# Same pattern as rss_news_pull.py / fetch_anilist_trending.py /
# fetch_youtube_trailers.py — see those modules' docstrings for the
# production incident that motivated it.
REDDIT_MAX_RETRIES = int(os.getenv("REDDIT_MAX_RETRIES", "3"))
REDDIT_RETRY_BACKOFF_SECONDS = float(os.getenv("REDDIT_RETRY_BACKOFF_SECONDS", "2"))

_CONNECTIVITY_EXCEPTION_TYPES = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    socket.gaierror,
    socket.timeout,
    ConnectionError,
    TimeoutError,
    prawcore.exceptions.RequestException,
    prawcore.exceptions.ServerError,
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
    linear backoff. Re-raises immediately on a non-connectivity error (a
    genuine 403/404 shouldn't be retried), and re-raises the last error if
    every attempt is exhausted."""
    last_error = None
    for attempt in range(1, REDDIT_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if not _is_connectivity_error(e):
                raise
            last_error = e
            if attempt < REDDIT_MAX_RETRIES:
                wait = REDDIT_RETRY_BACKOFF_SECONDS * attempt
                print(f"    Connectivity blip ({e}); retrying in {wait:.0f}s "
                      f"({attempt}/{REDDIT_MAX_RETRIES})...")
                time.sleep(wait)
    raise last_error
# -----------------------------


def fetch_subreddit(subreddit):
    """Returns the raw list of post data dicts from a subreddit's listing,
    fetched via authenticated PRAW/OAuth (the old unauthenticated .json
    GET was deprecated by Reddit on 2026-05-28 and now 403s). Raises on a
    genuine failure (private/banned subreddit, bad credentials, etc.) —
    caller treats that as this subreddit's failure, isolated from every
    other subreddit in the batch (per-item isolation, PROJECT_RULES.md
    Rule 5).

    Returns plain dicts (not PRAW Submission objects) so every downstream
    function in this file — find_image_url(), build_caption(), etc. —
    keeps working unchanged against .get()-style access. vars(submission)
    is used rather than attribute access because PRAW lazily re-fetches
    the FULL object (an extra API call) on any attribute not already
    cached from the listing response; reading the underlying __dict__
    avoids triggering that for fields a given post simply doesn't have
    (e.g. no "preview" on a text post)."""
    reddit = _get_reddit_client()
    sub = reddit.subreddit(subreddit)
    listing = {
        "hot": sub.hot,
        "new": sub.new,
        "top": sub.top,
    }[REDDIT_LISTING]
    submissions = listing(limit=MAX_ITEMS_PER_SUB)
    return [vars(s) for s in submissions]


def _clean_selftext(raw, max_chars=400):
    if not raw:
        return ""
    text = html.unescape(raw)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def build_caption(post):
    title = (post.get("title") or "").strip()
    body = _clean_selftext(post.get("selftext"))
    if body:
        return f"{title}. {body}"
    return title


def _looks_like_image_url(url):
    if not url:
        return False
    low = url.lower().split("?")[0]
    return low.endswith(IMAGE_EXTENSIONS)


def find_image_url(post):
    """Tries, in order: a direct i.redd.it image link, then the post's own
    Reddit-hosted preview image. Returns None for gallery posts, video
    posts, link posts pointing at a non-image URL, or anything else with
    no single unambiguous image — see module docstring for why those are
    skipped rather than guessed at.

    FIXED: gallery posts (is_gallery=True) and video posts (is_video=True)
    were never actually checked here — both have a url that correctly
    fails _looks_like_image_url() (a reddit.com/gallery/... link, or a
    v.redd.it link), but Reddit ALSO populates preview.images[0] for both
    (a gallery's cover image, a video's poster frame), so both were
    silently falling through to the preview fallback below and returning
    an image anyway — directly contradicting this function's own
    docstring and the module docstring's "skipped outright" claim. A
    video's poster frame isn't a real posted image (the caption text
    built from the post title would describe a video clip while the
    'image' is just a freeze-frame of it), and a gallery's cover image
    is only one of N images the post is actually about — neither is the
    single unambiguous image this function exists to find. Both are now
    explicitly excluded before the preview fallback ever runs."""
    if post.get("is_gallery") or post.get("is_video"):
        return None

    url = post.get("url_overridden_by_dest") or post.get("url")
    if _looks_like_image_url(url):
        return url

    preview = post.get("preview") or {}
    images = preview.get("images") or []
    if images:
        source = images[0].get("source") or {}
        preview_url = source.get("url")
        if preview_url:
            # Reddit's JSON API HTML-escapes this field (e.g. "&amp;"
            # instead of "&") — must unescape before it's usable as a
            # real URL, or the query string comes back mangled.
            return html.unescape(preview_url)

    return None


def download_image(url, out_path):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(resp.content)


def is_valid_image(path):
    """Same first-layer validation every other image source in this
    pipeline uses — see rss_news_pull.py's is_valid_image() for the full
    rationale (Reddit's own removed/unavailable placeholder can return
    HTTP 200 with non-image bytes, same class of bug as the Crunchyroll
    incident documented there)."""
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


def process_subreddit(db, subreddit):
    source_id = db.get_or_create_source(PLATFORM, subreddit)

    print(f"\n[r/{subreddit}] Fetching {REDDIT_LISTING} listing...")
    try:
        posts = _with_retry(fetch_subreddit, subreddit)
    except Exception as e:
        print(f"  FAILED to fetch this subreddit: {e}")
        return 0, 0, 0, 0, 0

    print(f"  {len(posts)} post(s) in listing.")

    inserted = 0
    skipped_dupe = 0
    skipped_nsfw = 0
    skipped_no_image = 0
    skipped_low_score = 0

    for post in posts:
        post_id = post.get("id")
        if not post_id:
            continue

        media_id = f"reddit_{subreddit}_{post_id}"
        if db.media_id_seen(media_id):
            skipped_dupe += 1
            continue

        # Hard content-safety floor — no override, no config knob.
        if post.get("over_18"):
            skipped_nsfw += 1
            continue

        if post.get("stickied") or post.get("is_self"):
            continue

        if (post.get("score") or 0) < REDDIT_MIN_SCORE:
            skipped_low_score += 1
            continue

        image_url = find_image_url(post)
        if not image_url:
            skipped_no_image += 1
            continue

        title = (post.get("title") or "").strip()
        if not title:
            continue

        caption = build_caption(post)
        post_dir = os.path.join(DOWNLOAD_DIR, subreddit, post_id)
        os.makedirs(post_dir, exist_ok=True)
        image_path = os.path.join(post_dir, "image.jpg")

        try:
            _with_retry(download_image, image_url, image_path)
        except Exception as e:
            print(f"  SKIPPED (image download failed): {title[:60]} ({e})")
            continue

        if not is_valid_image(image_path):
            print(f"  SKIPPED (downloaded file isn't a valid image): {title[:60]}")
            try:
                os.remove(image_path)
            except OSError:
                pass
            continue

        if not normalize_image(image_path):
            print(f"  SKIPPED (image failed normalization — would likely break "
                  f"downstream ffmpeg -loop usage): {title[:60]}")
            try:
                os.remove(image_path)
            except OSError:
                pass
            continue

        with open(os.path.join(post_dir, "caption_raw.txt"), "w", encoding="utf-8") as f:
            f.write(caption)
        with open(os.path.join(post_dir, "type.txt"), "w") as f:
            f.write("image")

        inserted_id = db.insert_post(
            source_id=source_id,
            external_id=post_id,
            media_type="image",
            caption_raw=caption,
            media_id=media_id,
            thumb_hash=None,
            raw_media_path=image_path,
        )
        if inserted_id is None:
            continue  # race/rerun — already recorded

        inserted += 1
        print(f"  + [{post.get('score', 0)}\u2191] {title[:70]}")

    return inserted, skipped_dupe, skipped_nsfw, skipped_no_image, skipped_low_score


def main():
    if not SUBREDDITS:
        print("REDDIT_SUBREDDITS resolved to an empty list — nothing to do.")
        return

    db = PipelineDB()

    total_inserted = total_dupe = total_nsfw = total_no_image = total_low_score = 0
    for subreddit in SUBREDDITS:
        i, d, n, ni, ls = process_subreddit(db, subreddit)
        total_inserted += i
        total_dupe += d
        total_nsfw += n
        total_no_image += ni
        total_low_score += ls

    print(f"\nDone. Added {total_inserted} new post(s) across {len(SUBREDDITS)} "
          f"subreddit(s). Skipped {total_dupe} already-seen, {total_nsfw} NSFW, "
          f"{total_low_score} under the {REDDIT_MIN_SCORE}-upvote floor, "
          f"{total_no_image} with no usable image (gallery/video/text posts).")


if __name__ == "__main__":
    main()
