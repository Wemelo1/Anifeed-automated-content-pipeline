"""
Anime News RSS Pull
----------------------
Primary content source. Pulls real, original-reporting anime news directly
from actual news outlets' own RSS feeds — Anime News Network, MyAnimeList
News, Otaku USA — instead of depending on Telegram channels, which are
usually just reposting these exact same outlets anyway, with the reposting
admin as a single point of failure in between. Feeds the exact same
pipeline.db `posts` table telegram_pull.py used to, so build_news_digest.py
needs zero changes.

WHY THIS REPLACES TELEGRAM AS THE PRIMARY SOURCE:
  - It's the original reporting, not a repost — faster, and immune to any
    one Telegram admin/channel going quiet.
  - Each feed is registered as its own `source` (platform="rss", handle=
    the outlet's slug), same as each Telegram channel used to be its own
    source — so build_news_digest.py's cross-channel corroboration
    clustering (resolve_trending_items) keeps working exactly the same
    way: if ANN and MAL both cover the same story, it's still detected
    as today's biggest story.
  - No API key, no login, no session file to expire — just HTTP GET.

PARSING: hand-rolled via xml.etree.ElementTree rather than a feedparser
dependency — one less package that has to build/install cleanly on
Termux, and RSS 2.0 is simple enough not to need one. Handles the
<enclosure>, <media:content>/<media:thumbnail>, and "first <img> inside
the HTML description" cases for finding each article's image, in that
order — different outlets put it in different places.

DEDUP: permanent, keyed on a hash of the article's guid/link. News
articles don't need a "same week" grace period the way build_news_digest.py's
AniList step does — a given article URL is only ever going to be posted
once, period.

IMAGE VALIDATION — TWO LAYERS:
  1. is_valid_image() — ffprobe confirms the downloaded bytes decode to
     SOME image stream with real width/height. Catches the common case
     (an HTML error page or access-denied placeholder saved as image.jpg).
  2. normalize_image() (video_utils.py) — re-encodes the file in place to
     a clean baseline JPEG. Exists because layer 1 passing does NOT mean
     ffmpeg's image2 demuxer will accept the file later: a WebP/AVIF/
     unusual-PNG-variant file saved under a .jpg extension can have a
     perfectly readable width/height (passes layer 1) and still blow up
     build_news_digest.py downstream with "Option loop not found" the
     moment it's used as a -loop'd background image — seen in production
     with a Crunchyroll-sourced image that took down an entire digest
     build. Layer 2 closes that off at the source instead of chasing
     individual downstream failure modes. A file that fails layer 2 is
     discarded exactly like a layer-1 failure.

CONNECTIVITY RETRY (added — see incident note below):
  fetch_feed() and download_image() are now wrapped in _with_retry(),
  mirroring the exact pattern identify_show.py / build_news_digest.py
  already use for their own network calls (Groq, OAuth refresh, search).
  Before this, a single transient DNS/connection blip on a mobile network
  killed that feed's ENTIRE contribution for the run — seen in production
  where NameResolutionError hit multiple UNRELATED hosts (feedburner.com,
  randomc.net, graphql.anilist.co) within the same few seconds, which is
  a network-layer outage, not those sites being down. Left unretried,
  a flaky connection for even a few seconds can starve
  build_news_digest.py's candidate pool below DIGEST_MIN_ITEMS run after
  run, even though the outlets themselves were fine. This does NOT fix a
  connection that's down for the whole run (nothing can) — it only
  absorbs the short blips, which is what production actually showed.

SETUP:
  pip install requests python-dotenv
  Nothing else — no API key needed.

USAGE:
  python rss_news_pull.py
  Run any time — recommended as the FIRST content-gathering stage in
  run_all.sh, before build_news_digest.py.

.env overrides (optional):
  ANIME_NEWS_RSS_FEEDS="Name|https://url,Name2|https://url2,..."
      Overrides the default feed list entirely if set. Format is
      comma-separated "Name|URL" pairs.
  RSS_MAX_ITEMS_PER_FEED=15   # cap per feed per run, oldest-in-feed-order ignored
                              # (RSS feeds are already newest-first)
  RSS_MAX_RETRIES=3           # connectivity-blip retries per network call
  RSS_RETRY_BACKOFF_SECONDS=2 # linear backoff base (2s, 4s, 6s, ...)
"""

import os
import re
import html
import hashlib
import socket
import subprocess
import time
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

from db import PipelineDB
from video_utils import normalize_image
import source_quality as sq

load_dotenv()

PLATFORM = "rss"
DOWNLOAD_DIR = os.path.join("downloads", "rss")

MAX_ITEMS_PER_FEED = int(os.getenv("RSS_MAX_ITEMS_PER_FEED", "15"))

# Default feed list — verified working RSS 2.0 feeds from real anime news
# outlets. Override entirely via ANIME_NEWS_RSS_FEEDS in .env if you find
# more (or one of these ever moves/dies) — format: "Name|URL,Name2|URL2".
DEFAULT_FEEDS = [
    ("Anime News Network", "https://www.animenewsnetwork.com/all/rss.xml"),
    ("MyAnimeList News", "https://myanimelist.net/rss/news.xml"),
    ("Otaku USA", "https://otakuusamagazine.com/anime/feed"),
]

_MEDIA_NS = "{http://search.yahoo.com/mrss/}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Connectivity retry ───────────────────────────────────────────────────
# Same pattern as identify_show.py's ConnectivityError / build_news_digest.py's
# _call_groq_with_retry — a transient DNS/connection blip shouldn't cost this
# feed its entire run. See module docstring's CONNECTIVITY RETRY note.
RSS_MAX_RETRIES = int(os.getenv("RSS_MAX_RETRIES", "3"))
RSS_RETRY_BACKOFF_SECONDS = float(os.getenv("RSS_RETRY_BACKOFF_SECONDS", "2"))

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
    linear backoff. Re-raises immediately on a non-connectivity error (a
    genuine 404/parse error shouldn't be retried), and re-raises the last
    error if every attempt is exhausted — callers keep their existing
    try/except around this exactly as before."""
    last_error = None
    for attempt in range(1, RSS_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if not _is_connectivity_error(e):
                raise
            last_error = e
            if attempt < RSS_MAX_RETRIES:
                wait = RSS_RETRY_BACKOFF_SECONDS * attempt
                print(f"    Connectivity blip ({e}); retrying in {wait:.0f}s "
                      f"({attempt}/{RSS_MAX_RETRIES})...")
                time.sleep(wait)
    raise last_error
# -----------------------------


def _slugify(name):
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _load_feeds():
    raw = os.getenv("ANIME_NEWS_RSS_FEEDS", "").strip()
    if not raw:
        return DEFAULT_FEEDS
    feeds = []
    for pair in raw.split(","):
        if "|" not in pair:
            continue
        name, url = pair.split("|", 1)
        name, url = name.strip(), url.strip()
        if name and url:
            feeds.append((name, url))
    return feeds or DEFAULT_FEEDS


def _clean_html(raw):
    if not raw:
        return ""
    text = re.sub(r"<br\s*/?>", " ", raw)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _find_image_in_item(item):
    """Tries, in order: <enclosure type="image/..">, <media:content>,
    <media:thumbnail>, then a regex scan for the first <img src=...>
    inside the raw <description>/<content:encoded> HTML. Different
    outlets put the article image in different places — this covers the
    common cases without needing feedparser."""
    enclosure = item.find("enclosure")
    if enclosure is not None:
        url = enclosure.get("url")
        mime = enclosure.get("type", "")
        if url and (not mime or mime.startswith("image")):
            return url

    for tag in (f"{_MEDIA_NS}content", f"{_MEDIA_NS}thumbnail"):
        media = item.find(tag)
        if media is not None and media.get("url"):
            return media.get("url")

    for tag in ("description", "{http://purl.org/rss/1.0/modules/content/}encoded"):
        el = item.find(tag)
        if el is not None and el.text:
            match = re.search(r'<img[^>]+src="([^"]+)"', el.text)
            if match:
                return match.group(1)

    return None


def _parse_feed(xml_text):
    """Returns a list of {title, link, guid, description, image_url} dicts.
    Raises on malformed XML — caller treats that as this feed's failure,
    isolated from every other feed."""
    root = ET.fromstring(xml_text)
    channel = root.find("channel")
    items = channel.findall("item") if channel is not None else root.findall(".//item")

    parsed = []
    for item in items:
        title_el = item.find("title")
        link_el = item.find("link")
        guid_el = item.find("guid")
        desc_el = item.find("description")

        title = (title_el.text or "").strip() if title_el is not None else ""
        link = (link_el.text or "").strip() if link_el is not None else ""
        guid = (guid_el.text or "").strip() if guid_el is not None else link
        description = _clean_html(desc_el.text if desc_el is not None else "")
        image_url = _find_image_in_item(item)

        if not title or not (guid or link):
            continue

        parsed.append({
            "title": title,
            "link": link,
            "guid": guid or link,
            "description": description,
            "image_url": image_url,
        })
    return parsed


def fetch_feed(url):
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return _parse_feed(resp.text)


def build_caption(entry):
    title = entry["title"]
    desc = entry["description"][:400]
    if desc:
        return f"{title}. {desc}"
    return title


def download_image(url, out_path):
    try:
        # Attempt direct download first
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        with open(out_path, "wb") as f:
            f.write(resp.content)
    except requests.exceptions.RequestException as e:
        # Fallback: if the site's CDN is blocking us (like Ani-Gamers), 
        # use a free image caching proxy to bypass the block.
        print(f"      Direct fetch failed, falling back to wsrv.nl proxy...")
        proxy_url = f"https://wsrv.nl/?url={url}"
        resp_fallback = requests.get(proxy_url, headers=HEADERS, timeout=30)
        resp_fallback.raise_for_status()
        with open(out_path, "wb") as f:
            f.write(resp_fallback.content)



def is_valid_image(path):
    """Validates that `path` is actually a decodable image, not just a
    file that exists. RSS/CDN image URLs occasionally return an HTML
    error page, an access-denied placeholder, or a truncated download —
    all while responding with HTTP 200, so download_image()'s own
    raise_for_status() check doesn't catch it (seen in production with a
    Crunchyroll-sourced image: saved fine as image.jpg, but wasn't
    actually image bytes). Left unvalidated, that file only fails much
    later, deep inside build_news_digest.py's ffmpeg pass — where it
    doesn't just skip that one story, it discards the ENTIRE digest being
    built around it (see build_news_digest.py's per-story try/except for
    the other half of this fix).

    NOTE: this only confirms the file decodes to SOME image stream — it
    does NOT guarantee ffmpeg's image2 demuxer will accept it later for a
    -loop'd still-image build (that's a stricter requirement a handful of
    valid-but-exotic formats can still fail). See normalize_image() in
    video_utils.py, called right after this in process_feed(), for the
    layer that actually closes that gap.

    Uses ffprobe (already a pipeline dependency everywhere else) rather
    than adding Pillow just for this one check."""
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


def process_feed(db, name, url):
    handle = _slugify(name)
    source_id = db.get_or_create_source(PLATFORM, handle)

    # Source quality (roadmap item N) — a source with a track record of
    # low views/high removal-rate/high quality-gate-rejection/low yield
    # gets a SOFT quota reduction, never a hard block. Unknown/thin-data
    # sources get multiplier=1.0 (no change) — never punished for lacking
    # history yet.
    quota_mult = sq.quota_multiplier(PLATFORM, handle)
    effective_max_items = max(1, round(MAX_ITEMS_PER_FEED * quota_mult))

    print(f"\n[{name}] Fetching {url}...")
    try:
        entries = _with_retry(fetch_feed, url)
    except Exception as e:
        print(f"  FAILED to fetch/parse this feed: {e}")
        return 0, 0, 0

    entries = entries[:effective_max_items]
    if quota_mult < 1.0:
        print(f"  Source quality quota reduction ACTIVE — pulling "
              f"{effective_max_items} (of normal {MAX_ITEMS_PER_FEED}) this run.")
    print(f"  {len(entries)} item(s) in feed.")

    inserted = 0
    skipped_dupe = 0
    skipped_no_image = 0

    for entry in entries:
        guid_hash = hashlib.sha1(entry["guid"].encode("utf-8")).hexdigest()[:20]
        media_id = f"rss_{handle}_{guid_hash}"

        if db.media_id_seen(media_id):
            skipped_dupe += 1
            continue

        if not entry["image_url"]:
            skipped_no_image += 1
            continue

        caption = build_caption(entry)
        post_dir = os.path.join(DOWNLOAD_DIR, handle, guid_hash)
        os.makedirs(post_dir, exist_ok=True)
        image_path = os.path.join(post_dir, "image.jpg")

        try:
            _with_retry(download_image, entry["image_url"], image_path)
        except Exception as e:
            print(f"  SKIPPED (image download failed): {entry['title'][:60]} ({e})")
            continue

        if not is_valid_image(image_path):
            print(f"  SKIPPED (downloaded file isn't a valid image — source likely "
                  f"blocked the request or returned an error page): {entry['title'][:60]}")
            try:
                os.remove(image_path)
            except OSError:
                pass
            continue

        if not normalize_image(image_path):
            # Decoded fine for ffprobe (layer 1) but ffmpeg itself couldn't
            # re-encode it — this is exactly the class of file that passes
            # is_valid_image() and then blows up build_news_digest.py's
            # -loop background build later ("Option loop not found").
            # Treated identically to a failed is_valid_image() check.
            print(f"  SKIPPED (image failed normalization — would likely break "
                  f"downstream ffmpeg -loop usage): {entry['title'][:60]}")
            try:
                os.remove(image_path)
            except OSError:
                pass
            continue

        with open(os.path.join(post_dir, "caption_raw.txt"), "w", encoding="utf-8") as f:
            f.write(caption)
        with open(os.path.join(post_dir, "type.txt"), "w") as f:
            f.write("image")

        post_id = db.insert_post(
            source_id=source_id,
            external_id=guid_hash,
            media_type="image",
            caption_raw=caption,
            media_id=media_id,
            thumb_hash=None,
            raw_media_path=image_path,
        )
        if post_id is None:
            continue  # race/rerun — already recorded

        inserted += 1
        print(f"  + {entry['title'][:70]}")

    return inserted, skipped_dupe, skipped_no_image


def main():
    db = PipelineDB()
    feeds = _load_feeds()

    total_inserted = total_dupe = total_no_image = 0
    for name, url in feeds:
        i, d, n = process_feed(db, name, url)
        total_inserted += i
        total_dupe += d
        total_no_image += n

    print(f"\nDone. Added {total_inserted} new article(s) across {len(feeds)} feed(s). "
          f"Skipped {total_dupe} already-seen, {total_no_image} with no usable image.")


if __name__ == "__main__":
    main()
