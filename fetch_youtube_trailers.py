"""
YouTube Trailer Puller
-------------------------
Pulls actual trailer/PV video (not just images) from official anime
publisher YouTube channels — Crunchyroll, Aniplex, studio channels, etc.
— and feeds it into the SAME video pipeline telegram_pull.py's video
posts used to (edit_clips.py -> burn_subtitles.py -> identify_show.py ->
generate_captions.py -> thumbnails -> hook overlay -> publish_youtube.py).
No changes needed downstream — this just gives edit_clips.py something to
pick up again, since RSS/AniList only ever produced image posts.

>>> READ THIS BEFORE ENABLING — COPYRIGHT RISK IS REAL, NOT HYPOTHETICAL <<<
Official trailers are near-certain to be registered with YouTube's
Content ID system by the rightsholder. Re-uploading one, even trimmed and
subtitled, can get auto-claimed. Nobody — not this script, not any tool —
can guarantee a specific upload won't be flagged; Content ID policy per
asset is private to the rightsholder and not something this pipeline can
see or predict. What IS true, empirically: trailers are promotional
material studios generally want spread (unlike full episode clips, which
compete with their paid product), so the realistic outcome for most
official trailers is a "monetize" claim (video stays up, ad revenue goes
to the claimant) rather than an outright block — but this varies by
publisher and is not guaranteed. This script defaults to conservative
settings for exactly this reason:
  - Title-keyword filtering so only things that look like trailers/PVs/
    teasers get pulled — not full episodes, streams, or interviews.
  - A hard duration cap (default 200s) — also filters out non-trailer
    content that happened to match a keyword.
  - Everything downstream still publishes as YOUTUBE_PRIVACY_STATUS=
    unlisted by default (see publish_youtube.py) until YOU decide to go
    public.
Recommendation: run this against a SMALL channel list first, watch what
happens to those specific uploads in YouTube Studio's Copyright tab for
a few days, before adding more channels or going public.

CHANNEL CONFIGURATION — you must supply channel IDs yourself:
No default channel list ships with this script. Guessing a channel ID
wrong means silently pulling from nothing (or the wrong channel) with no
error — worse than requiring one minute of manual setup. To find a
channel's ID:
  1. Go to the channel's YouTube page.
  2. View page source (or use a "channel ID finder" tool) and search for
     "channelId" — it looks like "UCxxxxxxxxxxxxxxxxxxxxxx".
  Then add to .env:
    YOUTUBE_TRAILER_CHANNELS=Crunchyroll|UCxxxx...,Aniplex USA|UCyyyy...

WHY RSS + yt-dlp, NOT THE YOUTUBE DATA API:
A channel's RSS feed (https://www.youtube.com/feeds/videos.xml?channel_id=)
needs no API key and no quota — just the last ~15 uploads, which is all a
daily pull needs. yt-dlp handles the actual video download.

CROSS-CHANNEL TITLE DEDUP (added — see incident note below):
Different official channels (a studio's own channel, Aniplex, a regional
distributor, Crunchyroll) frequently re-upload or re-title the SAME
underlying PV/trailer — different YouTube video_ids, same content. The
old dedup (media_id_seen(), keyed purely on video_id) couldn't catch
this: a re-upload has a different video_id by definition, so it sailed
straight through and produced 2-3 near-duplicate clips in the same batch
(seen in production: "Dororo Character Spotlight: The Sculptor PV",
"Hanbei the Undying Character PV — SEKIRO: Shadows Die Twice", and
"Dororo: Hanbei the Undying Character PV" all landing as separate posts
from what was clearly one PV).

_is_duplicate_title() now fuzzy-matches (char n-gram similarity, same
technique db.py's _similarity()/is_duplicate_topic() already use for
caption text) every new candidate title against:
  1. Every OTHER title already accepted THIS run, across every channel
     processed so far — catches all 3 Dororo uploads even if they're on
     3 different channels in the SAME run, not just repeats across runs.
  2. Every video-post title downloaded in the last
     TRAILER_DEDUP_LOOKBACK_DAYS days, read straight from posts.caption_raw
     (== the original YouTube title, see process_channel() below) — catches
     a re-upload that shows up in a LATER run than the original.
This is a cheap, no-API-call check (same n-gram approach the rest of the
pipeline already uses) that runs BEFORE the actual yt-dlp download, so a
caught duplicate costs nothing beyond one string comparison — no wasted
bandwidth/CPU on a video that was going to get discarded anyway.

SETUP:
  pip install yt-dlp python-dotenv requests

USAGE:
  python fetch_youtube_trailers.py
  Run any time — recommended as its own stage in run_all.sh, before
  edit_clips.py, so freshly pulled trailers are eligible the same run.

.env overrides (optional):
  YOUTUBE_TRAILER_CHANNELS=Name|channelId,Name2|channelId2   (required to
      pull anything at all — empty by default, see above)
  TRAILER_TITLE_KEYWORDS="trailer,pv,teaser,promo,preview"   (comma-
      separated, case-insensitive substring match against the video title)
  TRAILER_MAX_DURATION_SECONDS=200   # hard skip anything longer — also
      catches non-trailer uploads that happened to match a keyword
  TRAILER_MAX_HEIGHT=720   # caps download resolution — a phone doesn't
      need 4K source for a vertical Shorts crop, and it saves storage/data
  TRAILER_MAX_DOWNLOADS_PER_RUN=15   # hard cap on total videos downloaded
      in one run, across ALL channels combined — protects phone storage/
      data from a channel list that happens to have a lot of trailers in
      its recent uploads all at once. Whatever's left over is picked up
      automatically next run (already-downloaded videos are permanently
      deduped, so nothing is skipped forever).
  RSS_MAX_ITEMS_PER_CHANNEL=15
  TRAILER_TITLE_SIMILARITY_THRESHOLD=0.6   # char-ngram similarity above
      which two titles are considered the same underlying PV — same
      convention/scale as db.py's CROSS_DIGEST_SIMILARITY_THRESHOLD (0.6).
      Lower = more aggressive deduping (more false-positive skips of
      genuinely different PVs for the same show); higher = more re-uploads
      slip through.
  TRAILER_DEDUP_LOOKBACK_DAYS=30   # how far back to look for a
      previously-downloaded title to compare a new candidate against —
      bounded so this doesn't grow into an unbounded full-table scan
      as the channel accumulates months of history.
  YT_DLP_PROXY=socks5://host:port   # route yt-dlp's downloads through a
      proxy — fixes "not available in your country" errors on channels
      that geo-restrict some uploads (Netflix Anime does this often).
      Needs a proxy/VPN exit node actually located in a country the
      video IS available in (see the per-video error for the allowed
      list) — yt-dlp's header-only geo-bypass doesn't reliably fool
      YouTube's actual video CDN, only a real IP change does. Any
      http://, https://, or socks5://host:port proxy works: a VPN
      provider's SOCKS5 endpoint (many expose one), a paid residential/
      datacenter proxy geo-targeted to an allowed country, or your own
      cheap VPS in one of those countries running `ssh -D <port> -N
      user@vps` for a free local SOCKS5 proxy. Only affects yt-dlp
      downloads in this script — nothing else in the pipeline routes
      through it.
  YT_DLP_GEO_BYPASS_COUNTRY=SG   # optional, best-effort only: 2-letter
      country code yt-dlp fakes via X-Forwarded-For for the metadata
      request. Sometimes helps on top of YT_DLP_PROXY, does nothing
      reliable on its own for YouTube specifically — don't depend on
      this alone to fix a country-locked video.
"""

import os
import re
import hashlib
import socket
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import requests
from dotenv import load_dotenv

from db import PipelineDB, get_connection
import source_quality as sq

load_dotenv()

PLATFORM = "youtube_trailers"
DOWNLOAD_DIR = os.path.join("downloads", "youtube_trailers")

MAX_ITEMS_PER_CHANNEL = int(os.getenv("RSS_MAX_ITEMS_PER_CHANNEL", "15"))
MAX_DURATION_SECONDS = int(os.getenv("TRAILER_MAX_DURATION_SECONDS", "200"))
MAX_HEIGHT = int(os.getenv("TRAILER_MAX_HEIGHT", "720"))

# Safety cap so one run against a long channel list doesn't chew through a
# phone's storage/data in one go — same pattern edit_clips.py's BATCH_LIMIT
# already uses. Counts across ALL channels combined, not per-channel: once
# hit, remaining channels are simply skipped for this run and picked up
# again (from where they left off, since already-downloaded videos are
# permanently deduped) next time run_all.sh runs.
MAX_DOWNLOADS_PER_RUN = int(os.getenv("TRAILER_MAX_DOWNLOADS_PER_RUN", "15"))

# Geo-restriction workaround — see .env overrides in the module docstring.
# YT_DLP_PROXY is the one that actually works reliably; GEO_BYPASS_COUNTRY
# is a cheap best-effort extra that sometimes helps on top of it.
YT_DLP_PROXY = os.getenv("YT_DLP_PROXY", "").strip()
YT_DLP_GEO_BYPASS_COUNTRY = os.getenv("YT_DLP_GEO_BYPASS_COUNTRY", "").strip()

_DEFAULT_KEYWORDS = "trailer,pv,teaser,promo,preview"
TITLE_KEYWORDS = [
    kw.strip().lower()
    for kw in os.getenv("TRAILER_TITLE_KEYWORDS", _DEFAULT_KEYWORDS).split(",")
    if kw.strip()
]

# Cross-channel title-similarity dedup — see module docstring's
# CROSS-CHANNEL TITLE DEDUP section for the production incident that
# motivated this (Dororo PVs re-uploaded/re-titled across 3 channels,
# all with distinct video_ids so media_id_seen() alone missed them).
TRAILER_TITLE_SIMILARITY_THRESHOLD = float(
    os.getenv("TRAILER_TITLE_SIMILARITY_THRESHOLD", "0.6")
)
TRAILER_DEDUP_LOOKBACK_DAYS = int(os.getenv("TRAILER_DEDUP_LOOKBACK_DAYS", "30"))

_FEED_NS = "{http://www.w3.org/2005/Atom}"
_YT_NS = "{http://www.youtube.com/xml/schemas/2015}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Connectivity retry — DELIBERATELY BROADER than the other three acquire
# scripts' _with_retry() ─────────────────────────────────────────────────
# rss_news_pull.py / fetch_anilist_trending.py / reddit_pull.py all retry
# ONLY on true network-level blips (DNS, timeout, connection reset) and
# explicitly do NOT retry a genuine 4xx/5xx HTTP status — the reasoning
# there is "a real 404 means a wrong/dead URL, retrying won't help."
#
# That reasoning does NOT hold for this specific endpoint. Confirmed in
# production (see incident note): Aniplex USA's channel ID
# (UCDb0peSmF5rLX7BvuTcJfCw) is verified correct and the channel is
# active with ~1M subscribers — yet a run hit a 404 fetching its RSS
# feed, while Crunchyroll's verified-correct ID hit a 500 in the SAME
# run, while a third channel (TOHO animation) succeeded normally.
# YouTube's public, undocumented /feeds/videos.xml endpoint is evidently
# flaky PER-CHANNEL on its own serving side (caching/edge-node issues,
# not a wrong ID or a real "gone" channel) — this script previously had
# NO retry at all on this call, so any such blip cost that channel its
# entire run with no second attempt, unlike every other acquire script.
#
# This does NOT mean a persistently-404ing channel goes undiagnosed: the
# retry is still BOUNDED (YOUTUBE_TRAILERS_MAX_RETRIES) — a genuinely
# dead/renamed channel still ends up logged as FAILED after exhausting
# retries, same as before, just given a fair shot first instead of
# failing on the very first blip.
YOUTUBE_TRAILERS_MAX_RETRIES = int(os.getenv("YOUTUBE_TRAILERS_MAX_RETRIES", "3"))
YOUTUBE_TRAILERS_RETRY_BACKOFF_SECONDS = float(
    os.getenv("YOUTUBE_TRAILERS_RETRY_BACKOFF_SECONDS", "3"))

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


def _is_retryable_error(e):
    """True for a genuine network-level blip (same check every other
    acquire script uses) OR an HTTP error against THIS specific feed
    endpoint — 5xx (server-side, clearly transient) and 404 (per the
    incident note above, empirically also transient for this endpoint,
    not necessarily a dead channel). Any other HTTP status (e.g. 403
    forbidden, which would mean something more specific like an IP
    block) is NOT retried — this stays a narrow, evidence-based
    broadening, not "retry on anything."."""
    if isinstance(e, _CONNECTIVITY_EXCEPTION_TYPES):
        return True
    if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
        status = e.response.status_code
        if status >= 500 or status == 404:
            return True
    text = str(e).lower()
    return any(kw in text for kw in _CONNECTIVITY_KEYWORDS)


def _with_retry(fn, *args, **kwargs):
    """Runs fn(*args, **kwargs), retrying on _is_retryable_error() with
    linear backoff. Re-raises immediately on a non-retryable error, and
    re-raises the last error if every attempt is exhausted — caller's
    existing try/except around this works exactly as before."""
    last_error = None
    for attempt in range(1, YOUTUBE_TRAILERS_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if not _is_retryable_error(e):
                raise
            last_error = e
            if attempt < YOUTUBE_TRAILERS_MAX_RETRIES:
                wait = YOUTUBE_TRAILERS_RETRY_BACKOFF_SECONDS * attempt
                print(f"    Feed fetch blip ({e}); retrying in {wait:.0f}s "
                      f"({attempt}/{YOUTUBE_TRAILERS_MAX_RETRIES})...")
                time.sleep(wait)
    raise last_error
# -----------------------------


def _slugify(name):
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _load_channels():
    raw = os.getenv("YOUTUBE_TRAILER_CHANNELS", "").strip()
    if not raw:
        return []
    channels = []
    for pair in raw.split(","):
        if "|" not in pair:
            continue
        name, channel_id = pair.split("|", 1)
        name, channel_id = name.strip(), channel_id.strip()
        if name and channel_id:
            channels.append((name, channel_id))
    return channels


def _title_looks_like_trailer(title):
    low = title.lower()
    return any(kw in low for kw in TITLE_KEYWORDS)


# ── Title-similarity dedup ───────────────────────────────────────────────
# Same char-ngram technique db.py's _similarity()/is_duplicate_topic() and
# build_news_digest.py's similarity() already use for caption text — kept
# as a small local copy (same convention build_news_digest.py and
# dupe_check.py already follow) rather than importing db's underscore-
# prefixed helper directly, so this module's dedup logic doesn't depend on
# db.py's private implementation detail staying named/shaped the same way.

def _char_ngrams(text, n=4):
    text = re.sub(r"\s+", "", text.lower())
    return set(text[i:i + n] for i in range(len(text) - n + 1))


def _title_similarity(a, b):
    na, nb = _char_ngrams(a), _char_ngrams(b)
    if not na or not nb:
        return 0.0
    return len(na & nb) / min(len(na), len(nb))


def _recent_trailer_titles(lookback_days=TRAILER_DEDUP_LOOKBACK_DAYS):
    """Titles (== posts.caption_raw, which process_channel() below always
    sets to the raw YouTube title) of every video post downloaded in the
    last `lookback_days` — the cross-RUN half of the dedup check. Bounded
    by lookback_days rather than scanning the whole table forever, since
    this only needs to catch a re-upload of something recent, not
    something from months ago that's long since rotated out of relevance.
    Read once per script run (main() calls this, not process_channel()),
    same 'load once, not per-item' convention generate_captions.py already
    uses for its own insight cache."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    conn = get_connection()
    rows = conn.execute(
        "SELECT caption_raw FROM posts WHERE media_type = 'video' "
        "AND caption_raw IS NOT NULL AND downloaded_at >= ?",
        (cutoff,),
    ).fetchall()
    conn.close()
    return [r["caption_raw"] for r in rows if r["caption_raw"]]


def _is_duplicate_title(title, known_titles, threshold=TRAILER_TITLE_SIMILARITY_THRESHOLD):
    """True if `title` is a near-match (char-ngram similarity >= threshold)
    of anything in `known_titles` — catches the same underlying PV
    re-uploaded/re-titled across different official channels (e.g. a
    studio's own upload vs. Aniplex's vs. a regional distributor's, all
    with different video_ids). `known_titles` is expected to already
    combine both the cross-run history (_recent_trailer_titles()) AND
    every title accepted so far THIS run — see main()/process_channel()
    for how that combined list is threaded through, which is what lets
    this catch 3 re-uploads landing across 3 different channels in the
    SAME run, not just repeats across separate runs."""
    return any(_title_similarity(title, known) >= threshold for known in known_titles)
# -----------------------------


def _validate_proxy(proxy_url):
    """Catches a malformed YT_DLP_PROXY once, with a clear message, instead
    of yt-dlp failing cryptically on every single video ("Port could not be
    cast to integer value as 'port'" is what you get if the host:port
    example from the docstring got pasted in literally instead of being
    replaced with real values). Returns an error string, or None if the
    proxy URL looks structurally sound (this can't confirm the proxy
    actually works — only that it's a well-formed host:port)."""
    try:
        parsed = urlparse(proxy_url)
    except ValueError as e:
        return f"couldn't parse YT_DLP_PROXY={proxy_url!r}: {e}"
    if parsed.scheme not in ("http", "https", "socks4", "socks5"):
        return (f"YT_DLP_PROXY={proxy_url!r} needs an http://, https://, "
                f"socks4://, or socks5:// scheme, got {parsed.scheme!r}")
    if not parsed.hostname or parsed.hostname in ("host", "example.com"):
        return f"YT_DLP_PROXY={proxy_url!r} needs a real hostname/IP, not a placeholder"
    try:
        port = parsed.port
    except ValueError:
        return (f"YT_DLP_PROXY={proxy_url!r} has a non-numeric port — looks like "
                f"the 'host:port' example got pasted in literally instead of "
                f"being replaced with a real host and port number")
    if port is None:
        return f"YT_DLP_PROXY={proxy_url!r} is missing a port number"
    return None


def fetch_channel_feed(channel_id):
    """Returns a list of {video_id, title, link} dicts, newest first.
    Replaced the flaky YouTube RSS XML with yt-dlp's flat-playlist extractor
    which natively handles YouTube's bot-checks and WAF."""
    import yt_dlp
    
    ydl_opts = {
        "extract_flat": True,
        "playlistend": MAX_ITEMS_PER_CHANNEL,
        "quiet": True,
        "no_warnings": True,
    }
    
    if YT_DLP_PROXY:
        ydl_opts["proxy"] = YT_DLP_PROXY
    if YT_DLP_GEO_BYPASS_COUNTRY:
        ydl_opts["geo_bypass_country"] = YT_DLP_GEO_BYPASS_COUNTRY
        
    url = f"https://www.youtube.com/channel/{channel_id}/videos"
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
        if not info or "entries" not in info:
            return []
            
        entries = []
        for entry in info.get("entries", []):
            video_id = entry.get("id")
            title = entry.get("title")
            if video_id and title:
                entries.append({
                    "video_id": video_id,
                    "title": title,
                    "link": f"https://www.youtube.com/watch?v={video_id}",
                })
        return entries



def download_video(url, out_path, max_height=MAX_HEIGHT):
    """Downloads via yt-dlp's Python API. Returns the actual duration in
    seconds (read back from yt-dlp's own extracted info) so the caller can
    apply the duration cap using the REAL value, not a guess. Raises on
    failure — caller treats that as this video's failure, isolated from
    every other video in the batch."""
    import yt_dlp

    ydl_opts = {
        "format": f"bestvideo[height<={max_height}]+bestaudio/best[height<={max_height}]/best",
        "outtmpl": out_path,
        "merge_output_format": "mp4",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
    }
    if YT_DLP_PROXY:
        ydl_opts["proxy"] = YT_DLP_PROXY
    if YT_DLP_GEO_BYPASS_COUNTRY:
        ydl_opts["geo_bypass_country"] = YT_DLP_GEO_BYPASS_COUNTRY
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        return info.get("duration")


def process_channel(db, name, channel_id, downloads_remaining, known_titles):
    """`known_titles` is a list the CALLER owns and mutates in place (via
    .append()) every time a title is accepted here — that's what makes the
    duplicate-title check span across channels within a single run, not
    just within this one channel's own uploads. See main() for where the
    list is created and seeded with cross-run history."""
    handle = _slugify(name)
    source_id = db.get_or_create_source(PLATFORM, handle)

    # Source quality (roadmap item N) — same soft quota reduction
    # rss_news_pull.py applies, see source_quality.py's module docstring.
    # A channel with a poor removal-rate/quality-gate/yield track record
    # gets fewer of its uploads pulled per run, never zero.
    quota_mult = sq.quota_multiplier(PLATFORM, handle)
    effective_max_items = max(1, round(MAX_ITEMS_PER_CHANNEL * quota_mult))

    print(f"\n[{name}] Fetching channel feed...")
    try:
        entries = _with_retry(fetch_channel_feed, channel_id)
    except Exception as e:
        print(f"  FAILED to fetch/parse this channel's feed (after retries): {e}")
        return 0, 0, 0, 0, 0

    entries = entries[:effective_max_items]
    if quota_mult < 1.0:
        print(f"  Source quality quota reduction ACTIVE — pulling "
              f"{effective_max_items} (of normal {MAX_ITEMS_PER_CHANNEL}) this run.")
    print(f"  {len(entries)} recent upload(s) on this channel.")

    inserted = 0
    skipped_dupe = 0
    skipped_not_trailer = 0
    skipped_too_long = 0
    skipped_duplicate_title = 0

    for entry in entries:
        if downloads_remaining <= 0:
            print(f"  Hit TRAILER_MAX_DOWNLOADS_PER_RUN cap ({MAX_DOWNLOADS_PER_RUN}) — "
                  f"stopping here for this run. Remaining candidates on this "
                  f"channel (and any channels after it) are picked up next run.")
            break

        media_id = f"youtube_{entry['video_id']}"
        if db.media_id_seen(media_id):
            skipped_dupe += 1
            continue

        if not _title_looks_like_trailer(entry["title"]):
            skipped_not_trailer += 1
            continue

        # Cross-channel/cross-run title-similarity dedup — checked BEFORE
        # the (expensive, bandwidth-costing) download, so a caught
        # duplicate costs one string comparison, not a wasted yt-dlp pull.
        # See module docstring's CROSS-CHANNEL TITLE DEDUP section for the
        # production incident (Dororo PVs re-uploaded across 3 channels)
        # this specifically closes.
        if _is_duplicate_title(entry["title"], known_titles):
            print(f"  SKIPPED (looks like a re-upload/re-title of an already-"
                  f"seen PV — title similarity check): {entry['title'][:70]}")
            skipped_duplicate_title += 1
            continue

        post_dir = os.path.join(DOWNLOAD_DIR, handle, entry["video_id"])
        os.makedirs(post_dir, exist_ok=True)
        video_path = os.path.join(post_dir, "video.mp4")

        print(f"  Downloading: {entry['title'][:70]}")
        try:
            duration = download_video(entry["link"], video_path)
        except Exception as e:
            print(f"    SKIPPED (download failed): {e}")
            if "not available in your country" in str(e) and not YT_DLP_PROXY:
                print("    HINT: this is a geo-restriction, not a transient "
                      "failure — set YT_DLP_PROXY in .env to a proxy located "
                      "in one of the countries listed above (see this "
                      "script's docstring for details). Skipping for now.")
            continue

        if duration and duration > MAX_DURATION_SECONDS:
            print(f"    SKIPPED — {duration:.0f}s exceeds "
                  f"TRAILER_MAX_DURATION_SECONDS ({MAX_DURATION_SECONDS}s), "
                  f"probably not actually a trailer")
            if os.path.isfile(video_path):
                os.remove(video_path)
            skipped_too_long += 1
            continue

        with open(os.path.join(post_dir, "caption_raw.txt"), "w", encoding="utf-8") as f:
            f.write(entry["title"])
        with open(os.path.join(post_dir, "type.txt"), "w") as f:
            f.write("video")

        post_id = db.insert_post(
            source_id=source_id,
            external_id=entry["video_id"],
            media_type="video",
            caption_raw=entry["title"],
            media_id=media_id,
            thumb_hash=None,
            raw_media_path=video_path,
        )
        if post_id is None:
            continue  # race/rerun — already recorded

        inserted += 1
        downloads_remaining -= 1
        # Record this title so every later channel in THIS run also sees
        # it — this is the line that actually makes the dedup cross-
        # channel-within-a-run, not just cross-run.
        known_titles.append(entry["title"])
        print(f"    -> added ({duration:.0f}s)" if duration else "    -> added")

    return inserted, skipped_dupe, skipped_not_trailer, skipped_too_long, skipped_duplicate_title


def main():
    channels = _load_channels()
    if not channels:
        print("No channels configured. Set YOUTUBE_TRAILER_CHANNELS in .env "
              "first — see this script's docstring for how to find a "
              "channel ID. Nothing to do.")
        return

    try:
        import yt_dlp  # noqa: F401
    except ImportError:
        print("yt-dlp not installed. Run: pip install yt-dlp")
        return

    if YT_DLP_PROXY:
        proxy_error = _validate_proxy(YT_DLP_PROXY)
        if proxy_error:
            print(f"YT_DLP_PROXY is set but invalid — {proxy_error}. "
                  f"Fix it in .env before running (a broken proxy means every "
                  f"single download in this run fails, geo-restricted or not).")
            return

    db = PipelineDB()

    # Seeded with cross-run history, then grown in place by process_channel()
    # as each channel accepts new titles — see process_channel()'s docstring
    # for why this list is threaded through rather than recomputed per
    # channel (recomputing per channel would miss same-run duplicates from
    # channels already processed earlier in this same loop).
    known_titles = _recent_trailer_titles()
    print(f"Loaded {len(known_titles)} recent trailer title(s) from the last "
          f"{TRAILER_DEDUP_LOOKBACK_DAYS} day(s) for duplicate-PV detection "
          f"(TRAILER_TITLE_SIMILARITY_THRESHOLD={TRAILER_TITLE_SIMILARITY_THRESHOLD}).")

    total_inserted = total_dupe = total_not_trailer = total_too_long = 0
    total_duplicate_title = 0
    downloads_remaining = MAX_DOWNLOADS_PER_RUN
    for name, channel_id in channels:
        if downloads_remaining <= 0:
            print(f"\nTRAILER_MAX_DOWNLOADS_PER_RUN cap ({MAX_DOWNLOADS_PER_RUN}) already "
                  f"reached — skipping remaining channels this run, they'll be "
                  f"picked up next time.")
            break
        i, d, nt, tl, dt = process_channel(db, name, channel_id, downloads_remaining, known_titles)
        total_inserted += i
        total_dupe += d
        total_not_trailer += nt
        total_too_long += tl
        total_duplicate_title += dt
        downloads_remaining -= i

    print(f"\nDone. Added {total_inserted} new trailer(s) across {len(channels)} "
          f"channel(s). Skipped {total_dupe} already-seen, "
          f"{total_not_trailer} not trailer-shaped by title, "
          f"{total_too_long} too long to plausibly be a trailer, "
          f"{total_duplicate_title} likely re-uploads/re-titles of an "
          f"already-seen PV.")
    if total_inserted:
        print("\nReminder: these will publish as unlisted by default "
              "(YOUTUBE_PRIVACY_STATUS in publish_youtube.py) — check "
              "YouTube Studio's Copyright tab on these specific uploads "
              "before going public or adding more channels.")


if __name__ == "__main__":
    main()
