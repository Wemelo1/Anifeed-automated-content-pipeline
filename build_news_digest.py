"""
Anime News Digest Builder
--------------------------
Collects image posts from the pipeline database and stitches them into a
single vertical "anime news broadcast" video — one per batch of 3-8 stories.

ARCHITECTURE — the "Editing Brain": creative decisions and rendering are
now separate steps, not tangled together the way one big build_segment()
used to work:

  1. edit_planner.plan_story() — ONE Groq call per story returns
     narration + an emotion label + a 0-100 newsworthiness score, as JSON.
  2. motion_engine.profile_for(emotion) turns that into a concrete plan:
     candidate camera motions, a transition style, a subtitle accent
     color, and a background-music mood.
  3. Phase 2's render loop executes that plan — picks one motion (never
     repeating the previous story's), builds the ffmpeg filter chain via
     motion_engine + transitions + mega_captions, and renders.

This is what makes "camera motion matches the news" (a breaking/hype
story gets shake/dolly_push + a glitch flash; a sad story gets a slow
zoom-out + a plain fade) instead of every image getting the same
Ken-Burns drift the old version hard-coded.

URGENCY (added): a digest built from breaking/multi-corroborated news
has a shelf life measured in days, not the weeks an evergreen digest can
sit in a normal posting queue. _compute_urgency() flags this at build
time (see that function's docstring for the exact criteria) and the flag
is stored on the digests row via db.create_digest(urgent=...).
publish_scheduler.py reads it to decide whether this digest should skip
the normal daily-slot queue and go out at the next available slot
instead — see that module's docstring. This module only ever DECIDES
urgency; it has no publishing/scheduling logic of its own, per
PROJECT_RULES.md Rule 2 (Produce never touches Schedule/Publish concerns).

LEARN STAGE (added — self-learning loop expansion): DIGEST_SPEED_FACTOR
is no longer a plain os.getenv() constant — it's a learned_config param
(see learned_config.py's PARAM_REGISTRY: "news_digest_speed_factor"),
same pattern quality_gate.py already uses for QUALITY_APPROVE_THRESHOLD.
mutator.py does not yet have a rule proposing mutations for it (needs
performance_insights.py to have bucketed digest-level data first — it
now does, via db.digests_with_metrics()), so until a rule is added it
sits at its registry default (1.25) — same value this constant always
defaulted to. The EFFECTIVE value used for a given digest is recorded
into that digest's render_params_json via db.create_digest() — unlike
clips (which accumulate render_params_json across multiple production
stages and need a read-merge-write), a digest only ever goes through ONE
production pass here, so this is written once, directly, no merge
needed. See db.py's render_params_json docstring for the full picture
across clips and digests.

WHAT CHANGED FROM THE PREVIOUS VERSION (bugs fixed + new capability):
  - Zoom is no longer unbounded (old `zoompan=z='zoom+0.0015'` had no
    ceiling — a long narration ended up badly over-cropped by the end of
    its clip). Every motion in motion_engine.py is expressed as a function
    of the frame number with a hard MAX_ZOOM, so it's identical regardless
    of clip length.
  - `fontsdir` is now passed on every subtitle-burning ffmpeg call, not
    just the title card's — the custom font was silently only applying to
    the intro/outro before.
  - generate_caption()/generate_youtube_caption()'s empty-response retry
    (gpt-oss-120b spending its whole token budget on internal reasoning
    and returning nothing) is restored — this was dropped in an earlier
    pass and is a real, previously-fixed production bug.
  - Groq narration calls in Phase 1 now run through a semaphore
    (GROQ_CONCURRENCY) instead of firing all stories at once — an
    unbounded burst risks 429 rate-limiting on Groq's free tier, and a
    429 isn't currently treated as retryable.
  - Camera motion is now variety-aware: pick_motion() refuses to repeat
    the immediately previous story's motion.
  - Long spoken intro/outro are gone. The intro is now a ~0.6s silent
    branded stinger (logo flash + boom SFX if you have one) — first
    story dialogue starts almost immediately. Outro is a single ~1-2s
    "Follow for daily anime news" line instead of a full title card
    read. See NARRATION_PROMPT-equivalent guidance in edit_planner.py
    for the tightened 8-12s-per-story narration target.
  - Sound effects: narration text is scanned for trigger keywords
    (see sfx_engine.py) and, when a matching file exists, mixed in at
    the EXACT timestamp of the triggering word using the same
    word-level TTS timing already used for subtitles.
  - Background music now picks a track by MOOD (music/<mood>.mp3) instead
    of one static bgm.mp3, based on the digest's dominant story emotion —
    falls back to the old single BGM_PATH if no mood folder is set up.
  - Subtitles switched from multi-word karaoke lines to single-word
    "mega captions" (one giant word at a time, pop-in animated).
  - Stories now carry a 0-100 newsworthiness score from the planner;
    anything under STORY_SCORE_THRESHOLD is dropped before it's ever
    rendered — a full TTS+ffmpeg pass is no longer spent on a thin merch
    listing that would've scored low anyway.
  - Fast trend-spike reaction: trend_signal.get_trending_boosts() is
    checked once per run, after Phase 1 data is fetched and before the
    STORY_SCORE_THRESHOLD filter. A story whose franchise is currently
    spiking (matched via the same _franchise_key() heuristic used for
    franchise diversity, fuzzy-matched against known canonical_shows —
    no LLM call) gets its edit_planner newsworthiness score bumped by
    DIGEST_TREND_SCORE_BOOST, capped at 100. This is a same-run-only
    nudge, separate from performance_insights.py's slower, sample-gated
    signal — see trend_signal.py's own module docstring for the "two
    timescales" rationale.
  - DEMAND-RESPONSIVE BATCH SIZING: batch size is no longer a fixed
    MAX_ITEMS target. A wider pool of candidates (DIGEST_SCORE_POOL_SIZE,
    default 2x MAX_ITEMS) is planned/scored in Phase 1 — more than will
    necessarily render — and compute_adaptive_target() then decides how
    many stories to actually attempt, based on how many cleared
    STORY_SCORE_THRESHOLD, clamped to [MIN_ITEMS, MAX_ITEMS]. A thin news
    day no longer gets padded with filler to hit a quota; a loaded day
    can use the full MAX_ITEMS ceiling instead of being truncated at a
    cap that used to be applied BEFORE any story was ever scored. See
    compute_adaptive_target()'s docstring and the "DEMAND-RESPONSIVE
    BATCH SIZING" block in main().
  - URGENCY: a digest built from breaking/multi-corroborated news has a
    shelf life measured in days, not the weeks an evergreen digest can
    sit in a normal posting queue. _compute_urgency() flags this at
    build time (see that function's docstring for the exact criteria)
    and the flag is stored on the digests row via
    db.create_digest(urgent=...). publish_scheduler.py reads it to
    decide whether this digest should skip the normal daily-slot queue
    and go out at the next available slot instead — see that module's
    docstring. This module only ever DECIDES urgency; it has no
    publishing/scheduling logic of its own, per PROJECT_RULES.md Rule 2
    (Produce never touches Schedule/Publish concerns).

WHAT'S DELIBERATELY NOT IN THIS VERSION — B-ROLL MATCHING:
Automatic B-roll (trailer clips alternated with the still image) needs
fetch_youtube_trailers.py's actual save-path/DB-linking convention to
match a downloaded clip to a story by canonical show — that file's
current implementation wasn't available while writing this, and guessing
at its field names/paths would produce exactly the kind of fragile,
never-actually-tested integration this pipeline has hit before (see
fetch_analytics.py's docstring on the get_youtube_service() naming
mismatch). find_broll_clip() below is a real hook point — wire it in
once fetch_youtube_trailers.py's interface is on hand — but it always
returns None right now, so every story still renders from its one still
image, same as before.

STATE: candidate posts are image posts with status='downloaded' in the
pipeline database. Cross-digest topic dedup uses db.is_duplicate_topic().
On success, db.create_digest() atomically records the digest, links its
stories, and flips every used post to status='used_in_digest'.

TTS DELIVERY — THREE LAYERS, TRIED IN ORDER: Fish Audio (free, your
cloned voice, personal/non-commercial only) -> ElevenLabs (paid, no
commercial restriction) -> edge-tts (free, synthetic voice, no key
needed). See the original module history for why this is ONE call per
narration block, not per-sentence: per-sentence synthesis with rate
jitter was tried and reverted — it made delivery MORE erratic, not less.

SETUP:
  pip install edge-tts groq python-dotenv requests
  ffmpeg on PATH (rgbashift filter required for the "glitch" transition —
  standard in any ffmpeg build from the last several years; check with
  `ffmpeg -filters | grep rgbashift` if unsure)

  ASSETS TO GATHER (all optional — everything degrades gracefully if
  missing, exactly like the old bgm.mp3 did):
    Bangers.ttf (or any bold/condensed display font) in the project root
      -> activates the comic-style mega-caption/title-card font. Missing
         just means libass substitutes a default font — never breaks a
         render.
    sfx/ folder with any of: whoosh.mp3, boom.mp3, hit.mp3, pop.mp3,
      click.mp3, bass.mp3, glitch.mp3 (short, ~1s or under, pre-normalized
      volume) -> activates keyword-triggered sound effects AND the
      stinger's boom + transition-open sounds. Missing files just mean
      that specific cue never fires. See sfx_engine.py's docstring.
    music/ folder with any of: breaking.mp3, hype.mp3, dramatic.mp3,
      sad.mp3, funny.mp3 (looping-friendly, several minutes long is fine
      since it's trimmed to the digest's length) -> activates mood-based
      BGM selection. Falls back to a single BGM_PATH env var (old
      behavior) if this folder/mood file isn't found.
  Check the license of anything downloaded before use in monetized
  content — freesound.org (filter CC0), Pixabay, Mixkit are reasonable
  starting points for SFX/BGM.

USAGE:
  python build_news_digest.py
  Run after telegram_pull.py/rss_news_pull.py/fetch_anilist_trending.py.
  Waits until at least MIN_ITEMS (3) are ready.
"""

import os
import re
import time
import random
import socket
import ssl
import base64
import asyncio
import inspect
import json
import shutil
import subprocess
from collections import Counter
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq
import edge_tts

from db import PipelineDB
import db as db_module
from refusal_guard import is_refusal
import motion_engine
import transitions
import mega_captions
import sfx_engine
import edit_planner
import trend_signal
import learned_config as lc
import source_quality as sq
from video_utils import find_horizontal_focus_x, ffmpeg_filter_path

load_dotenv()
lc.init_schema()

# ---------- CONFIG ----------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MODEL = "openai/gpt-oss-120b"

TTS_VOICE = os.getenv("TTS_VOICE", "en-US-AndrewNeural")

FISH_API_KEY = os.getenv("FISH_API_KEY", "").strip()
FISH_VOICE_ID = os.getenv("FISH_VOICE_ID", "").strip()

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "").strip()
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "").strip()
ELEVENLABS_MODEL_ID = os.getenv("ELEVENLABS_MODEL_ID", "eleven_multilingual_v2")
ELEVENLABS_TIMEOUT_SECONDS = float(os.getenv("ELEVENLABS_TIMEOUT_SECONDS", "60"))

FINISHED_DIR = "finished"

MIN_ITEMS = int(os.getenv("DIGEST_MIN_ITEMS", "3"))
# MAX_ITEMS is now a CEILING on how many stories can be rendered, not a
# fixed target — see compute_adaptive_target() / DEMAND-RESPONSIVE BATCH
# SIZING in main(). A day whose pool doesn't produce this many
# well-scoring stories will simply render fewer, not get padded with
# filler to hit this number.
MAX_ITEMS = int(os.getenv("DIGEST_MAX_ITEMS", "12"))
# How many candidates get PLANNED/SCORED (Phase 1) before the adaptive
# batch-size decision is made — deliberately wider than MAX_ITEMS, since
# the whole point of demand-responsive sizing is deciding batch size
# AFTER seeing real scores, not before. Bounded by CANDIDATE_POOL_SIZE
# below (can't score more candidates than find_candidate_posts() ever
# hands back) and by Gemini's ~15 RPM ceiling in practice — each extra
# pooled story adds ~4s to fetch_story_data()'s stagger delay (see that
# function), so a very large pool trades wall-clock time for a better-
# informed sizing decision. Default is 2x MAX_ITEMS or 20, whichever is
# larger, which comfortably covers "today was way better than usual"
# without pooling the entire 60-candidate ceiling every run.
DIGEST_SCORE_POOL_SIZE = int(os.getenv("DIGEST_SCORE_POOL_SIZE", str(max(MAX_ITEMS * 2, 20))))
FPS = 25
OUT_W, OUT_H = 1080, 1920

# Stories scoring below this (edit_planner's 0-100 newsworthiness score)
# are dropped before rendering — a merch listing that would've read as
# thin content no longer burns a full TTS+ffmpeg pass to find that out.
STORY_SCORE_THRESHOLD = int(os.getenv("DIGEST_STORY_SCORE_THRESHOLD", "35"))

# A digest is "urgent" — skips publish_scheduler.py's normal daily queue
# and gets the next available slot instead — if either:
#   (a) its lead story is being corroborated by 2+ channels the same day
#       (build_news_digest.py's own "today's biggest story" signal, see
#       resolve_trending_items()), or
#   (b) ANY story in it scored at or above this threshold on edit_planner's
#       0-100 newsworthiness scale (90 = confirmed release/new season,
#       95 = major trailer or breaking announcement — see edit_planner.py's
#       PLANNER_PROMPT for the full rubric).
# This is a snapshot decision made once at build time, not a live
# recheck — a digest that WAS urgent when built stays urgent even if the
# story cools off by the time it's actually picked up for publishing.
# publish_scheduler.py is the only thing that ever reads this flag; see
# that module's docstring for what "jumping the queue" means in practice.
DIGEST_URGENT_SCORE_THRESHOLD = int(os.getenv("DIGEST_URGENT_SCORE_THRESHOLD", "88"))

# Fast trend-spike reaction (see trend_signal.py's module docstring for the
# full "two timescales" rationale) — applied ONLY to this run's in-memory
# scoring, never written to any config or DB row. A story whose franchise
# is currently spiking (per trend_signal.get_trending_boosts()) gets its
# edit_planner newsworthiness score bumped by this many points, capped at
# 100 — enough to pull a borderline story (just under STORY_SCORE_THRESHOLD)
# back into the digest, and to rank it higher within it, without a spike
# alone being able to force in something edit_planner scored as genuinely
# thin (a merch listing at score=10 still isn't getting to 35 off a +20).
DIGEST_TREND_SCORE_BOOST = int(os.getenv("DIGEST_TREND_SCORE_BOOST", "20"))

# Groq's free tier has an RPM cap — Phase 1 now plans every story
# concurrently, so this bounds how many narration calls are in flight at
# once rather than firing all 8 simultaneously and risking a burst of 429s
# (which _call_groq_with_retry doesn't currently treat as retryable).
GROQ_CONCURRENCY = int(os.getenv("DIGEST_GROQ_CONCURRENCY", "3"))

# LEARNED — was DIGEST_SPEED_FACTOR = float(os.getenv("DIGEST_SPEED_FACTOR", "1.25")).
# See learned_config.py's "news_digest_speed_factor" registry entry and
# this module's LEARN STAGE docstring note above.
DIGEST_SPEED_FACTOR = float(lc.get_param("news_digest_speed_factor"))
DIGEST_MAX_OUTPUT_SECONDS = float(os.getenv("DIGEST_MAX_OUTPUT_SECONDS", "170"))
OUTRO_DURATION_RESERVE_SECONDS = float(os.getenv("DIGEST_OUTRO_RESERVE_SECONDS", "4"))

CANDIDATE_POOL_SIZE = int(os.getenv("DIGEST_CANDIDATE_POOL", "60"))

# Intro is now a near-silent branded flash, not a spoken paragraph — see
# module docstring. Outro is a single short line, not a full title card
# read.
INTRO_TITLE = "ANIFEED"
INTRO_STINGER_SECONDS = float(os.getenv("DIGEST_INTRO_STINGER_SECONDS", "0.6"))
OUTRO_TEXT = "Follow for daily anime news."
OUTRO_TITLE = "FOLLOW"

BATCH_SIMILARITY_THRESHOLD = 0.65

DIGEST_MAX_PER_FRANCHISE = int(os.getenv("DIGEST_MAX_PER_FRANCHISE", "2"))
FRANCHISE_SIMILARITY_THRESHOLD = float(os.getenv("DIGEST_FRANCHISE_SIMILARITY_THRESHOLD", "0.45"))


def pick_bgm(mood):
    """Returns a file path to mix in as background music, or None to skip
    BGM entirely (both are valid — this must never raise or block a
    build). Tries music/<mood>.mp3 first, falls back to the single legacy
    BGM_PATH, then gives up quietly."""
    if mood:
        candidate = os.path.join(MUSIC_DIR, f"{mood}.mp3")
        if os.path.isfile(candidate):
            return candidate
    if os.path.isfile(BGM_PATH):
        return BGM_PATH
    return None


# ── Multi-track BGM by mood ─────────────────────────────────────────────────
MUSIC_DIR = os.getenv("MUSIC_DIR", "music")
BGM_PATH = os.getenv("BGM_PATH", "bgm.mp3")
BGM_VOLUME = float(os.getenv("BGM_VOLUME", "0.08"))


def dominant_mood(plans):
    """Majority-vote music mood across every story actually used in this
    digest (not just planned — see main()'s used_items/used_plans). Empty
    input -> None, which pick_bgm() already treats as 'skip BGM'."""
    moods = [motion_engine.profile_for(p["emotion"])["music_mood"] for p in plans]
    if not moods:
        return None
    return Counter(moods).most_common(1)[0][0]


def _compute_urgency(used_items, used_plans):
    """Returns True if this digest should skip publish_scheduler.py's
    normal daily-slot queue and be released at the next available slot
    instead. See DIGEST_URGENT_SCORE_THRESHOLD above for the exact two
    criteria."""
    if any(p.get("score", 0) >= DIGEST_URGENT_SCORE_THRESHOLD for p in used_plans):
        return True
    if used_items and used_items[0].get("corroboration_count", 1) > 1:
        return True
    return False


def compute_adaptive_target(prepped_stories, min_items=MIN_ITEMS, max_items=MAX_ITEMS):
    """DEMAND-RESPONSIVE BATCH SIZING — decide how many stories to
    actually render based on how many CLEARED STORY_SCORE_THRESHOLD, not
    a fixed count decided before any story was ever scored."""
    qualifying = 0
    for d in prepped_stories:
        try:
            if d.get("success") and d["plan"]["score"] >= STORY_SCORE_THRESHOLD:
                qualifying += 1
        except (KeyError, TypeError):
            continue
    if qualifying == 0:
        return min_items
    return max(min_items, min(qualifying, max_items))


# ── B-roll — hook point only, see module docstring ──────────────────────────

def find_broll_clip(item, canonical_show=None):
    """Returns a path to a pre-downloaded trailer/PV clip matching this
    story's show, or None to render from the still image as usual."""
    return None


# ── Connectivity retry (Groq LLM calls) ─────────────────────────────────────
import requests as _requests

try:
    from groq import APITimeoutError as _GroqAPITimeoutError, APIConnectionError as _GroqAPIConnectionError
    _GROQ_EXCEPTION_TYPES = (_GroqAPITimeoutError, _GroqAPIConnectionError)
except ImportError:
    _GROQ_EXCEPTION_TYPES = ()

_CONNECTIVITY_EXCEPTION_TYPES = (
    _requests.exceptions.ConnectionError,
    _requests.exceptions.Timeout,
    socket.gaierror,
    socket.timeout,
    ConnectionError,
    TimeoutError,
    ssl.SSLError,
) + _GROQ_EXCEPTION_TYPES

_CONNECTIVITY_KEYWORDS = (
    "connection", "network", "getaddrinfo", "name resolution",
    "name or service not known", "timed out", "timeout",
    "unreachable", "temporary failure in name resolution",
    "handshake", "ssl", "eof occurred",
)


def _is_connectivity_error(e):
    if isinstance(e, _CONNECTIVITY_EXCEPTION_TYPES):
        return True
    text = str(e).lower()
    return any(kw in text for kw in _CONNECTIVITY_KEYWORDS)


def _call_groq_with_retry(fn, *args, **kwargs):
    last_error = None
    for attempt in range(1, GROQ_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if not _is_connectivity_error(e):
                raise
            last_error = e
            if attempt < GROQ_MAX_RETRIES:
                wait = GROQ_RETRY_BACKOFF_SECONDS * attempt
                print(f"    Groq call attempt {attempt}/{GROQ_MAX_RETRIES} failed "
                      f"(connectivity: {e}); retrying in {wait:.0f}s...")
                time.sleep(wait)
    raise last_error


GROQ_MAX_RETRIES = int(os.getenv("GROQ_MAX_RETRIES", "3"))
GROQ_RETRY_BACKOFF_SECONDS = float(os.getenv("GROQ_RETRY_BACKOFF_SECONDS", "2"))
# -----------------------------


# ── Finding candidate image posts ────────────────────────────────────────────

def _is_usable_image(path):
    """Returns True if the image has a usable aspect ratio for 9:16 vertical
    video."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height",
             "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return True
        parts = r.stdout.strip().split(",")
        if len(parts) < 2:
            return True
        w, h = int(parts[0]), int(parts[1])
        if h == 0:
            return True
        ratio = w / h
        return ratio <= 1.6
    except Exception:
        return True


def find_candidate_posts(db):
    candidates = db.posts_with_status("downloaded", media_type="image", limit=CANDIDATE_POOL_SIZE)
    found = []
    for post in candidates:
        caption = (post["caption_raw"] or "").strip()
        if not caption:
            continue
        if not post["raw_media_path"] or not os.path.isfile(post["raw_media_path"]):
            continue
        if db.is_duplicate_topic(caption):
            continue
        source = db.get_source(post["source_id"])
        found.append({
            "id": post["id"],
            "channel": source["handle"] if source else "unknown_channel",
            "platform": source["platform"] if source else "unknown",
            "post_id": post["external_id"],
            "image_path": post["raw_media_path"],
            "caption": caption,
        })
    return found


# ── Topic similarity / cross-channel trend detection (unchanged logic) ──────

def char_ngrams(text, n=4):
    text = re.sub(r"\s+", "", text.lower())
    return set(text[i:i+n] for i in range(len(text) - n + 1))


def similarity(a, b):
    na, nb = char_ngrams(a), char_ngrams(b)
    if not na or not nb:
        return 0.0
    return len(na & nb) / min(len(na), len(nb))


def cluster_candidates(items, threshold=BATCH_SIMILARITY_THRESHOLD):
    clusters = []
    for item in items:
        placed = False
        for cluster in clusters:
            if any(similarity(item["caption"], other["caption"]) >= threshold
                   for other in cluster):
                cluster.append(item)
                placed = True
                break
        if not placed:
            clusters.append([item])
    return clusters


def pick_representative(cluster):
    return max(cluster, key=lambda it: len(it["caption"]))


def corroborating_channels(cluster):
    return sorted({it["channel"] for it in cluster})


def resolve_trending_items(anime_items):
    clusters = cluster_candidates(anime_items)
    clusters.sort(key=lambda c: len(corroborating_channels(c)), reverse=True)

    resolved = []
    for cluster in clusters:
        rep = pick_representative(cluster)
        channels = corroborating_channels(cluster)
        rep = dict(rep)
        rep["corroboration_count"] = len(channels)
        rep["corroborating_channels"] = channels
        resolved.append(rep)
        if len(channels) > 1:
            print(f"    Trending: \"{rep['caption'][:60]}...\" reported by "
                  f"{len(channels)} channels ({', '.join(channels)})")
    return resolved


# ── Franchise diversity cap ──────────────────────────────────────────────────

def _franchise_key(caption):
    """Cheap, no-API-call heuristic for 'what show is this probably about'."""
    caption = (caption or "").strip()
    if not caption:
        return ""

    anilist_marker = re.search(r"\s+is currently the #", caption)
    if anilist_marker:
        return caption[:anilist_marker.start()].strip()[:150]

    match = re.search(r"[.!?]", caption)
    key = caption[:match.start()] if match else caption[:150]
    return key.strip()[:150]


def enforce_franchise_diversity(items, max_per_franchise=DIGEST_MAX_PER_FRANCHISE,
                                 threshold=FRANCHISE_SIMILARITY_THRESHOLD):
    """Walks `items` in their existing priority order and keeps every item
    UNLESS accepting it would push some already-accepted franchise group
    over `max_per_franchise`."""
    if max_per_franchise <= 0 or not items:
        return items

    kept = []
    group_keys = []
    group_counts = []
    skipped = 0

    for item in items:
        key = _franchise_key(item["caption"])
        if not key:
            kept.append(item)
            continue

        matched_group = None
        for gi, gkey in enumerate(group_keys):
            if similarity(key, gkey) >= threshold:
                matched_group = gi
                break

        if matched_group is None:
            group_keys.append(key)
            group_counts.append(1)
            kept.append(item)
        elif group_counts[matched_group] < max_per_franchise:
            group_counts[matched_group] += 1
            kept.append(item)
        else:
            skipped += 1
            print(f"    Franchise diversity: skipping \"{item['caption'][:60]}...\" "
                  f"— already have {max_per_franchise} story/stories about this "
                  f"show in this digest (a future digest can still pick it up).")

    if skipped:
        print(f"    Franchise diversity cap (max {max_per_franchise}/show): "
              f"{skipped} story/stories held back this round, {len(kept)} remain eligible.")
    return kept


# ── Anime content classifier (unchanged) ────────────────────────────────────

CLEARLY_NOT_ANIME_KEYWORDS = [
    "buy now", "discount", "sale", "shop now", "promo code", "click here",
    "follow us on instagram", "giveaway", "sponsor", "ad:", "advertisement",
    "link in bio", "dm us", "coupon", "limited offer", "free shipping",
    "subscribe to our", "check out our", "available on amazon",
    "discover channels", "discover groups", "new telegram search",
    "telegram search engine", "search engine for telegram",
    "join our channel", "join our group", "add this bot", "add our bot",
]


def is_anime_content(client, caption, channel=""):
    caption_low = caption.lower()
    if any(kw in caption_low for kw in CLEARLY_NOT_ANIME_KEYWORDS):
        return False
    if len(caption.strip()) < 20:
        return True
    try:
        response = _call_groq_with_retry(
            client.chat.completions.create,
            model=MODEL,
            messages=[{"role": "user", "content":
                f"Is this text specifically NEWS about an anime, manga, or Japanese "
                f"animation — e.g. a series, movie, character, episode, trailer, "
                f"merchandise, or event? Answer NO for anything else, even if it's "
                f"posted inside an anime channel or mentions anime in passing — "
                f"this includes app/bot/tool promotions, channel-discovery services, "
                f"unrelated tech announcements, or generic community posts. "
                f"The text may be written in Japanese — Japanese text is NOT evidence "
                f"against it being anime news, but it must still be genuine news "
                f"content, not a promotion. Reply with only YES or NO.\n\n"
                f"Text: {caption[:300]}"}],
            temperature=0.1,
            max_tokens=5,
            reasoning_effort="low",
        )
        result = (response.choices[0].message.content or "").strip().upper()
        return not result.startswith("NO")
    except Exception:
        return True


# ── Web search for extra context (unchanged) ─────────────────────────────────

def search_context(query, max_results=3):
    try:
        resp = _requests.post(
            "https://html.duckduckgo.com/html/",
            data={"q": f"{query} anime"},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=15,
        )
        resp.raise_for_status()
        snippets = re.findall(
            r'class="result__snippet"[^>]*>(.*?)</a>', resp.text, re.S
        )
        clean = []
        for s in snippets[:max_results]:
            s = re.sub(r"<[^>]+>", "", s)
            s = re.sub(r"\s+", " ", s).strip()
            if s:
                clean.append(s)
        return " ".join(clean)[:800]
    except Exception as e:
        print(f"    Web search skipped: {e}")
        return ""


def _lead_story_note(items):
    if not items:
        return ""
    lead = items[0]
    count = lead.get("corroboration_count", 1)
    if count <= 1:
        return ""
    return (
        f"\n\nThe lead story — \"{lead['caption'][:80]}\" — is being reported "
        f"by {count} different channels simultaneously, making it today's "
        f"biggest story. Lead with it if it fits naturally."
    )


def _strip_markdown(text):
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__(.+?)__", r"\1", text)
    text = re.sub(r"(?<!\w)\*(.+?)\*(?!\w)", r"\1", text)
    text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"\1", text)
    text = re.sub(r"`(.+?)`", r"\1", text)
    text = re.sub(r"^#{1,6}\s+", "", text)
    text = re.sub(r"^[\*_~`\s]+|[\*_~`\s]+$", "", text)
    return text.strip()


def generate_caption(client, items):
    titles = "\n".join(f"- {item['caption'][:80]}" for item in items)
    prompt = (
        f"Write a punchy TikTok caption for an anime news roundup covering:\n{titles}\n\n"
        f"Under 120 chars + 3-5 hashtags. Broadcast teaser tone. Caption + hashtags only."
        f"{_lead_story_note(items)}"
    )
    response = _call_groq_with_retry(
        client.chat.completions.create,
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.9,
        max_tokens=800,
        reasoning_effort="low",
    )
    raw = _strip_markdown((response.choices[0].message.content or "").strip())

    if raw and is_refusal(raw):
        print(f"    WARNING: TikTok caption looks like a refusal — discarding: {raw[:80]!r}")
        raw = ""
    if not raw:
        print("    WARNING: TikTok caption came back empty — retrying with more headroom")
        try:
            retry = _call_groq_with_retry(
                client.chat.completions.create,
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.9,
                max_tokens=1500,
                reasoning_effort="low",
            )
            raw = _strip_markdown((retry.choices[0].message.content or "").strip())
        except Exception as e:
            print(f"    WARNING: retry also failed: {e}")

    if not raw:
        first_title = _strip_markdown(items[0]["caption"][:80]) if items else "Anime News"
        raw = f"{first_title} + more anime news! #anime #animenews #animetiktok"

    return raw


def generate_youtube_caption(client, items):
    titles = "\n".join(f"- {item['caption'][:80]}" for item in items)
    prompt = (
        f"Write YouTube Shorts metadata for an anime news roundup covering:\n{titles}\n\n"
        f"LINE 1 — Title: SEO-friendly, max 60 chars, include key anime names naturally\n"
        f"LINE 2 — Description: 2-3 informative sentences covering what's in the video\n"
        f"LINE 3 — Hashtags: 3-5 tags mixing broad (#Anime #AnimeFan) with specific show names\n\n"
        f"Output ONLY these 3 lines in order, nothing else. No labels like 'Title:' or 'LINE 1' "
        f"in the output itself — just the raw title text, then raw description text, "
        f"then raw hashtags, one per line."
        f"{_lead_story_note(items)}"
    )
    response = _call_groq_with_retry(
        client.chat.completions.create,
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=2000,
        reasoning_effort="low",
    )
    raw = (response.choices[0].message.content or "").strip()

    if raw and is_refusal(raw):
        print(f"    WARNING: Youtube caption looks like a refusal — discarding: {raw[:80]!r}")
        raw = ""
    if not raw:
        print("    WARNING: YouTube caption came back empty — retrying with more headroom")
        try:
            retry = _call_groq_with_retry(
                client.chat.completions.create,
                model=MODEL,
                messages=[{"role": "user", "content":
                    f"Write YouTube Shorts metadata for an anime news roundup covering:\n{titles}\n\n"
                    f"Line 1: title (max 60 chars). Line 2: 2-3 sentence description. "
                    f"Line 3: 3-5 hashtags. Output ONLY those 3 lines, no labels."}],
                temperature=0.7,
                max_tokens=2000,
                reasoning_effort="low",
            )
            raw = (retry.choices[0].message.content or "").strip()
        except Exception as e:
            print(f"    WARNING: retry also failed: {e}")

    if not raw:
        first_title = _strip_markdown(items[0]["caption"][:60]) if items else "Anime News Roundup"
        return f"{first_title}\nToday's anime news roundup.\n#Anime #AnimeNews"

    label_prefix = re.compile(
        r"^\s*(line\s*\d+\s*[—\-:]*\s*)?(title|description|hashtags)\s*[:\-—]\s*",
        re.IGNORECASE,
    )
    lines = []
    for line in raw.splitlines():
        line = _strip_markdown(line.strip())
        if not line:
            continue
        line = label_prefix.sub("", line).strip()
        if line:
            lines.append(line)

    if not lines:
        return raw

    if len(lines) >= 3:
        title = lines[0]
        hashtags = lines[-1]
        description = " ".join(lines[1:-1])
    elif len(lines) == 2:
        title, description, hashtags = lines[0], lines[1], ""
    else:
        title, description, hashtags = lines[0], "", ""

    return f"{title}\n{description}\n{hashtags}".strip()


# ── Text-to-speech (unchanged from previous version) ─────────────────────────

def _tts_fish(text, audio_path):
    try:
        from fishaudio import FishAudio
        from fishaudio.utils import save as _fish_save
    except ImportError:
        raise RuntimeError("fish-audio-sdk not installed — run: pip install fish-audio-sdk")
    client = FishAudio(api_key=FISH_API_KEY)
    audio = client.tts.convert(text=text, reference_id=FISH_VOICE_ID)
    _fish_save(audio, audio_path)
    return []


def _words_from_char_alignment(alignment):
    chars = alignment.get("characters") or []
    starts = alignment.get("character_start_times_seconds") or []
    ends = alignment.get("character_end_times_seconds") or []
    words = []
    cur_chars, cur_start, cur_end = [], None, None
    for ch, st, en in zip(chars, starts, ends):
        if ch.isspace():
            if cur_chars:
                words.append({"word": "".join(cur_chars), "start": cur_start, "end": cur_end})
                cur_chars, cur_start = [], None
            continue
        if cur_start is None:
            cur_start = st
        cur_chars.append(ch)
        cur_end = en
    if cur_chars:
        words.append({"word": "".join(cur_chars), "start": cur_start, "end": cur_end})
    return words


def _tts_elevenlabs(text, audio_path):
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}/with-timestamps"
    resp = _requests.post(
        url,
        headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
        json={"text": text, "model_id": ELEVENLABS_MODEL_ID},
        timeout=ELEVENLABS_TIMEOUT_SECONDS,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"ElevenLabs TTS failed: {resp.status_code} {resp.text[:400]}")
    data = resp.json()
    audio_bytes = base64.b64decode(data["audio_base64"])
    with open(audio_path, "wb") as f:
        f.write(audio_bytes)
    alignment = data.get("alignment") or {}
    return _words_from_char_alignment(alignment)


async def _tts_edge(text, audio_path):
    communicate = edge_tts.Communicate(text, TTS_VOICE, boundary="WordBoundary")
    words = []
    with open(audio_path, "wb") as f:
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                f.write(chunk["data"])
            elif chunk["type"] == "WordBoundary":
                words.append({
                    "word": chunk["text"],
                    "start": chunk["offset"] / 1e7,
                    "end": (chunk["offset"] + chunk["duration"]) / 1e7,
                })
    return words


TTS_MAX_RETRIES = int(os.getenv("TTS_MAX_RETRIES", "3"))
TTS_RETRY_BACKOFF_SECONDS = float(os.getenv("TTS_RETRY_BACKOFF_SECONDS", "2"))


async def _call_with_retry(engine_name, fn, *args):
    last_error = None
    for attempt in range(1, TTS_MAX_RETRIES + 1):
        try:
            if inspect.iscoroutinefunction(fn):
                return await fn(*args)
            return fn(*args)
        except Exception as e:
            last_error = e
            if attempt < TTS_MAX_RETRIES:
                wait = TTS_RETRY_BACKOFF_SECONDS * attempt
                print(f"    {engine_name} attempt {attempt}/{TTS_MAX_RETRIES} failed "
                      f"({e}); retrying in {wait:.0f}s...")
                await asyncio.sleep(wait)
    raise last_error


async def tts(text, audio_path):
    if FISH_API_KEY and FISH_VOICE_ID:
        try:
            return await _call_with_retry("Fish Audio", _tts_fish, text, audio_path)
        except Exception as e:
            print(f"    WARNING: Fish Audio TTS failed after retries, trying next option: {e}")
    if ELEVENLABS_API_KEY and ELEVENLABS_VOICE_ID:
        try:
            return await _call_with_retry("ElevenLabs", _tts_elevenlabs, text, audio_path)
        except Exception as e:
            print(f"    WARNING: ElevenLabs TTS failed after retries, falling back to edge-tts: {e}")
    return await _call_with_retry("edge-tts", _tts_edge, text, audio_path)


def _ts(sec):
    ms = int(round((sec % 1) * 1000))
    s = int(sec) % 60
    m = (int(sec) // 60) % 60
    h = int(sec) // 3600
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def make_static_srt(text, duration):
    return f"1\n{_ts(0)} --> {_ts(duration)}\n{text}\n\n"


# ── ffmpeg helpers ────────────────────────────────────────────────────────────

FFMPEG_ERROR_LOG = os.path.abspath("ffmpeg_errors.log")


def _run_ffmpeg_logged(cmd, context):
    """Runs `cmd`, and on failure appends the FULL stderr to FFMPEG_ERROR_LOG
    before raising."""
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        with open(FFMPEG_ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(
                f"\n{'=' * 70}\n{datetime.now().isoformat()} — {context}\n"
                f"CMD: {' '.join(cmd)}\n\nSTDERR:\n{r.stderr}\n"
            )
        raise RuntimeError(
            f"{context} failed — FULL error logged to {FFMPEG_ERROR_LOG} "
            f"(this file is kept even after temp files are cleaned up). "
            f"Last part of the error:\n{r.stderr[-800:]}"
        )
    return r


def audio_duration(path):
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", path],
        capture_output=True, text=True
    )
    return float(r.stdout.strip())


def write_digest_sync_sidecar(out_dir, expected_final_duration):
    """Writes subtitle_sync.json next to video.mp4, recording the digest's
    EXPECTED final duration."""
    try:
        with open(os.path.join(out_dir, "subtitle_sync.json"), "w", encoding="utf-8") as f:
            json.dump({
                "expected_final_duration": expected_final_duration,
                "source": "build_news_digest",
            }, f)
    except OSError as e:
        print(f"    NOTE: couldn't write subtitle_sync.json (non-fatal, "
              f"quality_gate.py will just skip the timing check for this "
              f"digest): {e}")


TITLE_STYLE = (
    f"PlayResX=1080,PlayResY=1920,"
    f"FontName={mega_captions.DEFAULT_FONT},FontSize=78,"
    f"PrimaryColour=&H000000FF,OutlineColour=&H00FFFFFF,"
    f"BorderStyle=1,Outline=5,Shadow=0,Bold=1,Spacing=2,"
    f"Alignment=8,MarginV=220"
)

_FONTSDIR = ffmpeg_filter_path(os.path.abspath("."))  # already quoted/escaped


def get_image_dimensions(path):
    """Returns (width, height) from ffprobe, or (0, 0) on failure."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height",
             "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return 0, 0
        parts = r.stdout.strip().split(",")
        if len(parts) < 2:
            return 0, 0
        return int(parts[0]), int(parts[1])
    except Exception:
        return 0, 0


def is_wide_image(path):
    """True if the image's aspect ratio is wider than WIDE_IMAGE_RATIO_THRESHOLD."""
    w, h = get_image_dimensions(path)
    if h == 0:
        return False
    return (w / h) > WIDE_IMAGE_RATIO_THRESHOLD


PAN_MAX_RATIO = float(os.getenv("PAN_MAX_RATIO", "3.0"))
PAN_MIN_HEIGHT_PX = int(os.getenv("PAN_MIN_HEIGHT_PX", "480"))
WIDE_IMAGE_RATIO_THRESHOLD = 1.6
PAN_ARRIVAL_FRACTION = float(os.getenv("PAN_ARRIVAL_FRACTION", "0.55"))
PAN_DRIFT_SPAN_FRACTION = float(os.getenv("PAN_DRIFT_SPAN_FRACTION", "0.35"))


def classify_wide_treatment(path):
    """Returns 'pan', 'blur-bg', or 'normal' for `path`."""
    w, h = get_image_dimensions(path)
    if h == 0 or w == 0:
        return "blur-bg"
    ratio = w / h
    if ratio <= WIDE_IMAGE_RATIO_THRESHOLD:
        return "normal"
    if ratio > PAN_MAX_RATIO or h < PAN_MIN_HEIGHT_PX:
        return "blur-bg"
    return "pan"


def build_wide_pan_segment(image_path, audio_path, ass_path, out_path,
                            duration, transition, pan_direction="left",
                            pan_style="linear", focus_x=0.5):
    """Panorama pan across a wide image, built AROUND the image's actual
    content instead of blindly sweeping the full width on a fixed timer."""
    frames = max(int(duration * FPS), 1)
    safe_ass = ffmpeg_filter_path(os.path.abspath(ass_path))
    has_subs = os.path.exists(ass_path) and os.path.getsize(ass_path) > 0

    focus_x = min(max(focus_x, 0.0), 1.0)

    if pan_style == "drift":
        span_frac = min(PAN_DRIFT_SPAN_FRACTION, 1.0)
        lo_frac = min(max(focus_x - span_frac / 2, 0.0), 1.0 - span_frac)
        hi_frac = lo_frac + span_frac
        t = f"(n/{frames})"
        if pan_direction == "right":
            crop_x = f"(iw-{OUT_W})*({hi_frac:.5f}-({hi_frac:.5f}-{lo_frac:.5f})*{t})"
        else:
            crop_x = f"(iw-{OUT_W})*({lo_frac:.5f}+({hi_frac:.5f}-{lo_frac:.5f})*{t})"
    else:
        arrival_frames = max(int(frames * PAN_ARRIVAL_FRACTION), 1)
        progress = f"min(1,(n/{arrival_frames}))"
        if pan_style == "ease":
            eased = f"(1-(1-{progress})*(1-{progress}))"
        else:
            eased = progress

        start_frac = 0.0 if pan_direction == "left" else 1.0
        crop_x = f"(iw-{OUT_W})*({start_frac:.5f}+({focus_x:.5f}-{start_frac:.5f})*{eased})"

    transition_vf = transitions.build_transition_vf(transition)
    sub_vf = f",subtitles={safe_ass}:fontsdir={_FONTSDIR}" if has_subs else ""

    vf = (
        f"scale=-2:{OUT_H},"
        f"crop={OUT_W}:{OUT_H}:'{crop_x}':0"
        f"{transition_vf}"
        f",fps={FPS}"
        f"{sub_vf}"
    )

    _run_ffmpeg_logged([
        "ffmpeg", "-y",
        "-loop", "1", "-i", image_path,
        "-i", audio_path,
        "-vf", vf,
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
        "-c:a", "aac", "-ar", "44100", "-ac", "2",
        "-t", str(duration + 0.4),
        "-shortest", out_path,
    ], context=f"build_wide_pan_segment({out_path})")


def build_blur_bg_segment(image_path, audio_path, ass_path, out_path,
                           duration, motion, transition):
    """Blurred background treatment for wide images."""
    frames = max(int(duration * FPS), 1)
    safe_ass = ffmpeg_filter_path(os.path.abspath(ass_path))
    has_subs = os.path.exists(ass_path) and os.path.getsize(ass_path) > 0

    motion_vf = motion_engine.build_motion_vf(motion, frames, OUT_W, OUT_H)
    transition_vf = transitions.build_transition_vf(transition)
    sub_vf = f",subtitles={safe_ass}:fontsdir={_FONTSDIR}" if has_subs else ""

    filter_complex = (
        f"[0:v]scale={OUT_W}:{OUT_H}:force_original_aspect_ratio=increase,"
        f"crop={OUT_W}:{OUT_H},gblur=sigma=30[bg];"
        f"[0:v]scale={OUT_W}:{OUT_H}:force_original_aspect_ratio=decrease,"
        f"pad={OUT_W}:{OUT_H}:(ow-iw)/2:(oh-ih)/2:color=black@0[fg];"
        f"[bg][fg]overlay=0:0,"
        f"{motion_vf}"
        f"{transition_vf}"
        f",fps={FPS}"
        f"{sub_vf}[out]"
    )

    _run_ffmpeg_logged([
        "ffmpeg", "-y",
        "-loop", "1", "-i", image_path,
        "-i", audio_path,
        "-filter_complex", filter_complex,
        "-map", "[out]", "-map", "1:a",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
        "-c:a", "aac", "-ar", "44100", "-ac", "2",
        "-t", str(duration + 0.4),
        "-shortest", out_path,
    ], context=f"build_blur_bg_segment({out_path})")


def build_image_segment(image_path, audio_path, ass_path, out_path, duration, motion, transition):
    """Renders one story clip: motion_engine picks the camera move,
    transitions adds the opening-frame effect, mega_captions burns the
    word-by-word subtitles."""
    frames = max(int(duration * FPS), 1)
    safe_ass = ffmpeg_filter_path(os.path.abspath(ass_path))
    has_subs = os.path.exists(ass_path) and os.path.getsize(ass_path) > 0

    vf = f"scale={OUT_W}:{OUT_H}:force_original_aspect_ratio=increase,crop={OUT_W}:{OUT_H},"
    vf += motion_engine.build_motion_vf(motion, frames, OUT_W, OUT_H)
    vf += transitions.build_transition_vf(transition)
    vf += f",fps={FPS}"
    if has_subs:
        vf += f",subtitles={safe_ass}:fontsdir={_FONTSDIR}"

    _run_ffmpeg_logged([
        "ffmpeg", "-y",
        "-loop", "1", "-i", image_path,
        "-i", audio_path,
        "-vf", vf,
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
        "-c:a", "aac", "-ar", "44100", "-ac", "2",
        "-t", str(duration + 0.4),
        "-shortest", out_path,
    ], context=f"build_image_segment({out_path})")


def build_black_segment(audio_path, ass_path, out_path, duration, motion, transition):
    black_img = out_path + "_black.jpg"
    subprocess.run([
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"color=black:s={OUT_W}x{OUT_H}",
        "-frames:v", "1", black_img,
    ], capture_output=True)
    try:
        build_image_segment(black_img, audio_path, ass_path, out_path, duration, motion, transition)
    finally:
        if os.path.exists(black_img):
            os.remove(black_img)


def build_titlecard_segment(image_path, audio_path, ass_path, out_path, duration, title_text):
    """Outro card only now."""
    frames = max(int(duration * FPS), 1)
    has_dialogue_subs = os.path.exists(ass_path) and os.path.getsize(ass_path) > 0
    safe_dialogue_ass = ffmpeg_filter_path(os.path.abspath(ass_path))

    title_srt_path = out_path + "_title.srt"
    with open(title_srt_path, "w", encoding="utf-8") as f:
        f.write(make_static_srt(title_text, duration))
    safe_title_srt = ffmpeg_filter_path(os.path.abspath(title_srt_path))

    def _vf_for(has_subs):
        vf = (
            f"scale={OUT_W}:{OUT_H}:force_original_aspect_ratio=increase,"
            f"crop={OUT_W}:{OUT_H},"
            f"gblur=sigma=18,"
            f"eq=brightness=-0.18,"
            f"zoompan=z='min(1.15,1+0.15*on/{frames})':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
            f"d={frames}:s={OUT_W}x{OUT_H},"
            f"fps={FPS},"
            f"subtitles={safe_title_srt}:fontsdir={_FONTSDIR}:force_style='{TITLE_STYLE}'"
        )
        if has_subs:
            vf += f",subtitles={safe_dialogue_ass}:fontsdir={_FONTSDIR}"
        return vf

    def _render(bg_path):
        return subprocess.run([
            "ffmpeg", "-y",
            "-loop", "1", "-i", bg_path,
            "-i", audio_path,
            "-vf", _vf_for(has_dialogue_subs),
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-c:a", "aac", "-ar", "44100", "-ac", "2",
            "-t", str(duration + 0.4),
            "-shortest", out_path,
        ], capture_output=True, text=True)

    def _log_and_raise(r, context):
        with open(FFMPEG_ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'=' * 70}\n{datetime.now().isoformat()} — {context}\n\nSTDERR:\n{r.stderr}\n")
        raise RuntimeError(
            f"{context} failed — FULL error logged to {FFMPEG_ERROR_LOG}. "
            f"Last part of the error:\n{r.stderr[-800:]}"
        )

    fallback_bg_path = None
    try:
        r = _render(image_path)
        if r.returncode != 0:
            print(f"    WARNING: title card background image failed to decode "
                  f"({r.stderr.strip()[-300:]!r}) — falling back to a plain "
                  f"background color for this segment")
            fallback_bg_path = out_path + "_fallback_bg.jpg"
            gen = subprocess.run([
                "ffmpeg", "-y", "-f", "lavfi",
                "-i", f"color=c=0x1a1a2e:s={OUT_W}x{OUT_H}",
                "-frames:v", "1", fallback_bg_path,
            ], capture_output=True, text=True)
            if gen.returncode != 0 or not os.path.isfile(fallback_bg_path):
                _log_and_raise(gen, f"build_titlecard_segment fallback-bg-gen({out_path})")
            r = _render(fallback_bg_path)
        if r.returncode != 0:
            _log_and_raise(r, f"build_titlecard_segment({out_path})")
    finally:
        if os.path.exists(title_srt_path):
            os.remove(title_srt_path)
        if fallback_bg_path and os.path.exists(fallback_bg_path):
            os.remove(fallback_bg_path)


def build_stinger_segment(bg_image_path, out_path, work_dir, duration=INTRO_STINGER_SECONDS):
    """The new intro: a near-silent branded flash. Falls back to a plain
    color card if `bg_image_path` is missing/fails to decode."""
    frames = max(int(duration * FPS), 1)
    ass_path = os.path.join(work_dir, "_stinger.ass")
    with open(ass_path, "w", encoding="utf-8") as f:
        f.write(mega_captions.make_mega_ass(
            [{"word": INTRO_TITLE, "start": 0.0, "end": duration}],
            accent_color="&H00FFFFFF", font_size=170,
        ))
    safe_ass = ffmpeg_filter_path(os.path.abspath(ass_path))

    vf = (
        f"scale={OUT_W}:{OUT_H}:force_original_aspect_ratio=increase,"
        f"crop={OUT_W}:{OUT_H},gblur=sigma=14,eq=brightness=-0.22,"
        f"zoompan=z='1.05+0.05*on/{frames}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
        f"d={frames}:s={OUT_W}x{OUT_H},fps={FPS},"
        f"subtitles={safe_ass}:fontsdir={_FONTSDIR}"
    )

    boom_path = os.path.join(sfx_engine.SFX_DIR, "boom.mp3")
    has_boom = os.path.isfile(boom_path)

    cmd = ["ffmpeg", "-y", "-loop", "1", "-i", bg_image_path]
    if has_boom:
        cmd += ["-i", boom_path]
    else:
        cmd += ["-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo"]
    cmd += [
        "-vf", vf, "-t", str(duration),
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
        "-c:a", "aac", "-ar", "44100", "-ac", "2", "-shortest", out_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    os.remove(ass_path)
    if r.returncode != 0:
        with open(FFMPEG_ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'=' * 70}\n{datetime.now().isoformat()} — build_stinger_segment({out_path})\n\nSTDERR:\n{r.stderr}\n")
        raise RuntimeError(
            f"build_stinger_segment failed — FULL error logged to {FFMPEG_ERROR_LOG}. "
            f"Last part of the error:\n{r.stderr[-800:]}"
        )


def speed_up_video(input_path, output_path, speed, bgm_path=None):
    """Speeds up the stitched raw video, and mixes in `bgm_path` if given."""
    if bgm_path and os.path.isfile(bgm_path):
        filter_complex = (
            f"[0:v]setpts=PTS/{speed}[v];"
            f"[0:a]atempo={speed}[voice];"
            f"[1:a]volume={BGM_VOLUME}[bg];"
            f"[voice][bg]amix=inputs=2:duration=first:dropout_transition=0[a]"
        )
        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-stream_loop", "-1", "-i", bgm_path,
            "-filter_complex", filter_complex,
            "-map", "[v]", "-map", "[a]",
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", output_path,
        ]
    else:
        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-filter:v", f"setpts=PTS/{speed}",
            "-filter:a", f"atempo={speed}",
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", output_path,
        ]
    _run_ffmpeg_logged(cmd, context=f"speed_up_video({output_path}, bgm={bgm_path})")


def concat_videos(segment_paths, out_path):
    list_file = out_path + "_list.txt"
    with open(list_file, "w") as f:
        for p in segment_paths:
            abs_p = os.path.abspath(p).replace(chr(92), '/')
            f.write(f"file '{abs_p}'\n")
    try:
        with open(list_file, "r") as f:
            list_contents = f.read()
        _run_ffmpeg_logged([
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0", "-i", list_file,
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", out_path,
        ], context=f"concat_videos({out_path}, {len(segment_paths)} segments)\nSEGMENT LIST:\n{list_contents}")
    finally:
        os.remove(list_file)


# ── Phase 1: concurrent data fetch (search + plan + TTS + SFX) ─────────────

_groq_semaphore = None  # created inside main() so it binds to the running loop


async def fetch_story_data(i, item, client, temp_dir):
    """Everything about a story that DOESN'T need to happen in ffmpeg's
    strict sequential order."""
    try:
        await asyncio.sleep((i - 1) * 4)

        print(f"    [{i}] Searching web for context...")
        context = search_context(item["caption"][:120])

        print(f"    [{i}] Planning (narration + emotion + score via Gemini)...")
        plan = await asyncio.to_thread(
            edit_planner.plan_story,
            item["caption"], context, item.get("corroboration_count", 1),
        )

        audio_path = os.path.join(temp_dir, f"{i:02d}_audio.mp3")
        print(f"    [{i}] Generating voiceover...")
        words = await tts(plan["narration"], audio_path)

        if not words:
            dur = audio_duration(audio_path)
            raw_words = plan["narration"].split()
            if raw_words:
                total_chars = sum(len(w) for w in raw_words) or 1
                t, words = 0.0, []
                for w in raw_words:
                    wdur = dur * (len(w) / total_chars)
                    words.append({"word": w, "start": t, "end": t + wdur})
                    t += wdur
            else:
                words = [{"word": plan["narration"], "start": 0.0, "end": dur}]

        profile = motion_engine.profile_for(plan["emotion"])
        keyword_cues = sfx_engine.find_cues(plan["narration"], words)
        transition_sfx = {
            "whoosh": "whoosh.mp3", "bounce": "pop.mp3", "glitch": "glitch.mp3",
        }.get(profile["transition"])

        cues = []
        if transition_sfx:
            cues.append((transition_sfx, 0.0))
        seen = {transition_sfx} if transition_sfx else set()
        for f, ts in keyword_cues[:sfx_engine.MAX_CUES_PER_STORY]:
            if f not in seen:
                cues.append((f, ts))
                seen.add(f)
        cues = cues[:sfx_engine.MAX_CUES_PER_STORY + 1]

        final_audio = sfx_engine.mix_cues(audio_path, cues, temp_dir)

        if final_audio == audio_path:
            norm_path = os.path.join(temp_dir, f"{i:02d}_audio_norm.mp3")
            r = subprocess.run([
                "ffmpeg", "-y", "-i", audio_path,
                "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
                norm_path,
            ], capture_output=True, text=True)
            if r.returncode == 0:
                final_audio = norm_path

        ass_path = os.path.join(temp_dir, f"{i:02d}_subs.ass")
        with open(ass_path, "w", encoding="utf-8") as f:
            f.write(mega_captions.make_mega_ass(words, accent_color=profile["color"]))

        duration = audio_duration(final_audio)

        return {
            "i": i, "item": item, "success": True, "plan": plan,
            "audio_path": final_audio, "ass_path": ass_path, "duration": duration,
            "corroboration": item.get("corroboration_count", 1),
        }
    except Exception as e:
        return {"i": i, "item": item, "success": False, "error": str(e)}


# ── DIGEST_REVIEW checkpoint (unchanged) ────────────────────────────────────

def review_items(items):
    print(f"\n{len(items)} story candidate(s) passed automated filtering:\n")
    print("=" * 70)
    for i, item in enumerate(items, 1):
        corroboration = item.get("corroboration_count", 1)
        trend_tag = f" \U0001f525 TRENDING (x{corroboration})" if corroboration > 1 else ""
        print(f"\n[{i}] {item['channel']} #{item['post_id']}{trend_tag}")
        print(f"    {item['caption'][:200]}")
    print("\n" + "=" * 70)

    choice = input(
        f"\nEnter number(s) to REMOVE (e.g. 2,5), or press Enter to keep all {len(items)}: "
    ).strip()
    if not choice:
        return items
    try:
        to_remove = {int(x.strip()) for x in choice.split(",") if x.strip()}
    except ValueError:
        print(f"Couldn't parse '{choice}' — expected numbers like '2,5'. Keeping all items.")
        return items
    kept = []
    for i, item in enumerate(items, 1):
        if i in to_remove:
            print(f"  Removed: {item['channel']} #{item['post_id']}")
        else:
            kept.append(item)
    return kept


# ── Main build pipeline ───────────────────────────────────────────────────────

async def main():
    global _groq_semaphore
    if not GROQ_API_KEY:
        print("GROQ_API_KEY not in .env — can't continue.")
        return

    db = PipelineDB()
    candidates = find_candidate_posts(db)

    if len(candidates) < MIN_ITEMS:
        print(f"Only {len(candidates)} image post(s) ready (need {MIN_ITEMS}). "
              f"Run rss_news_pull.py / fetch_anilist_trending.py first.")
        return

    print(f"\nBuilding anime news digest — filtering {len(candidates)} candidate posts...\n")
    client = Groq(api_key=GROQ_API_KEY)
    _groq_semaphore = asyncio.Semaphore(GROQ_CONCURRENCY)

    anime_items = [item for item in candidates if is_anime_content(client, item["caption"], item["channel"])]
    items = resolve_trending_items(anime_items)

    print(f"\n{len(items)} distinct stor{'y' if len(items) == 1 else 'ies'} after "
          f"cross-channel dedup — applying franchise diversity cap...")
    items = enforce_franchise_diversity(items)

    if len(items) < MIN_ITEMS:
        print(f"Only {len(items)} anime post(s) remain after filtering (need {MIN_ITEMS}).")
        return
    if len(items) > DIGEST_SCORE_POOL_SIZE:
        print(f"  {len(items)} candidates available — pooling the top "
              f"{DIGEST_SCORE_POOL_SIZE} into scoring (DIGEST_SCORE_POOL_SIZE). "
              f"How many actually render is decided AFTER scoring — see "
              f"compute_adaptive_target().")
        items = items[:DIGEST_SCORE_POOL_SIZE]

    if os.getenv("DIGEST_REVIEW", "0") == "1":
        items = review_items(items)
        if len(items) < MIN_ITEMS:
            print(f"\nOnly {len(items)} stories left after review. Aborting.")
            return

    print(f"Building with {len(items)} verified anime stories...\n")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(FINISHED_DIR, "news", f"digest_{timestamp}")
    temp_dir = os.path.join(out_dir, "_temp")
    os.makedirs(temp_dir, exist_ok=True)

    segments = []
    intro_bg = items[0]["image_path"] if items else None
    outro_bg = items[-1]["image_path"] if items else None

    try:
        print("→ Intro stinger")
        stinger_path = os.path.join(temp_dir, "00_seg.mp4")
        build_stinger_segment(intro_bg, stinger_path, temp_dir)
        segments.append(stinger_path)
        raw_duration_so_far = INTRO_STINGER_SECONDS

        print("\n→ PHASE 1: Planning, narrating, and voicing every story concurrently...")
        tasks = [fetch_story_data(i, item, client, temp_dir) for i, item in enumerate(items, 1)]
        prepped_stories = await asyncio.gather(*tasks)

        trending_shows = trend_signal.get_trending_boosts()
        if trending_shows:
            print(f"\n  {trend_signal.describe_boosts(trending_shows)}")
            for data in prepped_stories:
                if not data["success"]:
                    continue
                item, plan = data["item"], data["plan"]
                franchise_key = _franchise_key(item["caption"])
                match = db_module.find_matching_canonical_show(franchise_key) if franchise_key else None
                if match and match["canonical_name"] in trending_shows:
                    ratio = trending_shows[match["canonical_name"]]
                    boosted = min(100, plan["score"] + DIGEST_TREND_SCORE_BOOST)
                    if boosted != plan["score"]:
                        print(f"\n    Trend boost: {match['canonical_name']} is spiking "
                              f"({ratio}x baseline) — score {plan['score']} -> {boosted}")
                        plan["score"] = boosted

        target_count = compute_adaptive_target(prepped_stories)
        qualifying = [d for d in prepped_stories
                      if d.get("success") and d["plan"]["score"] >= STORY_SCORE_THRESHOLD]
        print(f"\n  Demand-responsive batch size: {len(qualifying)} of "
              f"{len(prepped_stories)} scored stor{'y' if len(prepped_stories) == 1 else 'ies'} "
              f"cleared STORY_SCORE_THRESHOLD={STORY_SCORE_THRESHOLD} — targeting "
              f"{target_count} (bounded to [{MIN_ITEMS}, {MAX_ITEMS}]).")
        if qualifying:
            # Content selection intelligence (roadmap item N) — the
            # threshold FILTER above stays purely newsworthiness-based
            # (edit_planner's score is the only story-specific signal),
            # but which of the qualifying stories actually gets a render
            # slot is now ranked by story_selection_score(), which layers
            # in source-quality history on top. canonical_show isn't known
            # yet at this stage (identify_show.py hasn't run — these are
            # raw image posts, not video clips), so that term is always
            # skipped here; source quality is the only additional signal
            # available at digest-build time.
            content_strategy_insight = db_module.get_insight("content_strategy")
            for d in qualifying:
                d["selection_score"] = sq.story_selection_score(
                    d["plan"]["score"], d["item"].get("platform", "unknown"),
                    d["item"]["channel"], canonical_show=None,
                    show_insight=content_strategy_insight,
                )
            top_ids = {d["i"] for d in
                       sorted(qualifying, key=lambda d: d["selection_score"], reverse=True)[:target_count]}
            prepped_stories = [d for d in prepped_stories if d["i"] in top_ids]

        print("\n→ PHASE 2: Rendering video clips sequentially...")
        used_items, used_plans = [], []
        previous_motion = None

        _PAN_STYLES = ["linear", "ease", "drift"]
        wide_indices = [
            d["i"] for d in prepped_stories
            if d.get("success") and is_wide_image(d["item"]["image_path"])
        ]
        wide_treatment = {}
        pan_directions = {}
        pan_styles = {}
        pan_focus_x = {}
        _dir = "left"
        _cycle = 0
        for idx in sorted(wide_indices):
            item = next(d["item"] for d in prepped_stories if d["i"] == idx)
            treatment = classify_wide_treatment(item["image_path"])
            wide_treatment[idx] = treatment
            if treatment == "pan":
                pan_directions[idx] = _dir
                pan_styles[idx] = _PAN_STYLES[_cycle % len(_PAN_STYLES)]
                pan_focus_x[idx] = find_horizontal_focus_x(
                    item["image_path"], OUT_W, OUT_H)
                _dir = "right" if _dir == "left" else "left"
                _cycle += 1
        if wide_indices:
            n_pan = sum(1 for t in wide_treatment.values() if t == "pan")
            n_blur = len(wide_indices) - n_pan
            print(f"  Wide images: {len(wide_indices)} total — {n_pan} get "
                  f"panorama pan (content-targeted), {n_blur} get blur-bg "
                  f"(extreme ratio or low resolution, see PAN_MAX_RATIO/"
                  f"PAN_MIN_HEIGHT_PX)")

        for data in prepped_stories:
            if not data["success"]:
                print(f"\n    SKIPPING Story {data['i']} — failed to fetch data: {data['error']}")
                continue

            item, plan = data["item"], data["plan"]

            if plan["score"] < STORY_SCORE_THRESHOLD:
                print(f"\n    SKIPPING Story {data['i']} — newsworthiness score "
                      f"{plan['score']} below threshold {STORY_SCORE_THRESHOLD} "
                      f"({item['caption'][:60]!r})")
                continue

            projected_post_speedup = (
                raw_duration_so_far + data["duration"] + OUTRO_DURATION_RESERVE_SECONDS
            ) / DIGEST_SPEED_FACTOR
            if projected_post_speedup >= DIGEST_MAX_OUTPUT_SECONDS:
                dropped = len(prepped_stories) - len(used_items)
                print(f"\n→ Stopping at {len(used_items)} stories — adding another "
                      f"would push the digest past the {DIGEST_MAX_OUTPUT_SECONDS:.0f}s "
                      f"Shorts budget. {dropped} remaining stories stay eligible.")
                break

            trend_tag = f" \U0001f525 TRENDING (x{data['corroboration']} channels)" if data['corroboration'] > 1 else ""
            print(f"\n→ Story {data['i']}/{len(prepped_stories)}: {item['channel']} #{item['post_id']}"
                  f"{trend_tag} [{plan['emotion']}, score={plan['score']}]")
            print(f"    Narration: {plan['narration'][:80]}...")

            motion = motion_engine.pick_motion(plan["emotion"], previous_motion)
            profile = motion_engine.profile_for(plan["emotion"])

            treatment = wide_treatment.get(data["i"], "normal")
            print(f"    Camera: {motion} | transition: {profile['transition']} | "
                  f"render: {treatment}"
                  + (f" (focus={pan_focus_x.get(data['i'], 0.5):.2f}, "
                     f"style={pan_styles.get(data['i'])})" if treatment == "pan" else ""))

            try:
                seg_path = os.path.join(temp_dir, f"{data['i']:02d}_seg.mp4")
                broll = find_broll_clip(item)
                if broll:
                    pass
                if treatment == "pan":
                    try:
                        build_wide_pan_segment(
                            item["image_path"], data["audio_path"], data["ass_path"],
                            seg_path, data["duration"], profile["transition"],
                            pan_direction=pan_directions.get(data["i"], "left"),
                            pan_style=pan_styles.get(data["i"], "linear"),
                            focus_x=pan_focus_x.get(data["i"], 0.5),
                        )
                    except Exception as e:
                        print(f"    Pan render failed ({e}) — falling back to blur-bg for this story")
                        build_blur_bg_segment(
                            item["image_path"], data["audio_path"], data["ass_path"],
                            seg_path, data["duration"], motion, profile["transition"],
                        )
                elif treatment == "blur-bg":
                    build_blur_bg_segment(
                        item["image_path"], data["audio_path"], data["ass_path"],
                        seg_path, data["duration"], motion, profile["transition"],
                    )
                else:
                    build_image_segment(
                        item["image_path"], data["audio_path"], data["ass_path"],
                        seg_path, data["duration"], motion, profile["transition"],
                    )
                segments.append(seg_path)
                raw_duration_so_far += data["duration"]
                used_items.append(item)
                used_plans.append(plan)
                previous_motion = motion
            except Exception as e:
                print(f"    SKIPPING this story — failed to render clip: {e}")
                continue

        if len(used_items) < MIN_ITEMS:
            raise RuntimeError(
                f"Only {len(used_items)} stories fit within the budget/score "
                f"threshold (need {MIN_ITEMS})."
            )

        print("\n→ Outro")
        outro_audio = os.path.join(temp_dir, "99_audio.mp3")
        outro_words = await tts(OUTRO_TEXT, outro_audio)
        if not outro_words:
            outro_words = [{"word": OUTRO_TEXT, "start": 0.0, "end": audio_duration(outro_audio)}]
        outro_ass = os.path.join(temp_dir, "99_subs.ass")
        with open(outro_ass, "w", encoding="utf-8") as f:
            f.write(mega_captions.make_mega_ass(outro_words, accent_color="&H00FFFFFF", font_size=90))
        outro_dur = audio_duration(outro_audio)
        outro_seg = os.path.join(temp_dir, "99_seg.mp4")
        build_titlecard_segment(outro_bg, outro_audio, outro_ass, outro_seg, outro_dur, OUTRO_TITLE)
        segments.append(outro_seg)

        print("\n→ Stitching all segments...")
        raw_video = os.path.join(out_dir, "video_raw.mp4")
        final_video = os.path.join(out_dir, "video.mp4")
        concat_videos(segments, raw_video)

        mood = dominant_mood(used_plans)
        bgm_path = pick_bgm(mood)
        print(f"→ Speeding up to {DIGEST_SPEED_FACTOR}x"
              f"{f' with {mood} BGM ({bgm_path})' if bgm_path else ' (no BGM found — skipping)'}...")
        speed_up_video(raw_video, final_video, speed=DIGEST_SPEED_FACTOR, bgm_path=bgm_path)
        os.remove(raw_video)

        actual_duration = audio_duration(final_video)
        if actual_duration > 180:
            print(f"    WARNING: final video is {actual_duration:.0f}s — over "
                  f"YouTube's 180s Shorts cutoff.")
        else:
            print(f"    Final duration: {actual_duration:.0f}s (Shorts-eligible)")

        expected_final_duration = (raw_duration_so_far + outro_dur) / DIGEST_SPEED_FACTOR
        write_digest_sync_sidecar(out_dir, expected_final_duration)

        print("→ Generating TikTok caption...")
        caption = generate_caption(client, used_items)
        with open(os.path.join(out_dir, "caption_final.txt"), "w", encoding="utf-8") as f:
            f.write(caption)

        print("→ Generating YouTube caption...")
        yt_caption = generate_youtube_caption(client, used_items)
        with open(os.path.join(out_dir, "caption_youtube.txt"), "w", encoding="utf-8") as f:
            f.write(yt_caption)

        shutil.rmtree(temp_dir, ignore_errors=True)

        urgent = _compute_urgency(used_items, used_plans)
        # render_params_json (self-learning loop expansion): a digest only
        # goes through ONE production pass here, so this is a direct write,
        # not a read-merge-write like the clip-side stages need — see
        # db.py's render_params_json docstring and this module's own
        # LEARN STAGE section for why clips and digests differ here.
        digest_render_params = json.dumps({"news_digest_speed_factor": DIGEST_SPEED_FACTOR})
        digest_id = db.create_digest(
            video_path=final_video, caption_tiktok=caption, caption_youtube=yt_caption,
            post_ids=[item["id"] for item in used_items],
            urgent=urgent,
            render_params_json=digest_render_params,
        )
        urgency_note = " \U0001f6a8 URGENT — will jump the publish queue" if urgent else ""
        print(f"\n\u2705 Digest #{digest_id} ready ({len(used_items)} stories, mood={mood}){urgency_note}:\n   {final_video}")

    except Exception:
        print(f"\nDigest build failed — cleaning up partial output at {out_dir}")
        shutil.rmtree(out_dir, ignore_errors=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
