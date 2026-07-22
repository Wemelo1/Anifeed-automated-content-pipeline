"""
Show Identifier
----------------
Three-layer identification for each clip, stopping as soon as one succeeds:

  1. VISUAL — grabs frames from the video, asks a vision model to name the anime
  2. TEXT   — reads caption_raw.txt, asks Groq to extract the title (handles Japanese)
  3. SEARCH — web search on the caption text for extra context

Saves result (or "UNKNOWN" if all three fail) to visual_hint.txt next to the
video, AND to pipeline.db via db.py — the database is the source of truth
for "already done," so this stays skip-safe even after finished/ files get
deleted post-publish. Safe to re-run.

NOTE (updated 2026-07-23): the vision call (Layer 1) was silently broken
since the meta-llama/llama-4-scout-17b-16e-instruct -> qwen/qwen3.6-27b
swap noted below. qwen3.6-27b is a REASONING model — it emits a
"<think>...</think>" block before its actual answer. identify_visual() was
calling it with max_tokens=50 and no reasoning_effort, so the response was
being cut off MID-REASONING, before the model ever produced a real title.
The truncated reasoning trace itself was then saved as visual_hint and fed
into show_canon.py, generate_captions.py's prompt context, and
canonical_shows — polluting all three with garbage text instead of erroring
loudly. Fixed by (a) passing reasoning_effort="low" so the model spends
less budget thinking and reaches an answer, (b) raising max_tokens so a
real answer isn't truncated even if some reasoning happens anyway, and (c)
_strip_think_tags() as defense-in-depth: any <think>...</think> block is
stripped from every layer's result before it's used, and an UNCLOSED
<think> tag (still truncated somehow) causes that result to be discarded
as empty rather than saved as garbage. See cleanup_poisoned_visual_hints.py
for the one-off cleanup of rows already poisoned before this fix.

NOTE (updated 2026-07-21): Groq deprecated meta-llama/llama-4-scout-17b-16e-
instruct on 2026-06-17 and it is now fully removed (calls 404 with
model_not_found, not a transient outage — confirmed against
https://console.groq.com/docs/deprecations). Groq's own migration guidance
for Scout points to EITHER openai/gpt-oss-120b or qwen/qwen3.6-27b — but
gpt-oss-120b is text-only, and Layer 1 below needs real image input, so
qwen/qwen3.6-27b (the multimodal option) is the correct swap, not
gpt-oss-120b. If Groq deprecates this too, check
https://console.groq.com/docs/vision for whatever's current — same as the
original note below, just with an already-hit example this time.

NOTE: Layer 3 search uses `requests` directly against DuckDuckGo's HTML
endpoint — same approach as build_news_digest.py. We deliberately do NOT
use the `ddgs`/`duckduckgo_search` package: its HTTP backend (curl_cffi/primp)
is Rust-based and tries to fetch Android's certificate store via JNI on
Termux, which hard-panics the process (SIGABRT: "android context was not
initialized") with no way to catch it from Python — even in a subprocess.

SETUP:
  pip install groq python-dotenv requests
  ffmpeg on PATH

USAGE:
  python identify_show.py   (run after burn_subtitles.py, before generate_captions.py)
"""

import os
import re
import socket
import base64
import subprocess
import requests as _requests
from dotenv import load_dotenv
from groq import Groq

import db
import show_canon
import cost_tracker as ct

load_dotenv()

# ---------- CONFIG ----------
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
# meta-llama/llama-4-scout-17b-16e-instruct was deprecated by Groq on
# 2026-06-17 and is now removed (404 model_not_found) — see NOTE above.
# qwen3.6-27b is the multimodal replacement Groq points to for Scout.
VISION_MODEL   = "qwen/qwen3.8-27b"  # was qwen/qwen3.6-27b — deprecated, shutdown 2026-09-14
TEXT_MODEL     = "openai/gpt-oss-120b"

FINISHED_DIR   = "finished"
DOWNLOADS_DIR  = "downloads"
NUM_FRAMES     = 3

VISION_PROMPT = (
    "You are looking at frame(s) taken from a short anime clip. "
    "If you can confidently recognize the SPECIFIC anime series this is from, "
    "respond with ONLY the anime's English title and nothing else. "
    "If you don't recognize it or aren't genuinely confident, respond with "
    "exactly: UNKNOWN. Do not guess — a wrong answer is worse than no answer. "
    "No explanation, no punctuation, no extra text either way."
)

TEXT_PROMPT = (
    "Extract ONLY the anime series title from the text below. "
    "The text may be in Japanese, English, or mixed — translate to English if needed. "
    "If you can identify a specific anime title with confidence, respond with ONLY that title. "
    "If you cannot identify a specific anime title, respond with exactly: UNKNOWN. "
    "No explanation, no punctuation, nothing else."
)


class ConnectivityError(Exception):
    """Raised instead of swallowing to 'UNKNOWN' when a layer fails because
    the network/API is unreachable, as opposed to genuinely not recognizing
    the content. This distinction matters: a real 'nothing matched' result
    is safe to mark done forever, but a connectivity failure should NOT be
    recorded as done — the clip needs to be retried once the connection is
    back, not silently written off as UNKNOWN for good."""
    pass


_CONNECTIVITY_EXCEPTION_TYPES = (
    _requests.exceptions.ConnectionError,
    _requests.exceptions.Timeout,
    socket.gaierror,
    ConnectionError,
    TimeoutError,
)
_CONNECTIVITY_KEYWORDS = (
    "connection", "network", "getaddrinfo", "name resolution",
    "name or service not known", "timed out", "timeout",
    "unreachable", "temporary failure in name resolution",
)


def _is_connectivity_error(e):
    if isinstance(e, _CONNECTIVITY_EXCEPTION_TYPES):
        return True
    text = str(e).lower()
    return any(kw in text for kw in _CONNECTIVITY_KEYWORDS)


_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _strip_think_tags(text):
    """Strips a reasoning model's <think>...</think> block from its output.
    If a CLOSED think block is found, it's removed and whatever real answer
    followed it is kept. If an UNCLOSED <think> tag is found (the response
    was cut off mid-reasoning, before any real answer was produced — the
    exact failure mode that originally caused this), the result is
    discarded entirely rather than saved as a truncated reasoning trace.
    Applied at every layer as defense-in-depth, not just the vision layer,
    since reasoning_effort isn't guaranteed to be honored identically by
    every model Groq might swap in here in the future."""
    if not text:
        return text
    text = _THINK_BLOCK_RE.sub("", text)
    if "<think>" in text.lower():
        return ""
    return text.strip()
# -----------------------------


def find_clips_needing_id():
    found = []
    if not os.path.isdir(FINISHED_DIR):
        return found
    for channel in os.listdir(FINISHED_DIR):
        channel_path = os.path.join(FINISHED_DIR, channel)
        if not os.path.isdir(channel_path):
            continue
        for post_id in os.listdir(channel_path):
            post_path = os.path.join(channel_path, post_id)
            video_path = os.path.join(post_path, "video.mp4")
            if not os.path.isfile(video_path):
                continue
            if db.is_stage_done(channel, post_id, "identified"):
                continue  # already identified, even if visual_hint.txt got deleted later
            found.append((channel, post_id, post_path, video_path))
    return found


def get_caption(channel, post_id, post_path):
    """Read the raw Telegram caption — check finished/ first, then downloads/."""
    for base in (post_path, os.path.join(DOWNLOADS_DIR, channel, post_id)):
        p = os.path.join(base, "caption_raw.txt")
        if os.path.isfile(p):
            with open(p, "r", encoding="utf-8") as f:
                return f.read().strip()
    return ""


# ── Layer 1: visual ───────────────────────────────────────────────────────────

def get_duration(video_path):
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", video_path],
        capture_output=True, text=True,
    )
    if r.returncode != 0 or not r.stdout.strip():
        raise RuntimeError(r.stderr[-400:])
    return float(r.stdout.strip())


def extract_frame(video_path, timestamp, out_path):
    r = subprocess.run([
        "ffmpeg", "-y", "-ss", str(timestamp), "-i", video_path,
        "-frames:v", "1", "-q:v", "2", out_path,
    ], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-400:])


def identify_visual(client, video_path, post_path):
    frame_paths = []
    try:
        duration = get_duration(video_path)
        for i in range(NUM_FRAMES):
            ts = duration * (i + 1) / (NUM_FRAMES + 1)
            fp = os.path.join(post_path, f"_tmp_frame_{i}.jpg")
            extract_frame(video_path, ts, fp)
            frame_paths.append(fp)

        content = [{"type": "text", "text": VISION_PROMPT}]
        for fp in frame_paths:
            with open(fp, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
            content.append({"type": "image_url",
                             "image_url": {"url": f"data:image/jpeg;base64,{b64}"}})

        resp = client.chat.completions.create(
            model=VISION_MODEL,
            messages=[{"role": "user", "content": content}],
            temperature=0.2,
            max_tokens=300,
            reasoning_effort="none",
        )
        result = (resp.choices[0].message.content or "").strip()
        result = _strip_think_tags(result)
        return result if result else "UNKNOWN"

    except Exception as e:
        if _is_connectivity_error(e):
            raise ConnectivityError(f"Visual ID: {e}") from e
        print(f"    Visual ID failed: {e}")
        return "UNKNOWN"
    finally:
        for fp in frame_paths:
            if os.path.isfile(fp):
                os.remove(fp)


# ── Layer 2: text extraction ──────────────────────────────────────────────────

def identify_from_text(client, caption):
    if not caption:
        return "UNKNOWN"
    try:
        resp = client.chat.completions.create(
            model=TEXT_MODEL,
            messages=[
                {"role": "system", "content": TEXT_PROMPT},
                {"role": "user",   "content": caption[:600]},
            ],
            temperature=0.1,
            max_tokens=60,
            reasoning_effort="low",
        )
        result = (resp.choices[0].message.content or "").strip()
        result = _strip_think_tags(result)
        return result if result and result.upper() != "UNKNOWN" else "UNKNOWN"
    except Exception as e:
        if _is_connectivity_error(e):
            raise ConnectivityError(f"Text ID: {e}") from e
        print(f"    Text ID failed: {e}")
        return "UNKNOWN"


# ── Layer 3: web search ───────────────────────────────────────────────────────

def search_anime_name(caption):
    """Search the web using the caption as a query, return best snippet."""
    if not caption:
        return "UNKNOWN"
    query = caption[:120] + " anime"
    try:
        resp = _requests.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=15,
        )
        resp.raise_for_status()
        snippets = re.findall(
            r'class="result__snippet"[^>]*>(.*?)</a>', resp.text, re.S
        )
        clean = []
        for s in snippets[:3]:
            s = re.sub(r"<[^>]+>", "", s)         # strip HTML tags
            s = re.sub(r"\s+", " ", s).strip()     # collapse whitespace
            if s:
                clean.append(s)
        joined = " ".join(clean)[:600]
        return joined if joined else "UNKNOWN"
    except Exception as e:
        if _is_connectivity_error(e):
            raise ConnectivityError(f"Web search: {e}") from e
        print(f"    Web search skipped: {e}")
        return "UNKNOWN"


def identify_from_search(client, caption):
    context = search_anime_name(caption)
    if context == "UNKNOWN":
        return "UNKNOWN"
    # Ask Groq to pull the anime name out of the search snippets
    try:
        resp = client.chat.completions.create(
            model=TEXT_MODEL,
            messages=[
                {"role": "system", "content": TEXT_PROMPT},
                {"role": "user",   "content":
                    f"CAPTION: {caption[:200]}\n\nWEB SEARCH RESULTS: {context}"},
            ],
            temperature=0.1,
            max_tokens=60,
            reasoning_effort="low",
        )
        result = (resp.choices[0].message.content or "").strip()
        result = _strip_think_tags(result)
        return result if result and result.upper() != "UNKNOWN" else "UNKNOWN"
    except Exception as e:
        if _is_connectivity_error(e):
            raise ConnectivityError(f"Search ID: {e}") from e
        print(f"    Search ID failed: {e}")
        return "UNKNOWN"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not GROQ_API_KEY:
        print("GROQ_API_KEY not found. Add it to your .env file.")
        return

    db.init_db()
    clips = find_clips_needing_id()
    if not clips:
        print("No clips need identification right now.")
        return

    client = Groq(api_key=GROQ_API_KEY)
    done = 0

    for channel, post_id, post_path, video_path in clips:
        print(f"Identifying {channel}/{post_id}...")
        caption = get_caption(channel, post_id, post_path)

        try:
            # Layer 1 — visual
            print("  [1/3] Visual...")
            result = identify_visual(client, video_path, post_path)
            ct.log_call("groq", "vision_call", content_key=f"{channel}/{post_id}", content_type="clip")
            if result != "UNKNOWN":
                print(f"  -> {result} (visual)")
            else:
                # Layer 2 — text
                print("  [2/3] Text extraction...")
                result = identify_from_text(client, caption)
                ct.log_call("groq", "text_call", content_key=f"{channel}/{post_id}", content_type="clip")
                if result != "UNKNOWN":
                    print(f"  -> {result} (text)")
                else:
                    # Layer 3 — web search
                    print("  [3/3] Web search...")
                    result = identify_from_search(client, caption)
                    ct.log_call("groq", "text_call", content_key=f"{channel}/{post_id}", content_type="clip")
                    print(f"  -> {result} (search)" if result != "UNKNOWN" else "  -> UNKNOWN (all layers failed)")
        except ConnectivityError as e:
            print(f"  STOPPED — no connectivity ({e}). This clip is NOT marked "
                  f"identified, so it'll be picked up again next run. Skipping "
                  f"the rest of this batch too, since they'd hit the same problem.")
            break

        hint_path = os.path.join(post_path, "visual_hint.txt")
        with open(hint_path, "w", encoding="utf-8") as f:
            f.write(result)

        # Merge this raw guess into a franchise-level canonical show name —
        # see show_canon.py for why (fragmented raw strings like "Chainsaw
        # Man" / "chainsaw man s2" would otherwise make analytics useless).
        # Failure here should never block marking the clip identified —
        # worst case canonical_show stays NULL and this post is simply
        # excluded from show-level insights, same as an UNKNOWN result.
        canonical = None
        if result.upper() != "UNKNOWN":
            try:
                canonical = show_canon.canonicalize(client, result)
            except Exception as e:
                print(f"    Canonicalization failed (non-fatal): {e}")

        db.mark_stage(channel, post_id, "identified",
                       visual_hint=result, canonical_show=canonical)
        done += 1

    print(f"\nDone. Processed {done} clip(s).")


if __name__ == "__main__":
    main()