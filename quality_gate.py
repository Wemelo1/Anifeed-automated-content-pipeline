"""
Quality Gate
--------------
The one automated check standing between "rendered" and "published" that
this pipeline didn't have before. Every other stage already has its own
defensive layer — refusal_guard at generation and publish boundaries,
normalize_image() at ingestion, is_valid_image() on downloads — but nothing
ever looked at the FINISHED clip/digest itself before it went out. A bad
render (desynced/silent audio, a corrupt or black-frame encode, a squashed
aspect ratio from a filter regression, a refusal string that slipped past
generate_captions.py's own check) could sail straight from 'thumbnailed'
to a live YouTube upload with nothing catching it.

WHERE THIS SITS: after generate_thumbnails.py / add_hook_overlay.py, and
— critically — BEFORE publish_scheduler.py, not just before
publish_youtube.py/publish_tiktok.py. db.py's get_publishable_posts() and
get_publishable_digests() are the single choke point both the scheduler and
the publishers read from, and this module's verdict (quality_status) is a
filter baked directly into those two functions. That means an unapproved
item never gets a publish slot computed for it either, not just never gets
uploaded — see db.py's QUALITY_GATE_ENABLED docstring.

VERDICT MODEL: every checked item gets a 0-100 score plus a list of short
flag strings, and lands in exactly one of three terminal states (same
"once decided, stays decided" convention as publish_log — see
reset_stage.py's new 'quality' command to force a re-check):

  approved  — score >= QUALITY_APPROVE_THRESHOLD, no critical flag.
              Eligible for scheduling/publishing immediately.
  held      — REJECT_THRESHOLD <= score < APPROVE_THRESHOLD, no critical
              flag. NOT auto-published, NOT auto-rejected — sits in the
              manual review queue (db.quality_held_items(), printed at the
              end of every run) until a human looks at it or a threshold
              changes. This is deliberate: most borderline renders are
              fine, but this pipeline runs fully unattended, so "probably
              fine" content should wait for a glance rather than either
              extreme (never publishing anything below-perfect, or
              publishing everything on a coin flip).
  rejected  — score < QUALITY_REJECT_THRESHOLD, OR any critical flag
              regardless of score (a critical flag — corrupt file, wrong
              canvas, a refusal string in the caption — overrides the
              numeric score entirely; there's no score high enough to
              excuse a genuinely broken render). Terminal: use
              reset_stage.py's 'quality' command after fixing the actual
              cause, not by nudging thresholds to sneak a specific item
              through.

CHECKS (per PROJECT_RULES.md Rule 5 — one bad check must never take the
whole batch down; every check below is individually try/except'd and a
check that fails to RUN counts as "unknown", contributing nothing to the
score, rather than silently passing OR silently failing the item):

  - File integrity / decodability (ffprobe) — critical if unreadable.
  - Canvas match — must be exactly 1080x1920 per FFMPEG_EXPERT.md's
    non-negotiable spec; a mismatch means some filter chain silently
    squashed or letterboxed the output. Critical.
  - Duration sanity — too short (likely a truncated/failed render) or over
    YouTube's 180s Shorts cutoff (documented threshold in
    build_news_digest.py). Major.
  - Audio silence — a digest that's silent for its ENTIRE runtime is
    critical (voiceover is the one hard dependency per
    AI_VOICEOVER_PRODUCTION.md / PROJECT_RULES.md Rule 3); a CLIP being
    fully silent is legitimate and expected (burn_subtitles.py already
    documents "no spoken dialogue -> copied through untouched") so it's
    only ever informational for clips, never a flag. An individual
    mid-track silence gap over QUALITY_MAX_SILENCE_GAP_SECONDS is only
    checked for digests (continuous TTS narration should never have a
    dead-air gap that long — QUALITY_METRICS.md's own 0.5s rule) and is
    major, not critical, since it degrades rather than breaks the video.
  - Black-frame ratio — a render whose visible content is mostly black
    (failed image decode fallback, a filter chain gone wrong) is major.
  - Timing consistency — reads the subtitle_sync.json sidecar written by
    burn_subtitles.py (clips) / build_news_digest.py (digests) at
    generation time and compares it against the finished video's actual
    duration. This is the check for the desync-class bug specifically:
    CAPTION_GENERATION.md documents subtitle desync as "one of the two
    critical breaking bugs in this pipeline," and VIDEO_PIPELINE_ARCHITECT.md's
    own spec for this stage lists a subtitle sync check as one of its four
    validations — this was the one missing until now. By the time a video
    reaches the quality gate, captions are burned into pixels (no separate
    subtitle stream to inspect), so the timing has to be captured upstream
    and checked here rather than reconstructed after the fact. A missing
    sidecar (content that predates this feature, or a clip with no spoken
    dialogue) is a clean no-op, not a flag. Major.
  - Caption refusal scan — re-checks caption_final.txt/caption_youtube.txt
    with refusal_guard.contains_refusal() as a SECOND layer beyond
    generate_captions.py's own generation-time check (see refusal_guard.py:
    "guards must be at every generation point and every publish boundary" —
    this is a publish boundary, and it runs earlier than
    publish_youtube.py/publish_tiktok.py's own checks, which only fire at
    actual upload time). Critical.
  - Show-level removal-rate signal (ADDED — self-learning loop expansion,
    roadmap item H) — clips only, not digests (a digest has no single
    canonical_show — see db.removal_rate_by_show()'s own "clips only"
    note). A show that's been getting an unusually high fraction of its
    published clips pulled (Content ID claims, manual takedowns — see
    fetch_analytics.py's docstring) is a content-strategy risk this
    pipeline previously had NO automated way to react to before a NEW
    clip of that same show went out — db.removal_rate_by_show() already
    computed the signal, nothing ever consulted it before this. Scored
    as MAJOR, not critical: this is not a technical defect in THIS
    render (the content itself may be perfectly fine) — it pushes a
    clip toward 'held' for a human glance rather than blocking it
    outright, unless other issues push it to 'rejected' anyway.
  - Thumbnail presence — missing/empty thumbnail.jpg is minor, not
    blocking: PROJECT_RULES.md Rule 3 (graceful degradation) already says
    a missing thumbnail should still publish with a fallback frame, not
    get held up.
  - Show diversity signal (ADDED — roadmap item T) — clips only, mirrors
    the show-removal-rate check above. A clip whose canonical_show is
    already over its cap of the last diversity_guard.DIVERSITY_WINDOW
    published clips gets flagged MAJOR, pushing it toward 'held' rather
    than auto-approved — see diversity_guard.py's module docstring for
    why this can't be gated pre-render.

SETUP:
  ffmpeg + ffprobe on PATH (already a pipeline dependency everywhere else)

USAGE:
  python quality_gate.py
  Run after generate_thumbnails.py / add_hook_overlay.py, BEFORE
  publish_scheduler.py.

.env overrides (all optional):
  QUALITY_GATE_ENABLED=1              # 0 disables the gate ENTIRELY — every
                                        # thumbnailed item is treated as
                                        # already approved (db.py reads this
                                        # same flag; see its docstring)
  QUALITY_APPROVE_THRESHOLD=75
  QUALITY_REJECT_THRESHOLD=40
  QUALITY_MIN_DURATION_SECONDS=3
  QUALITY_MAX_DURATION_SECONDS=180
  QUALITY_MAX_SILENCE_GAP_SECONDS=0.5  # digests only, see docstring above
  QUALITY_SILENCE_NOISE_DB=-50         # ffmpeg silencedetect noise floor
  QUALITY_MAX_BLACK_RATIO=0.3          # fraction of runtime allowed black
  QUALITY_BLACKDETECT_MIN_SECONDS=0.5  # min contiguous black stretch counted
  SUBTITLE_SYNC_MAX_TAIL_GAP_SECONDS=4.0  # clips: max seconds captions can
                                        # stop before the video ends
  SUBTITLE_SYNC_MAX_OVERRUN_SECONDS=1.0   # clips: max seconds captions can
                                        # extend past the video's end
  DIGEST_TIMING_TOLERANCE_SECONDS=2.5  # digests: max seconds actual vs.
                                        # expected final duration can differ
  QUALITY_REMOVAL_RATE_THRESHOLD_PCT=40   # show removal rate that triggers
                                        # the flag (clips only)
  QUALITY_REMOVAL_RATE_MIN_PUBLISHED=5    # min published clips of a show
                                        # before its removal rate is trusted
  QUALITY_CHECK_TIMEOUT_SECONDS=60     # per ffmpeg analysis pass, per item
"""

import json
import os
import re
import subprocess

from dotenv import load_dotenv
from video_utils import get_duration, get_video_dimensions
from refusal_guard import contains_refusal
import db
import diversity_guard as dg

load_dotenv()

FINISHED_DIR = "finished"

# Canvas spec — non-negotiable per FFMPEG_EXPERT.md, every output must match
# this exactly.
OUT_W, OUT_H = 1080, 1920

QUALITY_GATE_ENABLED = os.getenv("QUALITY_GATE_ENABLED", "1") != "0"
import learned_config as lc
lc.init_schema()
QUALITY_APPROVE_THRESHOLD = float(lc.get_param("quality_approve_threshold"))
QUALITY_REJECT_THRESHOLD = float(os.getenv("QUALITY_REJECT_THRESHOLD", "40"))
QUALITY_MIN_DURATION_SECONDS = float(os.getenv("QUALITY_MIN_DURATION_SECONDS", "3"))
QUALITY_MAX_DURATION_SECONDS = float(os.getenv("QUALITY_MAX_DURATION_SECONDS", "180"))
QUALITY_MAX_SILENCE_GAP_SECONDS = float(os.getenv("QUALITY_MAX_SILENCE_GAP_SECONDS", "0.5"))
QUALITY_SILENCE_NOISE_DB = os.getenv("QUALITY_SILENCE_NOISE_DB", "-50")
QUALITY_MAX_BLACK_RATIO = float(os.getenv("QUALITY_MAX_BLACK_RATIO", "0.3"))
QUALITY_BLACKDETECT_MIN_SECONDS = float(os.getenv("QUALITY_BLACKDETECT_MIN_SECONDS", "0.5"))
QUALITY_CHECK_TIMEOUT_SECONDS = int(os.getenv("QUALITY_CHECK_TIMEOUT_SECONDS", "60"))

# Timing-consistency check (reads the subtitle_sync.json sidecar written by
# burn_subtitles.py / build_news_digest.py — see those modules' docstrings
# for why the timing data has to be captured at generation time rather
# than reconstructed from the finished, burned-in video). A clip's last
# subtitle cue is allowed to end up to this many seconds before the video
# actually ends (trailing music/SFX after the last spoken word is normal,
# not a bug) before it's flagged. Cues ending noticeably AFTER the video
# ends is a much stronger signal (captions can't legitimately outlast the
# file) so that gets a tighter tolerance.
SUBTITLE_SYNC_MAX_TAIL_GAP_SECONDS = float(os.getenv("SUBTITLE_SYNC_MAX_TAIL_GAP_SECONDS", "4.0"))
SUBTITLE_SYNC_MAX_OVERRUN_SECONDS = float(os.getenv("SUBTITLE_SYNC_MAX_OVERRUN_SECONDS", "1.0"))
# Same idea for digests, but comparing the pre-speedup narration total
# (scaled by the digest's speed factor) against the actual final duration
# instead of a single cue timestamp — see build_news_digest.py's
# write_digest_sync_sidecar() docstring.
DIGEST_TIMING_TOLERANCE_SECONDS = float(os.getenv("DIGEST_TIMING_TOLERANCE_SECONDS", "2.5"))

# Show-level removal-rate signal (ADDED — roadmap item H). Same default
# thresholds as performance_insights.py's own SHOW_REMOVAL_RATE_
# THRESHOLD_PCT / SHOW_REMOVAL_RATE_MIN_PUBLISHED — kept as separate env
# vars (not read from that module) so this gate's behavior doesn't
# silently change if performance_insights.py's own reporting thresholds
# are tuned for a different purpose later; the two happening to agree by
# default is intentional, not a hard coupling.
QUALITY_REMOVAL_RATE_THRESHOLD_PCT = float(os.getenv("QUALITY_REMOVAL_RATE_THRESHOLD_PCT", "40"))
QUALITY_REMOVAL_RATE_MIN_PUBLISHED = int(os.getenv("QUALITY_REMOVAL_RATE_MIN_PUBLISHED", "5"))

# Score deductions per severity. "critical" bypasses the numeric score
# entirely (see module docstring) — its weight here is just to make the
# score reflect reality in the stored record, not to gate the decision.
_SEVERITY_WEIGHT = {"critical": 100, "major": 25, "minor": 10, "info": 0}

_SILENCE_START_RE = re.compile(r"silence_start:\s*([\d.]+)")
_SILENCE_END_RE = re.compile(r"silence_end:\s*([\d.]+)\s*\|\s*silence_duration:\s*([\d.]+)")
_BLACK_RE = re.compile(r"black_start:([\d.]+)\s+black_end:([\d.]+)\s+black_duration:([\d.]+)")


# ── low-level ffmpeg analysis passes ────────────────────────────────────────

def _ffmpeg_analysis_stderr(video_path, filter_str, is_audio):
    """Runs an ffmpeg analysis-only pass (silencedetect/blackdetect) piped
    to a null muxer — nothing is written to disk, the detection events are
    parsed out of stderr. Raises on a genuine failure (unreadable file,
    ffmpeg missing); caller wraps this per-check so one failed analysis
    pass never takes down the other checks for the same item.

    FIXED: previously returned `r.stderr` unconditionally, never checking
    `r.returncode`. get_duration() (step 1, before this is ever called)
    reads duration from container metadata, which can succeed even when
    the actual video/audio STREAM is corrupted — that combination used to
    slip through here silently: ffmpeg would fail to decode, the
    silencedetect/blackdetect regexes would simply find zero matches in
    whatever stderr it did produce, and run_checks() would read that as
    'no silence detected' / 'no black frames detected' — a corrupted file
    scoring as clean instead of getting flagged. Checking returncode here
    turns that into what it actually is: a failed check, caught by the
    per-check try/except in run_checks() and logged as an 'info' flag
    (contributes nothing to the score) rather than a false-clean pass."""
    stream_flag = "-af" if is_audio else "-vf"
    cmd = ["ffmpeg", "-i", video_path, stream_flag, filter_str, "-f", "null", "-"]
    r = subprocess.run(cmd, capture_output=True, text=True,
                        timeout=QUALITY_CHECK_TIMEOUT_SECONDS)
    if r.returncode != 0:
        raise RuntimeError(f"ffmpeg analysis pass failed (exit {r.returncode}): "
                            f"{r.stderr[-500:]}")
    return r.stderr or ""


def _detect_silence_gaps(video_path, duration):
    """Returns (gaps, is_fully_silent). `gaps` is a list of
    (start, end, duration_seconds) tuples for every detected silent stretch
    at or above the ffmpeg default detector granularity — caller filters
    by QUALITY_MAX_SILENCE_GAP_SECONDS itself. `is_fully_silent` is True
    when a single detected gap covers (approximately) the entire clip."""
    filt = f"silencedetect=noise={QUALITY_SILENCE_NOISE_DB}dB:d=0.3"
    stderr = _ffmpeg_analysis_stderr(video_path, filt, is_audio=True)

    starts = [float(m.group(1)) for m in _SILENCE_START_RE.finditer(stderr)]
    ends = [(float(m.group(1)), float(m.group(2))) for m in _SILENCE_END_RE.finditer(stderr)]

    gaps = []
    for i, start in enumerate(starts):
        if i < len(ends):
            end, gap_dur = ends[i]
            gaps.append((start, end, gap_dur))
        else:
            # silence ran to end-of-stream with no explicit silence_end line
            gaps.append((start, duration, max(duration - start, 0.0)))

    is_fully_silent = any(
        start <= 0.15 and gap_dur >= duration - 0.3
        for start, _, gap_dur in gaps
    )
    return gaps, is_fully_silent


def _detect_black_ratio(video_path, duration):
    """Returns the fraction (0.0-1.0) of `duration` covered by detected
    black stretches."""
    filt = f"blackdetect=d={QUALITY_BLACKDETECT_MIN_SECONDS}:pic_th=0.98"
    stderr = _ffmpeg_analysis_stderr(video_path, filt, is_audio=False)

    total_black = sum(float(m.group(3)) for m in _BLACK_RE.finditer(stderr))
    if duration <= 0:
        return 0.0
    return min(total_black / duration, 1.0)


def _load_subtitle_sync_sidecar(video_path):
    """Reads subtitle_sync.json next to `video_path`, written by
    burn_subtitles.py (clips) or build_news_digest.py (digests) — see
    those modules' docstrings for why this has to be captured at
    generation time. Returns the parsed dict, or None if the sidecar
    doesn't exist or can't be read/parsed — both are treated as 'no
    signal available', never an error, since a missing sidecar just means
    the clip predates this feature (or hit the no-dialogue copy-through
    path in burn_subtitles.py), not that something's wrong."""
    sidecar_path = os.path.join(os.path.dirname(video_path), "subtitle_sync.json")
    if not os.path.isfile(sidecar_path):
        return None
    try:
        with open(sidecar_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _check_timing_consistency(video_path, duration, is_digest):
    """Compares the subtitle_sync.json sidecar against the video's actual
    (ffprobe-measured) duration to catch the desync-class bug —
    CAPTION_GENERATION.md's own words: 'one of the two critical breaking
    bugs in this pipeline'. Returns (severity_or_None, message_or_None);
    the caller decides whether/how to deduct. Never raises: a missing or
    unreadable sidecar is a clean no-op, and this is explicitly NOT one of
    the try/except-wrapped calls in run_checks() below because it can't
    fail loudly enough to need one — every branch already degrades to
    'nothing to check' on its own.

    CLIPS: subtitles are burned into the pixels by the time this runs, so
    there's no separate subtitle stream left to inspect — the sidecar's
    last_cue_end (captured during burn_subtitles.py's own generation pass)
    is compared against the finished video's duration. Cues ending well
    BEFORE the video ends (beyond SUBTITLE_SYNC_MAX_TAIL_GAP_SECONDS) is
    the fingerprint of the estimation/desync bug; cues extending PAST the
    video's end (beyond SUBTITLE_SYNC_MAX_OVERRUN_SECONDS, tighter since
    that's never legitimate) is an even stronger signal.

    DIGESTS: no single audio-to-caption relationship exists (N stories'
    narration, concatenated, then globally sped up) — instead the
    sidecar's expected_final_duration (computed pre-speedup, then scaled
    by the digest's own speed factor — see build_news_digest.py's
    write_digest_sync_sidecar()) is compared against the actual final
    duration. A mismatch beyond DIGEST_TIMING_TOLERANCE_SECONDS means the
    concat/speed-up chain produced something other than what was planned
    — same failure class, surfaced at the whole-digest level."""
    sidecar = _load_subtitle_sync_sidecar(video_path)
    if not sidecar:
        return None, None

    if is_digest:
        expected = sidecar.get("expected_final_duration")
        if expected is None:
            return None, None
        diff = abs(duration - expected)
        if diff > DIGEST_TIMING_TOLERANCE_SECONDS:
            return "major", (
                f"final duration {duration:.1f}s doesn't match the "
                f"pre-speedup narration total scaled by its speed factor "
                f"(expected ~{expected:.1f}s, off by {diff:.1f}s) — the "
                f"concat/speed-up chain may have dropped or mis-timed a segment"
            )
        return None, None

    last_cue_end = sidecar.get("last_cue_end")
    if last_cue_end is None:
        return None, None
    if last_cue_end - duration > SUBTITLE_SYNC_MAX_OVERRUN_SECONDS:
        return "major", (
            f"last subtitle cue ends at {last_cue_end:.1f}s but the video "
            f"is only {duration:.1f}s long — captions extend past the end "
            f"of the file"
        )
    if duration - last_cue_end > SUBTITLE_SYNC_MAX_TAIL_GAP_SECONDS:
        return "major", (
            f"last subtitle cue ends at {last_cue_end:.1f}s but the video "
            f"runs to {duration:.1f}s — captions stop {duration - last_cue_end:.1f}s "
            f"before the video does, the signature of the estimation/desync "
            f"bug (see CAPTION_GENERATION.md)"
        )
    return None, None


def _check_show_diversity(canonical_show):
    """Roadmap item T (diversity guard) — clips only, same scope
    boundary as the removal-rate check below (a digest has no single
    canonical_show). canonical_show isn't known until identify_show.py
    runs, which happens AFTER a clip is already rendered (see
    VIDEO_PIPELINE_ARCHITECT.md's stage order) — there's no earlier point
    to redirect a pre-render decision based on show, so this is surfaced
    here as a MAJOR flag rather than gated before rendering. A clip whose
    show is already over-represented in the last DIVERSITY_WINDOW
    published clips gets pushed toward 'held' for a human glance (same
    treatment as the removal-rate signal), not auto-rejected — a
    saturated show isn't a technical defect in THIS render."""
    if not canonical_show:
        return None, None
    violates, pct, limit = dg.check_show(canonical_show)
    if violates:
        return "major", (
            f"show '{canonical_show}' would reach {pct*100:.0f}% of the last "
            f"{dg.DIVERSITY_WINDOW} published clips (diversity cap "
            f"{limit*100:.0f}%) — content-strategy flag, not a technical "
            f"defect in THIS render"
        )
    return None, None


def _check_show_removal_rate(canonical_show, removal_rate_map):
    """Returns (severity_or_None, message_or_None) — the show-level
    removal-rate signal (roadmap item H, see module docstring's CHECKS
    section). `removal_rate_map` is {canonical_show: row_dict} built from
    db.removal_rate_by_show("youtube") ONCE per quality_gate.py run (see
    main()) rather than re-queried per item — same "load once per run"
    principle edit_clips.py's insight load and generate_captions.py's
    insight load already follow.

    Never called for digests (no single canonical_show applies — see
    db.removal_rate_by_show()'s own "clips only" note) or for a clip
    with no identified show yet — both cases the caller already guards,
    but this function also degrades cleanly to (None, None) if somehow
    called with canonical_show=None."""
    if not canonical_show or not removal_rate_map:
        return None, None
    row = removal_rate_map.get(canonical_show)
    if not row or row.get("removal_rate_pct") is None:
        return None, None
    if row["published_count"] < QUALITY_REMOVAL_RATE_MIN_PUBLISHED:
        return None, None  # too few published clips of this show to trust the rate yet
    if row["removal_rate_pct"] >= QUALITY_REMOVAL_RATE_THRESHOLD_PCT:
        return "major", (
            f"show '{canonical_show}' has a {row['removal_rate_pct']:.0f}% "
            f"removal rate on YouTube ({row['removed_count']}/{row['published_count']} "
            f"published clips pulled) — over the "
            f"{QUALITY_REMOVAL_RATE_THRESHOLD_PCT:.0f}% threshold. This is a "
            f"content-strategy flag, not a technical defect in THIS render"
        )
    return None, None


# ── per-item check pipeline ──────────────────────────────────────────────

def _add_flag(flags, severity, message):
    flags.append({"severity": severity, "message": message})


def run_checks(video_path, caption_paths, is_digest, canonical_show=None, removal_rate_map=None):
    """Runs every check against one finished video, returning (score,
    flag_dicts). Never raises — a check that errors out is logged as an
    'info' flag noting it couldn't run, and simply contributes nothing to
    the score, per PROJECT_RULES.md Rule 5 (per-item isolation) and Rule 3
    (graceful degradation) — a quality check itself is not the one hard
    dependency here; only correctly SCORING what can be checked is.

    canonical_show/removal_rate_map (ADDED — roadmap item H) feed the new
    show-removal-rate check (clips only — see that function's docstring);
    both default to None so this signature stays backward-compatible with
    any caller that doesn't pass them (that check simply no-ops)."""
    flags = []
    score = 100.0

    def deduct(severity, message):
        nonlocal score
        _add_flag(flags, severity, message)
        score -= _SEVERITY_WEIGHT[severity]

    # 1. File integrity + duration -----------------------------------------
    duration = None
    try:
        duration = get_duration(video_path)
    except Exception as e:
        deduct("critical", f"file unreadable by ffprobe: {e}")
        # Nothing else below is meaningful without a duration/decodable
        # file — stop here rather than let every subsequent ffmpeg call
        # fail loudly for the same underlying reason.
        return max(score, 0.0), flags

    if duration < QUALITY_MIN_DURATION_SECONDS:
        deduct("major", f"duration {duration:.1f}s is under the "
                         f"{QUALITY_MIN_DURATION_SECONDS}s floor — likely a truncated render")
    elif duration > QUALITY_MAX_DURATION_SECONDS:
        deduct("major", f"duration {duration:.1f}s exceeds the "
                         f"{QUALITY_MAX_DURATION_SECONDS}s Shorts-eligibility cutoff")

    # 2. Canvas match ---------------------------------------------------------
    try:
        w, h = get_video_dimensions(video_path)
        if (w, h) != (OUT_W, OUT_H):
            deduct("critical", f"canvas is {w}x{h}, expected {OUT_W}x{OUT_H} — "
                                f"a filter chain likely squashed or letterboxed this render")
    except Exception as e:
        _add_flag(flags, "info", f"could not verify canvas dimensions: {e}")

    # 3. Audio silence ----------------------------------------------------
    try:
        gaps, is_fully_silent = _detect_silence_gaps(video_path, duration)
        if is_fully_silent:
            if is_digest:
                deduct("critical", "entire track is silent — voiceover is the one "
                                    "required asset for a digest and it's missing")
            else:
                _add_flag(flags, "info", "clip has no audible dialogue/audio "
                                          "(legitimate for a silent source clip)")
        elif is_digest:
            long_gaps = [g for g in gaps if g[2] >= QUALITY_MAX_SILENCE_GAP_SECONDS]
            if long_gaps:
                worst = max(g[2] for g in long_gaps)
                deduct("major", f"{len(long_gaps)} dead-air gap(s) in narration, "
                                 f"longest {worst:.1f}s (over the "
                                 f"{QUALITY_MAX_SILENCE_GAP_SECONDS}s threshold)")
    except Exception as e:
        _add_flag(flags, "info", f"could not run silence detection: {e}")

    # 4. Black-frame ratio --------------------------------------------------
    try:
        black_ratio = _detect_black_ratio(video_path, duration)
        if black_ratio >= QUALITY_MAX_BLACK_RATIO:
            deduct("major", f"{black_ratio * 100:.0f}% of runtime is black frames "
                             f"(threshold {QUALITY_MAX_BLACK_RATIO * 100:.0f}%)")
    except Exception as e:
        _add_flag(flags, "info", f"could not run black-frame detection: {e}")

    # 5. Timing consistency (subtitle/narration sync — see module docstring) -
    severity, message = _check_timing_consistency(video_path, duration, is_digest)
    if severity:
        deduct(severity, message)

    # 6. Caption refusal scan (second layer, see module docstring) ----------
    for label, path in caption_paths:
        if not path or not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
            if contains_refusal(text):
                deduct("critical", f"{label} contains an LLM-refusal-shaped string")
        except Exception as e:
            _add_flag(flags, "info", f"could not read {label} for refusal scan: {e}")

    # 7. Show-level removal-rate signal (clips only — see module docstring
    # and _check_show_removal_rate()'s own docstring, roadmap item H) -------
    if not is_digest:
        severity, message = _check_show_removal_rate(canonical_show, removal_rate_map)
        if severity:
            deduct(severity, message)

        severity, message = _check_show_diversity(canonical_show)
        if severity:
            deduct(severity, message)

    # 8. Thumbnail presence (non-blocking, see module docstring) -----------
    content_dir = os.path.dirname(video_path)
    thumb_path = os.path.join(content_dir, "thumbnail.jpg")
    if not os.path.isfile(thumb_path) or os.path.getsize(thumb_path) == 0:
        deduct("minor", "thumbnail.jpg missing or empty — will fall back to a "
                         "platform-generated cover frame")

    return max(score, 0.0), flags


def decide(score, flags):
    has_critical = any(f["severity"] == "critical" for f in flags)
    if has_critical or score < QUALITY_REJECT_THRESHOLD:
        return "rejected"
    if score >= QUALITY_APPROVE_THRESHOLD:
        return "approved"
    return "held"


# ── candidate gathering ──────────────────────────────────────────────────

def gather_clip_candidates():
    candidates = []
    for post in db.posts_awaiting_quality_gate(min_stage="thumbnailed"):
        content_dir = os.path.join(FINISHED_DIR, post["channel"], post["post_id"])
        video_path = os.path.join(content_dir, "video.mp4")
        if not os.path.isfile(video_path):
            # File already gone (deleted post-publish some other way, or
            # never actually landed) — nothing to check. Leave ungated
            # rather than guessing; if it truly never gets a video again
            # it'll just sit here, same as any other stalled item.
            continue
        candidates.append({
            "content_key": post["key"],
            "content_type": "clip",
            "video_path": video_path,
            "caption_paths": [
                ("caption_final.txt (TikTok)", os.path.join(content_dir, "caption_final.txt")),
                ("caption_youtube.txt", os.path.join(content_dir, "caption_youtube.txt")),
            ],
            "is_digest": False,
            "canonical_show": post.get("canonical_show"),
        })
    return candidates


def gather_digest_candidates():
    candidates = []
    for digest in db.digests_awaiting_quality_gate(min_stage="thumbnailed"):
        content_dir = os.path.join(FINISHED_DIR, "news", digest["digest_id"])
        video_path = os.path.join(content_dir, "video.mp4")
        if not os.path.isfile(video_path):
            continue
        candidates.append({
            "content_key": digest["digest_id"],
            "content_type": "digest",
            "video_path": video_path,
            "caption_paths": [
                ("caption_final.txt (TikTok)", os.path.join(content_dir, "caption_final.txt")),
                ("caption_youtube.txt", os.path.join(content_dir, "caption_youtube.txt")),
            ],
            "is_digest": True,
        })
    return candidates


# ── main ──────────────────────────────────────────────────────────────────

def main():
    if not QUALITY_GATE_ENABLED:
        print("QUALITY_GATE_ENABLED=0 — skipping this stage entirely. Every "
              "thumbnailed item is treated as already approved (see db.py).")
        return

    db.init_db()
    clips = gather_clip_candidates()
    digests = gather_digest_candidates()

    if not clips and not digests:
        print("Nothing awaiting the quality gate right now.")
        return

    # Loaded ONCE per run (roadmap item H) — see _check_show_removal_rate()'s
    # docstring for why this isn't re-queried per clip. A failure here is
    # non-fatal: the show-removal-rate check just no-ops for every item
    # this run (same as any other check that fails to run — see module
    # docstring's per-check try/except convention), everything else in the
    # gate proceeds normally.
    removal_rate_map = {}
    try:
        removal_rate_map = {r["canonical_show"]: r for r in db.removal_rate_by_show("youtube")}
    except Exception as e:
        print(f"NOTE: could not load show removal-rate data for this run "
              f"(non-fatal, that specific check will be skipped): {e}\n")

    print(f"Quality-gating {len(clips)} clip(s) and {len(digests)} digest(s)...\n")

    counts = {"approved": 0, "held": 0, "rejected": 0}

    for item in clips + digests:
        key = item["content_key"]
        print(f"  {key}...")
        try:
            score, flags = run_checks(
                item["video_path"], item["caption_paths"], item["is_digest"],
                canonical_show=item.get("canonical_show"),
                removal_rate_map=removal_rate_map,
            )
            status = decide(score, flags)
        except Exception as e:
            # Per-item isolation (PROJECT_RULES.md Rule 5) — one item's
            # unexpected failure must never take the rest of the batch
            # down, AND must never silently mark it approved by omission.
            # Leave it ungated (no db.mark_quality call) so it's retried
            # next run rather than permanently stuck on a transient error.
            print(f"    FAILED to run checks (leaving ungated, will retry next "
                  f"run): {e}")
            continue

        flag_strs = [f"[{f['severity']}] {f['message']}" for f in flags]
        db.mark_quality(key, item["content_type"], status, score, flag_strs)
        counts[status] += 1

        icon = {"approved": "\u2713", "held": "\u26a0", "rejected": "\u2717"}[status]
        print(f"    {icon} {status.upper()} — score {score:.0f}")
        for f in flags:
            if f["severity"] != "info":
                print(f"      [{f['severity']}] {f['message']}")

    print(f"\nDone. {counts['approved']} approved, {counts['held']} held for "
          f"review, {counts['rejected']} rejected.")

    held = db.quality_held_items()
    if held:
        print(f"\n{len(held)} item(s) awaiting manual review "
              f"(score between {QUALITY_REJECT_THRESHOLD:.0f}-{QUALITY_APPROVE_THRESHOLD:.0f}, "
              f"no critical issue):")
        for h in held:
            print(f"  {h['content_key']} ({h['content_type']}) — score {h['quality_score']:.0f}")
        print("\nApprove manually once reviewed with:")
        print("  python -c \"import db; db.mark_quality('<content_key>', "
              "'<clip|digest>', 'approved', 100, [])\"")
        print("Or re-run the gate from scratch after fixing the cause with:")
        print("  python reset_stage.py quality <content_key> <clip|digest>")


if __name__ == "__main__":
    main()
