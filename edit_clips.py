"""
Edit Clips (Speed-Optimized)
-----------------------------
Same look as before but significantly faster on a phone CPU:

SPEED CHANGES vs previous version:
  - zoompan REMOVED — replaced with a scale+crop zoom that's computed
    once per clip instead of per-frame. Near-identical visual result,
    fraction of the CPU cost.
  - preset ultrafast — 3-4x faster encode than 'fast', quality
    difference is invisible at TikTok/Shorts viewing size.
  - Hard trim to EDIT_MAX_SECONDS (default 45s) BEFORE the encode pass
    — the single biggest win. A 180s trailer that gets trimmed to 45s
    takes roughly 1/4 the time to encode.
  - Audio stream-copy when no BGM — if there's no BGM track to mix,
    audio is copied not re-encoded (saves another pass).
  - Gemini hook call is async-style (fires while ffprobe runs) so the
    API round-trip doesn't add to wall time.

VISUAL TREATMENT:
  - Blur-background composite for non-9:16 clips
  - Cinematic color grade (contrast + saturation + blue shadow tint)
  - Vignette (edge darkening, focuses attention on center)
  - Static zoom-in crop (5% push-in — same feel as slow zoom, zero cost)
  - White flash on frame 0
  - Gemini-generated hook text (punchy, uppercase, max 6 words)
  - BGM from bgm/ folder mixed under original audio — SELECTION IS NOW
    PERFORMANCE-BIASED, see BGM SELECTION below (was a flat random.choice).

BGM SETUP:
  mkdir bgm/
  Drop in 5-10 tracks named bgm_01.mp3, bgm_02.mp3 etc.
  Sources: YouTube Audio Library, Pixabay.com/music, Mixkit.co/free-music

BGM SELECTION (self-learning loop expansion — item E):
_pick_bgm() used to be a flat random.choice() across every file in
BGM_DIR. It now reads performance_insights.py's cached "content_strategy"
insight (specifically insight["render_params"]["bgm_file"] — see that
module's RENDER_PARAM_CATEGORICAL_KEYS) and, when at least one track on
disk has a reliable, positive performance edge, biases selection toward
those tracks — weighted by how much they're beating the channel average,
so a track that's +40% doesn't get picked exactly as often as one that's
only +6%. This is NOT a hard preference: BGM_EXPLORATION_RATE (default
20%) of picks are still a flat random.choice across every available
track, same "always keep ~20% default/exploration traffic" principle
learned_config.py's registry documents for its own mutated params — a
newly-added track, or one whose past performance was a fluke, still gets
a fair shot at being picked and eventually measured. A track with no
insight data yet (new file, or too few measured clips —
BGM_INSIGHT_MIN_SAMPLE) is simply not in the "reliable" pool; it's still
reachable via the exploration slice or via the plain random fallback
when NO track has reliable data yet (first few weeks of a fresh bgm/
folder, or no cached insight at all).

  bgm_selection_mode in render_params_json records which path a given
  render actually took ("biased" | "exploration" | "no_insight_random" |
  "only_option") — purely informational today (nothing reads it back),
  but it's cheap to record now and means a future audit of "is the bias
  actually kicking in" doesn't need to be reconstructed after the fact.

EMOJI FIX:
  Download NotoColorEmoji.ttf and drop it in your tiktok folder:
  curl -L "https://github.com/googlefonts/noto-emoji/raw/main/fonts/NotoColorEmoji.ttf" \
       -o /storage/emulated/0/tiktok/NotoColorEmoji.ttf

LEARN STAGE (added): BGM_VOLUME and the color grade's saturation value
are no longer .env constants — they're learned_config params (see
learned_config.py's PARAM_REGISTRY: "bgm_volume" and
"edit_color_grade_saturation"), same pattern quality_gate.py already
uses for QUALITY_APPROVE_THRESHOLD. mutator.py now has rules for both
(rule_edit_color_grade_saturation, rule_bgm_volume) that drift them
toward whichever performance_insights.py render-param bucket is
reliably winning — see mutator.py's own docstring. To see or manually
override either value:
  python -c "import learned_config as lc; print(lc.get_param('bgm_volume'))"
  python -c "import learned_config as lc; print(lc.get_param('edit_color_grade_saturation'))"

Every render also writes render_params_json onto the post's row — the
effective bgm_volume/saturation/zoom_crop/edit_max_seconds/bgm_file/
bgm_selection_mode actually used for THIS clip — via db.advance_stage()'s
extra_fields. This is what lets performance_insights.py bucket avg views
by param value, which is in turn what lets both mutator.py's rules above
and this file's own BGM-selection bias actually have real data to read.
"""

import os
import json
import random
import subprocess
from dotenv import load_dotenv

import db
from db import PipelineDB
import learned_config as lc
import diversity_guard as dg

load_dotenv()
lc.init_schema()

# ── Config ───────────────────────────────────────────────────────────────────
EDITED_DIR    = "edited"
BGM_DIR       = os.getenv("BGM_DIR", "bgm")
OUT_W         = 1080
OUT_H         = 1920
BATCH_LIMIT   = int(os.getenv("EDIT_BATCH_LIMIT", "50"))
# LEARNED — was BGM_VOLUME = float(os.getenv("BGM_VOLUME", "0.18")).
# See learned_config.py's "bgm_volume" registry entry.
BGM_VOLUME    = float(lc.get_param("bgm_volume"))
# Hard trim source clips to this many seconds before encoding.
# Single biggest speed win — a 180s trailer trimmed to 45s encodes 4x faster.
EDIT_MAX_SECONDS = int(os.getenv("EDIT_MAX_SECONDS", "45"))
# Static zoom: crop the center 95% of the frame — feels like a pushed-in
# camera without any per-frame computation.
ZOOM_CROP = 0.95
# LEARNED — cinematic color grade saturation. Was hardcoded at 1.35
# inside the eq= filter string below. See learned_config.py's
# "edit_color_grade_saturation" registry entry.
COLOR_GRADE_SATURATION = float(lc.get_param("edit_color_grade_saturation"))

# ── BGM selection bias (self-learning loop expansion — item E) ─────────────
# Same sample-size discipline as generate_captions.py's
# CAPTION_INSIGHT_MIN_SAMPLE / thumbnail_strategy.py's
# THUMBNAIL_INSIGHT_MIN_SAMPLE — a track with only 2-3 measured clips isn't
# a reliable signal yet, it's noise that happens to look like a signal.
BGM_INSIGHT_MIN_SAMPLE = int(os.getenv("BGM_INSIGHT_MIN_SAMPLE", "8"))
# Minimum vs_overall_avg_pct margin for a track to be considered "reliably
# winning" — mirrors mutator.py's RENDER_PARAM_MIN_MARGIN_PCT so the bar
# for "this is a real edge" is the same whether it's driving a mutation
# proposal or a selection bias.
BGM_MIN_MARGIN_PCT = float(os.getenv("BGM_MIN_MARGIN_PCT", "5"))
# Fraction of picks that stay a flat random.choice across ALL available
# tracks regardless of insight data — keeps new/unmeasured tracks in
# rotation and guards against a stale insight permanently starving
# everything except last month's one lucky track.
BGM_EXPLORATION_RATE = float(os.getenv("BGM_EXPLORATION_RATE", "0.2"))

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")


def _probe(path):
    """Returns (width, height, duration) via ffprobe."""
    try:
        r = subprocess.run([
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-show_entries", "format=duration",
            "-of", "csv=p=0", path,
        ], capture_output=True, text=True, timeout=15)
        w = h = 0
        dur = 0.0
        for line in r.stdout.strip().splitlines():
            parts = line.strip().split(",")
            if len(parts) == 2:
                try:
                    w, h = int(parts[0]), int(parts[1])
                except ValueError:
                    pass
            elif len(parts) == 1:
                try:
                    dur = float(parts[0])
                except ValueError:
                    pass
        return w, h, dur
    except Exception:
        return 0, 0, 30.0


def _list_bgm_tracks():
    if not os.path.isdir(BGM_DIR):
        return []
    return [
        os.path.join(BGM_DIR, f) for f in os.listdir(BGM_DIR)
        if f.lower().endswith((".mp3", ".wav", ".m4a", ".aac"))
    ]


def _reliable_bgm_stats(insight):
    """Returns the list of performance_insights.py bgm_file stat dicts
    (each with "value"/"avg_views"/"sample_size"/"vs_overall_avg_pct")
    that clear BGM_INSIGHT_MIN_SAMPLE and BGM_MIN_MARGIN_PCT — the
    "reliable enough to bias toward" pool. Empty list (not None) on any
    missing/insufficient data, so callers never need a None-check on top
    of a length-check."""
    if not insight:
        return []
    bgm_stats = (insight.get("render_params") or {}).get("bgm_file") or []
    return [
        s for s in bgm_stats
        if s["sample_size"] >= BGM_INSIGHT_MIN_SAMPLE
        and s["vs_overall_avg_pct"] > BGM_MIN_MARGIN_PCT
    ]


def _pick_bgm(insight=None):
    """Returns (path_or_None, selection_mode_str). selection_mode is
    always one of "no_tracks" | "only_option" | "no_insight_random" |
    "exploration" | "biased" | "biased_all_saturated" (diversity guard,
    roadmap item T — every reliable track was over its BGM diversity cap,
    so the pick fell back to the full reliable pool instead of excluding
    everything) — recorded into render_params_json purely for future
    auditability (see module docstring's BGM SELECTION section), never
    read back by anything today.

    Biasing logic: build the "reliable" pool by cross-referencing
    performance_insights.py's bgm_file stats against files that ACTUALLY
    still exist in BGM_DIR right now (a track can be measured from past
    renders and later deleted/renamed — stale insight data must never
    point at a file that isn't there). If that pool is non-empty and this
    call doesn't land in the exploration slice, weighted-random-pick
    (weight = vs_overall_avg_pct, so a bigger edge is proportionally more
    likely) among the reliable pool. Every other path falls back to a
    flat random.choice across every available track."""
    tracks = _list_bgm_tracks()
    if not tracks:
        return None, "no_tracks"
    if len(tracks) == 1:
        return tracks[0], "only_option"

    by_filename = {os.path.basename(t): t for t in tracks}
    reliable = [s for s in _reliable_bgm_stats(insight) if s["value"] in by_filename]

    if not reliable:
        return random.choice(tracks), "no_insight_random"

    if random.random() < BGM_EXPLORATION_RATE:
        return random.choice(tracks), "exploration"

    # DIVERSITY GUARD (roadmap item T) — a track that's reliably winning
    # can still be over its 20% cap of the last DIVERSITY_WINDOW published
    # clips (see diversity_guard.py). Excluding saturated tracks from the
    # weighted pool BEFORE the weighted pick keeps the bias mechanism
    # itself intact (still picks the best of what's left, still respects
    # BGM_MIN_MARGIN_PCT) while preventing it from converging onto one
    # track indefinitely. If every reliable track is currently saturated,
    # fall back to the plain reliable pool rather than blocking the pick
    # entirely — per PROJECT_RULES.md Rule 3.
    not_saturated = [
        s for s in reliable
        if not dg.check_bgm_file(s["value"])[0]
    ]
    pool = not_saturated or reliable

    weights = [max(s["vs_overall_avg_pct"], 0.01) for s in pool]
    chosen = random.choices(pool, weights=weights, k=1)[0]
    selection_mode = "biased" if pool is not_saturated else "biased_all_saturated"
    return by_filename[chosen["value"]], selection_mode


def edit_video(input_path, output_path, caption_raw="", insight=None):
    """Returns the render_params dict actually used for this clip (bgm
    volume/file/selection-mode, saturation, zoom crop, max seconds) —
    caller persists it via db.advance_stage(..., render_params_json=
    json.dumps(...)) so performance_insights.py can eventually bucket avg
    views by these values instead of only ever seeing whatever the
    CURRENT learned_config value happens to be at report time."""
    w, h, dur = _probe(input_path)
    clip_dur = min(dur, EDIT_MAX_SECONDS) if dur > 0 else EDIT_MAX_SECONDS

    # ── Video filter chain (speed-optimized) ─────────────────────────────
    # 1. Blur-bg composite (scale to fill + blur + overlay centered fg)
    # 2. Static zoom crop (center 95% of frame — no per-frame computation)
    # 3. Color grade: contrast/saturation boost + blue shadow tint
    # 4. Vignette
    # 5. White flash on frame 0
    # NOTE: hook text is NOT burned here — add_hook_overlay.py handles
    # that after generate_captions.py has produced the caption_hook field.

    # Cropped dimensions for 5% zoom
    crop_w = int(OUT_W * ZOOM_CROP)
    crop_h = int(OUT_H * ZOOM_CROP)
    crop_x = (OUT_W - crop_w) // 2
    crop_y = (OUT_H - crop_h) // 2

    color_grade = (
        f"eq=contrast=1.15:brightness=0.02:saturation={COLOR_GRADE_SATURATION},"
        "curves=r='0/0 0.5/0.47 1/1':g='0/0 0.5/0.5 1/1':b='0/0.04 0.5/0.52 1/1'"
    )

    filter_complex = (
        # Blur background
        f"[0:v]scale={OUT_W}:{OUT_H}:force_original_aspect_ratio=increase,"
        f"crop={OUT_W}:{OUT_H},gblur=sigma=25,eq=brightness=-0.15[bg];"
        # Foreground: scale to fit
        f"[0:v]scale={OUT_W}:{OUT_H}:force_original_aspect_ratio=decrease,"
        f"pad={OUT_W}:{OUT_H}:(ow-iw)/2:(oh-ih)/2:color=black@0[fg];"
        # Composite
        f"[bg][fg]overlay=0:0,"
        # Static zoom crop (fast — single crop, no per-frame math)
        f"crop={crop_w}:{crop_h}:{crop_x}:{crop_y},"
        f"scale={OUT_W}:{OUT_H},"
        # Color grade + vignette + flash
        f"{color_grade},"
        f"vignette=PI/4,"
        f"fade=t=in:st=0:d=0.08:color=white[outv]"
    )

    # ── Audio ─────────────────────────────────────────────────────────────
    bgm, bgm_selection_mode = _pick_bgm(insight)
    inputs = ["-i", input_path]

    if bgm:
        inputs += ["-i", bgm]
        audio_filter = (
            f"[0:a]volume=1.0[orig];"
            f"[1:a]volume={BGM_VOLUME},aloop=loop=-1:size=2e+09[bgm];"
            f"[orig][bgm]amix=inputs=2:duration=first[aout]"
        )
        full_filter = filter_complex + ";" + audio_filter
        map_args = ["-map", "[outv]", "-map", "[aout]"]
        audio_codec = ["-c:a", "aac", "-b:a", "192k"]
    else:
        full_filter = filter_complex
        map_args = ["-map", "[outv]", "-map", "0:a?"]
        audio_codec = ["-c:a", "copy"]  # stream-copy when no BGM — faster

    cmd = [
        "ffmpeg", "-y",
        *inputs,
        "-t", str(clip_dur),      # trim BEFORE encode — biggest speed win
        "-filter_complex", full_filter,
        *map_args,
        "-c:v", "libx264",
        "-preset", "ultrafast",   # 3-4x faster than 'fast'
        "-crf", "26",             # slightly lower quality, fine for phone screens
        "-pix_fmt", "yuv420p",    # force 4:2:0 — filter chain otherwise drifts to
                                   # yuv444p, which Android hw decoders can't play
                                   # (black video, audio still works)
        *audio_codec,
        output_path,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed:\n{result.stderr[-1500:]}")

    return {
        "bgm_volume": BGM_VOLUME,
        "bgm_file": os.path.basename(bgm) if bgm else None,
        "bgm_selection_mode": bgm_selection_mode,
        "color_grade_saturation": COLOR_GRADE_SATURATION,
        "zoom_crop": ZOOM_CROP,
        "edit_max_seconds": EDIT_MAX_SECONDS,
        "clip_duration_used": clip_dur,
    }


def main():
    pdb = PipelineDB()
    pending = pdb.posts_with_status("downloaded", media_type="video", limit=BATCH_LIMIT)

    if not pending:
        print("No videos need editing right now. Run fetch_youtube_trailers.py first.")
        return

    # Loaded ONCE per run, not per clip — performance_insights.py only
    # updates this insight daily-ish anyway, and re-fetching it per clip
    # would just be repeated identical DB reads. None until that script
    # has produced a "content_strategy" insight with real bgm_file data;
    # _reliable_bgm_stats()/_pick_bgm() both already treat None as "no
    # bias data, fall back to plain random" with zero special-casing
    # needed here.
    insight = db.get_insight("content_strategy")
    if insight:
        reliable = _reliable_bgm_stats(insight)
        if reliable:
            print(f"BGM selection bias ACTIVE this run — {len(reliable)} track(s) "
                  f"with a reliable edge (from {insight['sample_size']} measured "
                  f"clips), {BGM_EXPLORATION_RATE * 100:.0f}% of picks still "
                  f"exploration.\n")

    edited_count = failed_count = 0

    for post in pending:
        source     = pdb.get_source(post["source_id"])
        channel    = source["handle"] if source else "unknown_channel"
        post_id    = post["external_id"]
        input_path = post["raw_media_path"]
        caption    = post.get("caption_raw", "")

        if not input_path or not os.path.isfile(input_path):
            reason = f"raw_media_path missing: {input_path!r}"
            print(f"FAILED {channel}/{post_id}: {reason}")
            pdb.mark_failed(post["id"], reason)
            failed_count += 1
            continue

        out_dir   = os.path.join(EDITED_DIR, channel, post_id)
        os.makedirs(out_dir, exist_ok=True)
        out_video = os.path.join(out_dir, "video.mp4")

        print(f"Editing {channel}/{post_id}...")
        try:
            render_params = edit_video(input_path, out_video, caption, insight=insight)
        except RuntimeError as e:
            print(f"  FAILED: {e}")
            pdb.mark_failed(post["id"], str(e))
            failed_count += 1
            continue

        with open(os.path.join(out_dir, "caption_raw.txt"), "w", encoding="utf-8") as f:
            f.write(caption or "")

        pdb.advance_stage(post["id"], "edited", edited_path=out_video,
                           render_params_json=json.dumps(render_params))
        edited_count += 1
        print(f"  Done → {out_video}  (bgm_volume={render_params['bgm_volume']}, "
              f"saturation={render_params['color_grade_saturation']}, "
              f"bgm={render_params['bgm_file'] or 'none'} "
              f"[{render_params['bgm_selection_mode']}])")

    print(f"\nDone. Edited {edited_count} clip(s), {failed_count} failed.")


if __name__ == "__main__":
    main()
