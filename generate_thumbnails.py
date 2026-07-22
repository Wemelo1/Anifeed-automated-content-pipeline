"""
Thumbnail Generator
--------------------
For every finished clip AND every finished news digest, picks the sharpest
candidate frame (variance-of-Laplacian scoring, see video_utils.py) and
produces:

  thumbnail.jpg                 → used directly as the YouTube thumbnail
  thumbnail_timestamp_ms.txt    → same moment, in ms, for TikTok's
                                   video_cover_timestamp_ms param

One scoring pass, two platforms — avoids picking a blurry/motion-blurred
frame just because it happened to be at a fixed timestamp.

Skips anything that already has thumbnail.jpg. Safe to re-run.

CLEAN (PRE-SUBTITLE) FRAME SAMPLING (clips only): candidate frames are
sampled from edited/<channel>/<post_id>/video.mp4 (the pre-burn_subtitles.py
render) instead of the finished, subtitled video whenever that file still
exists — sampling from the subtitled video risked picking a frame with
caption text sitting on top of it, which looks messy and hurts CTR. See
_frame_source_for_clip()'s docstring for the full mechanics and the
digest-side limitation (no unsubtitled equivalent exists for digests).

THUMBNAIL PATTERN CLASSIFICATION (clips only):
Once a clip's thumbnail frame is picked, it's classified into a visual
pattern (face_forward / action / scenic / text_heavy — see
thumbnail_strategy.py) via one Groq vision call, and stored on the post's
row via db.mark_stage(..., thumbnail_pattern=...). This is what lets
performance_insights.py eventually answer "does a face-forward thumbnail
actually outperform an action shot for this channel?" — sharpness alone
(the score already being computed) is a technical quality metric, not a
click-through metric.

SELECTION BIASING (clips only, once real pattern data exists):
Once performance_insights.py's cached insight has a reliable
recommended_pattern() (see thumbnail_strategy.py — gated at
THUMBNAIL_INSIGHT_MIN_SAMPLE measured clips per pattern, not just "some
data exists"), thumbnail selection stops being pure-sharpness. Instead:
pick_candidate_frames() pulls every candidate (not just the winner),
the TOP_N_FOR_PATTERN_BIAS sharpest ones get classified, and the sharpest
CLASSIFIED candidate matching the recommended pattern wins over the
plain-sharpest one — falling back to plain sharpness if none of the
classified candidates match. Only the top few candidates are classified,
not all NUM_CANDIDATES, since this is a soft nudge among already-good
frames, not worth doubling the Groq vision spend on every clip to
guarantee a match. Before real data exists, recommended_pattern() returns
None and this whole path is a no-op — behavior is identical to before.

Classification is skipped with a clear one-line note (not per-clip noise)
if GROQ_API_KEY isn't set — thumbnail generation itself never depends on
it, same as every other optional-enrichment step in this pipeline
(canonical show ID, visual hints, etc. all degrade the same way). A failed
or skipped classification never blocks or delays writing the actual
thumbnail file; thumbnail_pattern is simply left NULL, which
performance_insights.py already excludes cleanly from its ranking.

SCOPE — CLIPS ONLY, NOT DIGESTS: a digest thumbnail represents an entire
multi-story roundup, not one show/one clip — there's no single
"canonical_show" a digest's performance could be attributed to the way a
clip's is, so classifying its thumbnail pattern wouldn't currently feed
anything in performance_insights.py. digests.thumbnail_pattern exists in
the schema for future use, but is deliberately left NULL here rather than
spending a Groq call for a signal nothing reads yet. (Same "clips only"
boundary add_hook_overlay.py already draws for its own reasons.)

RETROACTIVE NOTE: this only classifies pattern for thumbnails generated
FROM THIS POINT FORWARD. Clips that already have a thumbnail.jpg on disk
from before this feature existed are skipped by the normal "already has a
thumbnail" check and will never get a pattern recorded unless their
'thumbnailed' stage is explicitly reset (see reset_stage.py) and
thumbnail.jpg deleted so they're picked up again.

SETUP:
  ffmpeg on PATH
  pip install numpy python-dotenv groq   (groq/dotenv optional — only
                                           needed for pattern classification)

LEARN STAGE (added): TOP_N_FOR_PATTERN_BIAS is no longer read from .env —
it's now a learned_config param (see learned_config.py's PARAM_REGISTRY
and mutator.py's rule_thumbnail_bias_top_n), same pattern quality_gate.py
already uses for QUALITY_APPROVE_THRESHOLD. mutator.py nudges this ±1/run
based on whether the best thumbnail pattern shows a real, reliable edge —
lowest-risk param in the registry, since a wrong value only costs a few
extra/fewer Groq vision calls, not a content-quality risk. To see or
manually override the current value:
`python -c "import learned_config as lc; print(lc.get_param('thumbnail_bias_top_n'))"`.

USAGE:
  python generate_thumbnails.py   (run after generate_captions.py, before publishing)
"""

import os
from dotenv import load_dotenv
from video_utils import pick_sharpest_frame, pick_candidate_frames, get_duration
import db
import learned_config as lc
import thumbnail_strategy as ts
import diversity_guard as dg

load_dotenv()
lc.init_schema()

FINISHED_DIR = "finished"
# Pre-subtitle source for clip thumbnail sampling — see
# _frame_source_for_clip()'s docstring below for why this exists.
EDITED_DIR = "edited"
NUM_CANDIDATES = 6

# How many of the sharpest candidates get classified when selection-biasing
# is active (see module docstring SELECTION BIASING section) — deliberately
# less than NUM_CANDIDATES to bound the extra Groq vision spend this adds.
TOP_N_FOR_PATTERN_BIAS = int(lc.get_param("thumbnail_bias_top_n"))

GROQ_API_KEY = os.getenv("GROQ_API_KEY")


def get_groq_client():
    """Returns a Groq client for thumbnail pattern classification, or None
    if GROQ_API_KEY isn't set — callers must treat None as 'skip
    classification', not an error. Lazily imports groq so a missing/
    not-yet-installed groq package doesn't break thumbnail generation
    itself, only the optional classification step."""
    if not GROQ_API_KEY:
        return None
    from groq import Groq
    return Groq(api_key=GROQ_API_KEY)


def _frame_source_for_clip(channel, post_id, fallback_path):
    """Subtitles are burned directly into finished/<channel>/<post_id>/
    video.mp4 by burn_subtitles.py — sampling thumbnail candidates from
    it risks picking a moment with caption text sitting on screen, which
    reads as messy/unprofessional and hurts CTR. edited/<channel>/
    <post_id>/video.mp4 (written by edit_clips.py, read but never
    deleted by burn_subtitles.py) is the exact same clip BEFORE subtitles
    were burned in — same content, same duration (subtitle burn-in is a
    straight re-encode, it doesn't trim or pad the timeline), so a
    proportional timestamp picked against it lands at the same visual
    moment in the finished video. add_hook_overlay.py (which DOES modify
    the opening ~2.2s) runs AFTER thumbnailing in the pipeline order —
    see VIDEO_PIPELINE_ARCHITECT.md's stage order — so there's no
    hook-overlay-vs-thumbnail-frame mismatch to worry about either.

    Falls back to `fallback_path` (the finished/subtitled video) when the
    edited/ file is missing — a clip that predates this feature, or one
    that went through burn_subtitles.py's no-dialogue copy-through path
    (where finished/.../video.mp4 IS an untouched copy of the edited
    video already — nothing to gain from switching, nothing lost by not).
    Never blocks thumbnail generation over a missing edited/ file, per
    PROJECT_RULES.md Rule 3 (graceful degradation).

    DIGESTS ARE NOT COVERED — see find_digests_needing_thumbnails(),
    which intentionally does not call this. A digest's captions are
    burned in during build_news_digest.py's single-pass render; there is
    no separate pre-caption intermediate file saved to disk to sample
    from instead. This is an accepted limitation, not an oversight."""
    edited_path = os.path.join(EDITED_DIR, channel, post_id, "video.mp4")
    return edited_path if os.path.isfile(edited_path) else fallback_path


def find_clips_needing_thumbnails():
    found = []
    if not os.path.isdir(FINISHED_DIR):
        return found
    for channel in os.listdir(FINISHED_DIR):
        if channel == "news":
            continue
        channel_path = os.path.join(FINISHED_DIR, channel)
        if not os.path.isdir(channel_path):
            continue
        for post_id in os.listdir(channel_path):
            post_path = os.path.join(channel_path, post_id)
            finished_video_path = os.path.join(post_path, "video.mp4")
            thumb_path = os.path.join(post_path, "thumbnail.jpg")
            if os.path.isfile(finished_video_path) and not os.path.isfile(thumb_path):
                # post_path (content_dir, where thumbnail.jpg gets written)
                # stays the FINISHED directory regardless — only the frame
                # SOURCE changes. See _frame_source_for_clip()'s docstring.
                frame_source = _frame_source_for_clip(channel, post_id, finished_video_path)
                found.append((f"{channel}/{post_id}", post_path, frame_source))
    return found


def find_digests_needing_thumbnails():
    found = []
    news_dir = os.path.join(FINISHED_DIR, "news")
    if not os.path.isdir(news_dir):
        return found
    for digest_id in os.listdir(news_dir):
        digest_path = os.path.join(news_dir, digest_id)
        video_path = os.path.join(digest_path, "video.mp4")
        thumb_path = os.path.join(digest_path, "thumbnail.jpg")
        if os.path.isfile(video_path) and not os.path.isfile(thumb_path):
            found.append((digest_id, digest_path, video_path))
    return found


def build_thumbnail(content_key, content_dir, video_path, content_type, client=None, recommended_pattern=None):
    """content_type is "clip" or "digest" — determines whether pattern
    classification + the direct db.mark_stage(thumbnail_pattern=...) call
    happen (clips only; see module docstring SCOPE section).
    recommended_pattern (clips only) activates selection-biasing — see
    module docstring SELECTION BIASING section.

    video_path is the FRAME SOURCE to sample candidates from — for clips
    this is edited/.../video.mp4 (pre-subtitle) when available, see
    _frame_source_for_clip(); for digests it's always finished/news/.../
    video.mp4. content_dir (where thumbnail.jpg actually gets written) is
    always the finished/ directory regardless — the two are independent
    now, don't assume video_path and content_dir point at the same
    folder."""
    print(f"  {content_key}...")
    try:
        pattern = None  # set below if classification happens during selection biasing

        if content_type == "clip" and client is not None and recommended_pattern:
            candidates = pick_candidate_frames(
                video_path, num_candidates=NUM_CANDIDATES, work_dir=content_dir
            )
            if not candidates:
                print("    FAILED: could not extract any candidate frames")
                return False

            # Classify only the top TOP_N_FOR_PATTERN_BIAS sharpest — see
            # module docstring for why not all NUM_CANDIDATES.
            classified = []
            for i, (fp, fts, score) in enumerate(candidates):
                c_pattern = ts.classify_pattern(client, fp, content_key=content_key) if i < TOP_N_FOR_PATTERN_BIAS else None
                classified.append([fp, fts, score, c_pattern])

            # classified is already sharpest-first (pick_candidate_frames'
            # contract), so the first pattern match IS the sharpest
            # available match — no further ranking needed.
            match = next((c for c in classified if c[3] == recommended_pattern), None)
            chosen = match if match else classified[0]
            best_frame, best_ts, score, pattern = chosen

            if match and match is not classified[0]:
                print(f"    Selection biased toward recommended pattern "
                      f"'{recommended_pattern}' — picked t={best_ts:.2f}s "
                      f"over sharpest t={classified[0][1]:.2f}s")

            # pick_candidate_frames() leaves cleanup of EVERY returned file
            # to the caller (unlike pick_sharpest_frame(), which only ever
            # hands back the one it kept) — remove everything except the
            # frame that actually won.
            for fp, _, _, _ in classified:
                if fp != best_frame and os.path.exists(fp):
                    os.remove(fp)
        else:
            best_frame, best_ts, score = pick_sharpest_frame(
                video_path, num_candidates=NUM_CANDIDATES, work_dir=content_dir
            )
            if best_frame is None:
                print("    FAILED: could not extract any candidate frames")
                return False

        final_path = os.path.join(content_dir, "thumbnail.jpg")
        os.replace(best_frame, final_path)

        ts_path = os.path.join(content_dir, "thumbnail_timestamp_ms.txt")
        with open(ts_path, "w") as f:
            f.write(str(int(best_ts * 1000)))

        print(f"    -> {final_path} (t={best_ts:.2f}s, sharpness={score:.1f})")

        if content_type == "clip" and client is not None:
            # Only classify if selection-biasing didn't already do it for
            # this exact frame (avoids a redundant second Groq vision call
            # on the winner).
            if pattern is None:
                pattern = ts.classify_pattern(client, final_path, content_key=content_key)
            if pattern:
                print(f"    pattern: {pattern}")
            try:
                channel, post_id = content_key.split("/", 1)
                db.mark_stage(channel, post_id, "thumbnailed",
                               media_type="video", thumbnail_pattern=pattern)
            except Exception as e:
                # The thumbnail file itself is already saved successfully at
                # this point — a DB write failure here shouldn't be reported
                # as a thumbnail FAILURE. sync_posts_from_filesystem() in
                # main() is the safety net that'll pick this up (without a
                # pattern) if this direct mark_stage call didn't go through.
                print(f"    NOTE: could not record thumbnail_pattern in db.py "
                      f"({e}) — thumbnail file itself is fine, this only "
                      f"affects performance_insights.py's pattern ranking")

        return True
    except Exception as e:
        print(f"    FAILED: {e}")
        return False


def main():
    clips = find_clips_needing_thumbnails()
    digests = find_digests_needing_thumbnails()

    if not clips and not digests:
        print("No clips or digests need thumbnails right now.")
        return

    db.init_db()
    client = get_groq_client()
    if clips and client is None:
        print("NOTE: GROQ_API_KEY not set — thumbnails will still be generated "
              "normally, but pattern classification (face_forward/action/"
              "scenic/text_heavy) will be skipped for this run.\n")

    # Loaded once per run, not per clip — cheap, and performance_insights.py
    # only updates this daily-ish anyway. recommended_pattern() returns None
    # until enough classified clips exist (see thumbnail_strategy.py's
    # THUMBNAIL_INSIGHT_MIN_SAMPLE), at which point build_thumbnail's
    # selection-biasing path activates automatically — no other change
    # needed here or at that point.
    insight = db.get_insight("content_strategy")
    recommended = ts.recommended_pattern(insight)

    # DIVERSITY GUARD (roadmap item T) — a pattern that's winning on
    # performance can still be OVER quota for the last DIVERSITY_WINDOW
    # published clips (default cap 30%, see diversity_guard.py). Rather
    # than let selection-biasing keep reinforcing an already-saturated
    # pattern, fall back to the next-best classified pattern that isn't
    # saturated, or drop biasing entirely (plain sharpest-frame selection)
    # if every ranked pattern is saturated — never blocks thumbnail
    # generation itself, per PROJECT_RULES.md Rule 3.
    if recommended:
        violates, pct, limit = dg.check_thumbnail_pattern(recommended)
        if violates:
            ranked_patterns = [p["pattern"] for p in (insight.get("thumbnail_patterns") or [])]
            alternative = dg.pick_non_saturated("thumbnail_pattern", ranked_patterns)
            if alternative and alternative != recommended:
                print(f"Diversity guard: '{recommended}' would reach "
                      f"{pct*100:.0f}% of the last {dg.DIVERSITY_WINDOW} published "
                      f"clips (cap {limit*100:.0f}%) — biasing toward "
                      f"'{alternative}' instead this run.\n")
                recommended = alternative
            else:
                print(f"Diversity guard: '{recommended}' is over its "
                      f"{limit*100:.0f}% cap and no alternative pattern has "
                      f"reliable data — dropping selection bias this run "
                      f"(plain sharpest-frame selection).\n")
                recommended = None

    if recommended:
        print(f"Selection biasing ACTIVE this run — preferring '{recommended}' "
              f"among sharp candidates where possible (from {insight['sample_size']} "
              f"measured clips).\n")

    done = 0
    if clips:
        print(f"Generating thumbnails for {len(clips)} clip(s)...")
        for content_key, content_dir, video_path in clips:
            # Exploration slice (roadmap item T) — decided per clip, not
            # per run, so a run with 10 clips doesn't accidentally
            # exclude bias from ALL of them or NONE of them.
            per_clip_recommended = None if dg.should_explore() else recommended
            if build_thumbnail(content_key, content_dir, video_path, "clip",
                                client=client, recommended_pattern=per_clip_recommended):
                done += 1

    if digests:
        print(f"Generating thumbnails for {len(digests)} digest(s)...")
        for content_key, content_dir, video_path in digests:
            if build_thumbnail(content_key, content_dir, video_path, "digest"):
                done += 1

    print(f"\nDone. Generated {done} thumbnail(s).")

    # Keep the status ledger current — thumbnailed is the last pipeline
    # stage before publishing, so publish_youtube/publish_tiktok can query it.
    # For clips, build_thumbnail() above already called db.mark_stage()
    # directly (so thumbnail_pattern gets recorded) — this call is now a
    # no-op safety net for those (is_stage_done() skips anything already
    # marked). It's still the ONLY thing that marks digests thumbnailed,
    # since those don't go through the direct mark_stage path above.
    db.sync_posts_from_filesystem()
    db.sync_digests_from_filesystem()


if __name__ == "__main__":
    main()
