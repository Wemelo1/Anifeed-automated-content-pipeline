"""
Thumbnail Pattern Classification
-----------------------------------
Adds a cheap classification layer on top of video_utils.py's existing
pick_sharpest_frame() so generate_thumbnails.py can not only pick a sharp
frame, but know (and record) WHAT KIND of frame it picked — a face
close-up, an action shot, a scenic/establishing shot, etc. Without this,
performance_insights.py has no way to answer "does a face-forward
thumbnail actually outperform an action shot for this channel?" — sharpness
alone is a technical quality metric, not a click-through metric, and
pick_sharpest_frame() has no concept of WHAT it picked, only how in-focus
it is.

WHY GROQ VISION, NOT A NEW CV DEPENDENCY:
identify_show.py already calls Groq's vision model per clip for show
identification — this reuses that same API/credential path instead of
adding Haar cascades / dlib / mediapipe, which would need to build cleanly
on Termux and would only solve face-detection, not the other three
categories below. One extra vision call per clip, only at
thumbnail-selection time, is a small marginal cost on top of what
identify_show.py already spends per clip elsewhere in the pipeline.

CATEGORIES (deliberately small and mutually exclusive — this is a content-
strategy signal for performance_insights.py to rank, not a fine-grained
visual taxonomy):
  face_forward  — a character's face is the clear visual focus (portrait/
                   close-up framing)
  action        — motion, combat, a dynamic pose; face is not the focus
  scenic        — an establishing/background shot, no dominant character
  text_heavy    — on-screen text/UI/a title card dominates the frame
  other         — none of the above / ambiguous

WIRED IN (classify + record): generate_thumbnails.py calls
classify_pattern() on its chosen frame and passes the result through to
db.mark_stage(..., thumbnail_pattern=pattern). performance_insights.py
picks the resulting data up automatically.

SELECTION BIASING: recommended_pattern() is called from
generate_thumbnails.py to bias candidate selection toward whatever
pattern's actually been winning — see that module's build_thumbnail().

ENGAGEMENT PREFERENCE (added — self-learning loop expansion, roadmap item
G): recommended_pattern() now checks performance_insights.py's
insight["engagement"]["thumbnail_patterns"] breakdown FIRST (ranked by
engagement_score = (likes + comments*2 + shares*3)/views, not raw views —
see that module's ENGAGEMENT_MIN_SAMPLE for why this section is only
populated once a higher, more-trusted sample size is reached), and only
falls back to the views-based insight["thumbnail_patterns"] breakdown when
the engagement section isn't populated yet or doesn't have a reliable
pattern of its own. This is the concrete "move from view-count
optimization to engagement-score optimization once you have enough data"
behavior from the self-learning roadmap — it happens automatically, no
manual cutover step, and degrades gracefully back to the exact previous
(views-only) behavior whenever engagement data isn't there yet.

USAGE:
  from groq import Groq
  import thumbnail_strategy as ts

  client = Groq(api_key=...)
  pattern = ts.classify_pattern(client, frame_path)
  # -> "face_forward" / "action" / "scenic" / "text_heavy" / "other" / None

  # Later, in generate_thumbnails.py, to bias candidate SELECTION (not
  # just tag it after the fact) toward whatever's actually been winning:
  insight = db.get_insight("content_strategy")
  best_pattern = ts.recommended_pattern(insight)  # -> "face_forward" or None
"""

import base64
import os
import re

import cost_tracker as ct

# meta-llama/llama-4-scout-17b-16e-instruct was deprecated by Groq on
# 2026-06-17 and is now fully removed (404 model_not_found, not a transient
# outage — confirmed against https://console.groq.com/docs/deprecations).
# qwen/qwen3.6-27b is Groq's documented multimodal migration target for
# Scout — this is the exact same fix identify_show.py's VISION_MODEL
# already got; this module has its own separate VISION_MODEL constant that
# was simply missed at the time since it lives in a different file. If
# Groq deprecates this one too, check https://console.groq.com/docs/vision
# for whatever's current.
VISION_MODEL = "qwen/qwen3.8-27b"  # was qwen/qwen3.6-27b — deprecated, shutdown 2026-09-14

VALID_PATTERNS = {"face_forward", "action", "scenic", "text_heavy", "other"}

# FIXED (same bug identify_show.py's VISION_MODEL already hit and fixed —
# this call site uses the SAME qwen3.6-27b reasoning model but was missed
# at the time since it lives in a different file). qwen3.6-27b emits a
# "<think>...</think>" reasoning block before its actual answer. This call
# was using max_tokens=10 with no reasoning_effort set, so the response
# was being cut off MID-REASONING before the model ever produced one of
# VALID_PATTERNS — silently returning None (no exception, nothing to log)
# whenever the model's reasoning didn't happen to fit in 10 tokens by
# chance. Fixed the same way identify_show.py was: (a) reasoning_effort=
# "none" so the model spends minimal budget thinking before answering,
# (b) max_tokens raised so a real answer isn't truncated even if some
# reasoning happens anyway, (c) _strip_think_tags() as defense-in-depth —
# any <think>...</think> block is stripped before matching against
# VALID_PATTERNS, and an UNCLOSED <think> tag (still truncated somehow)
# causes the result to be discarded as empty rather than matched against
# garbage. Per this project's "private underscore-prefixed helpers are
# copied, not imported" convention, this is the same _strip_think_tags()
# logic as identify_show.py's, copied rather than imported across files.
_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _strip_think_tags(text):
    if not text:
        return text
    text = _THINK_BLOCK_RE.sub("", text)
    if "<think>" in text.lower():
        return ""
    return text.strip()


# Same reasoning and threshold as generate_captions.py's
# CAPTION_INSIGHT_MIN_SAMPLE — performance_insights.py itself uses
# min_sample=1 for thumbnail_patterns in its printed report, which is fine
# for human visibility but not for something that changes what thumbnail
# gets picked. Without this, recommended_pattern() would happily declare a
# "winning" pattern off of 1-2 classified clips the moment enough data
# exists to call it at all — the exact bug found (and fixed) in
# generate_captions.py's build_style_addendum/show_performance_note.
# Applied identically to BOTH the engagement-ranked and views-ranked
# pattern breakdowns — the bar for "reliable enough to steer selection"
# doesn't change just because the underlying metric changed.
THUMBNAIL_INSIGHT_MIN_SAMPLE = int(os.getenv("THUMBNAIL_INSIGHT_MIN_SAMPLE", "8"))

CLASSIFY_PROMPT = (
    "You are looking at a single candidate video thumbnail frame from an "
    "anime clip. Classify its VISUAL composition into exactly one category:\n"
    "- face_forward: a character's face/close-up is the clear focus\n"
    "- action: motion, combat, or a dynamic pose; face is not the focus\n"
    "- scenic: an establishing/background shot with no dominant character\n"
    "- text_heavy: on-screen text, UI, or a title card dominates the frame\n"
    "- other: none of the above clearly applies\n\n"
    "Respond with ONLY the category name, exactly as written above. No "
    "explanation, no punctuation, nothing else."
)


def classify_pattern(client, frame_path, content_key=None):
    """Returns one of VALID_PATTERNS, or None if classification failed
    (network error, malformed/empty response, an unclosed <think> block
    that couldn't be stripped cleanly, etc.) — caller should treat None
    the same as 'no pattern recorded' and continue with thumbnail
    generation as normal. A failed classification must never block or
    delay picking/saving the actual thumbnail — this is a nice-to-have
    analytics tag on top of that, not a dependency of it.

    content_key (roadmap item S, optional) — when given, this call is
    logged via cost_tracker.log_call() for cost-per-1000-views/mutator.py's
    cost-aware thumbnail_bias_top_n check. Omit for any caller that
    doesn't have a content_key handy — cost tracking degrades to simply
    not attributing that call to a specific clip, never blocks
    classification itself."""
    try:
        with open(frame_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        resp = client.chat.completions.create(
            model=VISION_MODEL,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": CLASSIFY_PROMPT},
                    {"type": "image_url",
                     "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                ],
            }],
            temperature=0.1,
            max_tokens=300,
            reasoning_effort="none",
        )
        ct.log_call("groq", "vision_call", content_key=content_key, content_type="clip")
        result = (resp.choices[0].message.content or "").strip().lower()
        result = _strip_think_tags(result)
        return result if result in VALID_PATTERNS else None
    except Exception as e:
        print(f"    Thumbnail pattern classification failed (non-fatal): {e}")
        return None


def _best_reliable_pattern(candidates):
    """Shared logic for both the engagement-ranked and views-ranked path
    below: needs at least 2 patterns clearing THUMBNAIL_INSIGHT_MIN_SAMPLE
    to rank against each other, and the top one must actually be beating
    the average — same "don't manufacture a signal that isn't there" gate
    either metric has to clear. Returns the pattern name, or None."""
    reliable = [p for p in candidates
                if p["sample_size"] >= THUMBNAIL_INSIGHT_MIN_SAMPLE]
    if len(reliable) < 2:
        return None
    best = reliable[0]
    if best["vs_overall_avg_pct"] <= 0:
        return None
    return best["pattern"]


def recommended_pattern(insight):
    """Reads performance_insights.py's cached 'content_strategy' insight
    and returns the best-performing thumbnail pattern name, or None if
    there's no insight yet, no pattern data in it, or no pattern has a
    reliable sample size / real edge yet (see
    THUMBNAIL_INSIGHT_MIN_SAMPLE / _best_reliable_pattern()).

    Checks the ENGAGEMENT-ranked breakdown first (insight["engagement"]
    ["thumbnail_patterns"]) — see this module's ENGAGEMENT PREFERENCE
    docstring section — and only falls back to the views-ranked
    breakdown (insight["thumbnail_patterns"]) when the engagement section
    isn't populated yet or doesn't have a reliable winner of its own.

    Deliberately returns just the top pattern name, not a full ranking —
    callers that want to WEIGH candidate frames by pattern performance
    (rather than a hard preference) should read the relevant breakdown
    dict directly instead."""
    if not insight:
        return None

    engagement = insight.get("engagement")
    if engagement and engagement.get("thumbnail_patterns"):
        pattern = _best_reliable_pattern(engagement["thumbnail_patterns"])
        if pattern:
            return pattern

    if not insight.get("thumbnail_patterns"):
        return None
    return _best_reliable_pattern(insight["thumbnail_patterns"])
