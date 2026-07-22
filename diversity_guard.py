"""
Diversity Guard — Roadmap Item T
------------------------------------
A self-learning system that always picks the statistically-best option
converges into repetition: the same show, the same caption style, the
same thumbnail pattern, the same BGM track, because they "work." That
kills long-term audience growth even while short-term metrics look fine.
This module is the guardrail — it tracks the mix across the last
DIVERSITY_WINDOW published CLIPS (not digests — a digest has no single
canonical_show/bgm_file, see generate_thumbnails.py's own "clips only"
scope note for thumbnail_pattern) and tells callers whether a candidate
choice would push any dimension over its cap.

FOUR DIMENSIONS, FOUR CAPS (from the roadmap doc):
  show              <= 40% of the last DIVERSITY_WINDOW clips
  caption_style     <= 40%
  thumbnail_pattern <= 30%
  bgm_file          <= 20%

WHY CLIPS ONLY, NOT DIGESTS: a digest has no single canonical_show (it's
a multi-story roundup) and no bgm_file (news_digest_speed_factor is its
only render param) — see db.py's schema and generate_thumbnails.py's
docstring for the same "clips only" boundary already drawn for thumbnail
pattern classification. build_news_digest.py's OWN franchise-diversity
cap (enforce_franchise_diversity) already covers show diversity WITHIN a
single digest — this module is a different, longer-horizon signal (the
last 30 published CLIPS over time), not a replacement for that.

WHY THIS NEVER HARD-BLOCKS: per PROJECT_RULES.md Rule 3 (graceful
degradation), a diversity violation should change WHICH option gets
picked, never abort a render entirely. Every check_*() function here
returns a verdict for the CALLER to act on (regenerate with a different
style, pick the next-best thumbnail pattern, exclude an over-quota BGM
track from selection) — nothing in this module itself raises or blocks.
The one exception is the show dimension: canonical_show isn't known
until identify_show.py runs, which is AFTER a clip is already rendered
(see VIDEO_PIPELINE_ARCHITECT.md's stage order) — there's no earlier
point to redirect a pre-render decision based on show. So show diversity
is surfaced as a quality-gate MAJOR flag instead (see quality_gate.py's
integration), the same treatment roadmap item H already gave show
removal-rate — a content-strategy signal that pushes a clip toward
manual review, not a technical defect in the render itself.

EXPLORATION RATE: separate from the quota caps above. should_explore()
is a single, shared coin-flip (default 15%, DIVERSITY_EXPLORATION_RATE)
that callers consult to decide whether to skip ALL performance-based
biasing for this one item — not just the diversity caps, but also
generate_captions.py's style/opener addenda and generate_thumbnails.py's
pattern-selection bias — so a slice of content is always made with
genuinely unbiased/default choices, which is what actually lets new
winners get discovered rather than just enforced not-to-repeat. This is
a SEPARATE mechanism from edit_clips.py's existing BGM_EXPLORATION_RATE
(20%, BGM-specific, already shipped) — that one stays as-is; this one is
the pipeline-wide version the roadmap calls for.

USAGE:
  import diversity_guard as dg

  # at generation time, after you have a candidate value:
  violates, pct, limit = dg.check_caption_style("Bold-claim")
  if violates and not dg.should_explore():
      # regenerate / pick something else

  # picking from a ranked list of options — returns the best one that
  # doesn't violate its quota, or the plain-best if ALL violate (never
  # blocks entirely):
  chosen = dg.pick_non_saturated("thumbnail_pattern", ["action", "face_forward", "scenic"])
"""

import os
import json
import random

import db

DIVERSITY_WINDOW = int(os.getenv("DIVERSITY_WINDOW", "30"))

DIVERSITY_LIMITS = {
    "show": 0.40,
    "caption_style": 0.40,
    "thumbnail_pattern": 0.30,
    "bgm_file": 0.20,
}

EXPLORATION_RATE = float(os.getenv("DIVERSITY_EXPLORATION_RATE", "0.15"))


def should_explore():
    """Shared coin-flip other modules consult to decide whether to skip
    ALL performance-based biasing for this one item. See module
    docstring's EXPLORATION RATE section for why this is separate from
    edit_clips.py's own BGM-specific exploration slice."""
    return random.random() < EXPLORATION_RATE


def _classify_style(caption_text):
    """Local import to avoid a hard dependency at module-import time —
    performance_insights.py is a heavier module and diversity_guard.py
    should stay importable even in a context that hasn't loaded it yet
    (e.g. a future standalone caller)."""
    from performance_insights import classify_caption_style
    return classify_caption_style(caption_text)


def _recent_clip_dimensions(window=DIVERSITY_WINDOW):
    """Returns a list of dicts — one per published clip, most-recent
    DIVERSITY_WINDOW only — with whatever dimension values that clip has
    (show/caption_style/thumbnail_pattern/bgm_file). A clip missing a
    given field (e.g. published before thumbnail_pattern classification
    existed) simply has None for that key, excluded from that
    dimension's mix rather than skewing it."""
    conn = db.get_connection()
    rows = conn.execute(
        """SELECT canonical_show, caption_tiktok, thumbnail_pattern,
                  render_params_json
           FROM posts
           WHERE published_at IS NOT NULL
           ORDER BY published_at DESC
           LIMIT ?""",
        (window,),
    ).fetchall()
    conn.close()

    out = []
    for r in rows:
        bgm_file = None
        raw_params = r["render_params_json"]
        if raw_params:
            try:
                bgm_file = json.loads(raw_params).get("bgm_file")
            except (TypeError, ValueError):
                bgm_file = None

        out.append({
            "show": r["canonical_show"],
            "caption_style": _classify_style(r["caption_tiktok"]) if r["caption_tiktok"] else None,
            "thumbnail_pattern": r["thumbnail_pattern"],
            "bgm_file": bgm_file,
        })
    return out


def compute_mix(window=DIVERSITY_WINDOW):
    """Returns {dimension: {value: {"count": n, "pct": 0.0-1.0}}} across
    the last `window` published clips — the current standing mix, for
    reporting/debugging. Values of None are excluded (missing data, not
    a real "no value" bucket)."""
    clips = _recent_clip_dimensions(window)
    mix = {}
    for dim in DIVERSITY_LIMITS:
        counts = {}
        total = 0
        for c in clips:
            v = c.get(dim)
            if v is None:
                continue
            counts[v] = counts.get(v, 0) + 1
            total += 1
        mix[dim] = {
            v: {"count": n, "pct": round(n / total, 3) if total else 0.0}
            for v, n in counts.items()
        }
    return mix


def _check(dim, value, window=DIVERSITY_WINDOW):
    """Shared logic: would adding one more clip with `value` on `dim`
    push that value's share of the window OVER the configured limit?
    Returns (violates: bool, projected_pct: float, limit: float).
    A dimension/value with too little history (window still filling up
    early in the channel's life) never blocks — projected_pct is
    computed against whatever history exists, which naturally starts
    small and can't trip a cap prematurely."""
    limit = DIVERSITY_LIMITS[dim]
    if not value:
        return False, 0.0, limit

    clips = _recent_clip_dimensions(window)
    total = sum(1 for c in clips if c.get(dim) is not None)
    current = sum(1 for c in clips if c.get(dim) == value)

    projected_total = total + 1
    projected_pct = (current + 1) / projected_total
    return projected_pct > limit, round(projected_pct, 3), limit


def check_show(candidate_show):
    """Clips-only — see module docstring for why this is surfaced via
    quality_gate.py rather than gated pre-render."""
    return _check("show", candidate_show)


def check_caption_style(candidate_style):
    return _check("caption_style", candidate_style)


def check_thumbnail_pattern(candidate_pattern):
    return _check("thumbnail_pattern", candidate_pattern)


def check_bgm_file(candidate_bgm_filename):
    return _check("bgm_file", candidate_bgm_filename)


def pick_non_saturated(dim, candidates, window=DIVERSITY_WINDOW):
    """Given `candidates` (an ordered list of values, best-first — e.g.
    performance_insights.py's ranked pattern/style list), returns the
    first one that would NOT violate its quota. If every candidate would
    violate (a genuinely thin catalog, e.g. only 2 BGM tracks exist and
    both are already saturated), falls back to the first candidate rather
    than returning None — never blocks a render outright, per
    PROJECT_RULES.md Rule 3. Returns None only if `candidates` is empty."""
    if not candidates:
        return None
    for c in candidates:
        violates, _, _ = _check(dim, c, window)
        if not violates:
            return c
    return candidates[0]
