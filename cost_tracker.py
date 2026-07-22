"""
Economic Optimization — Roadmap Item S
-------------------------------------------
Every learned_config mutation this pipeline makes is scored purely on
performance (views, engagement, retention) — never on what producing
that performance actually COSTS. A thumbnail-pattern classification call,
a caption-generation call, a Whisper transcription — none of it is free
of quota/API cost even when the current per-service RATE happens to be
$0 (Groq/Gemini free tiers, edge-tts). This module is the missing
denominator: log every meaningfully-costed API call, and let mutator.py
weigh a performance gain against what it actually costs to keep buying.

WHY EVERY RATE DEFAULTS TO $0.0: this pipeline currently runs entirely on
free-tier services (Groq, Gemini Flash-Lite, edge-tts — see
DEPENDENCIES.md / TERMUX_ENVIRONMENT.md). Hardcoding a guessed dollar
figure for a free call would be actively misleading. COST_RATES is fully
.env-overridable specifically so that the moment ANY of these move to a
paid tier (or a paid tier is added deliberately, e.g. ElevenLabs), the
existing call-count instrumentation below immediately produces a
meaningful cost_per_1000_views without touching a single call site again
— only the rate table changes. Until then, this module still tracks call
COUNTS (a real, always-meaningful signal — "how many Groq vision calls
did thumbnail selection actually make this month" is useful on its own,
independent of dollar cost) and reports $0.00 honestly rather than a
fabricated number.

CALL SITES INSTRUMENTED (the highest-frequency/highest-marginal-cost
ones — see module docstring's SCOPE note for what's deliberately NOT yet
wired):
  identify_show.py    — Groq vision (Layer 1) + Groq text (Layer 2/3)
  thumbnail_strategy.py — Groq vision (pattern classification)
  generate_captions.py  — Gemini text (TikTok + YouTube caption calls)
  burn_subtitles.py     — Groq Whisper (transcription)

SCOPE — NOT YET INSTRUMENTED: build_news_digest.py's per-story
edit_planner.plan_story() (Gemini) and TTS calls (Fish/ElevenLabs/edge-tts)
are real cost drivers too, especially ElevenLabs (a genuinely paid
service if configured) — left for a focused follow-up rather than
patched hastily into an already-enormous file in the same pass as
everything else in this session; the pattern established here
(cost_tracker.log_call(...) right after the API call returns) is
mechanical to extend once that file gets its own attention.

MUTATOR INTEGRATION: mutator.py's rule_thumbnail_bias_top_n now checks
cost_per_classification_call before proposing an INCREASE (never blocks
a decrease — a decrease is already the cost-saving direction). This is
the concrete example the roadmap itself calls out: "if a Groq vision
call for thumbnail classification costs more than it gains,
thumbnail_bias_top_n should shrink automatically" — implemented as "...
should stop growing automatically," since thumbnail_bias_top_n's floor
(2) is already the cost-minimum non-zero setting, not something a cost
rule should push below.

USAGE:
  import cost_tracker as ct
  ct.log_call("groq", "vision_call", content_key="crunchyroll/vid1", content_type="clip")
  ct.cost_per_1000_views()          # None if no cost data or no view data yet
  ct.print_report()
"""

import os

import db

# $/call (or $/unit, e.g. $/second for TTS) for each (provider, operation)
# pair actually logged below. Every default is 0.0 — see module docstring
# for why. Override via .env, e.g.:
#   COST_RATE_GROQ_VISION_CALL=0.0005
COST_RATES = {
    ("groq", "vision_call"): float(os.getenv("COST_RATE_GROQ_VISION_CALL", "0.0")),
    ("groq", "text_call"): float(os.getenv("COST_RATE_GROQ_TEXT_CALL", "0.0")),
    ("groq", "whisper_call"): float(os.getenv("COST_RATE_GROQ_WHISPER_CALL", "0.0")),
    ("gemini", "text_call"): float(os.getenv("COST_RATE_GEMINI_TEXT_CALL", "0.0")),
    ("edge_tts", "synthesis"): float(os.getenv("COST_RATE_EDGE_TTS_SYNTHESIS", "0.0")),
    ("elevenlabs", "synthesis"): float(os.getenv("COST_RATE_ELEVENLABS_SYNTHESIS", "0.0")),
    ("fish_audio", "synthesis"): float(os.getenv("COST_RATE_FISH_AUDIO_SYNTHESIS", "0.0")),
}


def log_call(provider, operation, content_key=None, content_type=None, units=1.0):
    """Records one API call. Never raises — a cost-logging failure must
    never take down the actual generation call it's instrumenting (same
    graceful-degradation posture PROJECT_RULES.md Rule 3 applies to every
    other optional/observability path in this pipeline)."""
    rate = COST_RATES.get((provider, operation), 0.0)
    cost = rate * units
    try:
        db.record_api_cost(provider, operation, cost, content_key=content_key,
                            content_type=content_type, units=units)
    except Exception as e:
        print(f"    NOTE: cost logging failed (non-fatal): {e}")


def _total_views_for_measured_content():
    """Sum of views across every measured clip AND digest — the
    denominator side of cost_per_1000_views. Digests aren't covered by
    db.posts_with_metrics() (clips only), so this adds
    db.digests_with_metrics() too — same "clips and digests are separate
    populations, both worth reading" pattern performance_insights.py's
    own digest-render-param bucketing already established."""
    total = sum(r["views"] or 0 for r in db.posts_with_metrics())
    try:
        total += sum(r["views"] or 0 for r in db.digests_with_metrics())
    except Exception:
        pass
    return total


def cost_per_1000_views(window_days=None):
    """Returns a float, or None if there's no cost data or no view data
    yet — callers must treat None as 'not computable yet', never as 0."""
    rows = db.cost_by_operation(window_days)
    total_cost = sum(r["total_cost"] or 0.0 for r in rows)
    total_views = _total_views_for_measured_content()
    if total_views <= 0:
        return None
    return round((total_cost / total_views) * 1000, 4)


def cost_per_classification_call():
    """The specific figure mutator.py's cost-aware thumbnail rule needs
    — $/call for the thumbnail-pattern classification operation
    specifically (thumbnail_strategy.py's Groq vision call), not the
    whole-pipeline blended rate. Returns 0.0 (not None) when there's no
    logged data yet, since "unknown cost" should never block a rule the
    same way "known, non-zero cost" would — a rule reading this can treat
    0.0 as 'no cost signal, don't let cost override the performance
    decision', which is the correct default until this operation has
    actually been logged."""
    rows = db.cost_by_operation()
    for r in rows:
        if r["provider"] == "groq" and r["operation"] == "vision_call" and r["call_count"]:
            return (r["total_cost"] or 0.0) / r["call_count"]
    return 0.0


def print_report(window_days=None):
    rows = db.cost_by_operation(window_days)
    if not rows:
        print("No API costs logged yet.")
        return

    window_note = f" (last {window_days}d)" if window_days else " (all-time)"
    print(f"API cost breakdown{window_note}:\n")
    total_cost = 0.0
    for r in rows:
        total_cost += r["total_cost"] or 0.0
        print(f"  {r['provider']}/{r['operation']}: {r['call_count']} call(s), "
              f"${r['total_cost']:.4f} total"
              + (f" (${r['total_cost']/r['call_count']:.5f}/call)" if r['call_count'] else ""))

    cpm = cost_per_1000_views(window_days)
    print(f"\nTotal logged cost{window_note}: ${total_cost:.4f}")
    if cpm is not None:
        print(f"Cost per 1000 views: ${cpm:.4f}")
    else:
        print("Cost per 1000 views: not computable yet (no measured views)")


if __name__ == "__main__":
    db.init_db()
    print_report()
