"""
Learn Stage — Config Mutator
------------------------------
Closes the loop performance_insights.py has been computing signals INTO
without anything ever reading them and closing the loop for real: this is
the piece that actually writes the config forward, so raising a threshold
or biasing a selection knob is no longer a person deciding to edit an
env var, it's the pipeline deciding based on its own data.

SCOPE BOUNDARY (load-bearing, not incidental): per VIDEO_PIPELINE_ARCHITECT.md's
Stage 7 contract, Learn NEVER touches an individual post/digest row — it
only ever writes to the learned-config store (learned_config.py). This
script does not import anything from db.py's posts/digests tables and
never will; if a future mutation rule seems to need that, that's a sign
it belongs in a different stage, not a reason to relax this boundary.

WHY EIGHT MUTABLE PARAMS RIGHT NOW (was three, then five, now eight):
performance_insights.py's cached "content_strategy" insight is a single,
LATEST-only snapshot (db.py's `insights` table is an upsert, see its own
docstring) — with nothing to diff against, a mutation rule can only ever
react to one instant, which is how a "self-improving" system quietly
becomes a self-degrading one on a lucky/unlucky week. This script fixes
that by snapshotting the insight into learned_config.insight_snapshots
every run (see that module's docstring). The five newest rules below
(edit_color_grade_saturation, bgm_volume, hook_overlay_duration,
words_per_subtitle_cue, news_digest_speed_factor) are all deliberately
SNAPSHOT-based rather than trend-based, same posture as
rule_thumbnail_bias_top_n: they read performance_insights.py's
"render_params" bucketed breakdown (see that module's
RENDER_PARAM_BUCKET_SPECS / DIGEST_RENDER_PARAM_BUCKET_SPECS) and move
the current value toward whichever bucket's midpoint is reliably beating
the (clip- or digest-specific) average. This is a legitimate read of a
single current snapshot because the downside of being wrong is a few
items render with a slightly-off creative value next production cycle,
not a scheduling/safety regression — same risk class
rule_thumbnail_bias_top_n already accepted for a single-snapshot read.

GUARDRAILS LIVE IN learned_config.py, NOT HERE: every rule below just
computes a PROPOSED new value and hands it to
learned_config.propose_mutation(), which applies floor/ceiling clamping,
max-delta-per-run clamping, minimum-sample gating, cooldown gating, and
(for quality_approve_threshold specifically) the loosen-requires-manual-
approval rule. A rule here can be directionally wrong without doing real
damage — the blast radius of any single bad proposal is bounded by the
registry entry in learned_config.py, not by this script's own logic.

USAGE:
  python mutator.py
  Run in run_analytics.sh, immediately after performance_insights.py —
  this script assumes db.get_insight("content_strategy") is fresh.
"""

import db
import learned_config as lc
import cost_tracker as ct
import os
from datetime import datetime, timezone

INSIGHT_KEY = "content_strategy"

# Below this margin (percent vs. the relevant average), a render-param
# bucket's edge isn't considered reliable enough to act on — same "don't
# manufacture a signal that isn't there" bar rule_thumbnail_bias_top_n
# uses for its own margin > 15 / margin < 5 thresholds, just centralized
# here since every snapshot-based render-param rule shares this exact
# reasoning.
RENDER_PARAM_MIN_MARGIN_PCT = 5.0


def _pct_change(new, old):
    if not old:
        return None
    return (new - old) / old


def _parse_bucket_bounds(label):
    """performance_insights.py's _bucket_label() produces "lo-hi" strings
    (e.g. "1.35-1.5") via Python's `:g` format. Returns (lo, hi) as
    floats, or None if the label doesn't parse (defensive — a malformed
    label should never crash the whole rule, just make this one bucket
    unusable)."""
    try:
        lo_str, hi_str = label.split("-")
        return float(lo_str), float(hi_str)
    except (ValueError, AttributeError):
        return None


def _bucket_midpoint_rule(insight, render_param_key, learned_config_param_name,
                           round_to_int=False):
    """Shared logic for every "move current value toward the best render-
    param bucket's midpoint" rule. Returns a propose_mutation() status
    string, or None if there's no reliable signal to act on yet.

    round_to_int (added for words_per_subtitle_cue): that param is
    fundamentally a discrete word count, not a continuous value — a raw
    bucket-midpoint target like 3.5 doesn't mean anything as a "words per
    cue" setting. When True, the computed target is rounded to the
    nearest whole number BEFORE the current-vs-target comparison and
    before being handed to propose_mutation(), so this rule never
    proposes a fractional value for an inherently integer param.

    Deliberately does NOT need trend history (unlike rule_publish_max_per_day
    / rule_quality_approve_threshold below) — see this module's docstring
    for why a single current snapshot is an acceptable evidence bar for a
    creative-production param, same class of risk rule_thumbnail_bias_top_n
    already accepts."""
    render_params = insight.get("render_params") or {}
    buckets = render_params.get(render_param_key)
    if not buckets:
        return None  # no data yet — that param's producer script hasn't
                       # populated render_params_json on enough measured
                       # items yet (or at all, if it's still unmigrated)

    # performance_insights.py's _ranked_stats() already sorts buckets by
    # avg_views descending, so buckets[0] is the best-performing one.
    best = buckets[0]
    sample = best["sample_size"]
    margin = best["vs_overall_avg_pct"]
    if margin < RENDER_PARAM_MIN_MARGIN_PCT:
        return None  # no bucket reliably beats the average — nothing to act on

    bounds = _parse_bucket_bounds(best["value"])
    if bounds is None:
        return None
    lo, hi = bounds
    target = (lo + hi) / 2
    if round_to_int:
        target = round(target)

    current = lc.get_param(learned_config_param_name)
    if abs(target - current) < 1e-6:
        return None  # already sitting at (effectively) the target — nothing to propose

    reasoning = (f"render-param bucket {best['value']} beating its average by "
                 f"{margin:.0f}% at n={sample} — moving {learned_config_param_name} "
                 f"from {current} toward that bucket's midpoint "
                 f"({target if round_to_int else f'{target:.3f}'})")

    return lc.propose_mutation(learned_config_param_name, target, reasoning,
                                sample_size=sample,
                                insight_computed_at=insight.get("computed_at"))


# ── Rule 1: thumbnail_bias_top_n ─────────────────────────────────────────
def rule_thumbnail_bias_top_n(insight):
    if not insight or not insight.get("thumbnail_patterns"):
        return None
    best = insight["thumbnail_patterns"][0]
    current = lc.get_param("thumbnail_bias_top_n")
    sample = best["sample_size"]
    margin = best["vs_overall_avg_pct"]

    if sample >= 8 and margin > 15:
        # Cost-aware check (roadmap item S) — the concrete example the
        # roadmap itself calls out: "if a Groq vision call for thumbnail
        # classification costs more than it gains, thumbnail_bias_top_n
        # should shrink automatically." Implemented as "...should stop
        # GROWING automatically" — thumbnail_bias_top_n's registry floor
        # (2) is already the cost-minimum non-zero setting, so a cost
        # rule pushing it lower than the existing performance-only branch
        # below already does isn't meaningful; what cost awareness
        # actually changes is refusing to propose MORE spend when the
        # logged per-call cost is high enough that one extra classified
        # candidate per clip isn't obviously worth it. Only engages once
        # a real, non-zero rate is configured (see cost_tracker.py's
        # COST_RATES — everything defaults to $0.0 on this pipeline's
        # current free-tier services, so this is a no-op today and
        # activates automatically the moment a real rate is set).
        cost_per_call = ct.cost_per_classification_call()
        if cost_per_call > 0:
            # Rough value bar: a classification call is "worth it" if its
            # cost is small relative to the margin it's chasing — margin
            # is a percentage, cost is a dollar figure, so this compares
            # cost against a configurable dollar-per-margin-point budget
            # rather than pretending the two units are directly comparable.
            budget_per_margin_point = float(
                os.getenv("THUMBNAIL_COST_BUDGET_PER_MARGIN_POINT", "0.01"))
            max_justified_cost = margin * budget_per_margin_point
            if cost_per_call > max_justified_cost:
                print(f"    (cost-aware: skipping thumbnail_bias_top_n increase — "
                      f"${cost_per_call:.5f}/call exceeds the ${max_justified_cost:.5f} "
                      f"budget justified by a {margin:.0f}% margin)")
                return None

        proposed = current + 1
        reasoning = (f"'{best['pattern']}' beating channel avg by {margin:.0f}% "
                      f"at n={sample} — worth classifying more candidates to find it")
    elif sample < 8 or margin < 5:
        proposed = current - 1
        reasoning = (f"no thumbnail pattern currently shows a reliable, meaningful "
                      f"edge (best: '{best['pattern']}' at {margin:.0f}%, n={sample}) "
                      f"— not worth the extra vision spend")
    else:
        return None

    return lc.propose_mutation("thumbnail_bias_top_n", proposed, reasoning,
                                sample_size=sample,
                                insight_computed_at=insight.get("computed_at"))


# ── Rule 2: publish_max_per_day ──────────────────────────────────────────
def _trend_metric(insight, prior_insight):
    """Shared by both trend-based rules below. Prefers the RETENTION
    trend (roadmap item K) over the raw-views trend whenever BOTH the
    current and prior snapshot have retention data — insight["retention"]
    is only ever non-None once performance_insights.py's
    RETENTION_MIN_SAMPLE gate clears, so no additional sample check is
    needed here beyond that. Falls back to overall_avg_views (the
    original behavior) whenever either snapshot lacks retention data,
    which is exactly "act on views until there's enough retention
    history to act on that instead" — automatic, no manual cutover, same
    posture the engagement-preference cascade already uses elsewhere in
    this pipeline (generate_captions.py/thumbnail_strategy.py).

    Returns (current_value, prior_value, metric_label)."""
    current_retention = insight.get("retention")
    prior_retention = prior_insight.get("retention")
    if current_retention and prior_retention:
        return (current_retention["overall_avg_retention_pct"],
                prior_retention["overall_avg_retention_pct"],
                "avg retention")
    return insight.get("overall_avg_views"), prior_insight.get("overall_avg_views"), "avg views"


def rule_publish_max_per_day(insight, run_started_at):
    if not insight:
        return None
    prior = lc.prior_snapshot(INSIGHT_KEY, before_snapshotted_at=run_started_at)
    if prior is None:
        return None

    prior_insight = prior["value"]
    current_avg, prior_avg, metric_label = _trend_metric(insight, prior_insight)
    change = _pct_change(current_avg, prior_avg)
    if change is None:
        return None

    current = lc.get_param("publish_max_per_day")
    sample = insight.get("sample_size", 0)

    if change >= -0.05:
        proposed = current + 1
        reasoning = (f"{metric_label} {'up' if change >= 0 else 'roughly flat'} "
                      f"({change * 100:+.0f}%) vs. the last measured run "
                      f"({prior_avg:.1f} -> {current_avg:.1f}) — cadence isn't "
                      f"diluting performance, room to post more")
    elif change <= -0.15:
        proposed = current - 1
        reasoning = (f"{metric_label} down {change * 100:.0f}% vs. the last measured "
                      f"run ({prior_avg:.1f} -> {current_avg:.1f}) — pulling back "
                      f"cadence rather than assuming it's unrelated")
    else:
        return None

    return lc.propose_mutation("publish_max_per_day", proposed, reasoning,
                                sample_size=sample,
                                insight_computed_at=insight.get("computed_at"))


# ── Rule 3: quality_approve_threshold — TIGHTEN-ONLY ─────────────────────
def rule_quality_approve_threshold(insight, run_started_at):
    if not insight:
        return None
    prior = lc.prior_snapshot(INSIGHT_KEY, before_snapshotted_at=run_started_at)
    if prior is None:
        return None

    prior_insight = prior["value"]
    current_avg, prior_avg, metric_label = _trend_metric(insight, prior_insight)
    change = _pct_change(current_avg, prior_avg)
    if change is None or change > -0.15:
        return None

    current = lc.get_param("quality_approve_threshold")
    sample = insight.get("sample_size", 0)
    proposed = current + 5
    reasoning = (f"{metric_label} down {change * 100:.0f}% vs. the last measured run "
                 f"({prior_avg:.1f} -> {current_avg:.1f}) — tightening the "
                 f"auto-approve bar as a precaution. (Loosening this threshold "
                 f"is never proposed by this rule — no quality_score-vs-"
                 f"performance signal exists yet to justify it; see this "
                 f"script's module docstring.)")

    return lc.propose_mutation("quality_approve_threshold", proposed, reasoning,
                                sample_size=sample,
                                insight_computed_at=insight.get("computed_at"))


# ── Rule 4: edit_color_grade_saturation ──────────────────────────────────
def rule_edit_color_grade_saturation(insight):
    if not insight:
        return None
    return _bucket_midpoint_rule(insight, "color_grade_saturation",
                                  "edit_color_grade_saturation")


# ── Rule 5: bgm_volume ────────────────────────────────────────────────────
def rule_bgm_volume(insight):
    if not insight:
        return None
    return _bucket_midpoint_rule(insight, "bgm_volume", "bgm_volume")


# ── Rule 6: hook_overlay_duration (ADDED — gap-fill migration) ───────────
# add_hook_overlay.py now writes the EFFECTIVE (post-clamp) hook duration
# into render_params_json — see that module's LEARN STAGE docstring
# section. This reads the resulting performance_insights.py bucket
# exactly like Rules 4/5.
def rule_hook_overlay_duration(insight):
    if not insight:
        return None
    return _bucket_midpoint_rule(insight, "hook_overlay_duration",
                                  "hook_overlay_duration")


# ── Rule 7: words_per_subtitle_cue (ADDED — gap-fill migration) ──────────
# Rounded to the nearest integer — see _bucket_midpoint_rule()'s
# round_to_int docstring for why: a subtitle cue can't display a
# fractional number of words.
def rule_words_per_subtitle_cue(insight):
    if not insight:
        return None
    return _bucket_midpoint_rule(insight, "words_per_subtitle_cue",
                                  "words_per_subtitle_cue", round_to_int=True)


# ── Rule 8: news_digest_speed_factor (ADDED — gap-fill migration) ────────
# DIGEST-side, not clip-side — performance_insights.py computes this
# bucket from db.digests_with_metrics() against a digest-specific overall
# average (see that module's _digest_render_param_stats() docstring for
# why it can't share the clip-side average). The bucket lives in the SAME
# insight["render_params"] dict as the clip-side params though (no key
# collision — see that module's merge comment), so this rule reads it via
# the exact same _bucket_midpoint_rule() helper as every other render
# param, no special-casing needed here.
def rule_news_digest_speed_factor(insight):
    if not insight:
        return None
    return _bucket_midpoint_rule(insight, "news_digest_speed_factor",
                                  "news_digest_speed_factor")


RULES = [
    ("thumbnail_bias_top_n", rule_thumbnail_bias_top_n, False),
    ("publish_max_per_day", rule_publish_max_per_day, True),
    ("quality_approve_threshold", rule_quality_approve_threshold, True),
    ("edit_color_grade_saturation", rule_edit_color_grade_saturation, False),
    ("bgm_volume", rule_bgm_volume, False),
    ("hook_overlay_duration", rule_hook_overlay_duration, False),
    ("words_per_subtitle_cue", rule_words_per_subtitle_cue, False),
    ("news_digest_speed_factor", rule_news_digest_speed_factor, False),
]


def main():
    db.init_db()
    lc.init_schema()

    insight = db.get_insight(INSIGHT_KEY)
    if not insight:
        print("No 'content_strategy' insight yet — run performance_insights.py "
              "first (and let it clear its own MIN_SAMPLE_SIZE gate). Nothing "
              "for the Learn stage to act on this run.")
        return

    print(f"Learn stage — evaluating {len(RULES)} mutation rule(s) against "
          f"the insight computed {insight.get('computed_at')} "
          f"(sample_size={insight.get('sample_size')}).\n")

    run_started_at = datetime.now(timezone.utc).isoformat()
    lc.snapshot_insight(INSIGHT_KEY, insight)

    outcomes = {}
    for name, rule_fn, needs_history in RULES:
        try:
            status = rule_fn(insight, run_started_at) if needs_history else rule_fn(insight)
        except Exception as e:
            print(f"  {name}: RULE FAILED ({e}) — left untouched this run.")
            continue
        outcomes[name] = status
        if status is None:
            print(f"  {name}: no proposal this run (signal too thin or unchanged).")
        else:
            current_val = lc.get_param(name)
            print(f"  {name}: {status} — current value is now {current_val}.")

    pending = lc.pending_proposals()
    if pending:
        print(f"\n{len(pending)} proposal(s) awaiting manual approval "
              f"(loosening a safety threshold is never auto-applied):")
        for p in pending:
            print(f"  [id {p['id']}] {p['param_name']}: "
                  f"{p['old_value_json']} -> {p['proposed_value_json']}")
            print(f"      {p['reasoning']}")
        print("\nApprove one with:")
        print("  python -c \"import learned_config as lc; lc.apply_proposal(<id>)\"")

    print("\nDone.")


if __name__ == "__main__":
    main()
