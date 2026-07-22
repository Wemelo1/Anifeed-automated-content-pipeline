"""
Source Health — Roadmap Item Q
-----------------------------------
source_quality.py (roadmap item N) answers "is this source's CONTENT
good" (views, removal rate, quality-gate outcomes) with a soft quota
reduction. This module answers a different question — "is this source's
PIPELINE healthy" (is it handing back garbage image files, is it timing
out, is its content getting Content-ID claimed at an alarming rate) —
with a harder gate: `blocked` sources are skipped entirely, not just
deprioritized.

THREE RULES (exact thresholds from the roadmap doc):
  image_validation_failure_rate > 30% (last 7 days)  -> degraded
  removal_rate > 50% (published clips)               -> blocked
  quality_gate_rejection_rate > 20%                  -> degraded

`blocked` wins over `degraded` if a source somehow trips both — being
unpublishable (Content ID risk) is a harder problem than being
occasionally low-quality.

WHERE THE DATA COMES FROM:
  - image_validation_failure_rate: NEW instrumentation. Before this,
    rss_news_pull.py / fetch_anilist_trending.py / reddit_pull.py /
    fetch_youtube_trailers.py silently `continue`d past a failed
    is_valid_image()/normalize_image()/download and left NO trace in
    pipeline.db — there was nothing to compute this rate FROM. Those
    four scripts now call db.record_source_event() at each failure
    point (see their own docstrings' "SELF-HEALING" note); this module
    reads db.source_event_counts() + db.posts_inserted_by_source() to
    compute failures / (failures + successes) over the last 7 days.
  - removal_rate / quality_gate_rejection_rate: reused directly from
    source_quality.rank_sources() (roadmap item N) rather than
    recomputed here — those numbers are already correct and
    sample-gated there; duplicating that logic would risk the two
    modules silently drifting apart on the same underlying question.

SAMPLE-SIZE GATE: same principle as everywhere else in this pipeline —
a source with fewer than MIN_ATTEMPTS_FOR_VALIDATION_RATE image attempts
this week doesn't get flagged on validation-failure-rate alone (a single
failed image out of 2 attempts is 50%, not a real signal). removal_rate/
quality_fail_rate inherit source_quality.py's own SOURCE_QUALITY_MIN_SAMPLE
gate for the same reason.

WHAT THIS DOES NOT DO: this module only WRITES the classification
(db.upsert_source_health) — it doesn't itself change any acquire
script's behavior. rss_news_pull.py and fetch_youtube_trailers.py read
the result via db.get_source_health() and act on it independently (skip
`blocked` entirely, halve `degraded` quotas ON TOP OF source_quality.py's
separate soft multiplier) — same separation-of-concerns PROJECT_RULES.md
Rule 2 asks for (this module classifies, it doesn't acquire).

USAGE:
  python source_health.py
  Run daily (see run_analytics.sh) — classification only needs to be
  fresh once a day, not on every acquire-script invocation; acquire
  scripts read whatever the last daily run computed.
"""

import os

import db
import source_quality as sq

IMAGE_FAILURE_WINDOW_DAYS = int(os.getenv("SOURCE_HEALTH_IMAGE_WINDOW_DAYS", "7"))
IMAGE_FAILURE_THRESHOLD_PCT = float(os.getenv("SOURCE_HEALTH_IMAGE_FAILURE_THRESHOLD_PCT", "30"))
REMOVAL_RATE_BLOCK_THRESHOLD_PCT = float(os.getenv("SOURCE_HEALTH_REMOVAL_BLOCK_THRESHOLD_PCT", "50"))
QUALITY_REJECT_DEGRADE_THRESHOLD_PCT = float(os.getenv("SOURCE_HEALTH_QUALITY_REJECT_THRESHOLD_PCT", "20"))

# A source needs at least this many image attempts (successes + failures
# combined) in the window before its validation-failure-rate is trusted
# — same "don't manufacture a signal that isn't there" gate every other
# insight module in this pipeline applies.
MIN_ATTEMPTS_FOR_VALIDATION_RATE = int(os.getenv("SOURCE_HEALTH_MIN_ATTEMPTS", "5"))

_IMAGE_FAILURE_EVENT_TYPES = ("image_invalid", "image_normalize_fail")


def _image_validation_rates(window_days=IMAGE_FAILURE_WINDOW_DAYS):
    """Returns {(platform, handle): failure_rate_0_to_1} for every
    (platform, handle) that clears MIN_ATTEMPTS_FOR_VALIDATION_RATE this
    window. Sources below that floor are simply absent from the returned
    dict (not present with a 0.0 or None) — callers should treat absence
    as 'no reliable signal', not 'definitely fine'."""
    event_counts = db.source_event_counts(window_days)
    inserted_counts = db.posts_inserted_by_source(window_days)

    rates = {}
    all_keys = set(event_counts) | set(inserted_counts)
    for key in all_keys:
        failures = sum(event_counts.get(key, {}).get(t, 0) for t in _IMAGE_FAILURE_EVENT_TYPES)
        successes = inserted_counts.get(key, 0)
        total_attempts = failures + successes
        if total_attempts < MIN_ATTEMPTS_FOR_VALIDATION_RATE:
            continue
        rates[key] = failures / total_attempts
    return rates


def classify_all():
    """Computes and PERSISTS (via db.upsert_source_health) a status for
    every source with enough data on at least one of the three signals.
    Returns a list of {platform, handle, status, reasons} dicts — the
    same records just written, for print_report()'s use."""
    validation_rates = _image_validation_rates()
    quality_rows = sq.rank_sources(min_sample=1)  # own gate is per-signal below, not sq's
    quality_by_key = {(r["platform"], r["handle"]): r for r in quality_rows}

    all_keys = set(validation_rates) | set(quality_by_key)
    results = []

    for key in all_keys:
        platform, handle = key
        reasons = []
        status = "healthy"

        val_rate = validation_rates.get(key)
        if val_rate is not None and val_rate * 100 > IMAGE_FAILURE_THRESHOLD_PCT:
            status = "degraded"
            reasons.append(
                "image_validation_failure_rate=%.0f%% (last %dd, threshold %.0f%%)"
                % (val_rate * 100, IMAGE_FAILURE_WINDOW_DAYS, IMAGE_FAILURE_THRESHOLD_PCT)
            )

        q = quality_by_key.get(key)
        if q and q["sample_size"] >= sq.SOURCE_QUALITY_MIN_SAMPLE:
            if q["removal_rate"] * 100 > REMOVAL_RATE_BLOCK_THRESHOLD_PCT:
                status = "blocked"  # blocked always wins over degraded
                reasons.append(
                    "removal_rate=%.0f%% (threshold %.0f%%)"
                    % (q["removal_rate"] * 100, REMOVAL_RATE_BLOCK_THRESHOLD_PCT)
                )
            if q["quality_fail_rate"] * 100 > QUALITY_REJECT_DEGRADE_THRESHOLD_PCT:
                if status != "blocked":
                    status = "degraded"
                reasons.append(
                    "quality_gate_rejection_rate=%.0f%% (threshold %.0f%%)"
                    % (q["quality_fail_rate"] * 100, QUALITY_REJECT_DEGRADE_THRESHOLD_PCT)
                )

        db.upsert_source_health(platform, handle, status, reasons)
        results.append({"platform": platform, "handle": handle,
                         "status": status, "reasons": reasons})

    order = {"blocked": 0, "degraded": 1, "healthy": 2}
    results.sort(key=lambda r: order[r["status"]])
    return results


def print_report():
    results = classify_all()
    if not results:
        print("No source has enough data yet on any health signal — "
              "nothing to classify.")
        return

    blocked = [r for r in results if r["status"] == "blocked"]
    degraded = [r for r in results if r["status"] == "degraded"]
    healthy = [r for r in results if r["status"] == "healthy"]

    print("Source health (%d source(s) classified): %d blocked, %d degraded, %d healthy.\n"
          % (len(results), len(blocked), len(degraded), len(healthy)))

    for r in blocked + degraded:
        icon = "X BLOCKED " if r["status"] == "blocked" else "! DEGRADED"
        print("  %s  %s/%s" % (icon, r["platform"], r["handle"]))
        for reason in r["reasons"]:
            print("      %s" % reason)

    if healthy:
        healthy_labels = ", ".join("%s/%s" % (r["platform"], r["handle"]) for r in healthy)
        print("\n  Healthy: %s" % healthy_labels)


if __name__ == "__main__":
    db.init_db()
    print_report()
