"""
Trend Signal (fast spike detection)
--------------------------------------
Two different timescales for two different jobs:

  performance_insights.py  — SLOW, confident signal. Refuses to say
      anything below MIN_SAMPLE_SIZE=15 measured clips, and is meant to
      steer stable things (caption style, thumbnail pattern, posting
      time) via generate_captions.py / thumbnail_strategy.py /
      publish_scheduler.py. Correctly conservative — but that means a
      show spiking RIGHT NOW gets zero reaction until weeks of data pile
      up.

  trend_signal.py (this file) — FAST, ephemeral signal. Compares each
      canonical show's current total view count (db.views_by_canonical_show)
      against a snapshot of that same total taken at least
      TREND_BASELINE_MIN_AGE_HOURS ago. A show whose views have grown by
      TREND_SPIKE_RATIO or more since that baseline is "trending now" for
      THIS run only — nothing is written to config, nothing steers future
      captions/thumbnails/scheduling. It only ever affects the CURRENT
      build_news_digest.py pass, via get_trending_boosts().

WHY A SEPARATE BASELINE INSTEAD OF "LAST RUN'S TOTAL":
run_all.sh runs 2-3x/day. Comparing against literally the last run's
totals would mean the "baseline" is often only a few hours old, so
almost every show would show *some* view growth and the spike ratio
would be meaningless noise. The baseline is only refreshed once it's at
least TREND_BASELINE_MIN_AGE_HOURS old — every save_baseline() call
between refreshes is a no-op that just returns the still-valid existing
baseline unchanged. This is the same "don't manufacture a signal that
isn't there" instinct as performance_insights.py's own MIN_SAMPLE_SIZE
gate, applied to time instead of sample count.

STORAGE: reuses db.py's generic insights key/value cache (save_insight/
get_insight) under key TREND_BASELINE_KEY — no new table needed, same
convention db.py already documents for that table.

DEGRADATION: get_trending_boosts() never raises. No baseline yet (first
run ever) -> empty dict, same as "nothing is trending" -> zero behavior
change until a second run establishes a comparison point. A canonical
show with no metrics yet, or below MIN_VIEWS_FOR_SIGNAL -> excluded, same
as db.views_by_canonical_show()'s own floor.

USAGE:
  import trend_signal
  trending = trend_signal.get_trending_boosts()
  # -> {canonical_show: velocity_ratio} for shows spiking right now

  # Call ONCE per build_news_digest.py run, after Phase 1 data is fetched
  # and before the STORY_SCORE_THRESHOLD filter — see that file's
  # integration for the exact call site.

.env overrides (all optional):
  TREND_SPIKE_RATIO=1.4               # current/baseline views ratio that
                                        # counts as "spiking"
  TREND_BASELINE_MIN_AGE_HOURS=12      # how old the stored baseline must
                                        # be before it's trusted as a real
                                        # comparison point (also how often
                                        # it gets refreshed)
  TREND_MIN_VIEWS_FOR_SIGNAL=200       # floor so a show with a handful of
                                        # views total doesn't "spike" off
                                        # near-zero-base noise
  DIGEST_TREND_SCORE_BOOST=20          # added to edit_planner's 0-100
                                        # newsworthiness score for a
                                        # trending show's story, capped
                                        # at 100 — see build_news_digest.py
"""

import os
from datetime import datetime, timezone

import db

TREND_BASELINE_KEY = "trend_velocity_baseline"

TREND_SPIKE_RATIO = float(os.getenv("TREND_SPIKE_RATIO", "1.4"))
TREND_BASELINE_MIN_AGE_HOURS = float(os.getenv("TREND_BASELINE_MIN_AGE_HOURS", "12"))
TREND_MIN_VIEWS_FOR_SIGNAL = int(os.getenv("TREND_MIN_VIEWS_FOR_SIGNAL", "200"))


def _now():
    return datetime.now(timezone.utc)


def _hours_since(iso_ts):
    try:
        then = datetime.fromisoformat(iso_ts)
    except (TypeError, ValueError):
        return None
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    return (_now() - then).total_seconds() / 3600.0


def _get_comparable_baseline(current_totals):
    """Reads the stored baseline and decides whether THIS run gets a real
    comparison point:

      - no baseline exists yet (first run ever) -> write `current_totals`
        as the baseline, return None (nothing to compare against yet).
      - baseline exists but is younger than TREND_BASELINE_MIN_AGE_HOURS
        -> too soon for a meaningful ratio (most of the "growth" would
        just be normal accumulation, not a spike) -> leave the baseline
        UNTOUCHED and return None, so it keeps aging toward a trustworthy
        comparison point instead of being reset every run.
      - baseline exists AND is old enough -> THIS is the valid comparison
        point: return it for the caller to diff against `current_totals`,
        AND slide the window forward by overwriting the stored baseline
        with `current_totals` now, so the NEXT comparison starts counting
        from here rather than from the same old point forever.

    A baseline is therefore either (a) freshly created, (b) left alone to
    keep aging, or (c) used for one comparison and then advanced — never
    advanced without being used, which was the earlier bug this replaces
    (overwriting before comparing meant a real spike was always measured
    against itself and could never be detected)."""
    stored = db.get_insight(TREND_BASELINE_KEY)
    if not stored:
        db.save_insight(TREND_BASELINE_KEY, current_totals)
        return None

    age_hours = _hours_since(stored.get("computed_at"))
    if age_hours is None or age_hours < TREND_BASELINE_MIN_AGE_HOURS:
        return None

    db.save_insight(TREND_BASELINE_KEY, current_totals)
    return stored


def get_trending_boosts(spike_ratio=TREND_SPIKE_RATIO,
                         min_views=TREND_MIN_VIEWS_FOR_SIGNAL):
    """Returns {canonical_show: velocity_ratio} for every show whose
    current total views (db.views_by_canonical_show) are at least
    `spike_ratio`x its stored baseline — an empty dict is always a safe,
    valid result (no baseline yet, nothing spiking, or metrics table
    empty all look identical to a caller: 'no boost this run').

    Never raises: a DB read failure here should never take down
    build_news_digest.py over what is, at most, a nice-to-have priority
    nudge — caller still gets a normal (empty) dict to iterate."""
    try:
        current_totals = db.views_by_canonical_show(min_views=min_views)
    except Exception as e:
        print(f"    NOTE: trend_signal couldn't read current view totals "
              f"(non-fatal, no trend boost applied this run): {e}")
        return {}

    try:
        baseline = _get_comparable_baseline(current_totals)
    except Exception as e:
        print(f"    NOTE: trend_signal couldn't read/write its baseline "
              f"(non-fatal, no trend boost applied this run): {e}")
        return {}

    if not baseline:
        # Either the very first run ever, or the baseline just got
        # refreshed to `current_totals` this call — either way there's
        # nothing yet to compare against for a spike THIS run.
        return {}

    baseline_totals = {k: v for k, v in baseline.items() if k != "computed_at"}

    boosts = {}
    for show, current in current_totals.items():
        prior = baseline_totals.get(show)
        if not prior or prior < min_views:
            # New show since the baseline was taken, or one that was too
            # small to trust back then — nothing reliable to compare
            # against; skip rather than treat "prior=0" as infinite growth.
            continue
        ratio = current / prior
        if ratio >= spike_ratio:
            boosts[show] = round(ratio, 2)

    return boosts


def describe_boosts(boosts):
    """Human-readable one-line-per-show summary for print()ing in
    build_news_digest.py's log output — kept separate from
    get_trending_boosts() so callers that just want the dict (no logging)
    don't pay for string formatting they'll never show."""
    if not boosts:
        return "No shows currently spiking against their baseline."
    lines = [f"  {show}: {ratio}x baseline views" for show, ratio in
              sorted(boosts.items(), key=lambda kv: kv[1], reverse=True)]
    return "Trending now (vs. last baseline):\n" + "\n".join(lines)
