"""
Audience-Aware Scheduling — Roadmap Item O
------------------------------------------------
publish_scheduler.py's existing ranked_hours() already prefers REAL
measured performance-by-time-bucket data (performance_insights.py's
posting_time_bucket) — that's the strongest possible signal, since it's
actual outcomes, not a proxy. This module adds a middle tier for when
that's not available yet (or as an additional prior even when it is): a
computed "audience-local peak hour," derived from WHERE the audience
actually is (fetch_youtube_analytics.py's audience_country_top, roadmap
item K), converted to UTC.

THREE-TIER WATERFALL (publish_scheduler.py's ranked_hours() implements
this, this module only supplies tier 2):
  1. Real measured performance-by-time-bucket (strongest — actual outcomes)
  2. Audience-weighted target hour (THIS MODULE — a reasoned proxy)
  3. Flat PUBLISH_DAILY_HOUR_SPREAD default (weakest — no signal at all)

APPROXIMATION, NOT A PRECISE SOLVE: YouTube Analytics' per-video
audience_country_top (as currently collected — see fetch_youtube_
analytics.py) is only the SINGLE top country per video, not a full
per-video percentage breakdown. This module approximates a channel-level
"60% US / 30% SE Asia"-style distribution by treating each measured
clip's top country as one vote, WEIGHTED by that clip's views — a clip
with 10,000 views voting for "US" counts far more than one with 50 views
voting for "PH". This is a reasonable proxy for the real distribution
without a second schema migration; if per-video country PERCENTAGE
breakdowns are ever needed for something more precise, that's a real
follow-up (extend metrics.audience_country_top into a JSON breakdown
column) — not something this version silently pretends to already have.

COUNTRY -> TIMEZONE: a deliberately small, practical mapping (not a full
country-to-multiple-timezones table — most countries this channel's
audience realistically spans have one dominant practical timezone for
"when is prime evening viewing" purposes; the US entry uses America/
Chicago as a rough central compromise across US timezones, an
approximation the roadmap's own "weighted target timezone" framing
already accepts). COUNTRY_TIMEZONES is intentionally easy to extend —
add an entry, nothing else changes.

CIRCULAR MEAN: averaging hours-of-day naively is wrong at the wrap
(23:00 and 01:00 average to 12:00, not the correct ~00:00) — this module
converts each country's target-local-hour to UTC, then to a point on the
unit circle, weight-averages the (x, y) coordinates, and converts back —
the standard fix for averaging circular/periodic quantities.

USAGE:
  import audience_scheduling as asch
  weights = asch.compute_audience_weights()          # {"US": 0.6, "PH": 0.3, ...} or {}
  target_hour, summary = asch.weighted_target_utc_hour()  # (14, "US 60%/PH 30% -> ~14:00 UTC") or (None, None)
"""

import math
import os

import db

TARGET_LOCAL_HOUR = int(os.getenv("AUDIENCE_TARGET_LOCAL_HOUR", "19"))
MIN_SAMPLE = int(os.getenv("AUDIENCE_SCHEDULING_MIN_SAMPLE", "10"))

COUNTRY_TIMEZONES = {
    "US": "America/Chicago", "CA": "America/Toronto", "MX": "America/Mexico_City",
    "BR": "America/Sao_Paulo", "GB": "Europe/London", "DE": "Europe/Berlin",
    "FR": "Europe/Paris", "IT": "Europe/Rome", "ES": "Europe/Madrid",
    "NL": "Europe/Amsterdam", "PL": "Europe/Warsaw", "RU": "Europe/Moscow",
    "IN": "Asia/Kolkata", "PH": "Asia/Manila", "ID": "Asia/Jakarta",
    "TH": "Asia/Bangkok", "VN": "Asia/Ho_Chi_Minh", "MY": "Asia/Kuala_Lumpur",
    "SG": "Asia/Singapore", "JP": "Asia/Tokyo", "KR": "Asia/Seoul",
    "CN": "Asia/Shanghai", "TW": "Asia/Taipei", "HK": "Asia/Hong_Kong",
    "AU": "Australia/Sydney", "NZ": "Pacific/Auckland",
    "AE": "Asia/Dubai", "SA": "Asia/Riyadh", "EG": "Africa/Cairo",
    "NG": "Africa/Lagos", "ZA": "Africa/Johannesburg",
    "AR": "America/Argentina/Buenos_Aires", "CL": "America/Santiago",
    "CO": "America/Bogota", "PE": "America/Lima",
}


def compute_audience_weights():
    """Returns {country_code: weight_0_to_1} across every measured
    YouTube clip with an audience_country_top value, weighted by views.
    Empty dict if fewer than MIN_SAMPLE qualifying clips exist — callers
    must treat {} as 'no reliable audience signal yet', not 'audience is
    evenly distributed'."""
    rows = db.posts_with_metrics()
    weighted = {}
    total_weight = 0.0
    qualifying = 0

    for r in rows:
        if r.get("platform") != "youtube":
            continue
        country = r.get("audience_country_top")
        if not country:
            continue
        views = r.get("views") or 0
        weight = max(views, 1)
        weighted[country] = weighted.get(country, 0.0) + weight
        total_weight += weight
        qualifying += 1

    if qualifying < MIN_SAMPLE or total_weight == 0:
        return {}

    return {country: w / total_weight for country, w in weighted.items()}


def _local_hour_to_utc_hour(local_hour, tz_name):
    """Converts `local_hour` (0-23, today's date) in `tz_name` to a UTC
    hour (float). Uses zoneinfo (stdlib, Python 3.9+). Returns None if
    the timezone name isn't found on this system's tz database."""
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime, timezone
    except ImportError:
        return None
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        return None

    now_local = datetime.now(tz)
    local_dt = now_local.replace(hour=local_hour, minute=0, second=0, microsecond=0)
    utc_dt = local_dt.astimezone(timezone.utc)
    return utc_dt.hour + utc_dt.minute / 60


def weighted_target_utc_hour(target_local_hour=TARGET_LOCAL_HOUR):
    """Returns (utc_hour_int_0_23, summary_str) via a circular (vector)
    mean across each weighted country's UTC-converted target hour, or
    (None, None) if there's no reliable audience data yet."""
    weights = compute_audience_weights()
    if not weights:
        return None, None

    sin_sum, cos_sum, mapped_weight = 0.0, 0.0, 0.0
    contributions = []

    for country, weight in weights.items():
        tz_name = COUNTRY_TIMEZONES.get(country)
        if not tz_name:
            continue
        utc_hour = _local_hour_to_utc_hour(target_local_hour, tz_name)
        if utc_hour is None:
            continue
        angle = (utc_hour / 24) * 2 * math.pi
        sin_sum += weight * math.sin(angle)
        cos_sum += weight * math.cos(angle)
        mapped_weight += weight
        contributions.append((country, weight))

    if mapped_weight == 0:
        return None, None

    mean_angle = math.atan2(sin_sum, cos_sum)
    if mean_angle < 0:
        mean_angle += 2 * math.pi
    target_utc_hour = round((mean_angle / (2 * math.pi)) * 24) % 24

    contributions.sort(key=lambda c: c[1], reverse=True)
    breakdown = "/".join("%s%.0f%%" % (c, w * 100) for c, w in contributions[:3])
    summary = "Audience-weighted (%s) peak hour -> ~%02d:00 UTC" % (breakdown, target_utc_hour)

    return target_utc_hour, summary


if __name__ == "__main__":
    db.init_db()
    weights = compute_audience_weights()
    if not weights:
        print("No reliable audience data yet (need %d+ measured YouTube "
              "clips with audience_country_top set — run "
              "fetch_youtube_analytics.py after clips have been live a "
              "while)." % MIN_SAMPLE)
    else:
        print("Audience distribution (view-weighted, by top country per clip):")
        for country, weight in sorted(weights.items(), key=lambda kv: kv[1], reverse=True):
            print("  %s: %.1f%%" % (country, weight * 100))
        target_hour, summary = weighted_target_utc_hour()
        print("\n" + summary if summary else "\nNo timezone-mappable countries in the data.")
