"""
Publish Scheduler
-------------------
v3 — PRIORITY FIX on top of v2's throughput fix. v2 solved "5-10 items in
one run only got one slot/day and pushed weeks out" by allowing multiple
slots/day (PUBLISH_MAX_PER_DAY). This version adds two more things,
requested directly after seeing v1's every-post-gets-its-own-day behavior
and after wanting the pipeline's main deliverable to actually behave like
one:

  1. DIGESTS FIRST: a news digest is the pipeline's main deliverable (a
     multi-story roundup vs. a single clip) — within any run, every
     digest gets scheduled before any clip, and within a day's slots,
     digests claim the earliest hours. Clips fill in whatever's left.

  2. URGENT DIGESTS JUMP THE QUEUE: build_news_digest.py tags a digest
     urgent=1 at creation time (see that module's _compute_urgency() —
     corroborated-by-multiple-channels-today, or a story scored at/above
     DIGEST_URGENT_SCORE_THRESHOLD). An urgent digest completely skips
     the normal day-by-day/PUBLISH_MAX_PER_DAY queue and gets the NEXT
     available slot on that platform — respecting only a much smaller
     PUBLISH_URGENT_MIN_GAP_HOURS from other already-claimed slots, not
     the normal daily cap or hour spread. This is what "if it has an
     expiration date, use it fast" means in practice: the content is
     already built (that's Produce's job, upstream) — this is Schedule
     choosing to release it immediately instead of waiting its turn in a
     spread designed for evergreen content.

PROCESSING ORDER PER PLATFORM: urgent digests -> non-urgent digests ->
clips. All three phases share ONE reserved_dt list per platform, updated
in-memory as each item is assigned, so nothing in a later phase can
collide with a slot chosen earlier this same run.

EVERYTHING ELSE (ranked_hours/fallback spread, weekday soft-preference,
non-urgent day/gap logic) is the same throughput-fix design as before —
see those functions' docstrings.

USAGE:
  python publish_scheduler.py
  Run as its own stage in run_all.sh, after generate_thumbnails.py/
  add_hook_overlay.py and before publish_youtube.py.

.env overrides (all optional):
  PUBLISH_MIN_GAP_HOURS=4           # minimum spacing between two
                                       NON-URGENT slots on the same
                                       platform
  PUBLISH_URGENT_MIN_GAP_HOURS=1    # minimum spacing an urgent digest
                                       needs from any other already-
                                       claimed slot (urgent or not) —
                                       kept small since the whole point
                                       is "post this now," not "post
                                       this on the next normal cadence"
  PUBLISH_URGENT_BUFFER_MINUTES=20  # how far past "now" an urgent
                                       digest's slot starts — a small
                                       buffer so publish_youtube.py's
                                       upload has time to finish before
                                       the scheduled publishAt arrives
  PUBLISH_DAILY_HOUR_SPREAD=9,15,21 # fallback hours (UTC) per day when
                                       there's no reliable time-of-day
                                       signal yet
  PUBLISH_INSIGHT_MIN_SAMPLE=5      # min sample size per weekday/time-
                                       bucket before it's trusted
  PUBLISH_LOOKAHEAD_DAYS=30         # how far forward to search for a
                                       free non-urgent slot
  PUBLISH_WEEKDAY_SOFT_WINDOW_DAYS=3 # soft weekday-preference window
                                       for non-urgent slots

MIGRATING FROM v1: if you were relying on PUBLISH_FALLBACK_HOUR_UTC,
replace it with PUBLISH_DAILY_HOUR_SPREAD (a comma-separated list now,
not a single hour) — e.g. PUBLISH_FALLBACK_HOUR_UTC=18 becomes
PUBLISH_DAILY_HOUR_SPREAD=18 if you want to keep exactly one slot/day,
or expand it to a real spread once you raise PUBLISH_MAX_PER_DAY.

LEARN STAGE (added): PUBLISH_MAX_PER_DAY is no longer read from .env —
it's now a learned_config param (see learned_config.py's PARAM_REGISTRY
and mutator.py's rule_publish_max_per_day), same pattern quality_gate.py
already uses for QUALITY_APPROVE_THRESHOLD. mutator.py nudges this ±1/run
based on whether avg views are holding up as cadence increases, gated at
n>=15 measured clips with a 7-day cooldown — see learned_config.py's
registry entry for the exact guardrails. To see or manually override the
current value: `python -c "import learned_config as lc; print(lc.get_param('publish_max_per_day'))"`.
"""

import os
from datetime import datetime, timedelta, timezone

import db
import learned_config as lc
import audience_scheduling as asch
from performance_insights import TIME_BUCKETS, _WEEKDAY_NAMES

lc.init_schema()

PUBLISH_INSIGHT_MIN_SAMPLE = int(os.getenv("PUBLISH_INSIGHT_MIN_SAMPLE", "5"))
PUBLISH_MAX_PER_DAY = int(lc.get_param("publish_max_per_day"))
PUBLISH_MIN_GAP_HOURS = float(os.getenv("PUBLISH_MIN_GAP_HOURS", "4"))
PUBLISH_LOOKAHEAD_DAYS = int(os.getenv("PUBLISH_LOOKAHEAD_DAYS", "30"))
PUBLISH_WEEKDAY_SOFT_WINDOW_DAYS = int(os.getenv("PUBLISH_WEEKDAY_SOFT_WINDOW_DAYS", "3"))

PUBLISH_URGENT_MIN_GAP_HOURS = float(os.getenv("PUBLISH_URGENT_MIN_GAP_HOURS", "1"))
PUBLISH_URGENT_BUFFER_MINUTES = float(os.getenv("PUBLISH_URGENT_BUFFER_MINUTES", "20"))

_DEFAULT_SPREAD = "9,15,21"
PUBLISH_DAILY_HOUR_SPREAD = sorted({
    int(h.strip()) for h in os.getenv("PUBLISH_DAILY_HOUR_SPREAD", _DEFAULT_SPREAD).split(",")
    if h.strip().isdigit()
}) or [9, 15, 21]

PLATFORMS = ("youtube", "tiktok")


def _bucket_midpoint_hour(label):
    """TIME_BUCKETS entries are (label, range(start,end)) — returns the
    middle hour of that range. Falls back to the first configured spread
    hour if the label isn't found (shouldn't happen, but never trust a
    string round-trip through JSON blindly)."""
    for bucket_label, hours in TIME_BUCKETS:
        if bucket_label == label:
            hours_list = list(hours)
            return hours_list[len(hours_list) // 2]
    return PUBLISH_DAILY_HOUR_SPREAD[0]


def ranked_hours(insight, max_n=PUBLISH_MAX_PER_DAY):
    """Returns up to `max_n` candidate hours (UTC, chronologically sorted)
    to offer as slots on a single day. THREE-TIER waterfall (roadmap item
    O — audience-aware scheduling):
      1. Real measured performance-by-time-bucket data — every bucket
         that meets PUBLISH_INSIGHT_MIN_SAMPLE and beats the channel
         average, best-performing first (strongest signal: actual
         outcomes).
      2. audience_scheduling.weighted_target_utc_hour() — a computed
         'where does the audience actually live' proxy, used to fill in
         when tier 1 doesn't produce enough hours on its own.
      3. PUBLISH_DAILY_HOUR_SPREAD flat default — used only once tiers 1
         and 2 are both exhausted/unavailable.
    This is what lets throughput scale independently of whether analytics
    exist: a fresh channel with zero insight still gets PUBLISH_MAX_PER_DAY
    real daily slots from the fallback spread."""
    hours = []
    if insight:
        buckets = sorted(
            insight.get("posting_time_bucket", []),
            key=lambda b: b["avg_views"], reverse=True,
        )
        for b in buckets:
            if len(hours) >= max_n:
                break
            if b["sample_size"] < PUBLISH_INSIGHT_MIN_SAMPLE or b["vs_overall_avg_pct"] <= 0:
                continue
            h = _bucket_midpoint_hour(b["time_bucket"])
            if h not in hours:
                hours.append(h)

    if len(hours) < max_n:
        # Tier 2 — audience-weighted target hour, only used to TOP UP
        # what tier 1 didn't already fill (real measured performance
        # data always wins when it exists).
        audience_hour, _ = asch.weighted_target_utc_hour()
        if audience_hour is not None and audience_hour not in hours:
            hours.append(audience_hour)

    if not hours:
        hours = list(PUBLISH_DAILY_HOUR_SPREAD[:max_n])
    elif len(hours) < max_n:
        # Reliable data gave us SOME good hours but not enough to fill the
        # day's full quota — top up from the fallback spread rather than
        # under-using PUBLISH_MAX_PER_DAY just because insight data is thin.
        for h in PUBLISH_DAILY_HOUR_SPREAD:
            if len(hours) >= max_n:
                break
            if h not in hours:
                hours.append(h)

    return sorted(hours)


def pick_preferred_weekdays(insight):
    """Returns a set of weekday names that reliably beat the channel
    average, or None if there isn't enough data to prefer any day over
    another."""
    if not insight:
        return None
    reliable_good_days = {
        d["weekday"] for d in insight.get("posting_weekday", [])
        if d["sample_size"] >= PUBLISH_INSIGHT_MIN_SAMPLE and d["vs_overall_avg_pct"] > 0
    }
    return reliable_good_days or None


def _reasoning_for_hour(insight, hour):
    """Best-effort human-readable note for why `hour` was picked — checked
    in the same three-tier order ranked_hours() itself uses: real
    measured bucket data first, then the audience-weighted computed hour
    (roadmap item O — this is what makes schedule_status.py's printed
    reasoning read as "audience-local peak hour" instead of just a UTC
    number, since schedule_status.py already displays this string
    verbatim with no changes needed there), then the flat fallback note."""
    if insight:
        for b in insight.get("posting_time_bucket", []):
            if _bucket_midpoint_hour(b["time_bucket"]) == hour and b["sample_size"] >= PUBLISH_INSIGHT_MIN_SAMPLE:
                sign = "+" if b["vs_overall_avg_pct"] >= 0 else ""
                return f"{b['time_bucket']} {sign}{b['vs_overall_avg_pct']:.0f}% vs avg (n={b['sample_size']})"

    audience_hour, audience_summary = asch.weighted_target_utc_hour()
    if audience_hour == hour and audience_summary:
        return audience_summary

    return f"{hour:02d}:00 UTC (fallback spread — no reliable time-of-day or audience signal yet)"


def _day_slot_count(reserved_dt, day):
    return sum(1 for r in reserved_dt if r.date() == day)


def _slot_far_enough(candidate_dt, reserved_dt, gap_hours):
    gap = timedelta(hours=gap_hours)
    return all(abs((candidate_dt - r).total_seconds()) >= gap.total_seconds() for r in reserved_dt)


def find_next_urgent_slot(reserved_dt, now=None):
    """Returns (scheduled_at_dt, reasoning_str) for an urgent digest —
    ignores PUBLISH_MAX_PER_DAY and the hour spread entirely. Starts
    searching from now + PUBLISH_URGENT_BUFFER_MINUTES, walking forward
    in small steps until it clears PUBLISH_URGENT_MIN_GAP_HOURS from
    every already-reserved slot (urgent or not — an urgent item still
    shouldn't land in the exact same minute as something else already
    queued). This deliberately has no day cap: urgency means 'post this
    as soon as reasonably possible,' not 'post this within today's
    normal quota.'"""
    now = now or datetime.now(timezone.utc)
    candidate = now + timedelta(minutes=PUBLISH_URGENT_BUFFER_MINUTES)
    step = timedelta(minutes=15)
    # Bounded to avoid a pathological infinite loop — 4 days of 15-minute
    # steps is far more headroom than this path should ever need in
    # practice (it exists to jump the queue, not join the back of it).
    max_steps = int(timedelta(days=4) / step)
    for _ in range(max_steps):
        if _slot_far_enough(candidate, reserved_dt, PUBLISH_URGENT_MIN_GAP_HOURS):
            return candidate, "URGENT — time-sensitive content, released as soon as possible"
        candidate += step
    return candidate, "URGENT (queue saturated even for urgent slots — released anyway)"


def find_next_slot(platform, reserved_dt, insight=None, now=None):
    """Non-urgent path — respects PUBLISH_MAX_PER_DAY and
    PUBLISH_MIN_GAP_HOURS, with a soft weekday preference."""
    now = now or datetime.now(timezone.utc)
    hours = ranked_hours(insight)
    preferred_weekdays = pick_preferred_weekdays(insight)

    def candidates_for_day(offset):
        day = (now + timedelta(days=offset)).date()
        if _day_slot_count(reserved_dt, day) >= PUBLISH_MAX_PER_DAY:
            return
        for h in hours:
            dt = datetime(day.year, day.month, day.day, h, 0, tzinfo=timezone.utc)
            if dt <= now:
                continue
            yield dt

    if preferred_weekdays:
        window = min(PUBLISH_WEEKDAY_SOFT_WINDOW_DAYS, PUBLISH_LOOKAHEAD_DAYS)
        for offset in range(window):
            weekday_name = _WEEKDAY_NAMES[(now + timedelta(days=offset)).weekday()]
            if weekday_name not in preferred_weekdays:
                continue
            for dt in candidates_for_day(offset):
                if _day_slot_count(reserved_dt, dt.date()) >= PUBLISH_MAX_PER_DAY:
                    break
                if _slot_far_enough(dt, reserved_dt, PUBLISH_MIN_GAP_HOURS):
                    reasoning = _reasoning_for_hour(insight, dt.hour) + f", {weekday_name}"
                    return dt, reasoning

    for offset in range(PUBLISH_LOOKAHEAD_DAYS):
        for dt in candidates_for_day(offset):
            if _day_slot_count(reserved_dt, dt.date()) >= PUBLISH_MAX_PER_DAY:
                break
            if _slot_far_enough(dt, reserved_dt, PUBLISH_MIN_GAP_HOURS):
                return dt, _reasoning_for_hour(insight, dt.hour)

    fallback_dt = (reserved_dt[-1] if reserved_dt else now) + timedelta(hours=PUBLISH_MIN_GAP_HOURS)
    return fallback_dt, "schedule fully saturated within lookahead window — gap relaxed"


def _schedule_one(content_key, content_type, platform, reserved_dt, insight, now, urgent=False):
    if urgent:
        slot_dt, reasoning = find_next_urgent_slot(reserved_dt, now)
    else:
        slot_dt, reasoning = find_next_slot(platform, reserved_dt, insight, now)
    reserved_dt.append(slot_dt)
    scheduled_at = slot_dt.isoformat()
    status = "manual_pending" if platform == "tiktok" else "pending"
    db.upsert_schedule(content_key, content_type, platform, scheduled_at, status, reasoning)
    print(f"  [{platform}] {content_key}: {scheduled_at} — {reasoning}")


def schedule_ready_content():
    """Finds everything at 'thumbnailed' stage or later with no
    publish_log entry yet, computes a slot for any (content, platform)
    pair missing a publish_schedule row. PROCESSING ORDER per platform:
    urgent digests -> non-urgent digests -> clips (see module docstring).
    Returns (clips_scheduled, digests_scheduled)."""
    insight = db.get_insight("content_strategy")
    now = datetime.now(timezone.utc)

    clips_done = digests_done = 0

    for platform in PLATFORMS:
        reserved_dt = [
            datetime.fromisoformat(t) for t in db.reserved_schedule_times(platform, now.isoformat())
        ]

        all_digests = db.get_publishable_digests(platform, min_stage="thumbnailed")
        pending_digests = [d for d in all_digests if not db.has_schedule_entry(d["digest_id"], platform)]
        urgent_digests = [d for d in pending_digests if d.get("urgent")]
        normal_digests = [d for d in pending_digests if not d.get("urgent")]

        for digest in urgent_digests:
            _schedule_one(digest["digest_id"], "digest", platform, reserved_dt, insight, now, urgent=True)
            digests_done += 1

        for digest in normal_digests:
            _schedule_one(digest["digest_id"], "digest", platform, reserved_dt, insight, now, urgent=False)
            digests_done += 1

        for post in db.get_publishable_posts(platform, min_stage="thumbnailed"):
            if db.has_schedule_entry(post["key"], platform):
                continue
            _schedule_one(post["key"], "clip", platform, reserved_dt, insight, now, urgent=False)
            clips_done += 1

    return clips_done, digests_done


def main():
    db.init_db()
    insight = db.get_insight("content_strategy")
    if insight:
        print(f"Using performance insight from {insight['sample_size']} measured clips "
              f"(computed {insight['computed_at']}).\n")
    else:
        print(f"No performance insight yet — non-urgent slots use the fallback hour "
              f"spread ({PUBLISH_DAILY_HOUR_SPREAD}) with no weekday preference.\n")

    print(f"Priority order: urgent digests -> normal digests -> clips.")
    print(f"Non-urgent: max {PUBLISH_MAX_PER_DAY} slot(s)/day/platform, "
          f"{PUBLISH_MIN_GAP_HOURS}h min gap. "
          f"Urgent: next slot, {PUBLISH_URGENT_MIN_GAP_HOURS}h min gap, no daily cap.\n")

    print("Computing publish schedule for ready content...")
    clips_done, digests_done = schedule_ready_content()

    if clips_done == 0 and digests_done == 0:
        print("Nothing new to schedule — everything ready already has a slot.")
    else:
        print(f"\nScheduled {clips_done} clip(s) and {digests_done} digest(s). "
              f"Run schedule_status.py any time to see the full queue.")


if __name__ == "__main__":
    main()
