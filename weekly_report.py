"""
Weekly Report
--------------
The visibility piece the self-learning loop was missing (roadmap item J):
mutator.py has been quietly nudging learned_config.py's params run after
run, and performance_insights.py has been recomputing "content_strategy"
daily-ish — but nothing ever surfaced "did any of this actually help" in
one place. Without this, the pipeline is a black box that may or may not
be improving; this script makes that visible and debuggable.

WHAT IT COMPARES: two adjacent 7-day windows — "this week" (now-7d..now)
vs "last week" (now-14d..now-7d) — across:
  1. Average views per clip, for clips whose FIRST successful publish
     (posts.published_at) falls inside each window.
  2. Average engagement_score per clip, same window/grouping, where
     engagement_score = (likes + comments*2 + shares*3) / views — the
     weighting scheme from the roadmap doc: a comment is worth 2x a like,
     a share 3x, since both are stronger buy-in signals than a passive
     like. Computed per-clip (summed across platforms first, then
     divided), then averaged across clips in the window — NOT a single
     ratio of window-wide totals, so one huge-view/low-engagement clip
     can't single-handedly drag the window average around the way a
     totals-based ratio would.
  3. Quality-gate rejection rate (rejected / total items checked, clips
     + digests combined) for items whose quality_checked_at falls inside
     each window.
  4. Every learned_config.py mutation that actually TOOK EFFECT (status
     applied / clamped_and_applied / approved_manually) inside "this
     week" — read straight from learned_config_log rather than trying to
     reconstruct a full before/after diff of every registered param, most
     of which won't have changed at all in a given week.
  5. The full current snapshot of every learned_config param's effective
     value right now, for at-a-glance reference alongside the changes above.

HONEST LIMITATIONS (read before trusting a number blindly):
  - "This week's avg views" reflects views EARNED SO FAR by clips
    published this week, not final lifetime performance — a clip
    published 2 days ago has had far less time to accumulate views than
    one published 6 days ago, so a low week-over-week number in the days
    right after a change can just mean "not enough time has passed yet,"
    not "the change hurt." Reading this weekly, over several weeks, is
    what makes it meaningful — a single week's number is a data point,
    not a verdict.
  - Sample sizes (n_this/n_last, printed alongside every metric) can be
    small on a phone-scale posting cadence — this script does NOT apply
    performance_insights.py's MIN_SAMPLE_SIZE gate (this is a reporting
    tool for a human to read with context, not an automated decision
    input like performance_insights.py's cached insight is), so always
    check the n before reacting to a percentage swing.
  - Engagement/quality-rejection data depends on fetch_analytics.py /
    quality_gate.py having actually run during each window — a week with
    zero analytics runs shows "n/a", not a misleading 0%.

USAGE:
  python weekly_report.py            # human-readable report to stdout
  python weekly_report.py --json     # same data as a single JSON object,
                                       # for piping into another tool/log
"""

import argparse
import json as json_module
from datetime import datetime, timedelta, timezone

import db
import learned_config as lc

WINDOW_DAYS = 7


def _iso(dt):
    return dt.isoformat()


def _window_bounds(now=None):
    """Returns ((this_start, this_end), (last_start, last_end)) as ISO
    strings — two adjacent, non-overlapping 7-day windows ending at
    `now` and `now - 7d` respectively."""
    now = now or datetime.now(timezone.utc)
    this_start = now - timedelta(days=WINDOW_DAYS)
    last_start = now - timedelta(days=WINDOW_DAYS * 2)
    return (_iso(this_start), _iso(now)), (_iso(last_start), _iso(this_start))


def _clip_stats_for_window(start_iso, end_iso):
    """Returns (avg_views, avg_engagement_score, sample_size) for every
    clip whose posts.published_at falls in [start_iso, end_iso). Reuses
    db.posts_with_metrics() (same source performance_insights.py reads)
    rather than a new query, so this stays consistent with however that
    function already defines "a clip's latest snapshot per platform."

    avg_engagement_score is None (not 0) if no clip in the window has any
    views at all — dividing by zero views would be meaningless, not
    zero-engagement, and this function's callers already print "n/a" for
    a None the same way they do for "no clips at all this window"."""
    rows = db.posts_with_metrics()
    by_key = {}
    for r in rows:
        published_at = r.get("published_at")
        if not published_at or not (start_iso <= published_at < end_iso):
            continue
        by_key.setdefault(r["key"], []).append(r)

    if not by_key:
        return None, None, 0

    views_list = []
    engagement_list = []
    for platform_rows in by_key.values():
        views = sum(r["views"] or 0 for r in platform_rows)
        likes = sum(r["likes"] or 0 for r in platform_rows)
        comments = sum(r["comments"] or 0 for r in platform_rows)
        shares = sum(r["shares"] or 0 for r in platform_rows)
        views_list.append(views)
        if views > 0:
            engagement_list.append((likes + comments * 2 + shares * 3) / views)

    avg_views = round(sum(views_list) / len(views_list), 1) if views_list else None
    avg_engagement = (
        round(sum(engagement_list) / len(engagement_list), 4)
        if engagement_list else None
    )
    return avg_views, avg_engagement, len(by_key)


def _quality_rejection_rate(start_iso, end_iso):
    """Returns (rejection_rate_pct, total_checked) across posts + digests
    combined, for items whose quality_checked_at falls in the window.
    Direct SQL against db.get_connection() rather than a new db.py
    function — same pattern check_digest_state.py / dupe_check.py already
    use for one-off diagnostic queries that don't need to be a permanent
    part of db.py's API surface."""
    conn = db.get_connection()
    total = 0
    rejected = 0
    for table in ("posts", "digests"):
        rows = conn.execute(
            f"""SELECT quality_status FROM {table}
                WHERE quality_checked_at >= ? AND quality_checked_at < ?""",
            (start_iso, end_iso),
        ).fetchall()
        for r in rows:
            total += 1
            if r["quality_status"] == "rejected":
                rejected += 1
    conn.close()

    if total == 0:
        return None, 0
    return round(rejected / total * 100, 1), total


def _param_changes_in_window(start_iso, end_iso):
    """Every learned_config mutation that actually took effect within
    [start_iso, end_iso) — see module docstring point 4 for why this
    reads learned_config_log directly rather than reconstructing a full
    param-by-param diff."""
    conn = db.get_connection()
    rows = conn.execute(
        """SELECT param_name, old_value_json, applied_value_json, reasoning, created_at
           FROM learned_config_log
           WHERE status IN ('applied', 'clamped_and_applied', 'approved_manually')
             AND created_at >= ? AND created_at < ?
           ORDER BY created_at ASC""",
        (start_iso, end_iso),
    ).fetchall()
    conn.close()

    changes = []
    for r in rows:
        d = dict(r)
        d["old_value"] = json_module.loads(d["old_value_json"]) if d["old_value_json"] else None
        d["applied_value"] = json_module.loads(d["applied_value_json"]) if d["applied_value_json"] else None
        changes.append(d)
    return changes


def _pct_change(new, old):
    if new is None or old is None or old == 0:
        return None
    return round((new - old) / old * 100, 1)


def build_report(now=None):
    lc.init_schema()
    (this_start, this_end), (last_start, last_end) = _window_bounds(now)

    this_views, this_engagement, this_n = _clip_stats_for_window(this_start, this_end)
    last_views, last_engagement, last_n = _clip_stats_for_window(last_start, last_end)

    this_rejection, this_checked = _quality_rejection_rate(this_start, this_end)
    last_rejection, last_checked = _quality_rejection_rate(last_start, last_end)

    return {
        "window": {
            "this_week": {"start": this_start, "end": this_end},
            "last_week": {"start": last_start, "end": last_end},
        },
        "avg_views_per_clip": {
            "this_week": this_views, "last_week": last_views,
            "pct_change": _pct_change(this_views, last_views),
            "n_this_week": this_n, "n_last_week": last_n,
        },
        "avg_engagement_score": {
            "this_week": this_engagement, "last_week": last_engagement,
            "pct_change": _pct_change(this_engagement, last_engagement),
        },
        "quality_rejection_rate_pct": {
            "this_week": this_rejection, "last_week": last_rejection,
            "n_checked_this_week": this_checked, "n_checked_last_week": last_checked,
        },
        "learned_param_changes_this_week": _param_changes_in_window(this_start, this_end),
        "current_learned_param_values": lc.all_current_values(),
    }


def _fmt_num(v, decimals=0):
    return "n/a" if v is None else f"{v:.{decimals}f}"


def _fmt_pct_change(v):
    if v is None:
        return ""
    sign = "+" if v >= 0 else ""
    return f" ({sign}{v:.0f}%)"


def print_report(report):
    w = report["window"]
    print("=" * 70)
    print("WEEKLY REPORT")
    print(f"  This week: {w['this_week']['start'][:10]} .. {w['this_week']['end'][:10]}")
    print(f"  Last week: {w['last_week']['start'][:10]} .. {w['last_week']['end'][:10]}")
    print("=" * 70)

    v = report["avg_views_per_clip"]
    print(f"\nAvg views/clip:        {_fmt_num(v['last_week'])} -> "
          f"{_fmt_num(v['this_week'])}{_fmt_pct_change(v['pct_change'])}"
          f"  (n={v['n_last_week']} -> {v['n_this_week']})")

    e = report["avg_engagement_score"]
    print(f"Avg engagement score:  {_fmt_num(e['last_week'], 4)} -> "
          f"{_fmt_num(e['this_week'], 4)}{_fmt_pct_change(e['pct_change'])}")

    q = report["quality_rejection_rate_pct"]
    last_q = "n/a" if q["last_week"] is None else f"{q['last_week']:.0f}%"
    this_q = "n/a" if q["this_week"] is None else f"{q['this_week']:.0f}%"
    print(f"Quality rejection rate: {last_q} -> {this_q}"
          f"  (n={q['n_checked_last_week']} -> {q['n_checked_this_week']})")

    changes = report["learned_param_changes_this_week"]
    if changes:
        print(f"\nLearned param changes this week ({len(changes)}):")
        for c in changes:
            print(f"  {c['param_name']}: {c['old_value']} -> {c['applied_value']}")
            if c.get("reasoning"):
                print(f"      {c['reasoning']}")
    else:
        print("\nNo learned param changes took effect this week "
              "(no mutation cleared its sample-size/cooldown gates, or "
              "mutator.py didn't run).")

    print("\nCurrent learned param values:")
    for name, value in sorted(report["current_learned_param_values"].items()):
        print(f"  {name}: {value}")

    print("\n" + "=" * 70)
    print("Note: avg views/clip reflects views EARNED SO FAR by clips "
          "published in each window, not final lifetime performance — a "
          "clip published a day ago has had far less time to accumulate "
          "views than one published six days ago. Read this over several "
          "consecutive weeks, not as a single verdict. See this script's "
          "module docstring for the full caveats.")


def main():
    parser = argparse.ArgumentParser(description="Week-over-week performance + learned-config diff")
    parser.add_argument("--json", action="store_true",
                         help="print the raw report as JSON instead of a formatted summary")
    args = parser.parse_args()

    db.init_db()
    report = build_report()

    if args.json:
        print(json_module.dumps(report, indent=2))
    else:
        print_report(report)


if __name__ == "__main__":
    main()
