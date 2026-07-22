"""
Schedule Status
-----------------
The single "what's queued and when" view across both platforms — this is
the direct answer to "I want visibility into the schedule regardless of
platform," separate from whatever each platform actually does with that
timing (YouTube may or may not act on it depending on YOUTUBE_USE_SCHEDULING;
TikTok never auto-acts on it, since posting there is manual).

Reads db.py's publish_schedule table (written by publish_scheduler.py and
updated by publish_youtube.py) and cross-checks each row against
publish_log — publish_log remains the ONLY source of truth for "did this
actually go out"; this script never trusts publish_schedule's own status
field for that, only for scheduling intent (see db.py's publish_schedule
docstring for why those two are kept deliberately separate).

DISPLAY STATUS (computed here, not stored anywhere):
  ✓ LIVE        — publish_log has a 'success' row for this (content, platform)
  ✗ FAILED       — publish_log has a 'failed' row for this (content, platform)
  → SCHEDULED    — YouTube native scheduling engaged (publish_schedule
                    status='scheduled'), not yet confirmed live
  ⏰ DUE NOW      — TikTok manual_pending item whose slot has already
                    arrived — the "post this now" reminder list
  … QUEUED       — recommended slot is still in the future

USAGE:
  python schedule_status.py            # everything upcoming, both platforms
  python schedule_status.py --platform tiktok
  python schedule_status.py --all      # include already-resolved (live/failed) rows too
"""

import argparse
from datetime import datetime, timezone

import db

_STATUS_ICONS = {
    "live": "\u2713 LIVE",
    "failed": "\u2717 FAILED",
    "scheduled": "\u2192 SCHEDULED",
    "due_now": "\u23f0 DUE NOW",
    "queued": "\u2026 queued",
    "cancelled": "\u2718 cancelled",
}


def _publish_log_status(content_key, platform):
    """Returns 'live', 'failed', or None (no publish_log entry yet)."""
    conn = db.get_connection()
    row = conn.execute(
        "SELECT status FROM publish_log WHERE content_key = ? AND platform = ?",
        (content_key, platform),
    ).fetchone()
    conn.close()
    if row is None:
        return None
    return "live" if row["status"] == "success" else "failed"


def _display_status(schedule_row, now):
    log_status = _publish_log_status(schedule_row["content_key"], schedule_row["platform"])
    if log_status:
        return log_status

    if schedule_row["status"] == "cancelled":
        return "cancelled"
    if schedule_row["status"] == "scheduled":
        return "scheduled"

    # pending (youtube, not yet acted on) or manual_pending (tiktok) —
    # distinguish "still upcoming" from "the slot already passed and
    # nobody's acted on it" since that second case is the actionable one
    # for TikTok's always-manual flow.
    try:
        scheduled_dt = datetime.fromisoformat(schedule_row["scheduled_at"])
    except ValueError:
        return "queued"
    if scheduled_dt <= now and schedule_row["platform"] == "tiktok":
        return "due_now"
    return "queued"


def _format_row(row, now):
    status = _display_status(row, now)
    icon = _STATUS_ICONS.get(status, status)
    try:
        dt = datetime.fromisoformat(row["scheduled_at"])
        time_str = dt.strftime("%a %b %d, %H:%M UTC")
    except ValueError:
        time_str = row["scheduled_at"]
    return status, (
        f"  [{row['platform']:7s}] {icon:14s} {time_str:22s} "
        f"{row['content_key']} ({row['content_type']})"
        + (f"\n           {row['reasoning']}" if row.get("reasoning") else "")
    )


def main():
    parser = argparse.ArgumentParser(description="View the publish schedule across platforms")
    parser.add_argument("--platform", choices=["youtube", "tiktok"], default=None)
    parser.add_argument("--all", action="store_true",
                         help="include already-resolved (live/failed) rows too")
    args = parser.parse_args()

    db.init_db()
    rows = db.list_schedule(platform=args.platform)

    if not rows:
        print("Nothing scheduled yet — run publish_scheduler.py after content "
              "reaches the thumbnailed stage.")
        return

    now = datetime.now(timezone.utc)
    due_now, upcoming, resolved = [], [], []

    for row in rows:
        status, line = _format_row(row, now)
        if status in ("live", "failed", "cancelled"):
            resolved.append((row, status, line))
        elif status == "due_now":
            due_now.append((row, status, line))
        else:
            upcoming.append((row, status, line))

    if due_now:
        print(f"\u23f0 DUE NOW — post these manually ({len(due_now)}):\n")
        for _, _, line in due_now:
            print(line)
        print()

    if upcoming:
        print(f"Upcoming ({len(upcoming)}):\n")
        for _, _, line in sorted(upcoming, key=lambda t: t[0]["scheduled_at"]):
            print(line)
        print()

    if not due_now and not upcoming:
        print("Nothing currently queued or due.\n")

    if args.all and resolved:
        print(f"Already resolved ({len(resolved)}):\n")
        for _, _, line in resolved:
            print(line)
    elif resolved and not args.all:
        print(f"({len(resolved)} already-resolved item(s) hidden — pass --all to show them)")


if __name__ == "__main__":
    main()
