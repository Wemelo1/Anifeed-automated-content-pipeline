"""
Run Summary / Alerting
------------------------
Closes the "fully automated but silent when broken" gap: run_all.sh uses
`set -e`, so if any stage fails, the whole script stops immediately with no
signal beyond terminal output nobody's watching. This script is called from
run_all.sh via a bash `trap ... EXIT` (see run_all.sh), so it fires whether
the run completes normally, dies partway through a stage, or gets killed —
`trap ... EXIT` runs on every exit path, not just a clean one.

Two modes:

  --start
      Records a new row in pipeline_runs (status='running') and prints the
      new run_id to stdout — run_all.sh captures this into $RUN_ID via
      command substitution, since it needs to pass the same id to --finish
      later in the same script.

  --finish --run-id N --exit-code N [--log-file PATH]
      Diffs pipeline.db against the run's start time (db.build_run_summary)
      to see what actually happened, classifies the run as
      success/partial/failed, records it, and sends a notification.

STATUS CLASSIFICATION:
  - exit_code != 0                          -> "failed"
  - exit_code == 0 but any *_failed count > 0 -> "partial" (ran to
    completion, but at least one item terminally failed along the way —
    e.g. a YouTube upload that hit a genuine error, not a quota pause)
  - exit_code == 0 and no failures            -> "success"

NOTIFICATION DELIVERY:
  Uses the `termux-notification` CLI (Termux:API) if it's on PATH. Falls
  back to printing the summary if it isn't — keeps this usable in a plain
  dev/test environment and means a missing Termux:API install degrades
  gracefully instead of crashing the whole run_all.sh script via the trap.

USAGE (called by run_all.sh, not normally run directly):
  RUN_ID=$(python run_summary.py --start)
  ... (pipeline stages run) ...
  python run_summary.py --finish --run-id "$RUN_ID" --exit-code $? --log-file "$LOG_FILE"

  python run_summary.py --history        # print recent runs, any time
"""

import argparse
import json
import os
import shutil
import subprocess
import sys

import db

# Fields in db.build_run_summary()'s output that represent a terminal
# failure (as opposed to a count of successful work) — any of these being
# nonzero downgrades an exit_code==0 run from "success" to "partial".
_FAILURE_FIELDS = ["clips_failed", "youtube_failed", "tiktok_failed"]

# Fields that represent real forward progress — used to distinguish a
# genuinely dead run ("failed": nothing worked at all) from a run where
# one stage hit a problem but the pipeline still got real work done
# ("partial"). Motivated by a real production run: a transient TTS
# timeout inside build_news_digest.py killed the whole run under the old
# all-or-nothing run_all.sh, despite 11 posts having been pulled and 7
# clips fully edited/subtitled/identified/captioned in that same run —
# calling that "failed" was misleading; "partial" is the honest label.
_ACTIVITY_FIELDS = [
    "posts_pulled", "clips_edited", "clips_subtitled", "clips_identified",
    "clips_captioned", "clips_thumbnailed", "clips_hooked", "digests_built",
    "youtube_published", "tiktok_published",
]

LOG_TAIL_LINES = 15


def classify_status(exit_code, summary):
    had_activity = any(summary.get(f, 0) > 0 for f in _ACTIVITY_FIELDS)
    if exit_code != 0:
        return "partial" if had_activity else "failed"
    if any(summary.get(f, 0) > 0 for f in _FAILURE_FIELDS):
        return "partial"
    return "success"


def format_summary_text(summary, status, exit_code):
    parts = []
    if summary["posts_pulled"]:
        parts.append(f"{summary['posts_pulled']} pulled")
    if summary["clips_thumbnailed"]:
        parts.append(f"{summary['clips_thumbnailed']} ready")
    if summary["youtube_published"]:
        parts.append(f"{summary['youtube_published']} → YouTube")
    if summary["tiktok_published"]:
        parts.append(f"{summary['tiktok_published']} → TikTok")
    if summary["digests_built"]:
        parts.append(f"{summary['digests_built']} digest(s) built")
    if summary["new_canonical_shows"]:
        parts.append(f"{summary['new_canonical_shows']} new show(s) identified")

    failures = []
    if summary["clips_failed"]:
        failures.append(f"{summary['clips_failed']} clip(s) failed processing")
    if summary["youtube_failed"]:
        failures.append(f"{summary['youtube_failed']} YouTube upload(s) failed")
    if summary["tiktok_failed"]:
        failures.append(f"{summary['tiktok_failed']} TikTok upload(s) failed")

    if status == "failed":
        headline = f"Pipeline run FAILED (exit code {exit_code})"
    elif status == "partial":
        headline = "Pipeline run completed with errors"
    elif parts:
        headline = "Pipeline run completed"
    else:
        headline = "Pipeline run completed — nothing new"

    body_lines = []
    if parts:
        body_lines.append(", ".join(parts))
    if failures:
        body_lines.append("⚠ " + "; ".join(failures))

    return headline, "\n".join(body_lines) if body_lines else "No new activity."


def send_notification(title, body):
    if shutil.which("termux-notification") is None:
        print(f"\n[{title}]\n{body}")
        # The Termux:API install hint only makes sense on Termux/Android.
        if "com.termux" in sys.prefix or "TERMUX_VERSION" in os.environ:
            print("\n(termux-notification not found — printed instead of notifying. "
                  "Install Termux:API for real push notifications: "
                  "pkg install termux-api, plus the Termux:API app from F-Droid/Play Store.)")
        return
    try:
        subprocess.run(
            ["termux-notification", "--title", title, "--content", body,
             "--id", "anime_pipeline_run"],
            capture_output=True, text=True, timeout=20,
        )
    except Exception as e:
        print(f"\n[{title}]\n{body}")
        print(f"\n(termux-notification call failed, printed instead: {e})")


STALE_RUN_HOURS = float(os.getenv("STALE_RUN_HOURS", "3"))


def cmd_start():
    closed = db.close_stale_runs(older_than_hours=STALE_RUN_HOURS)
    for run_id in closed:
        # stderr, not stdout — see the module docstring's note on why
        # --start's stdout must contain ONLY the new run_id for bash's
        # $(...) capture to work.
        print(f"NOTE: run #{run_id} was still 'running' after {STALE_RUN_HOURS}h "
              f"with no clean exit — closed as 'interrupted' (see "
              f"run_summary.py --show --run-id {run_id} for details, or "
              f"--history for the full list).", file=sys.stderr)
    run_id = db.start_run()
    print(run_id)  # only stdout output in --start mode — bash captures this


def cmd_finish(run_id, exit_code, log_file):
    run = db.get_run(run_id)
    if run is None:
        print(f"WARNING: run_id {run_id} not found in pipeline_runs — was --start "
              f"skipped or pipeline.db reset mid-run? Recording a fresh run instead.")
        run = {"started_at": db._now()}
        run_id = db.start_run()

    summary = db.build_run_summary(run["started_at"])
    status = classify_status(exit_code, summary)

    if status == "failed" and log_file:
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            summary["log_tail"] = "".join(lines[-LOG_TAIL_LINES:]).strip()
        except OSError:
            pass

    db.finish_run(run_id, status, exit_code, summary)

    headline, body = format_summary_text(summary, status, exit_code)
    if status == "failed" and summary.get("log_tail"):
        body += f"\n\nLast output:\n{summary['log_tail'][-500:]}"

    print(f"\n=== Run #{run_id}: {headline} ===")
    print(body)

    send_notification(headline, body[:1000])  # notification bodies should stay short


def cmd_history(limit):
    runs = db.recent_runs(limit)
    if not runs:
        print("No pipeline runs recorded yet.")
        return
    print(f"Last {len(runs)} run(s):\n")
    for r in runs:
        summary = json.loads(r["summary_json"]) if r["summary_json"] else {}
        status = r["status"] or "running"
        marker = {"success": "✓", "partial": "⚠", "failed": "✗",
                  "interrupted": "⏸", "running": "…"}.get(status, "?")
        print(f"{marker} Run #{r['id']} — {status.upper()} — started {r['started_at']}")
        if summary:
            bits = [f"{k}={v}" for k, v in summary.items()
                    if k != "log_tail" and v]
            if bits:
                print(f"    {', '.join(bits)}")
        print()


def cmd_show(run_id):
    """Full detail for one run, including the complete log tail on a
    failure — --history intentionally omits this to stay scannable, so
    this is the follow-up command for 'ok but WHY did run #8 fail'."""
    run = db.get_run(run_id)
    if run is None:
        print(f"No run found with id {run_id}.")
        return

    status = run["status"] or "running"
    print(f"Run #{run['id']} — {status.upper()}")
    print(f"  started:   {run['started_at']}")
    print(f"  finished:  {run['finished_at'] or '(not finished)'}")
    print(f"  exit code: {run['exit_code']}")

    summary = json.loads(run["summary_json"]) if run["summary_json"] else {}
    log_tail = summary.pop("log_tail", None)

    if summary:
        print("\nSummary:")
        for k, v in summary.items():
            if v:
                print(f"  {k}: {v}")

    if log_tail:
        print("\nLast output before this run ended:")
        print("-" * 60)
        print(log_tail)
        print("-" * 60)
    elif status == "failed":
        print("\n(No log tail captured for this run — it may predate the "
              "--log-file wiring in run_all.sh, or the log file was already "
              "cleaned up.)")


def main():
    parser = argparse.ArgumentParser(description="Pipeline run tracking and alerting")
    parser.add_argument("--start", action="store_true")
    parser.add_argument("--finish", action="store_true")
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--show", action="store_true")
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--exit-code", type=int, default=0)
    parser.add_argument("--log-file")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    db.init_db()

    if args.start:
        cmd_start()
    elif args.finish:
        if args.run_id is None:
            print("ERROR: --finish requires --run-id", file=sys.stderr)
            sys.exit(2)
        cmd_finish(args.run_id, args.exit_code, args.log_file)
    elif args.history:
        cmd_history(args.limit)
    elif args.show:
        if args.run_id is None:
            print("ERROR: --show requires --run-id", file=sys.stderr)
            sys.exit(2)
        cmd_show(args.run_id)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
