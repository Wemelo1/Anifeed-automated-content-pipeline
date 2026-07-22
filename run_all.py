#!/usr/bin/env python3
"""
AniFeed Studio runner (Windows/Linux/Termux) — replaces run_all.sh,
run_analytics.sh and run_weekly_report.sh.

  python run_all.py                      # pipeline (default)
  python run_all.py analytics            # daily analytics + learn stage
  python run_all.py weekly               # weekly report + caption bank
  python run_all.py pipeline --only edit_clips,burn_subtitles   # names or 1-based numbers
  python run_all.py --list               # show stages (all kinds, or the chosen one)
  python run_all.py --force              # break an existing lock

Behaviour:
  * Every stage runs isolated: one failing stage never stops the rest.
    Exit code is 1 if any stage failed (so schedulers / run_summary see it).
  * analytics: mutator is SKIPPED if performance_insights failed (it would
    act on stale insight).
  * pipeline only: wrapped in run_summary.py --start/--finish, like run_all.sh.
    (analytics/weekly are NOT recorded there: run_summary diffs pipeline
    stage timestamps, so it would log them as "nothing new".)
  * Logs in logs/ (last 30 kept) + per-stage timing in logs/stage_timings.csv.
  * Windows keep-awake via SetThreadExecutionState. NOTE: this stops idle
    sleep only; lid-close needs the powercfg step in WINDOWS_SETUP.md.
  * Children get PYTHONUTF8=1 so '→ ✓ ⚠' prints don't crash on cp1252 pipes.
  * Lockfile via O_EXCL. Never uses os.kill(pid, 0) (that TERMINATES a
    process on Windows). Staleness = lock file not touched for
    ANIFEED_LOCK_STALE_HOURS (default 4); it's touched after every stage.
"""

import argparse
import csv
import datetime as dt
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import NamedTuple, Tuple

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
LOCK = LOG_DIR / "run.lock"
TIMINGS_CSV = LOG_DIR / "stage_timings.csv"
KEEP_LOGS = 30
STALE_HOURS = float(os.getenv("ANIFEED_LOCK_STALE_HOURS", "4"))


class Stage(NamedTuple):
    name: str
    args: Tuple[str, ...]
    requires: Tuple[str, ...] = ()   # skip this stage if any of these ran and failed


PIPELINE = [
    Stage("rss_news_pull", ("rss_news_pull.py",)),
    Stage("fetch_anilist_trending", ("fetch_anilist_trending.py",)),
    Stage("reddit_pull", ("reddit_pull.py",)),
    Stage("fetch_youtube_trailers", ("fetch_youtube_trailers.py",)),
    Stage("edit_clips", ("edit_clips.py",)),
    Stage("burn_subtitles", ("burn_subtitles.py",)),
    Stage("identify_show", ("identify_show.py",)),
    Stage("generate_captions", ("generate_captions.py",)),
    Stage("build_news_digest", ("build_news_digest.py",)),
    Stage("generate_thumbnails", ("generate_thumbnails.py",)),
    Stage("ab_test", ("ab_test.py",)),
    Stage("add_hook_overlay", ("add_hook_overlay.py",)),
    Stage("quality_gate", ("quality_gate.py",)),
    Stage("publish_scheduler", ("publish_scheduler.py",)),
    Stage("publish_youtube", ("publish_youtube.py",)),
    # publish_tiktok stays OFF until the TikTok audit passes.
]
ANALYTICS = [
    Stage("fetch_analytics", ("fetch_analytics.py",)),
    Stage("fetch_youtube_analytics", ("fetch_youtube_analytics.py",)),
    Stage("performance_insights", ("performance_insights.py",)),
    Stage("ab_test_report", ("ab_test.py", "--report")),
    Stage("cost_tracker", ("cost_tracker.py",)),
    Stage("mutator", ("mutator.py",), requires=("performance_insights",)),
]
WEEKLY = [
    Stage("weekly_report", ("weekly_report.py",)),
    Stage("evolve_caption_examples", ("evolve_caption_examples.py",)),
]
KINDS = {"pipeline": PIPELINE, "analytics": ANALYTICS, "weekly": WEEKLY}


# ── console / env ────────────────────────────────────────────────────────

def setup_console():
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(errors="replace")
            if not s.isatty():
                s.reconfigure(encoding="utf-8")
        except Exception:
            pass


def child_env():
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONUNBUFFERED"] = "1"
    return env


def keep_awake(on):
    if sys.platform != "win32":
        return
    try:
        import ctypes
        ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED if on else ES_CONTINUOUS)
    except Exception as e:
        print(f"NOTE: couldn't set keep-awake ({e}) — run may be suspended on idle.")


# ── lock ──────────────────────────────────────────────────────────────────

def acquire_lock(kind, force):
    LOG_DIR.mkdir(exist_ok=True)
    for _ in range(3):
        try:
            fd = os.open(str(LOCK), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                age_h = (time.time() - LOCK.stat().st_mtime) / 3600
                holder = LOCK.read_text(errors="replace").strip()
            except FileNotFoundError:
                continue  # released between our attempts
            if force or age_h > STALE_HOURS:
                why = "--force" if force else f"stale ({age_h:.1f}h > {STALE_HOURS}h)"
                print(f"Removing existing lock [{why}]: {holder}")
                try:
                    LOCK.unlink()
                except FileNotFoundError:
                    pass
                force = False
                continue
            print(f"Another run looks active: {holder}\n"
                  f"(lock age {age_h:.1f}h). Wait for it, or use --force if you're "
                  f"sure nothing is running.")
            return False
        else:
            with os.fdopen(fd, "w") as f:
                f.write(f"pid={os.getpid()} kind={kind} started={dt.datetime.now().isoformat(timespec='seconds')}")
            return True
    return False


def touch_lock():
    try:
        os.utime(LOCK, None)
    except OSError:
        pass


def release_lock():
    try:
        if f"pid={os.getpid()} " in LOCK.read_text(errors="replace"):
            LOCK.unlink()
    except OSError:
        pass


# ── running things ────────────────────────────────────────────────────────

def kill_tree(proc):
    try:
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                           capture_output=True)
        else:
            proc.terminate()
    except Exception:
        pass


def stream(cmd, logf, env):
    """Run cmd, tee merged stdout/stderr to console + log. Returns exit code."""
    proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True,
                            encoding="utf-8", errors="replace", bufsize=1)
    try:
        for line in proc.stdout:
            print(line, end="", flush=True)
            logf.write(line)
            logf.flush()
        return proc.wait()
    except KeyboardInterrupt:
        kill_tree(proc)
        raise


def say(logf, msg=""):
    print(msg, flush=True)
    logf.write(msg + "\n")
    logf.flush()


def select_stages(stages, only):
    if not only:
        return list(stages)
    by_name = {s.name: i for i, s in enumerate(stages)}
    picked = set()
    for tok in [t.strip() for t in only.split(",") if t.strip()]:
        tok = tok[:-3] if tok.endswith(".py") else tok
        if tok.isdigit() and 1 <= int(tok) <= len(stages):
            picked.add(int(tok) - 1)
        elif tok in by_name:
            picked.add(by_name[tok])
        else:
            print(f"Unknown stage '{tok}'. Use --list to see names/numbers.")
            sys.exit(2)
    return [s for i, s in enumerate(stages) if i in picked]


def prune_logs(current):
    logs = sorted(LOG_DIR.glob("run_*.log"))
    for old in logs[:-KEEP_LOGS]:
        if old != current:
            try:
                old.unlink()
            except OSError:
                pass


def format_secs(s):
    m, sec = divmod(int(round(s)), 60)
    return f"{m}m{sec:02d}s" if m else f"{sec}s"


def main():
    setup_console()
    ap = argparse.ArgumentParser(description="AniFeed Studio runner")
    ap.add_argument("kind", nargs="?", default=None, choices=list(KINDS))
    ap.add_argument("--only", help="comma-separated stage names or 1-based numbers")
    ap.add_argument("--list", action="store_true", help="list stages and exit")
    ap.add_argument("--force", action="store_true", help="break an existing lock")
    args = ap.parse_args()

    if args.list:
        kinds = [args.kind] if args.kind else list(KINDS)
        for k in kinds:
            print(f"{k}:")
            for i, s in enumerate(KINDS[k], 1):
                extra = f"  (skipped if {', '.join(s.requires)} fails)" if s.requires else ""
                print(f"  {i:2d}. {s.name:26s} python {' '.join(s.args)}{extra}")
        return 0

    args.kind = args.kind or "pipeline"
    stages = select_stages(KINDS[args.kind], args.only)
    if not stages:
        print("No stages selected.")
        return 2

    os.chdir(ROOT)
    if not acquire_lock(args.kind, args.force):
        return 3

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"run_{args.kind}_{stamp}.log"
    env = child_env()
    results = []          # (name, seconds, status)
    failed = set()
    run_id = None
    exit_code = 0
    interrupted = False

    keep_awake(True)
    logf = open(log_path, "w", encoding="utf-8", errors="replace")
    try:
        say(logf, f"=== AniFeed {args.kind} run — {stamp} — Python {sys.version.split()[0]} "
                  f"on {sys.platform} — {len(stages)} stage(s) ===")
        say(logf, f"Log: {log_path}")

        if args.kind == "pipeline":
            r = subprocess.run([sys.executable, "run_summary.py", "--start"], cwd=ROOT,
                               env=env, capture_output=True, text=True,
                               encoding="utf-8", errors="replace")
            out = (r.stdout or "").strip().splitlines()
            if r.stderr.strip():
                say(logf, r.stderr.strip())
            if r.returncode == 0 and out and out[-1].strip().isdigit():
                run_id = out[-1].strip()
            else:
                say(logf, "WARNING: run_summary.py --start failed — this run won't be recorded.")

        total = len(stages)
        for n, st in enumerate(stages, 1):
            say(logf, f"\n=== {n}/{total} {st.name} ===")
            blockers = [r for r in st.requires if r in failed]
            if blockers:
                say(logf, f"  SKIPPED — depends on {', '.join(blockers)}, which failed.")
                results.append((st.name, 0.0, "skipped"))
                continue
            if not (ROOT / st.args[0]).is_file():
                say(logf, f"  FAILED — {st.args[0]} not found in {ROOT}")
                failed.add(st.name)
                results.append((st.name, 0.0, "missing"))
                continue
            t0 = time.monotonic()
            rc = stream([sys.executable, "-u", *st.args], logf, env)
            secs = time.monotonic() - t0
            if rc == 0:
                results.append((st.name, secs, "ok"))
                say(logf, f"  [{st.name}: ok in {format_secs(secs)}]")
            else:
                failed.add(st.name)
                results.append((st.name, secs, f"FAILED({rc})"))
                say(logf, f"  [{st.name}: FAILED exit {rc} after {format_secs(secs)} — "
                          f"continuing with remaining stages]")
            touch_lock()

        exit_code = 1 if failed else 0
    except KeyboardInterrupt:
        interrupted = True
        exit_code = 130
        say(logf, "\nInterrupted (Ctrl+C) — stopping.")
    finally:
        try:
            say(logf, "\n--- Stage timings ---")
            for name, secs, status in results:
                say(logf, f"  {name:26s} {format_secs(secs):>8s}  {status}")
            say(logf, f"  {'TOTAL':26s} {format_secs(sum(r[1] for r in results)):>8s}")
            new_csv = not TIMINGS_CSV.exists()
            with open(TIMINGS_CSV, "a", newline="", encoding="utf-8") as cf:
                w = csv.writer(cf)
                if new_csv:
                    w.writerow(["timestamp", "kind", "stage", "seconds", "status"])
                now = dt.datetime.now().isoformat(timespec="seconds")
                for name, secs, status in results:
                    w.writerow([now, args.kind, name, round(secs, 1), status])

            if run_id is not None:
                say(logf, "\n=== run_summary --finish ===")
                try:
                    stream([sys.executable, "run_summary.py", "--finish", "--run-id", run_id,
                            "--exit-code", str(exit_code), "--log-file", str(log_path)],
                           logf, env)
                except KeyboardInterrupt:
                    pass

            if failed:
                say(logf, f"\n=== Done, with {len(failed)} stage failure(s): "
                          f"{', '.join(sorted(failed))} ===")
            elif not interrupted:
                say(logf, "\n=== All done! ===")
        finally:
            logf.close()
            keep_awake(False)
            release_lock()
            prune_logs(log_path)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
