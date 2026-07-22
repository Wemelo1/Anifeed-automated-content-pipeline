#!/data/data/com.termux/files/usr/bin/bash
set -e
cd ~/storage/shared/tiktok

# Without this, Python buffers its output in chunks whenever stdout isn't a
# real terminal — which is exactly what happens once this script pipes
# everything through `tee` below. Every print() in every stage script would
# queue up silently instead of appearing live, making a perfectly normal
# multi-minute stage (e.g. downloading a batch of images one at a time)
# look identical to a genuinely frozen one, with no way to tell the
# difference from the screen. This forces every `python` call in this
# script to flush output immediately instead.
export PYTHONUNBUFFERED=1

# --- Wake lock ---
# FIXED (real incident — see run_summary.py --history runs #210/#211,
# both auto-closed as INTERRUPTED with "no clean exit within 3.0h...
# most likely the OS killed the process"). TERMUX_ENVIRONMENT.md has
# always documented this requirement ("Run termux-wake-lock before any
# long job... this includes the nightly assets.producer render job in
# particular") but run_all.sh itself — the actual nightly orchestrator —
# never applied it. Android's battery optimization suspends a
# backgrounded/screen-locked Termux session, and this pipeline's full
# chain (video re-encodes, Whisper calls, several LLM calls per item)
# routinely runs well past the couple-minute mark the docs already
# warned about. Without a wake lock, a run that gets this far into a
# batch doesn't just slow down — it gets killed outright, leaving
# whatever was mid-stage (confirmed: clips sitting 'thumbnailed' with no
# 'hooked_at') stranded until a later run happens to pick them back up.
#
# Guarded with `command -v`: if Termux:API isn't installed, this prints
# one clear warning and the script continues WITHOUT the lock — same
# graceful-degradation posture run_summary.py's own termux-notification
# fallback already uses, never a hard failure over a missing optional
# tool.
if command -v termux-wake-lock >/dev/null 2>&1 && termux-wake-lock; then
  WAKE_LOCK_HELD=1
else
  echo "WARNING: termux-wake-lock unavailable or failed — this run is NOT"
  echo "protected against the OS killing it mid-batch on a screen lock."
  echo "If termux-wake-lock isn't installed: pkg install termux-api, plus"
  echo "the Termux:API app from F-Droid/Play Store. If it IS installed and"
  echo "still failed, check the Termux:API app has been granted its"
  echo "permissions. Either way, the run continues unprotected rather than"
  echo "aborting over this."
  WAKE_LOCK_HELD=0
fi

# --- Run tracking / alerting ---
# `trap ... EXIT` fires on every exit path — a clean finish, OR the script
# stopping partway through. See run_summary.py's docstring for the full
# design.
LOG_FILE=$(mktemp)
exec > >(tee -a "$LOG_FILE") 2>&1

RUN_ID=$(python run_summary.py --start) || RUN_ID=""

trap '
  EXIT_CODE=$?
  if [ -n "$RUN_ID" ]; then
    python run_summary.py --finish --run-id "$RUN_ID" --exit-code "$EXIT_CODE" --log-file "$LOG_FILE"
  else
    echo "run_summary.py --start never completed — no run recorded for this failure (exit code $EXIT_CODE)"
  fi
  rm -f "$LOG_FILE"
  if [ "$WAKE_LOCK_HELD" = "1" ]; then
    termux-wake-unlock
  fi
' EXIT

# --- Stage isolation ---
# Each stage below runs through run_stage instead of being called directly.
# WHY: with a bare `set -e`, ANY stage failing — even a transient network
# blip in one unrelated stage — used to kill every stage after it,
# including fully independent ones. run_stage lets the script keep going
# through the remaining stages regardless of an earlier failure, while
# still recording that a failure happened (STAGE_FAILURES) so the run
# gets correctly reported as degraded rather than silently looking like a
# clean success — see the exit code handling at the bottom of this
# script, and run_summary.py's classify_status for how "partial" (real
# progress was still made) is distinguished from "failed" (nothing was).
STAGE_FAILURES=0
FAILED_STAGES=""

run_stage() {
  local desc="$1"
  shift
  echo ""
  echo "=== $desc ==="
  if "$@"; then
    return 0
  fi
  echo "  (this stage failed — see output above; continuing with the remaining stages)"
  STAGE_FAILURES=$((STAGE_FAILURES + 1))
  FAILED_STAGES="$FAILED_STAGES [$desc]"
  return 0
}

# --- Content sourcing ---
# RSS is primary: pulls directly from the actual news outlets (Anime News
# Network, MyAnimeList News, Otaku USA) — original reporting, no repost
# middleman, no single Telegram admin/channel to depend on. AniList
# trending is a secondary signal layered on top.
run_stage "1/14 Pulling anime news (RSS — ANN, MAL, Otaku USA)" python rss_news_pull.py
run_stage "2/14 Pulling AniList trending (secondary source)" python fetch_anilist_trending.py
run_stage "3/14 Pulling Reddit posts (r/anime — cheapest redundant source)" python reddit_pull.py
run_stage "4/14 Pulling trailers from official YouTube channels" python fetch_youtube_trailers.py
run_stage "5/14 Editing video clips" python edit_clips.py
run_stage "6/14 Burning subtitles" python burn_subtitles.py
run_stage "7/14 Identifying shows visually" python identify_show.py
run_stage "8/14 Generating clip captions" python generate_captions.py
run_stage "9/14 Building news digest (if 3+ image posts are ready)" python build_news_digest.py
run_stage "10/14 Generating thumbnails" python generate_thumbnails.py
run_stage "11/14 Applying hook overlay to opening seconds" python add_hook_overlay.py
run_stage "12/14 Quality-gating finished content" python quality_gate.py
run_stage "13/14 Computing publish schedule" python publish_scheduler.py
run_stage "14/14 Publishing to YouTube" python publish_youtube.py

# --- TikTok auto-publish is intentionally OFF ---
# Reason: this app hasn't passed TikTok's Content Posting API audit yet, so
# every post is forced private (SELF_ONLY) and account-private-only anyway —
# there's nothing to gain by auto-posting right now. Post manually instead:
# every clip/digest already has its TikTok-ready caption sitting right next
# to the video file, ready to copy-paste:
#   finished/<channel>/<post_id>/video.mp4 + caption_final.txt
#   finished/news/digest_<timestamp>/video.mp4 + caption_final.txt
#
# Once the audit passes, uncomment the line below to resume automated
# TikTok publishing:
#
# run_stage "TikTok publishing" python publish_tiktok.py

echo ""
if [ "$STAGE_FAILURES" -gt 0 ]; then
  echo "=== Done, with $STAGE_FAILURES stage failure(s):$FAILED_STAGES ==="
  echo "For full error details on this run: python run_summary.py --show --run-id <id> (see run_summary.py --history for the id)"
else
  echo "=== All done! ==="
fi
echo "Clips ready:       finished/<channel>/<post_id>/video.mp4"
echo "News digest ready: finished/news/digest_<timestamp>/video.mp4"
echo "TikTok captions:   caption_final.txt next to each video.mp4 above — post these manually for now"
echo "Pipeline status:   python db.py"
echo "Publish schedule:  python schedule_status.py"
echo "Run history:       python run_summary.py --history"

# Exit nonzero if anything failed, so run_summary.py's --finish call (fired
# by the EXIT trap above) receives an exit code that actually reflects it —
# otherwise a degraded run would be misreported as a full success.
if [ "$STAGE_FAILURES" -gt 0 ]; then
  exit 1
fi
