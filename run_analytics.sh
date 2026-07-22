#!/data/data/com.termux/files/usr/bin/bash
# Daily analytics job — separate cadence from run_all.sh's per-content-batch
# run, per fetch_analytics.py's own docstring ("run any time — e.g. daily,
# via cron/Termux:Boot"). fetch_youtube_analytics.py (roadmap item K) runs
# right after fetch_analytics.py — same "views/likes/comments first, then
# the heavier retention signal" ordering, both feeding the SAME metrics
# table performance_insights.py reads next. performance_insights.py runs
# after both since there's no point recomputing insights against stale
# metrics. mutator.py (the Learn stage) runs last — it reads
# performance_insights.py's just-recomputed insight, so it must never run
# ahead of it in this file.
#
# Set up with Termux:Boot / a cron-like scheduler to run once daily, e.g.:
#   0 6 * * * bash ~/storage/shared/tiktok/run_analytics.sh
set -e
cd ~/storage/shared/tiktok

echo "=== Fetching analytics ==="
python fetch_analytics.py

echo ""
echo "=== Fetching YouTube retention signal (roadmap item K) ==="
python fetch_youtube_analytics.py

echo ""
echo "=== Updating performance insights ==="
python performance_insights.py

echo ""
echo "=== A/B test results (roadmap item L) ==="
python ab_test.py --report

echo ""
echo "=== API cost report (roadmap item S) ==="
python cost_tracker.py

echo ""
echo "=== Learn stage — evaluating config mutations ==="
python mutator.py

echo ""
echo "=== Done ==="
