#!/data/data/com.termux/files/usr/bin/bash
# Weekly Report Job
# -------------------
# Deliberately its OWN cron entry, separate from run_analytics.sh's daily
# job. weekly_report.py diffs two adjacent 7-day windows — running that
# diff every day would show two almost-entirely-overlapping windows each
# time (today's "this week" and yesterday's "this week" share 6 of 7
# days), which is noisy rather than useful. A once-a-week cadence is what
# actually makes the week-over-week comparison mean something. See
# weekly_report.py's own module docstring for the full reasoning and the
# honest limitations of reading a single week's numbers in isolation.
#
# evolve_caption_examples.py (added — self-learning loop expansion, roadmap
# item I) runs right after, same reasoning for the weekly (not daily)
# cadence: "top-performing clips this run" doesn't meaningfully shift day
# to day at a phone-scale posting cadence, so evolving the caption-example
# bank daily would mostly just re-save the same handful of examples for no
# benefit. Both belong on the same slow, weekly cadence for the same
# underlying reason — this is one job, not two coincidentally-similar ones.
#
# Set up with a cron-like scheduler to run once a week, e.g. every Sunday
# at 8am:
#   0 8 * * 0 bash ~/storage/shared/tiktok/run_weekly_report.sh
#
# Not wrapped in a run_summary.py-style trap/run-tracking harness the way
# run_all.sh is — this is a reporting/curation script, not a multi-stage
# pipeline run, so there's no partial-completion state worth recording in
# pipeline_runs. A failure here just means "no report/no bank update this
# week," visible directly in cron's own output/log, not a silent
# content-pipeline outage.
set -e
cd ~/storage/shared/tiktok

echo "=== Weekly report ==="
python weekly_report.py

echo ""
echo "=== Evolving caption examples ==="
python evolve_caption_examples.py

echo ""
echo "=== Done ==="
