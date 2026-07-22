"""
Auto-Evolve Caption Examples (weekly entry point)
----------------------------------------------------
Thin cron entry point for caption_assets.py's evolve() — the actual
mining/merge logic (mine top-performing published captions, filter
through refusal/generic/length checks, merge into the DB-backed learned
bank) now lives in caption_assets.py, which owns the "learned caption
prompt bank" end to end (mining here, prompt-formatting for
generate_captions.py). This script's only job is to be the weekly cron
target and print a human-readable summary — see caption_assets.py's
module docstring for the full design rationale (why one owning module,
why DB-backed not a JSON file, why generate_captions.py never mines this
itself).

WHERE THIS FITS: a weekly job (see run_weekly_report.sh), not the daily
run_analytics.sh — the underlying signal (which clips are "top-performing"
this run) doesn't meaningfully shift day to day on a phone-scale posting
cadence, so evolving the bank daily would mostly just re-save the same
handful of examples over and over for no benefit.

USAGE:
  python evolve_caption_examples.py
  Recommended as its own step in run_weekly_report.sh, right after
  weekly_report.py — see that script.

.env overrides — see caption_assets.py's module-level constants
(EVOLVE_CAPTION_EXAMPLES_MIN_SAMPLE, _TOP_PERCENT, _MAX, _MIN_CHARS,
_MAX_CHARS) for the full list; kept there now, not duplicated here.
"""

import db
import caption_assets as ca


def main():
    db.init_db()
    examples, sample_size = ca.evolve()

    if examples is None:
        if sample_size < ca.EVOLVE_MIN_SAMPLE:
            print(f"Only {sample_size} measured clip(s) — need {ca.EVOLVE_MIN_SAMPLE} "
                  f"before evolving caption examples. Any previously-learned "
                  f"examples are left in place.")
        else:
            print("No usable top-performing captions found this run (all "
                  "filtered out as refusal-shaped, generic, or outside the "
                  "length bounds) — leaving previously-learned examples in place.")
        return

    print(f"Learned caption examples updated from the top {ca.EVOLVE_TOP_PERCENT:.0f}% of "
          f"{sample_size} measured clip(s) — {len(examples)} example(s) in the "
          f"rotating bank now:\n")
    for e in examples:
        print(f'  - "{e}"')
    print("\ngenerate_captions.py's caption_assets.build_example_addendum() will "
          "pick these up automatically next run, sampled alongside the static "
          "CAPTION_EXAMPLES bank.")


if __name__ == "__main__":
    main()
