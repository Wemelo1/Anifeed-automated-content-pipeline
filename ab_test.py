"""
A/B Testing Framework — Roadmap Item L
------------------------------------------
mutator.py's rules already nudge learned_config params toward whatever
performance_insights.py's LATEST snapshot suggests is winning — but a
single snapshot showing "saturation=1.5 clips did better than
saturation=1.2 clips" is correlational, not causal: those clips also
differ in show, posting time, thumbnail, caption wording, etc. This
module is the causal check: for a sampled fraction of ready clips, it
renders a genuine TWO-VARIANT pair that's IDENTICAL except for ONE
learned_config param, publishes both as separate videos, and reports the
actual uplift once both have real metrics — a controlled experiment,
not an inference from noisy correlated data.

SINGLE-VARIABLE, NOT MULTI-VARIABLE: deliberately different from
diversity_guard.py's exploration slice (which skips ALL biasing at once)
— an A/B pair changes exactly one param so any measured difference in
outcome can actually be attributed to that param. Title/thumbnail
variant testing (multiple simultaneous variables) is roadmap item P, a
separate concern layered on top of this later, not something this module
tries to also cover.

WHAT GETS TESTED: currently only hook_overlay_duration (see
AB_TEST_PARAMS) — chosen because it's the cheapest param to create a
genuine second variant for. The "control" variant is the clip's EXISTING
render, continuing through the pipeline completely unchanged. The
"treatment" variant is a CLONE of that same post row (same source video,
same captions, same thumbnail — see db.create_ab_variant_post()) that
gets its own hook overlay burned in at a nudged duration
(learned_config's current value ± its own max_delta, so a test never
proposes a param value outside what mutator.py could have proposed
anyway) via add_hook_overlay.py's new hook_duration_override parameter.

WHY NOT edit_color_grade_saturation/bgm_volume TOO (yet): both of those
are baked in during edit_clips.py, several stages before this module
runs (right before add_hook_overlay.py, at the 'thumbnailed' stage) —
creating a genuine second variant for either would mean re-running
edit_clips.py + burn_subtitles.py (a full re-encode + a second Whisper
transcription call) from the ORIGINAL raw download, not a cheap clone.
raw_media_path often still points at the original file (edit_clips.py
doesn't delete it), so this is a real, buildable follow-up — just a
heavier one, deliberately out of scope for this first version so L ships
with something that actually works end to end rather than something
half-built across two param types.

PIPELINE POSITION: runs AFTER generate_thumbnails.py, BEFORE
add_hook_overlay.py (see run_all.sh) — right where a clip is a fully
finished, thumbnailed asset but hasn't had its hook burned in yet, which
is the exact fork point a hook-duration test needs.

SAMPLING: AB_TEST_RATE (default 20%) of eligible clips per run — never
all of them, since every test costs a second full hook-overlay re-encode
and a second YouTube upload/quota unit. Eligible = thumbnailed, quality-
approved (if the gate is on), video (not digest — digests have no single
hook), not already part of an A/B group.

REPORTING: report_results() (also exposed as `python ab_test.py
--report`) finds every ab_group_id where BOTH variants are published AND
measured, and prints the uplift. Deliberately does NOT auto-apply
anything back into learned_config — a single A/B pair is n=1, exactly
the kind of thin sample this project's own sample-size discipline
(CAPTION_INSIGHT_MIN_SAMPLE, THUMBNAIL_INSIGHT_MIN_SAMPLE, etc.) says not
to act on. Results accumulate here for a human to read; a future rule
that aggregates MANY completed A/B pairs before proposing a mutation is
the natural next step, not something this version does.

USAGE:
  python ab_test.py            # create new A/B pairs for this run's ready clips
  python ab_test.py --report   # print uplift for every completed pair
  Run as its own stage in run_all.sh, between generate_thumbnails.py and
  add_hook_overlay.py.

.env overrides (all optional):
  AB_TEST_ENABLED=1        # 0 disables entirely — no pairs created,
                             existing pairs still reportable
  AB_TEST_RATE=0.2         # fraction of eligible clips tested per run
"""

import argparse
import json
import os
import shutil

import db
import learned_config as lc

FINISHED_DIR = "finished"

AB_TEST_ENABLED = os.getenv("AB_TEST_ENABLED", "1") != "0"
AB_TEST_RATE = float(os.getenv("AB_TEST_RATE", "0.2"))

# Which learned_config param(s) this module can A/B test, and how far to
# nudge for the "treatment" variant — reuses each param's OWN
# max_delta_up from learned_config.PARAM_REGISTRY rather than a separate
# constant here, so a test never proposes a step mutator.py itself
# couldn't have proposed.
AB_TEST_PARAMS = ["hook_overlay_duration"]


def _eligible_clips():
    """Clips ready for hook overlay (mirrors add_hook_overlay.py's own
    find_clips_needing_hook() scan) that aren't already part of an A/B
    group. Quality-gated the same way get_publishable_posts() is —
    QUALITY_GATE_ENABLED respected via db.QUALITY_GATE_ENABLED, same
    convention every other consumer of the gate's verdict uses."""
    found = []
    if not os.path.isdir(FINISHED_DIR):
        return found
    for channel in os.listdir(FINISHED_DIR):
        if channel == "news":
            continue  # digests out of scope, same boundary add_hook_overlay.py draws
        channel_path = os.path.join(FINISHED_DIR, channel)
        if not os.path.isdir(channel_path):
            continue
        for post_id in os.listdir(channel_path):
            video_path = os.path.join(channel_path, post_id, "video.mp4")
            if not os.path.isfile(video_path):
                continue
            post = db.get_post(channel, post_id)
            if not post:
                continue
            if post.get("hooked_at"):
                continue  # already past the fork point this module needs
            if post.get("ab_group_id"):
                continue  # already part of a test (control OR treatment)
            if db.QUALITY_GATE_ENABLED and post.get("quality_status") != "approved":
                continue
            if not post.get("caption_tiktok") and not post.get("caption_hook"):
                continue  # generate_captions.py hasn't produced this yet
            found.append((channel, post_id, post))
    return found


def _nudge_value(param_name, current_value):
    """Nudges `current_value` by that param's registry max_delta_up,
    clamped to the registry's ceiling — same clamp logic
    learned_config._clamp() applies, duplicated narrowly here since this
    is a PROPOSED render value for a not-yet-existing clip, not something
    that goes through propose_mutation() itself (no clip has been
    rendered yet to attach a sample_size/reasoning to)."""
    spec = lc.PARAM_REGISTRY[param_name]
    nudged = current_value + spec["max_delta_up"]
    return min(spec["ceiling"], nudged)


def create_ab_pairs():
    if not AB_TEST_ENABLED:
        print("AB_TEST_ENABLED=0 — skipping this stage entirely (existing "
              "pairs are still reportable via --report).")
        return 0

    candidates = _eligible_clips()
    if not candidates:
        print("No clips currently eligible for A/B testing.")
        return 0

    n_to_test = max(0, round(len(candidates) * AB_TEST_RATE))
    if n_to_test == 0:
        print(f"{len(candidates)} eligible clip(s), but AB_TEST_RATE="
              f"{AB_TEST_RATE} rounds down to 0 this run.")
        return 0

    import random
    selected = random.sample(candidates, k=min(n_to_test, len(candidates)))
    print(f"Creating A/B pairs for {len(selected)} of {len(candidates)} "
          f"eligible clip(s) (AB_TEST_RATE={AB_TEST_RATE})...")

    created = 0
    for channel, post_id, post in selected:
        param_name = random.choice(AB_TEST_PARAMS)
        current_value = lc.get_param(param_name)
        treatment_value = _nudge_value(param_name, current_value)
        if treatment_value == current_value:
            print(f"  {channel}/{post_id}: SKIPPED — {param_name} is already "
                  f"at its ceiling, no room to nudge for a treatment variant")
            continue

        ab_group_id = f"ab_{channel}_{post_id}"
        variant_post_id = f"{post_id}_abB"
        source_video = os.path.join(FINISHED_DIR, channel, post_id, "video.mp4")
        variant_dir = os.path.join(FINISHED_DIR, channel, variant_post_id)
        variant_video = os.path.join(variant_dir, "video.mp4")

        try:
            os.makedirs(variant_dir, exist_ok=True)
            shutil.copy(source_video, variant_video)

            # Copy the thumbnail too — this test isolates hook_overlay_
            # duration only, so the thumbnail must be identical between
            # variants, not a second independently-picked frame (that
            # would reintroduce a second variable, defeating the point).
            source_thumb = os.path.join(FINISHED_DIR, channel, post_id, "thumbnail.jpg")
            source_thumb_ts = os.path.join(FINISHED_DIR, channel, post_id, "thumbnail_timestamp_ms.txt")
            if os.path.isfile(source_thumb):
                shutil.copy(source_thumb, os.path.join(variant_dir, "thumbnail.jpg"))
            if os.path.isfile(source_thumb_ts):
                shutil.copy(source_thumb_ts, os.path.join(variant_dir, "thumbnail_timestamp_ms.txt"))

            db.mark_ab_control(channel, post_id, ab_group_id)
            db.create_ab_variant_post(
                channel, post_id, variant_post_id, ab_group_id,
                json.dumps({param_name: treatment_value}),
            )
        except Exception as e:
            print(f"  {channel}/{post_id}: FAILED to create variant — {e}")
            if os.path.isdir(variant_dir):
                shutil.rmtree(variant_dir, ignore_errors=True)
            continue

        created += 1
        print(f"  {channel}/{post_id}: A/B pair created ({ab_group_id}) — "
              f"control={param_name}={current_value}, "
              f"treatment={param_name}={treatment_value}")

    print(f"\nDone. {created} A/B pair(s) created — both variants will flow "
          f"through add_hook_overlay.py/quality_gate.py/publish_youtube.py "
          f"normally from here (control unchanged, treatment reads its "
          f"ab_override_json override).")
    return created


def report_results():
    groups = db.ab_groups_with_both_variants()
    if not groups:
        print("No completed A/B pairs yet (need both variants published — "
              "check back once publish_youtube.py has run for both).")
        return

    views_by_key = {r["key"]: r for r in db.posts_with_metrics()}

    print(f"{len(groups)} completed A/B pair(s):\n")
    for group_id in groups:
        posts = db.posts_in_ab_group(group_id)
        control = next((p for p in posts if p["ab_variant"] == "control"), None)
        treatment = next((p for p in posts if p["ab_variant"] == "treatment"), None)
        if not control or not treatment:
            continue

        control_metrics = views_by_key.get(control["key"])
        treatment_metrics = views_by_key.get(treatment["key"])
        if not control_metrics or not treatment_metrics:
            print(f"  {group_id}: both published but not yet measured "
                  f"(run fetch_analytics.py) — skipping for now")
            continue

        try:
            override = json.loads(treatment.get("ab_override_json") or "{}")
        except (TypeError, ValueError):
            override = {}
        param_summary = ", ".join(f"{k}={v}" for k, v in override.items()) or "unknown param"

        c_views = control_metrics["views"] or 0
        t_views = treatment_metrics["views"] or 0
        uplift = ((t_views - c_views) / c_views * 100) if c_views else None

        print(f"  {group_id}  [{param_summary}]")
        print(f"      control:   {control['key']}  {c_views} views")
        print(f"      treatment: {treatment['key']}  {t_views} views"
              + (f"  ({uplift:+.0f}% vs control)" if uplift is not None else ""))


def main():
    parser = argparse.ArgumentParser(description="A/B testing framework (roadmap item L)")
    parser.add_argument("--report", action="store_true",
                         help="print uplift for every completed A/B pair instead of creating new ones")
    args = parser.parse_args()

    db.init_db()
    lc.init_schema()

    if args.report:
        report_results()
    else:
        create_ab_pairs()


if __name__ == "__main__":
    main()
