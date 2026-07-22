"""
Source Quality — Roadmap Item N
------------------------------------
A–J improved HOW content is made. This is the first "WHAT to make" layer:
right now every source (RSS feed, AniList, a YouTube trailer channel,
Reddit) gets pulled and rendered equally as long as it clears the
per-item filters (is_anime_content, image validation, etc). This module
adds a per-SOURCE reliability/performance score so acquire-stage scripts
can pull less from a source that's historically wasted slots, and so
build_news_digest.py can rank candidate stories by more than just
edit_planner's newsworthiness score alone.

FOUR INPUTS PER SOURCE (db.py already has every one of these — this is
a read-only aggregation layer, no new columns):
  avg_views         — mean views across that source's published,
                       measured clips (db.posts_with_metrics(), joined
                       back to sources via posts.source_id)
  removal_rate       — fraction of that source's published clips later
                       confirmed removed on YouTube (mirrors
                       db.removal_rate_by_show(), grouped by source
                       instead of by show)
  quality_fail_rate  — fraction of that source's quality-gated clips
                       that landed at 'rejected' (posts.quality_status)
  yield_rate         — fraction of that source's PULLED posts that ever
                       made it to 'published' — the "how often does this
                       source produce usable content at all" signal
                       (roadmap's "time-to-publish" framed as a rate,
                       since this pipeline doesn't currently time-stamp
                       individual stage transitions precisely enough to
                       compute a true latency — see db.py's per-stage
                       *_at timestamps, which record WHEN but the gap
                       between pull and publish already exists via
                       downloaded_at -> published_at if ever needed later)

SCORE: a single 0-100 composite, NOT a replacement for looking at the
four inputs individually (source_report() prints all four) — just what
callers that need one number (deprioritization thresholds) can act on
without re-deriving the weighting themselves.

  score = 100
        - min(40, removal_rate * 100 * 0.8)       # up to -40
        - min(30, quality_fail_rate * 100 * 0.6)  # up to -30
        - min(20, max(0, (1 - yield_rate)) * 20)  # up to -20
        + min(10, avg_views_vs_channel_avg_pct * 0.1)  # up to +10, can go negative too

SAMPLE-SIZE GATE: a source with fewer than SOURCE_QUALITY_MIN_SAMPLE
published+measured clips gets score=None (not a punitive low score) —
same "don't manufacture a signal that isn't there" principle every
other insight module in this pipeline already applies
(CAPTION_INSIGHT_MIN_SAMPLE, THUMBNAIL_INSIGHT_MIN_SAMPLE, etc). Callers
treat None as "unknown, don't deprioritize" — a brand-new source
shouldn't be punished for not having data yet.

WHAT THIS DOES NOT DO: per VIDEO_PIPELINE_ARCHITECT.md's Acquire-stage
contract ("collect+dedupe, no scoring, no ranking"), this module does
NOT decide to skip a source outright — that's roadmap item Q's job
(source_health.py, a harder "blocked" gate). This module only produces
a SCORE and a soft QUOTA MULTIPLIER that acquire scripts use to pull
fewer items from a weak source, never zero.

USAGE:
  import source_quality as sq
  scores = sq.rank_sources()              # every source with enough data, best first
  mult = sq.quota_multiplier("rss", "anime_news_network")  # 0.0-1.0, default 1.0
  story_score = sq.story_selection_score(edit_planner_score, source_platform, source_handle, canonical_show)
"""

import os
import statistics

import db

SOURCE_QUALITY_MIN_SAMPLE = int(os.getenv("SOURCE_QUALITY_MIN_SAMPLE", "5"))

# Below this composite score, acquire scripts should start pulling less
# (soft quota reduction, never zero — see module docstring).
QUOTA_REDUCTION_THRESHOLD = float(os.getenv("SOURCE_QUALITY_QUOTA_THRESHOLD", "45"))
# Floor on the quota multiplier — a source is never reduced to 0 pulls by
# this module alone (that's item Q's "blocked" tier, a harder gate this
# module deliberately doesn't implement).
QUOTA_FLOOR_MULTIPLIER = float(os.getenv("SOURCE_QUALITY_QUOTA_FLOOR", "0.4"))


def _source_published_clips():
    """Returns {(platform, handle): [post_dict, ...]} for every post that
    has ever been PULLED (regardless of outcome) — the denominator for
    yield_rate. Each post_dict carries status/quality_status/canonical_show
    so downstream aggregation doesn't need a second query per source."""
    conn = db.get_connection()
    rows = conn.execute(
        """SELECT s.platform, s.handle, p.status, p.quality_status,
                  p.canonical_show, p.key
           FROM posts p
           INNER JOIN sources s ON s.id = p.source_id"""
    ).fetchall()
    conn.close()

    by_source = {}
    for r in rows:
        by_source.setdefault((r["platform"], r["handle"]), []).append(dict(r))
    return by_source


def _views_by_key():
    """{post_key: total_views} from db.posts_with_metrics() — same
    aggregation convention every other insight module uses (sum across
    platforms' latest snapshot)."""
    views = {}
    for r in db.posts_with_metrics():
        views[r["key"]] = views.get(r["key"], 0) + (r["views"] or 0)
    return views


def _removal_map():
    """{canonical_show: removal_rate_pct} — reused rather than
    recomputed per-source; source-level removal rate is derived by
    averaging the removal rate of shows that source's clips map to,
    since content_removals is tracked per content_key/canonical_show,
    not per source directly."""
    try:
        rows = db.removal_rate_by_show("youtube")
    except Exception:
        return {}
    return {r["canonical_show"]: r["removal_rate_pct"] for r in rows
            if r.get("removal_rate_pct") is not None}


def rank_sources(min_sample=SOURCE_QUALITY_MIN_SAMPLE):
    """Returns a list of dicts, one per source with enough data, sorted
    by composite score descending:
      {platform, handle, score, avg_views, removal_rate, quality_fail_rate,
       yield_rate, sample_size}
    Sources below `min_sample` published+measured clips are excluded
    entirely (not scored as 0) — see module docstring's SAMPLE-SIZE GATE."""
    by_source = _source_published_clips()
    views = _views_by_key()
    removal_map = _removal_map()

    all_views = list(views.values())
    channel_avg_views = statistics.mean(all_views) if all_views else 0

    results = []
    for (platform, handle), posts in by_source.items():
        published = [p for p in posts if p["status"] and p.get("key") in views]
        sample_size = len(published)
        if sample_size < min_sample:
            continue

        pulled_total = len(posts)
        published_total = sum(1 for p in posts if p["status"] in ("published", "used_in_digest") or p.get("key") in views)
        yield_rate = published_total / pulled_total if pulled_total else 0.0

        quality_checked = [p for p in posts if p.get("quality_status")]
        quality_fail_rate = (
            sum(1 for p in quality_checked if p["quality_status"] == "rejected") / len(quality_checked)
            if quality_checked else 0.0
        )

        shows = {p["canonical_show"] for p in published if p.get("canonical_show")}
        show_removal_rates = [removal_map[s] for s in shows if s in removal_map]
        removal_rate = (statistics.mean(show_removal_rates) / 100) if show_removal_rates else 0.0

        source_views = [views[p["key"]] for p in published if p.get("key") in views]
        avg_views = statistics.mean(source_views) if source_views else 0.0
        vs_channel_pct = ((avg_views / channel_avg_views - 1) * 100) if channel_avg_views else 0.0

        score = 100.0
        score -= min(40.0, removal_rate * 100 * 0.8)
        score -= min(30.0, quality_fail_rate * 100 * 0.6)
        score -= min(20.0, max(0.0, (1 - yield_rate)) * 20)
        score += max(-20.0, min(10.0, vs_channel_pct * 0.1))
        score = max(0.0, min(100.0, score))

        results.append({
            "platform": platform, "handle": handle, "score": round(score, 1),
            "avg_views": round(avg_views, 1), "removal_rate": round(removal_rate, 3),
            "quality_fail_rate": round(quality_fail_rate, 3),
            "yield_rate": round(yield_rate, 3), "sample_size": sample_size,
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    return results


def get_source_score(platform, handle):
    """Returns the composite score for one source, or None if it doesn't
    have enough data yet (see SAMPLE-SIZE GATE) — callers must treat None
    as 'unknown, don't deprioritize', never as a low score."""
    for r in rank_sources():
        if r["platform"] == platform and r["handle"] == handle:
            return r["score"]
    return None


def quota_multiplier(platform, handle):
    """Returns a 0.0-1.0 multiplier acquire scripts apply to their normal
    per-run item cap (e.g. RSS_MAX_ITEMS_PER_FEED) for this specific
    source. 1.0 (no reduction) whenever the score is unknown or at/above
    QUOTA_REDUCTION_THRESHOLD. Below threshold, scales linearly down to
    QUOTA_FLOOR_MULTIPLIER at score=0 — never harder than that floor;
    outright blocking a source is item Q's job, not this module's."""
    score = get_source_score(platform, handle)
    if score is None or score >= QUOTA_REDUCTION_THRESHOLD:
        return 1.0
    frac = score / QUOTA_REDUCTION_THRESHOLD
    return round(QUOTA_FLOOR_MULTIPLIER + frac * (1.0 - QUOTA_FLOOR_MULTIPLIER), 3)


def story_selection_score(edit_planner_score, platform, handle, canonical_show=None,
                           show_insight=None):
    """Combines edit_planner's per-story newsworthiness score (0-100)
    with source quality and (if available) canonical-show performance
    history into one ranking score build_news_digest.py can sort
    candidates by, instead of edit_planner's score alone. Weighted
    60/25/15 — newsworthiness stays the dominant signal (it's the only
    one that's story-specific; source/show quality are background
    priors), source/show quality nudge it.

    `show_insight` is performance_insights.py's cached "content_strategy"
    insight (pass db.get_insight("content_strategy")) — used to look up
    whether `canonical_show` is a known top/bottom performer. Both
    `canonical_show` and `show_insight` are optional; missing either
    just drops that term (weight redistributed to newsworthiness), same
    graceful-degradation posture as every other optional-signal check in
    this pipeline."""
    source_score = get_source_score(platform, handle)
    show_pct = None
    if canonical_show and show_insight:
        for s in show_insight.get("top_shows", []):
            if s["show"] == canonical_show:
                show_pct = s["vs_overall_avg_pct"]
                break

    weight_news, weight_source, weight_show = 0.6, 0.25, 0.15
    if source_score is None:
        weight_news += weight_source
        weight_source = 0.0
    if show_pct is None:
        weight_news += weight_show
        weight_show = 0.0

    total = edit_planner_score * weight_news
    if source_score is not None:
        total += source_score * weight_source
    if show_pct is not None:
        # show_pct is a +/- percentage; map it onto a 0-100-ish scale
        # centered at 50 so it combines sensibly with the other two terms.
        total += max(0.0, min(100.0, 50 + show_pct)) * weight_show

    return round(total, 1)


def print_report():
    ranked = rank_sources()
    if not ranked:
        print(f"No source has {SOURCE_QUALITY_MIN_SAMPLE}+ published+measured "
              f"clips yet — nothing to rank.")
        return
    print(f"Source quality ranking ({len(ranked)} source(s) with "
          f"{SOURCE_QUALITY_MIN_SAMPLE}+ measured clips):\n")
    for r in ranked:
        flag = " [QUOTA REDUCED]" if r["score"] < QUOTA_REDUCTION_THRESHOLD else ""
        print(f"  {r['platform']}/{r['handle']}: score={r['score']:.0f}{flag}")
        print(f"      avg_views={r['avg_views']:.0f}  removal_rate={r['removal_rate']*100:.0f}%  "
              f"quality_fail_rate={r['quality_fail_rate']*100:.0f}%  "
              f"yield_rate={r['yield_rate']*100:.0f}%  n={r['sample_size']}")


if __name__ == "__main__":
    db.init_db()
    print_report()
