"""
Performance Insights
----------------------
Closes the feedback loop the pipeline didn't have before: fetch_analytics.py
collects view/like/comment counts, but nothing was actually acting on them —
every caption/narration/thumbnail prompt was static regardless of what had
already proven to land with viewers.

This script mines db.posts_with_metrics() (published clips) and
db.digests_with_metrics() (published digests — see that function's own
docstring for why this join didn't exist until now) for EIGHT signals:

  1. Which canonical shows outperform the channel average
  2. Which TikTok caption STYLE (the four styles generate_captions.py's
     TIKTOK_PROMPT already rotates through: Question-hook, Bold-claim, POV,
     Hype/reaction) tends to outperform the others
  3. Which THUMBNAIL PATTERN (face_forward / action / scenic / text_heavy —
     see thumbnail_strategy.py) tends to outperform the others
  4. Whether POSTING TIME correlates with performance — both by day-of-week
     and by a coarse time-of-day bucket (see TIME_BUCKETS below)
  5. Which RENDER PARAM VALUE correlates with performance — CLIP-side:
     bucketed color-grade saturation, bucketed BGM volume, bucketed hook-
     overlay duration, bucketed subtitle words-per-cue, and specific BGM
     track (see RENDER_PARAM_BUCKET_SPECS / RENDER_PARAM_CATEGORICAL_KEYS).
     DIGEST-side: bucketed news-digest speed factor (see
     DIGEST_RENDER_PARAM_BUCKET_SPECS) — computed from a SEPARATE overall
     average (digest view counts and clip view counts are entirely
     different distributions; mixing them into one average would be
     misleading, not just imprecise). This is the piece that lets
     mutator.py tune HOW content is produced, not just WHEN/WHICH gets
     published/selected.
  6. Which CAPTION OPENER (the first few words of the published
     caption_tiktok text) correlates with performance — grouped by
     normalized opening phrase. See OPENER_WORD_COUNT / _extract_opener().
  7. The SAME show/style/pattern breakdowns as 1-3, but ranked by
     ENGAGEMENT SCORE instead of raw views — see ENGAGEMENT_MIN_SAMPLE /
     _engagement_score() below. A parallel "engagement" section, not a
     replacement for the views-based breakdowns.
  8. FAILURE/REMOVAL signals: the most common quality-gate rejection/hold
     flags across the pipeline's whole history, which canonical shows
     have a high YouTube removal rate, and which Acquire-stage sources
     have a high edit/download failure rate. Purely REPORTING — see
     FAILURE SIGNAL ACTION BOUNDARY below.

...and caches the result via db.save_insight("content_strategy", ...) for
generate_captions.py, thumbnail_strategy.py, and quality_gate.py to read
cheaply on every run.

FAILURE SIGNAL ACTION BOUNDARY: shows_with_high_removal_rate and
sources_with_high_failure_rate are computed here for VISIBILITY, but this
script never gates anything based on them. Per
VIDEO_PIPELINE_ARCHITECT.md's stage contract, Acquire's only job is
collect+dedupe (no scoring, no ranking) — automatically skipping a source
in fetch_youtube_trailers.py based on its historical failure rate would
violate that boundary. The show-removal-rate signal IS acted on
automatically, but that action lives in quality_gate.py (Quality stage's
actual job is validating a rendered asset before scheduling) — not here.

MINIMUM SAMPLE SIZE: refuses to produce insights below MIN_SAMPLE_SIZE
published+measured CLIPS (default 15) — this gate covers everything except
the digest-side render-param bucket, which has its own, separately-gated
sample size (digests are a much lower-volume content type than clips on a
phone-scale posting cadence, and gating digest-level insight behind the
CLIP sample size would be an unrelated coincidence, not a real dependency).
Below MIN_SAMPLE_SIZE this script leaves any previously-saved insight
untouched and just reports how many more data points are needed.

USAGE:
  python performance_insights.py   (run after fetch_analytics.py; e.g. same
                                     daily cron/Termux:Boot job, right after)
"""

import os
import re
import json
import statistics
from datetime import datetime

import db

MIN_SAMPLE_SIZE = int(os.getenv("INSIGHTS_MIN_SAMPLE", "15"))
TOP_N_SHOWS = 5
TOP_N_OPENERS = 8

MIN_BUCKET_SAMPLE = 2

_STYLE_PATTERNS = [
    ("POV",          re.compile(r"^\s*pov\s*[:\-]", re.IGNORECASE)),
    ("Question-hook", re.compile(r"^[^.!]{0,60}\?", re.IGNORECASE)),
    ("Bold-claim",    re.compile(r"^\s*(this is|the most|nobody|no one)\b", re.IGNORECASE)),
    ("Hype/reaction", re.compile(r"(💀|🔥|bro\b|ngl\b|no way\b|!{2,})", re.IGNORECASE)),
]

TIME_BUCKETS = [
    ("Late Night (12am\u20135am)", range(0, 6)),
    ("Morning (6am\u201311am)",    range(6, 12)),
    ("Afternoon (12pm\u20135pm)",  range(12, 18)),
    ("Evening (6pm\u201311pm)",    range(18, 24)),
]

_WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday",
                  "Friday", "Saturday", "Sunday"]

# ── Render-param bucketing — CLIP side ──────────────────────────────────────
# edit_clips.py, burn_subtitles.py, and add_hook_overlay.py each write/merge
# a key into posts.render_params_json at their respective stages — see
# db.py's render_params_json docstring for the full accumulation picture.
# "floor"/"ceiling"/"width" mirror the corresponding learned_config.py
# registry entry's floor/ceiling so bucket edges line up with the actual
# range the value can ever take.
RENDER_PARAM_BUCKET_SPECS = {
    "color_grade_saturation": {"floor": 1.0, "ceiling": 1.8, "width": 0.15},
    "bgm_volume": {"floor": 0.05, "ceiling": 0.35, "width": 0.05},
    "hook_overlay_duration": {"floor": 1.0, "ceiling": 4.0, "width": 0.5},
    "words_per_subtitle_cue": {"floor": 2, "ceiling": 7, "width": 1},
}
RENDER_PARAM_CATEGORICAL_KEYS = ["bgm_file"]

# ── Render-param bucketing — DIGEST side ────────────────────────────────────
# build_news_digest.py writes news_digest_speed_factor into
# digests.render_params_json once, at db.create_digest() time.
DIGEST_RENDER_PARAM_BUCKET_SPECS = {
    "news_digest_speed_factor": {"floor": 1.0, "ceiling": 1.6, "width": 0.1},
}
# Digests are lower-volume than clips at a phone-scale posting cadence
# (maybe 1-3/day vs. several clips/day) — a separate, lower sample bar
# than MIN_SAMPLE_SIZE reflects that this is a genuinely different-sized
# population, not a lowered evidence standard applied inconsistently.
DIGEST_MIN_SAMPLE_SIZE = int(os.getenv("INSIGHTS_DIGEST_MIN_SAMPLE", "8"))

OPENER_WORD_COUNT = int(os.getenv("CAPTION_OPENER_WORD_COUNT", "3"))
_LEADING_QUOTE_CHARS = "\"\u201c\u201d'"
_OPENER_PUNCT_RE = re.compile(r"[^\w\s]")

ENGAGEMENT_MIN_SAMPLE = int(os.getenv("INSIGHTS_ENGAGEMENT_MIN_SAMPLE", "25"))

# Roadmap item K — retention signal. Same sample-gating principle as
# ENGAGEMENT_MIN_SAMPLE, kept separate (not just reusing that constant)
# since retention data can lag behind engagement data — fetch_youtube_
# analytics.py only started populating metrics.retention_pct once K
# shipped, so a channel with plenty of engagement history can still have
# very little retention history for a while. Default set slightly lower
# than engagement's 25 since retention is a narrower, single-platform
# (YouTube-only) signal that will naturally accumulate slower.
RETENTION_MIN_SAMPLE = int(os.getenv("INSIGHTS_RETENTION_MIN_SAMPLE", "20"))

FAILURE_SIGNAL_TOP_N = int(os.getenv("INSIGHTS_FAILURE_SIGNAL_TOP_N", "5"))
SHOW_REMOVAL_RATE_THRESHOLD_PCT = float(os.getenv("INSIGHTS_SHOW_REMOVAL_RATE_THRESHOLD_PCT", "40"))
SHOW_REMOVAL_RATE_MIN_PUBLISHED = int(os.getenv("INSIGHTS_SHOW_REMOVAL_RATE_MIN_PUBLISHED", "5"))
SOURCE_FAILURE_RATE_THRESHOLD_PCT = float(os.getenv("INSIGHTS_SOURCE_FAILURE_RATE_THRESHOLD_PCT", "30"))
SOURCE_FAILURE_RATE_MIN_ATTEMPTS = int(os.getenv("INSIGHTS_SOURCE_FAILURE_RATE_MIN_ATTEMPTS", "10"))


def classify_caption_style(caption_text):
    if not caption_text:
        return "Other"
    for style, pattern in _STYLE_PATTERNS:
        if pattern.search(caption_text):
            return style
    return "Other"


def classify_time_bucket(hour):
    for label, hours in TIME_BUCKETS:
        if hour in hours:
            return label
    return "Unknown"


def _bucket_label(value, floor, ceiling, width):
    """Buckets a continuous value into a "lo-hi" string label, clamping
    out-of-range values into the nearest edge bucket rather than dropping
    them."""
    if value is None:
        return None
    if width <= 0:
        return None
    clamped = max(floor, min(ceiling, value))
    idx = int((clamped - floor) / width)
    lo = round(floor + idx * width, 3)
    hi = round(min(lo + width, ceiling), 3)
    if hi <= lo:
        hi = round(lo + width, 3)
    return f"{lo:g}-{hi:g}"


def _extract_opener(caption_text, n_words=OPENER_WORD_COUNT):
    """Returns a normalized (lowercased, punctuation-stripped) string of
    the first `n_words` words of `caption_text`, or None."""
    if not caption_text:
        return None
    text = caption_text.strip().strip(_LEADING_QUOTE_CHARS).strip()
    if not text:
        return None

    words = []
    for w in text.split():
        if w.startswith("#"):
            break
        words.append(w)
        if len(words) >= n_words:
            break

    if not words:
        return None

    opener = " ".join(words).lower()
    opener = _OPENER_PUNCT_RE.sub("", opener).strip()
    return opener or None


def _combine_views(rows_for_target):
    return sum(r["views"] or 0 for r in rows_for_target)


def _retention_score(platform_rows):
    """Roadmap item K. YouTube-only (metrics.retention_pct is only ever
    populated by fetch_youtube_analytics.py) — returns None if none of
    this clip's platform snapshots have a retention_pct value, same
    'missing data, not zero' convention _engagement_score() uses for
    views=0. A clip published to both platforms uses whichever row(s)
    actually have retention data; TikTok rows simply don't contribute
    since they never carry retention_pct."""
    values = [r["retention_pct"] for r in platform_rows if r.get("retention_pct") is not None]
    if not values:
        return None
    return statistics.mean(values)


def _engagement_score(platform_rows):
    """Sums likes/comments/shares/views across every platform snapshot for
    one clip and returns the weighted engagement ratio, or None if the
    clip has zero combined views."""
    views = sum(r["views"] or 0 for r in platform_rows)
    if views <= 0:
        return None
    likes = sum(r["likes"] or 0 for r in platform_rows)
    comments = sum(r["comments"] or 0 for r in platform_rows)
    shares = sum(r["shares"] or 0 for r in platform_rows)
    return (likes + comments * 2 + shares * 3) / views


def _ranked_stats(grouped_views, overall_avg, label_key, min_sample=MIN_BUCKET_SAMPLE):
    """Shared aggregation used by every VIEWS-based breakdown: takes
    {label: [view counts]}, returns a list of dicts sorted by avg_views
    descending."""
    stats_list = []
    for label, views_list in grouped_views.items():
        if len(views_list) < min_sample:
            continue
        avg = statistics.mean(views_list)
        stats_list.append({
            label_key: label,
            "avg_views": round(avg, 1),
            "sample_size": len(views_list),
            "vs_overall_avg_pct": round((avg / overall_avg - 1) * 100, 1) if overall_avg else 0.0,
        })
    stats_list.sort(key=lambda s: s["avg_views"], reverse=True)
    return stats_list


def _top_rejected_flags():
    conn = db.get_connection()
    counter = {}
    for table in ("posts", "digests"):
        rows = conn.execute(
            f"""SELECT quality_flags FROM {table}
                WHERE quality_status IN ('rejected', 'held') AND quality_flags IS NOT NULL"""
        ).fetchall()
        for r in rows:
            try:
                flags = json.loads(r["quality_flags"])
            except (TypeError, ValueError):
                continue
            for f in flags:
                counter[f] = counter.get(f, 0) + 1
    conn.close()
    ranked = sorted(counter.items(), key=lambda kv: kv[1], reverse=True)
    return [{"flag": f, "count": c} for f, c in ranked[:FAILURE_SIGNAL_TOP_N]]


def _shows_with_high_removal_rate():
    try:
        rows = db.removal_rate_by_show("youtube")
    except Exception:
        return []
    result = [
        r for r in rows
        if r.get("removal_rate_pct") is not None
        and r["removal_rate_pct"] >= SHOW_REMOVAL_RATE_THRESHOLD_PCT
        and r.get("published_count", 0) >= SHOW_REMOVAL_RATE_MIN_PUBLISHED
    ]
    result.sort(key=lambda r: r["removal_rate_pct"], reverse=True)
    return result[:FAILURE_SIGNAL_TOP_N]


def _sources_with_high_failure_rate():
    conn = db.get_connection()
    rows = conn.execute(
        """SELECT s.platform, s.handle,
                  COUNT(*) AS total,
                  SUM(CASE WHEN p.status = 'failed' THEN 1 ELSE 0 END) AS failed
           FROM posts p
           INNER JOIN sources s ON s.id = p.source_id
           GROUP BY s.platform, s.handle
           HAVING COUNT(*) >= ?""",
        (SOURCE_FAILURE_RATE_MIN_ATTEMPTS,),
    ).fetchall()
    conn.close()

    result = []
    for r in rows:
        total = r["total"] or 0
        failed = r["failed"] or 0
        rate = round(failed / total * 100, 1) if total else 0.0
        if rate >= SOURCE_FAILURE_RATE_THRESHOLD_PCT:
            result.append({
                "platform": r["platform"], "handle": r["handle"],
                "failure_rate_pct": rate, "total_posts": total, "failed_posts": failed,
            })
    result.sort(key=lambda s: s["failure_rate_pct"], reverse=True)
    return result[:FAILURE_SIGNAL_TOP_N]


def _digest_render_param_stats():
    """Builds the digest-side render_params breakdown from
    db.digests_with_metrics() — a SEPARATE data pool and SEPARATE overall
    average from the clip-side breakdown, since digest view counts and
    clip view counts are different distributions (a digest is a multi-
    story roundup, not a single clip) and averaging them together would
    misrepresent both. Gated at DIGEST_MIN_SAMPLE_SIZE, not
    MIN_SAMPLE_SIZE — see that constant's own comment. Returns {} (not
    None) when there isn't enough digest data yet, so the caller can
    merge it into the clip-side render_param_stats dict unconditionally
    without a None-check."""
    rows = db.digests_with_metrics()
    by_key = {}
    for r in rows:
        by_key.setdefault(r["key"], []).append(r)

    if len(by_key) < DIGEST_MIN_SAMPLE_SIZE:
        return {}

    digest_views = {}
    digest_params = {}
    for key, platform_rows in by_key.items():
        digest_views[key] = _combine_views(platform_rows)
        raw_params = platform_rows[0].get("render_params_json")
        try:
            digest_params[key] = json.loads(raw_params) if raw_params else {}
        except (TypeError, ValueError):
            digest_params[key] = {}

    overall_avg = statistics.mean(digest_views.values())

    stats = {}
    for param_key, spec in DIGEST_RENDER_PARAM_BUCKET_SPECS.items():
        bucketed = {}
        for key, params in digest_params.items():
            raw_value = params.get(param_key)
            if raw_value is None:
                continue
            label = _bucket_label(raw_value, spec["floor"], spec["ceiling"], spec["width"])
            if label is None:
                continue
            bucketed.setdefault(label, []).append(digest_views[key])
        stats_list = _ranked_stats(bucketed, overall_avg, "value")
        if stats_list:
            stats[param_key] = stats_list
    return stats


def build_insights():
    rows = db.posts_with_metrics()
    if not rows:
        return None, 0

    by_key = {}
    for r in rows:
        by_key.setdefault(r["key"], []).append(r)

    sample_size = len(by_key)
    if sample_size < MIN_SAMPLE_SIZE:
        return None, sample_size

    clip_views = {}
    clip_engagement = {}
    clip_retention = {}
    clip_show = {}
    clip_style = {}
    clip_opener = {}
    clip_pattern = {}
    clip_weekday = {}
    clip_time_bucket = {}
    clip_render_params = {}

    for key, platform_rows in by_key.items():
        first = platform_rows[0]
        clip_views[key] = _combine_views(platform_rows)
        clip_engagement[key] = _engagement_score(platform_rows)
        clip_retention[key] = _retention_score(platform_rows)
        clip_show[key] = first["canonical_show"]
        clip_style[key] = classify_caption_style(first["caption_tiktok"])
        clip_opener[key] = _extract_opener(first["caption_tiktok"])
        clip_pattern[key] = first.get("thumbnail_pattern")

        raw_params = first.get("render_params_json")
        if raw_params:
            try:
                clip_render_params[key] = json.loads(raw_params)
            except (TypeError, ValueError):
                clip_render_params[key] = {}
        else:
            clip_render_params[key] = {}

        published_at = first.get("published_at")
        if published_at:
            try:
                dt = datetime.fromisoformat(published_at)
                clip_weekday[key] = _WEEKDAY_NAMES[dt.weekday()]
                clip_time_bucket[key] = classify_time_bucket(dt.hour)
            except ValueError:
                pass

    overall_avg = statistics.mean(clip_views.values())

    show_views = {}
    for key, show in clip_show.items():
        if show:
            show_views.setdefault(show, []).append(clip_views[key])
    show_stats = _ranked_stats(show_views, overall_avg, "show")[:TOP_N_SHOWS]

    style_views = {}
    for key, style in clip_style.items():
        style_views.setdefault(style, []).append(clip_views[key])
    style_stats = _ranked_stats(style_views, overall_avg, "style", min_sample=1)

    opener_views = {}
    for key, opener in clip_opener.items():
        if opener:
            opener_views.setdefault(opener, []).append(clip_views[key])
    opener_stats = _ranked_stats(opener_views, overall_avg, "opener")[:TOP_N_OPENERS]

    pattern_views = {}
    for key, pattern in clip_pattern.items():
        if pattern:
            pattern_views.setdefault(pattern, []).append(clip_views[key])
    pattern_stats = _ranked_stats(pattern_views, overall_avg, "pattern", min_sample=1)

    weekday_views = {}
    for key, weekday in clip_weekday.items():
        if weekday:
            weekday_views.setdefault(weekday, []).append(clip_views[key])
    weekday_stats = _ranked_stats(weekday_views, overall_avg, "weekday", min_sample=1)

    bucket_views = {}
    for key, bucket in clip_time_bucket.items():
        if bucket:
            bucket_views.setdefault(bucket, []).append(clip_views[key])
    time_bucket_stats = _ranked_stats(bucket_views, overall_avg, "time_bucket", min_sample=1)

    # ── clip-side render-param averages ──
    render_param_stats = {}

    for param_key, spec in RENDER_PARAM_BUCKET_SPECS.items():
        bucketed_views = {}
        for key, params in clip_render_params.items():
            raw_value = params.get(param_key)
            if raw_value is None:
                continue
            label = _bucket_label(raw_value, spec["floor"], spec["ceiling"], spec["width"])
            if label is None:
                continue
            bucketed_views.setdefault(label, []).append(clip_views[key])
        stats_list = _ranked_stats(bucketed_views, overall_avg, "value")
        if stats_list:
            render_param_stats[param_key] = stats_list

    for param_key in RENDER_PARAM_CATEGORICAL_KEYS:
        cat_views = {}
        for key, params in clip_render_params.items():
            raw_value = params.get(param_key)
            if not raw_value:
                continue
            cat_views.setdefault(raw_value, []).append(clip_views[key])
        stats_list = _ranked_stats(cat_views, overall_avg, "value")
        if stats_list:
            render_param_stats[param_key] = stats_list

    # ── digest-side render-param averages ──
    # Merged into the SAME render_params dict as the clip-side entries —
    # no key collision is possible since news_digest_speed_factor only
    # ever appears on digest rows and every clip-side key only ever
    # appears on post rows. mutator.py's rules read insight["render_params"]
    # by key name without needing to know which "side" a key came from.
    render_param_stats.update(_digest_render_param_stats())

    # ── engagement-based breakdowns ──
    engagement_section = None
    engagement_values = [v for v in clip_engagement.values() if v is not None]
    if len(engagement_values) >= ENGAGEMENT_MIN_SAMPLE:
        overall_avg_engagement = statistics.mean(engagement_values)

        def _ranked_by_engagement(label_map, label_key, min_sample=MIN_BUCKET_SAMPLE):
            grouped = {}
            for key, label in label_map.items():
                if not label:
                    continue
                score = clip_engagement.get(key)
                if score is None:
                    continue
                grouped.setdefault(label, []).append(score)
            stats_list = []
            for label, scores in grouped.items():
                if len(scores) < min_sample:
                    continue
                avg = statistics.mean(scores)
                stats_list.append({
                    label_key: label,
                    "avg_engagement_score": round(avg, 4),
                    "sample_size": len(scores),
                    "vs_overall_avg_pct": round((avg / overall_avg_engagement - 1) * 100, 1)
                                          if overall_avg_engagement else 0.0,
                })
            stats_list.sort(key=lambda s: s["avg_engagement_score"], reverse=True)
            return stats_list

        engagement_section = {
            "sample_size": len(engagement_values),
            "overall_avg_engagement_score": round(overall_avg_engagement, 4),
            "top_shows": _ranked_by_engagement(clip_show, "show")[:TOP_N_SHOWS],
            "caption_styles": _ranked_by_engagement(clip_style, "style", min_sample=1),
            "thumbnail_patterns": _ranked_by_engagement(clip_pattern, "pattern", min_sample=1),
        }

    # ── retention-based breakdowns (roadmap item K) ──
    # Same shape/pattern as the engagement section above, so
    # generate_captions.py/thumbnail_strategy.py/mutator.py can eventually
    # extend their existing "check retention -> fall back to engagement ->
    # fall back to views" cascade the same way engagement was added on top
    # of views without disturbing the views-based path. YouTube-only (see
    # _retention_score()'s docstring) — a channel that's TikTok-heavy will
    # simply have fewer retention-eligible clips, gated the same as any
    # other thin-sample case.
    retention_section = None
    retention_values = [v for v in clip_retention.values() if v is not None]
    if len(retention_values) >= RETENTION_MIN_SAMPLE:
        overall_avg_retention = statistics.mean(retention_values)

        def _ranked_by_retention(label_map, label_key, min_sample=MIN_BUCKET_SAMPLE):
            grouped = {}
            for key, label in label_map.items():
                if not label:
                    continue
                score = clip_retention.get(key)
                if score is None:
                    continue
                grouped.setdefault(label, []).append(score)
            stats_list = []
            for label, scores in grouped.items():
                if len(scores) < min_sample:
                    continue
                avg = statistics.mean(scores)
                stats_list.append({
                    label_key: label,
                    "avg_retention_pct": round(avg, 2),
                    "sample_size": len(scores),
                    "vs_overall_avg_pct": round((avg / overall_avg_retention - 1) * 100, 1)
                                          if overall_avg_retention else 0.0,
                })
            stats_list.sort(key=lambda s: s["avg_retention_pct"], reverse=True)
            return stats_list

        retention_section = {
            "sample_size": len(retention_values),
            "overall_avg_retention_pct": round(overall_avg_retention, 2),
            "top_shows": _ranked_by_retention(clip_show, "show")[:TOP_N_SHOWS],
            "caption_styles": _ranked_by_retention(clip_style, "style", min_sample=1),
            "thumbnail_patterns": _ranked_by_retention(clip_pattern, "pattern", min_sample=1),
        }

    failure_signals = {
        "top_rejected_flags": _top_rejected_flags(),
        "shows_with_high_removal_rate": _shows_with_high_removal_rate(),
        "sources_with_high_failure_rate": _sources_with_high_failure_rate(),
    }

    insight = {
        "sample_size": sample_size,
        "overall_avg_views": round(overall_avg, 1),
        "top_shows": show_stats,
        "caption_styles": style_stats,
        "caption_openers": opener_stats,
        "thumbnail_patterns": pattern_stats,
        "posting_weekday": weekday_stats,
        "posting_time_bucket": time_bucket_stats,
        "render_params": render_param_stats,
        "engagement": engagement_section,
        "retention": retention_section,
        "failure_signals": failure_signals,
    }
    return insight, sample_size


def main():
    db.init_db()
    insight, sample_size = build_insights()

    if insight is None:
        if sample_size == 0:
            print("No published clips with metrics yet — run fetch_analytics.py "
                  "after some content has been live for a while.")
        else:
            print(f"Only {sample_size} measured clip(s) — need {MIN_SAMPLE_SIZE} "
                  f"before generating insights. Any previously-saved insight is "
                  f"left in place and still in use by generate_captions.py / "
                  f"thumbnail_strategy.py.")
        return

    db.save_insight("content_strategy", insight)

    print(f"Insights updated from {insight['sample_size']} measured clip(s) "
          f"(overall avg {insight['overall_avg_views']:.0f} views/clip):\n")

    if insight["top_shows"]:
        print("Top-performing shows (by views):")
        for s in insight["top_shows"]:
            sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
            print(f"  {s['show']}: {s['avg_views']:.0f} avg views "
                  f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. channel avg, "
                  f"n={s['sample_size']})")
    else:
        print("Not enough shows with 2+ measured clips yet to rank.")

    print("\nCaption style performance (by views):")
    for s in insight["caption_styles"]:
        sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
        print(f"  {s['style']}: {s['avg_views']:.0f} avg views "
              f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. channel avg, "
              f"n={s['sample_size']})")

    if insight["caption_openers"]:
        print(f"\nTop caption openers (first {OPENER_WORD_COUNT} words, "
              f"2+ measured clips using it):")
        for s in insight["caption_openers"]:
            sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
            print(f"  \"{s['opener']}...\": {s['avg_views']:.0f} avg views "
                  f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. channel avg, "
                  f"n={s['sample_size']})")
    else:
        print("\nNo repeated caption openers yet.")

    if insight["thumbnail_patterns"]:
        print("\nThumbnail pattern performance (by views):")
        for s in insight["thumbnail_patterns"]:
            sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
            print(f"  {s['pattern']}: {s['avg_views']:.0f} avg views "
                  f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. channel avg, "
                  f"n={s['sample_size']})")
    else:
        print("\nNo thumbnail pattern data yet.")

    if insight["posting_weekday"]:
        print("\nPerformance by day of week posted (UTC):")
        for s in insight["posting_weekday"]:
            sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
            print(f"  {s['weekday']}: {s['avg_views']:.0f} avg views "
                  f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. channel avg, "
                  f"n={s['sample_size']})")

    if insight["posting_time_bucket"]:
        print("\nPerformance by time of day posted (UTC):")
        for s in insight["posting_time_bucket"]:
            sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
            print(f"  {s['time_bucket']}: {s['avg_views']:.0f} avg views "
                  f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. channel avg, "
                  f"n={s['sample_size']})")

    if insight["render_params"]:
        print("\nRender-param performance (by views — clip- and digest-side "
              "params share this section; digest-side uses its own separate "
              "average, see module docstring):")
        for param_key, stats_list in insight["render_params"].items():
            print(f"  {param_key}:")
            for s in stats_list:
                sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
                print(f"    {s['value']}: {s['avg_views']:.0f} avg views "
                      f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. channel avg, "
                      f"n={s['sample_size']})")
    else:
        print("\nNo render-param data yet.")

    if insight["engagement"]:
        eng = insight["engagement"]
        print(f"\nEngagement-based ranking ACTIVE (n={eng['sample_size']} clips "
              f"with measurable engagement, overall avg score "
              f"{eng['overall_avg_engagement_score']:.4f}):")
        if eng["top_shows"]:
            print("  Top shows by engagement:")
            for s in eng["top_shows"]:
                sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
                print(f"    {s['show']}: {s['avg_engagement_score']:.4f} "
                      f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. avg, n={s['sample_size']})")
        if eng["caption_styles"]:
            print("  Caption style by engagement:")
            for s in eng["caption_styles"]:
                sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
                print(f"    {s['style']}: {s['avg_engagement_score']:.4f} "
                      f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. avg, n={s['sample_size']})")
        if eng["thumbnail_patterns"]:
            print("  Thumbnail pattern by engagement:")
            for s in eng["thumbnail_patterns"]:
                sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
                print(f"    {s['pattern']}: {s['avg_engagement_score']:.4f} "
                      f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. avg, n={s['sample_size']})")
    else:
        print(f"\nNot enough clips with measurable engagement data yet "
              f"(need {ENGAGEMENT_MIN_SAMPLE}).")

    if insight["retention"]:
        ret = insight["retention"]
        print(f"\nRetention-based ranking ACTIVE (roadmap item K, YouTube-only, "
              f"n={ret['sample_size']} clips, overall avg retention "
              f"{ret['overall_avg_retention_pct']:.1f}%):")
        if ret["top_shows"]:
            print("  Top shows by retention:")
            for s in ret["top_shows"]:
                sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
                print(f"    {s['show']}: {s['avg_retention_pct']:.1f}% "
                      f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. avg, n={s['sample_size']})")
        if ret["caption_styles"]:
            print("  Caption style by retention:")
            for s in ret["caption_styles"]:
                sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
                print(f"    {s['style']}: {s['avg_retention_pct']:.1f}% "
                      f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. avg, n={s['sample_size']})")
        if ret["thumbnail_patterns"]:
            print("  Thumbnail pattern by retention:")
            for s in ret["thumbnail_patterns"]:
                sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
                print(f"    {s['pattern']}: {s['avg_retention_pct']:.1f}% "
                      f"({sign}{s['vs_overall_avg_pct']:.0f}% vs. avg, n={s['sample_size']})")
    else:
        print(f"\nNot enough clips with retention data yet (need "
              f"{RETENTION_MIN_SAMPLE} — run fetch_youtube_analytics.py "
              f"after clips have been live a while).")

    fs = insight["failure_signals"]
    if fs["top_rejected_flags"]:
        print("\nMost common quality-gate rejection/hold flags (all-time):")
        for f in fs["top_rejected_flags"]:
            print(f"  ({f['count']}x) {f['flag']}")
    if fs["shows_with_high_removal_rate"]:
        print(f"\nShows with a high YouTube removal rate "
              f"(>={SHOW_REMOVAL_RATE_THRESHOLD_PCT:.0f}%, "
              f"{SHOW_REMOVAL_RATE_MIN_PUBLISHED}+ published clips):")
        for r in fs["shows_with_high_removal_rate"]:
            print(f"  {r['canonical_show']}: {r['removal_rate_pct']:.0f}% "
                  f"({r['removed_count']}/{r['published_count']})")
    if fs["sources_with_high_failure_rate"]:
        print(f"\nSources with a high acquire/edit failure rate "
              f"(>={SOURCE_FAILURE_RATE_THRESHOLD_PCT:.0f}%, "
              f"{SOURCE_FAILURE_RATE_MIN_ATTEMPTS}+ attempts):")
        for s in fs["sources_with_high_failure_rate"]:
            print(f"  {s['platform']}/{s['handle']}: {s['failure_rate_pct']:.0f}% "
                  f"({s['failed_posts']}/{s['total_posts']})")

    print("\nSaved — generate_captions.py, thumbnail_strategy.py, and "
          "quality_gate.py will pick this up automatically next run.")


if __name__ == "__main__":
    main()
