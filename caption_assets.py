"""
Caption Assets — Learned Prompt Bank
--------------------------------------
Owns everything about the "style reference" material generate_captions.py
injects into its TikTok prompt: the rotating example bank and the
opener-performance addendum. Previously this logic was split across two
files with no single owner — evolve_caption_examples.py did the mining/
merge, generate_captions.py did the sampling/prompt-building, and
evolve_caption_examples.py had to `from generate_captions import
is_generic`, a backwards import (the weekly curation job depending on the
per-clip generation script). This module is the fix: one file owns the
bank, both callers import FROM here, never from each other.

STORAGE: reuses db.py's generic insights key/value cache
(db.save_insight/db.get_insight) under key "learned_caption_examples" —
the exact same key the old evolve_caption_examples.py already wrote to,
so this is a drop-in replacement with no migration and no data loss.
Deliberately NOT a separate caption_assets.json file — this project's
single source of truth is pipeline.db (see db.py's own module
docstring); a second, unsynced storage location for the same kind of
data is exactly the class of bug PROJECT_RULES.md Rule 8 (No Global
State) exists to prevent.

TWO THINGS THIS BANK PROVIDES TO THE PROMPT:
  1. EXAMPLES — style-reference lines (tone/energy only, never verbatim
     copy) pooled from a small static fallback list + a DB-backed
     learned list that evolve() mines from top-performing published
     captions once a week.
  2. OPENERS — the opener-performance addendum. This is NOT a separate
     persisted bank; it's read live from performance_insights.py's
     cached "content_strategy" insight (insight["caption_openers"]) every
     call, gated at CAPTION_INSIGHT_MIN_SAMPLE, same as it always was in
     generate_captions.py — just relocated here so both prompt-injection
     concerns live in one file. performance_insights.py itself remains
     the actual source of truth for opener performance data; this module
     only formats it as a prompt addendum.

USAGE:
  import caption_assets as ca
  addendum = ca.build_example_addendum()
  opener_addendum = ca.build_opener_addendum(insight)   # insight from db.get_insight("content_strategy")
  merged, sample_size = ca.evolve()    # called weekly by evolve_caption_examples.py
"""

import os
import re

import db

# ── Static fallback bank ─────────────────────────────────────────────────
# A small, hand-picked STATIC/FALLBACK set of GOOD real captions — what
# the pool looks like before any learned examples exist via evolve(), and
# stays in the pool afterward for variety even once learned examples
# dominate. A single example gets copied near-verbatim far too often, so
# build_example_addendum() always samples several rather than handing the
# model one literal string.
CAPTION_EXAMPLES = [
    "wait the way this arc just casually confirmed the theory??",
    "the animation budget they saved for THIS frame alone 😭",
    "POV: you forgot Season 2 hadn't dropped this scene yet",
    "not the studio actually letting the villain win this cleanly",
    "this fight choreography is doing things Shonen usually doesn't bother with",
    "bro really said 'one more episode' and then THIS happened",
]

# Known dead/generic phrases — last-resort filter shared by both the
# per-clip generation path (generate_captions.py rejects and regenerates
# once) and the weekly mining path (evolve() refuses to promote a
# generic-sounding caption into the learned bank even if it got views).
GENERIC_PHRASES = [
    "this is the most", "this is the hottest", "you won't believe",
    "this changes everything", "the most underrated scene",
]

EXAMPLES_INSIGHT_KEY = "learned_caption_examples"

# Same bar for both consumers of this module: "reliable enough to
# actually steer generation," a materially higher bar than
# performance_insights.py's own min_sample=1 used for a human-readable
# report. See generate_captions.py's build_style_addendum/
# show_performance_note for the same principle applied to style/show data.
CAPTION_INSIGHT_MIN_SAMPLE = int(os.getenv("CAPTION_INSIGHT_MIN_SAMPLE", "8"))

# How many top-performing openers to surface in the addendum —
# performance_insights.py's TOP_N_OPENERS already caps the underlying
# list at 8; this is a further trim for prompt brevity, not a data limit.
OPENER_ADDENDUM_TOP_N = int(os.getenv("CAPTION_OPENER_ADDENDUM_TOP_N", "5"))

# ── Weekly mining (evolve()) config — same defaults evolve_caption_examples.py
# always used, kept here since this module now owns the mining logic.
EVOLVE_MIN_SAMPLE = int(os.getenv("EVOLVE_CAPTION_EXAMPLES_MIN_SAMPLE", "15"))
EVOLVE_TOP_PERCENT = float(os.getenv("EVOLVE_CAPTION_EXAMPLES_TOP_PERCENT", "10"))
MAX_LEARNED_EXAMPLES = int(os.getenv("EVOLVE_CAPTION_EXAMPLES_MAX", "10"))
MIN_EXAMPLE_CHARS = int(os.getenv("EVOLVE_CAPTION_EXAMPLES_MIN_CHARS", "20"))
MAX_EXAMPLE_CHARS = int(os.getenv("EVOLVE_CAPTION_EXAMPLES_MAX_CHARS", "100"))

_HASHTAG_RE = re.compile(r"#\S+")
_LEADING_QUOTE_CHARS = "\"\u201c\u201d'"


def is_generic(text):
    if not text:
        return False
    low = text.lower()
    return any(p in low for p in GENERIC_PHRASES)


# ── Example bank: read + prompt-format ──────────────────────────────────

def _learned_examples():
    learned = db.get_insight(EXAMPLES_INSIGHT_KEY)
    return learned.get("examples", []) if learned else []


def get_examples():
    """Pooled bank: DB-backed learned examples first (dominate once
    enough exist), static CAPTION_EXAMPLES fallback after, deduped
    case-insensitively. With zero learned examples yet (fresh pipeline),
    this is identical to the old static-only behavior."""
    pool, seen = [], set()
    for line in _learned_examples() + CAPTION_EXAMPLES:
        low = line.lower()
        if low in seen:
            continue
        pool.append(line)
        seen.add(low)
    return pool


def build_example_addendum(sample_k=3):
    """Randomly samples `sample_k` lines from the pooled bank and formats
    them as a prompt addendum. Import random here (not module-level) so
    every call re-samples fresh rather than caching a fixed set for the
    life of the process."""
    import random
    pool = get_examples()
    picks = random.sample(pool, k=min(sample_k, len(pool)))
    lines = "\n".join(f'- "{p}"' for p in picks)
    return (
        f"\n\nSTYLE REFERENCE (tone/energy only — do NOT reuse these words "
        f"or their sentence structure, write something new for THIS clip):\n{lines}"
    )


# ── Opener-performance addendum (live-read from performance_insights.py) ──

def build_opener_addendum(insight):
    """Turns performance_insights.py's cached caption_openers ranking into
    a short system-prompt addendum naming which OPENING MOVES have
    actually driven views. Returns "" if there's no insight yet, no
    opener data yet, if nothing clears CAPTION_INSIGHT_MIN_SAMPLE, or if
    the best reliable opener isn't actually beating the average — same
    "don't manufacture a signal that isn't there" gate build_example_addendum's
    sibling (build_style_addendum, in generate_captions.py) applies to
    style data. Opener ranking is always views-based (performance_insights.py
    doesn't currently compute an engagement-ranked opener breakdown)."""
    if not insight or not insight.get("caption_openers"):
        return ""

    reliable = [o for o in insight["caption_openers"]
                if o["sample_size"] >= CAPTION_INSIGHT_MIN_SAMPLE
                and o["vs_overall_avg_pct"] > 0]
    if not reliable:
        return ""

    top = reliable[:OPENER_ADDENDUM_TOP_N]
    lines = "\n".join(
        f'- "{o["opener"]}..." (+{o["vs_overall_avg_pct"]:.0f}% vs. avg, n={o["sample_size"]})'
        for o in top
    )
    return (
        f"\n\nOPENING-MOVE PERFORMANCE DATA (real published captions — these "
        f"exact opening phrasings have measurably outperformed, but do NOT "
        f"copy them verbatim; the RECENT CAPTIONS rule still applies. Treat "
        f"this as a signal for which OPENING STYLE tends to land, same as "
        f"the style ranking above — write a NEW line in that spirit for "
        f"THIS clip, not a reworded copy of one of these):\n{lines}"
    )


# ── Weekly mining/evolution ──────────────────────────────────────────────

def _strip_hashtags(caption_text):
    text = _HASHTAG_RE.sub("", caption_text)
    return re.sub(r"\s+", " ", text).strip()


def _candidate_line(caption_text):
    """Returns a cleaned, usable example line from a raw caption_tiktok
    value, or None if it fails any filter (refusal-shaped, generic
    phrase, or outside the length bounds). Mirrors the exact filters
    generate_captions.py applies at generation time — a caption that got
    views despite tripping one of those isn't a good style reference
    regardless of its view count."""
    from refusal_guard import contains_refusal
    if not caption_text:
        return None
    if contains_refusal(caption_text) or is_generic(caption_text):
        return None
    stripped = _strip_hashtags(caption_text)
    stripped = stripped.strip(_LEADING_QUOTE_CHARS).strip()
    if not (MIN_EXAMPLE_CHARS <= len(stripped) <= MAX_EXAMPLE_CHARS):
        return None
    return stripped


def evolve():
    """Mines top-performing published captions (top EVOLVE_TOP_PERCENT%
    of measured clips by views) and folds usable ones into the DB-backed
    learned bank. Returns (merged_examples_list_or_None, sample_size).
    None means nothing changed this run — either too few measured clips
    (sample_size < EVOLVE_MIN_SAMPLE) or none of the top performers
    produced a usable candidate line; either way any previously-learned
    bank is left untouched, same "don't wipe a good prior result over a
    temporarily thin run" posture performance_insights.py uses for its
    own insight cache.

    MERGE POLICY: new qualifying candidates from THIS run go to the FRONT
    of the bank (freshest signal), the previously-learned bank follows,
    deduped case-insensitively, capped at MAX_LEARNED_EXAMPLES."""
    rows = db.posts_with_metrics()
    by_key = {}
    for r in rows:
        by_key.setdefault(r["key"], []).append(r)

    sample_size = len(by_key)
    if sample_size < EVOLVE_MIN_SAMPLE:
        return None, sample_size

    clip_views = {}
    clip_caption = {}
    for key, platform_rows in by_key.items():
        first = platform_rows[0]
        clip_views[key] = sum(r["views"] or 0 for r in platform_rows)
        clip_caption[key] = first.get("caption_tiktok")

    ranked_keys = sorted(clip_views, key=lambda k: clip_views[k], reverse=True)
    top_n = max(1, int(len(ranked_keys) * EVOLVE_TOP_PERCENT / 100))
    top_keys = ranked_keys[:top_n]

    candidates = []
    seen = set()
    for key in top_keys:
        line = _candidate_line(clip_caption.get(key))
        if line and line.lower() not in seen:
            candidates.append(line)
            seen.add(line.lower())

    if not candidates:
        return None, sample_size

    prior = db.get_insight(EXAMPLES_INSIGHT_KEY)
    existing = list(prior.get("examples", [])) if prior else []

    merged = []
    seen_all = set()
    for line in candidates + existing:
        low = line.lower()
        if low in seen_all:
            continue
        merged.append(line)
        seen_all.add(low)
        if len(merged) >= MAX_LEARNED_EXAMPLES:
            break

    db.save_insight(EXAMPLES_INSIGHT_KEY, {
        "examples": merged, "source_sample_size": sample_size,
    })
    return merged, sample_size
