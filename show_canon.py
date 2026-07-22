"""
Show Title Canonicalization
-----------------------------
identify_show.py's three ID layers (visual/text/search) each produce their
own free-text guess at a title — "Chainsaw Man", "chainsaw man s2",
"CHAINSAW MAN: Season 2", "Chainsaw Man (2022)" have all shown up in
production for the same underlying franchise. Left as raw strings, every
downstream analytics question ("which show performs best?") gets answered
against fragments instead of the real signal.

This module maps any raw title to one canonical_shows row, in three tiers,
cheapest first:

  1. Exact/near-exact match against a known alias (free — no API call)
  2. Fuzzy char-ngram match against all known aliases, threshold set in
     db.CANONICAL_SHOW_SIMILARITY_THRESHOLD (still free)
  3. LLM normalization — only hit when tiers 1-2 both miss, i.e. this is
     plausibly a genuinely new show. Asks the model for a clean official
     title AND strips season/part/cour suffixes, so future entries for the
     same franchise fuzzy-match against tier 2 instead of hitting the LLM
     again.

DESIGN CALL — franchise-level, not season-level, grouping:
Canonical names deliberately collapse "Chainsaw Man" and "Chainsaw Man
Season 2" into one entry. The insight this whole system feeds
(performance_insights.py -> "should I post more of X?") is a
content-strategy question, and franchise identity is what's actionable
there — splitting by season would fragment an already-small dataset for no
strategic benefit. If per-season granularity ever becomes useful (e.g. a
franchise has one weak season), that's a query-time slice of the aliases
list, not a reason to change what's canonical.

USAGE:
  from groq import Groq
  import show_canon
  client = Groq(api_key=...)
  canonical = show_canon.canonicalize(client, "chainsaw man s2")
  # -> "Chainsaw Man" (creates or matches the canonical_shows row as needed)
"""

import re

import db

TEXT_MODEL = "openai/gpt-oss-120b"

_SEASON_SUFFIX_RE = re.compile(
    r"""\s*[:\-–—(]?\s*
        (season\s*\d+|s\d+|part\s*\d+|cour\s*\d+|
         \d+(st|nd|rd|th)\s*season|final\s*season)
        \)?\s*$""",
    re.IGNORECASE | re.VERBOSE,
)
_YEAR_SUFFIX_RE = re.compile(r"\s*\(\d{4}\)\s*$")

NORMALIZE_PROMPT = """You clean up anime titles for a content database.

Given a raw, possibly messy anime title, respond with ONLY the official
English title, normalized:
- Fix casing (Title Case for the title itself)
- Remove season/part/cour/year suffixes (e.g. "Season 2", "S2", "Part 1",
  "(2022)") — respond with the base franchise title only
- If it's not a real anime title (e.g. garbage text), respond with exactly:
  UNKNOWN

No explanation, no punctuation beyond what belongs in the title itself,
nothing else."""


def _strip_suffixes(title):
    """Cheap, no-API-call pass that handles the common cases (season/part/
    year suffixes) before ever considering an LLM call."""
    t = title.strip()
    t = _YEAR_SUFFIX_RE.sub("", t)
    t = _SEASON_SUFFIX_RE.sub("", t)
    return t.strip(" -:–—")


def _llm_normalize(client, raw_title):
    try:
        resp = client.chat.completions.create(
            model=TEXT_MODEL,
            messages=[
                {"role": "system", "content": NORMALIZE_PROMPT},
                {"role": "user", "content": raw_title[:200]},
            ],
            temperature=0.1,
            max_tokens=40,
            reasoning_effort="low",
        )
        result = (resp.choices[0].message.content or "").strip()
        return result if result and result.upper() != "UNKNOWN" else None
    except Exception as e:
        print(f"    Show normalization failed, falling back to suffix-stripped raw title: {e}")
        return None


def canonicalize(client, raw_title):
    """Returns a canonical show name for `raw_title`, creating or updating
    the canonical_shows row as needed. Returns None if raw_title is empty/
    UNKNOWN — callers should treat None as 'no show identified', same as
    identify_show.py already treats its own UNKNOWN result."""
    if not raw_title or raw_title.strip().upper() == "UNKNOWN":
        return None
    raw_title = raw_title.strip()

    # Tier 1 + 2: fuzzy match against everything already known. Checked
    # BEFORE any suffix-stripping or LLM call, since an exact/near-exact
    # alias hit is the cheapest possible path and most repeat titles will
    # land here.
    match = db.find_matching_canonical_show(raw_title)
    if match:
        db.add_show_alias(match["canonical_name"], raw_title)
        return match["canonical_name"]

    # Cheap suffix-stripping pass, then re-check the fuzzy match — catches
    # "Chainsaw Man Season 2" the first time it's ever seen, without an
    # LLM call, as long as "Chainsaw Man" is already known.
    stripped = _strip_suffixes(raw_title)
    if stripped and stripped != raw_title:
        match = db.find_matching_canonical_show(stripped)
        if match:
            db.add_show_alias(match["canonical_name"], raw_title)
            return match["canonical_name"]

    # Tier 3: genuinely new — ask the LLM for a clean canonical form so
    # future variants of THIS title fuzzy-match cheaply from here on.
    normalized = _llm_normalize(client, raw_title) if client else None
    canonical_name = normalized or stripped or raw_title

    # One more fuzzy check against the LLM's normalized form, in case it
    # landed on wording that matches an existing show the raw string didn't
    # (e.g. raw "chainsaw man s2" was too mangled to fuzzy-match "Chainsaw
    # Man" directly, but the LLM's clean output does).
    match = db.find_matching_canonical_show(canonical_name)
    if match:
        db.add_show_alias(match["canonical_name"], raw_title)
        return match["canonical_name"]

    db.create_canonical_show(canonical_name, raw_title)
    if canonical_name != raw_title:
        db.add_show_alias(canonical_name, canonical_name)
    return canonical_name
