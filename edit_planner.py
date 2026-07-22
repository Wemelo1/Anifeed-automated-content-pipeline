"""
Edit Planner
-------------
Uses Google Gemini 1.5 Flash (via google-genai SDK) for narration/planning
instead of Groq — Gemini's free tier gives 1M TPM vs Groq's 8K TPM, which
eliminates the rate-limit skips that were dropping stories during Phase 1.

Still returns the same {"narration", "emotion", "score"} dict so nothing
else in the pipeline changes. Falls back to a neutral plan on any API or
parse failure so one bad response never kills a story.

USAGE:
  import edit_planner
  plan = edit_planner.plan_story(caption, web_context, corroboration_count)
  # -> {"narration": str, "emotion": str, "score": int}
"""

import os
import json
import re
import time
from google import genai
from google.genai import types

from motion_engine import EMOTIONS

_API_KEY = os.getenv("GEMINI_API_KEY", "")
_client  = genai.Client(api_key=_API_KEY) if _API_KEY else None

GEMINI_MODEL = "gemini-2.0-flash-lite"

PLANNER_PROMPT = """You're an anime fan making short TikTok voiceovers. Your job is to take raw anime news data and rewrite it as something a real person would actually say — casual, punchy, with personality. NOT a news anchor. NOT a bot reading a list.

Write narration for this anime news item, then classify it, and respond with ONLY a single JSON object (no markdown fences, no extra text):

{
  "narration": "...",
  "emotion": "...",
  "score": 0
}

NARRATION rules:
- 2-3 short sentences max. Target 25-35 words total — this is spoken over a fast clip.
- The input may be a dry data dump (e.g. "X is currently #4 trending on AniList. Episode 7 airs August 5."). Your job is to TRANSFORM it — extract what's actually interesting and say it like a fan, not like a feed reader.
- Lead with the most interesting fact. Trending rank, episode drop date, what the show is about — pick ONE thing and lead with that.
- DO NOT open with "Bro", "BRO", or any variation. It's overused and sounds fake.
- DO NOT open with the show's full formal title — shorten it or refer to it naturally.
- NEVER use the same opener style back to back. Each story must sound different from the last.
- Vary your opener. Examples by vibe:
    Excited  → "Episode [N] of [short title] just dropped—"
               "If you're not watching [title] yet, fix that now."
    Trending → "[Short title] is sitting at #[N] and not slowing down."
               "Everyone's on [title] right now and honestly I get it."
    Hype     → "The new [title] episode is out and people are losing it."
               "Just in—[title] episode [N] is here."
    Chill    → "[Title] is having its moment."
               "Somehow [title] keeps climbing—episode [N] out now."
    Curious  → "Not sure how [title] keeps doing it but here we are."
               "[Title] at #[N]? Episode [N] is out, go watch."
- Sentence 2: give it context — pull a plot hook from the synopsis, the genre combo, or what makes this show different. Don't just repeat the air date.
- Sentence 3 (optional): one short reaction. Hype, a question, or a vibe check.
- Forbidden: "Bro", "BRO", "sources confirm", "it has been announced", "according to", "fans are excited", "in today's news", "stay tuned", "as we know", "currently trending".
- CRITICAL: always produce narration. Never refuse or apologize.

EMOTION — pick exactly one. Read the descriptions carefully — most stories are NOT "serious":
  breaking  → use when: major trailer drop, cancellation, scandal, unexpected news, studio announcement. HIGH energy, urgent.
  hype      → use when: popular show trending high (#1-#5), hyped sequel confirmed, big episode drop everyone's waiting for. HYPE energy.
  excited   → use when: new season confirmed, release date just announced, beloved show returning. CELEBRATORY energy.
  mystery   → use when: unexplained ranking spike, unknown show suddenly trending, cryptic announcement, niche/underground show. CURIOUS energy.
  funny     → use when: absurd plot, meme-able show, comedy anime, ironic or unexpected situation. PLAYFUL energy.
  sad       → use when: series finale, show ending, beloved character death, cancellation of a fan-favourite. EMOTIONAL energy.
  serious   → use ONLY when none of the above fit. General spotlight with no strong angle. This should be RARE — maybe 1 in 6 stories.

EXAMPLES so you understand the spread:
  "Demon Slayer S4 trailer just dropped" → breaking
  "Solo Leveling is #1 on AniList" → hype
  "Chainsaw Man S2 confirmed for next year" → excited
  "Some obscure isekai nobody's heard of suddenly at #3" → mystery
  "Konosuba is trending again" → funny
  "Anohana is trending for its anniversary" → sad
  "Show X is at #12 with no particular news" → serious

SCORE — newsworthiness 0-100:
  95 = major trailer or breaking announcement
  90 = confirmed release date or new season
  83 = cast/VA announcement
  71 = trending episode drop (most AniList trending items land here)
  50 = general series spotlight with no breaking news
  28 = merch or product listing

Output ONLY the JSON object."""

_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)
# "hype" as fallback gives a warmer, more energetic default than "serious"
# when Gemini fails to parse — most anime news leans upbeat anyway, and
# "serious" was making every parse-failure story feel identical and dull.
DEFAULT_EMOTION = "hype"
DEFAULT_SCORE   = 60


def _extract_json(raw):
    match = _JSON_BLOCK_RE.search(raw)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def _sanitize_plan(parsed, fallback_text):
    narration = (parsed.get("narration") or "").strip().strip('"\u201c\u201d\'').strip()
    if not narration:
        narration = fallback_text[:200].strip()

    emotion = str(parsed.get("emotion") or "").strip().lower()
    if emotion not in EMOTIONS:
        emotion = DEFAULT_EMOTION

    try:
        score = int(parsed.get("score"))
        score = max(0, min(100, score))
    except (TypeError, ValueError):
        score = DEFAULT_SCORE

    return {"narration": narration, "emotion": emotion, "score": score}


def plan_story(caption, web_context="", corroboration_count=1):
    """Calls Gemini 1.5 Flash to produce narration + emotion + score.
    Returns {"narration": str, "emotion": str, "score": int}.
    Never raises — falls back to a neutral plan on any failure."""
    if not _client:
        return {"narration": caption[:200].strip(),
                "emotion": DEFAULT_EMOTION, "score": DEFAULT_SCORE}

    content = f"NEWS TEXT:\n{caption}"
    if web_context:
        content += f"\n\nWEB CONTEXT (use relevant facts to enrich narration):\n{web_context}"
    if corroboration_count > 1:
        content += (
            f"\n\n(NOTE: this story is being reported by {corroboration_count} "
            f"different anime news channels simultaneously — today's biggest "
            f"story. Let the narration's energy reflect that weight.)"
        )

    last_err = None
    for attempt in range(3):
        try:
            response = _client.models.generate_content(
                model=GEMINI_MODEL,
                contents=content,
                config=types.GenerateContentConfig(
                    system_instruction=PLANNER_PROMPT,
                    temperature=0.95,
                    max_output_tokens=500,
                ),
            )
            raw = (response.text or "").strip()
            parsed = _extract_json(raw)
            if parsed is None:
                return {"narration": (raw or caption[:200]).strip()[:400],
                        "emotion": DEFAULT_EMOTION, "score": DEFAULT_SCORE}
            return _sanitize_plan(parsed, caption)

        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            if "429" in err_str or "quota" in err_str or "rate" in err_str:
                wait = 4 ** attempt  # 1s, 4s, 16s
                print(f"    Gemini rate limit — retrying in {wait}s...")
                time.sleep(wait)
            else:
                break

    print(f"    Gemini plan_story failed: {last_err}")
    return {"narration": caption[:200].strip(),
            "emotion": DEFAULT_EMOTION, "score": DEFAULT_SCORE}
