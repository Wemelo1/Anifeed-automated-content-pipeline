"""
Caption Generator
------------------
Generates both TikTok and YouTube Shorts captions for each finished clip.

- caption_final.txt    → TikTok: punchy hook + hashtags, under 150 chars
- caption_youtube.txt  → YouTube: SEO title (60 chars) + description + hashtags

Skips clips that already have BOTH captions recorded in pipeline.db — the
database is the source of truth, not the caption_final.txt/caption_youtube.txt
files, so this stays skip-safe even after finished/ gets deleted post-publish.
Safe to re-run.

ANTI-GENERIC / ANTI-REPEAT (added after captions were observed recycling
the same "this is the most..." / "bold claim" phrasing constantly):
  - TIKTOK_PROMPT no longer hands the model one literal example string per
    style — a single concrete example gets copied near-verbatim far too
    often. caption_assets.py's rotating bank is used instead, and
    caption_assets.build_example_addendum() randomly samples a few each call.
  - build_anti_repeat_addendum() reads the last several ACTUAL generated
    captions out of pipeline.db (db.recent_captions()) and tells the model
    not to repeat their openings/phrasing.
  - caption_assets.is_generic() catches known dead phrases as a
    last-resort filter; if a generated caption still matches one,
    generate_tiktok() is called a second time before giving up.
  - The TikTok output is now TWO lines: a standalone HOOK line (burned onto
    the video by add_hook_overlay.py) and the CAPTION line (saved as
    caption_final.txt) — previously add_hook_overlay.py extracted its hook
    text from the caption itself, so the on-screen hook and the caption
    below the video ended up nearly identical. caption_hook is stored
    separately in pipeline.db (db.py's _EXTRA_COLUMNS/_EXPECTED_COLUMNS).

OPENER-PERFORMANCE ADDENDUM (self-learning loop expansion, roadmap item F):
performance_insights.py ranks caption OPENERS (the first few words of a
published caption_tiktok, normalized) by average views, the same way it
already ranks caption STYLE. caption_assets.build_opener_addendum()
surfaces the reliable top performers as a system-prompt addendum —
explicitly framed as "which opening MOVE tends to work," NOT as literal
text to copy. Kept separate from the CAPTION_EXAMPLES bank and the RECENT
CAPTIONS anti-repeat list — the anti-repeat instruction still wins if the
two ever point in different directions for a given clip.

LEARNED CAPTION ASSETS (self-learning loop expansion, roadmap item I —
CONSOLIDATED): the rotating example bank and the opener-performance
addendum both now live in caption_assets.py, a dedicated module that owns
this DB-backed "learned prompt bank" concern end to end (mining,
merging, and prompt-formatting), rather than being split across this
file and evolve_caption_examples.py the way it used to be. This file only
ever CONSUMES caption_assets.py — it never mines or merges the bank
itself. evolve_caption_examples.py (run weekly — see run_weekly_report.sh)
is now a thin entry point that calls caption_assets.evolve().

ENGAGEMENT PREFERENCE (self-learning loop expansion, roadmap item G):
build_style_addendum() and show_performance_note() now check
performance_insights.py's insight["engagement"] breakdown first (ranked
by engagement_score, not raw views — see that module's
ENGAGEMENT_MIN_SAMPLE) and only fall back to the views-based breakdown
when the engagement section isn't populated/reliable yet. This is the
"move from view-count to engagement-score optimization once you have
enough data" behavior from the roadmap — automatic, no manual cutover,
degrades gracefully to the exact previous (views-only) behavior when
engagement data isn't there.

SETUP:
  pip install groq python-dotenv

USAGE:
  python generate_captions.py  (run after burn_subtitles.py and identify_show.py)
"""

import os
from dotenv import load_dotenv
from google import genai
from google.genai import types
from refusal_guard import is_refusal

import db
import caption_assets as ca
import diversity_guard as dg
import cost_tracker as ct
from performance_insights import classify_caption_style

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-3.1-flash-lite"
EDITED_DIR     = "finished"

# ── TikTok prompt ─────────────────────────────────────────────────────────────
TIKTOK_PROMPT = """You write TikTok captions for an anime clips page.

You will be given a raw description for a short video clip. Your job:

1. Pull out SPECIFIC details: anime title, character names, arc/episode info,
   what's happening in the scene. Use every concrete detail given.
2. Judge the MOOD (intense/action, emotional/sad, funny, shocking, hype/epic).
3. Pick the style that fits the mood (rotate — see RECENT CAPTIONS below,
   never repeat the same style or opening word two clips in a row):
   - Question-hook: open with a genuine question about the moment
   - Bold-claim: state a strong, specific opinion about this exact scene
   - POV: put the viewer inside the moment ("POV: ...")
   - Hype/reaction: react like you just watched it, using real slang
   Do NOT reuse a phrase, sentence structure, or opening word from the
   RECENT CAPTIONS list below — those are what you've ALREADY written,
   copying their pattern is the one thing you must not do.
4. Weave specific details INTO the caption text, not just hashtags.
   "this clip hit different" is a last resort only.
5. Match language to mood — heavy slang for funny/hype, sincere for emotional.
6. Under ~150 characters.
7. End with 3-5 hashtags — DO NOT use #fyp or #foryou; TikTok's algorithm
   ranks on watch-time/completion, not hashtags, so those two are dead
   weight that describe nothing. Instead mix broad-but-topical (#anime,
   #animetiktok, #animeedit) with specific (show name, arc, character name)
   so the tags actually connect to a real interest graph.

Output exactly two lines:
LINE 1 — HOOK: a short standalone punch line (under 8 words), DIFFERENT
  wording than the caption below — this gets burned onto the video itself,
  it must NOT just repeat the caption.
LINE 2 — CAPTION: the full caption + hashtags as described above.
Nothing else, no labels, no "LINE 1"/"HOOK:" text in the actual output."""

# ── YouTube prompt ────────────────────────────────────────────────────────────
YOUTUBE_PROMPT = """You write YouTube Shorts metadata for an anime clips channel.

Given a raw description for a short anime clip, produce:

LINE 1 — Title: SEO-friendly, max 60 characters, no clickbait, naturally includes
  the anime name if known. Example: "Chainsaw Man Assassins Arc — New Trailer Reaction"
LINE 2 — Description: 2-3 sentences. Informative, clear, slightly more formal than
  TikTok. Include the anime name, what the clip shows, and why it's worth watching.
LINE 3 — Hashtags: 3-5 tags, mix broad (#Anime #AnimeFan) with specific (#Chainsaw Man).

Output ONLY these three lines, in this exact order, nothing else."""

# CAPTION_INSIGHT_MIN_SAMPLE now lives in caption_assets.py (the opener
# addendum needs it too) — reused here for build_style_addendum() and
# show_performance_note() so there's exactly one place this bar is
# defined, not two copies that could drift apart.
CAPTION_INSIGHT_MIN_SAMPLE = ca.CAPTION_INSIGHT_MIN_SAMPLE


def build_anti_repeat_addendum():
    recent = db.recent_captions(limit=8)
    if not recent:
        return ""
    lines = "\n".join(f"- {c[:80]}" for c in recent[:5])
    return (
        f"\n\nRECENT CAPTIONS (already used — do not repeat these openings, "
        f"phrasings, or styles):\n{lines}"
    )


def build_style_addendum(insight):
    """Turns the cached performance_insights.py output into a short
    system-prompt addendum ranking the four caption styles by what's
    actually driven performance so far. Prefers the ENGAGEMENT-ranked
    breakdown (insight["engagement"]["caption_styles"]) when it's
    populated and reliable — see this module's ENGAGEMENT PREFERENCE
    docstring section — falling back to the views-ranked breakdown
    (insight["caption_styles"]) otherwise, exactly as before this
    feature existed.

    Returns "" if there's no insight yet, if fewer than 2 styles have a
    reliable sample size yet (see CAPTION_INSIGHT_MIN_SAMPLE), or if the
    best reliable style isn't actually beating the average. Deliberately
    phrased as guidance ("lean toward", "when it fits the mood") rather
    than a hard rule — TIKTOK_PROMPT's own mood-matching logic should
    still win when a top style doesn't fit a given clip."""
    if not insight or not insight.get("caption_styles"):
        return ""

    engagement = insight.get("engagement")
    if engagement and engagement.get("caption_styles"):
        candidates = engagement["caption_styles"]
        metric_label = "engagement"
    else:
        candidates = insight["caption_styles"]
        metric_label = "views"

    reliable = [s for s in candidates if s["sample_size"] >= CAPTION_INSIGHT_MIN_SAMPLE]
    if len(reliable) < 2:
        return ""  # not enough reliably-sized styles to rank against each other yet

    best, worst = reliable[0], reliable[-1]
    if best["vs_overall_avg_pct"] <= 0:
        return ""  # no reliable style is actually beating the average — nothing useful to say

    lines = [
        f"\n\nRECENT PERFORMANCE DATA (from {insight['sample_size']} published clips, "
        f"ranked by {metric_label}, styles with at least {CAPTION_INSIGHT_MIN_SAMPLE} "
        f"measured clips only — use as a tiebreaker, mood-fit above still comes first):",
    ]
    for s in reliable:
        sign = "+" if s["vs_overall_avg_pct"] >= 0 else ""
        lines.append(f"- {s['style']}: {sign}{s['vs_overall_avg_pct']:.0f}% vs. channel average")
    lines.append(
        f"When the mood genuinely fits more than one style, lean toward "
        f"{best['style']} over {worst['style']}."
    )
    return "\n".join(lines)


def show_performance_note(insight, canonical_show):
    """Per-clip note when this specific clip's show is a known top
    performer — separate from the style addendum since it's post-specific,
    not a blanket system-prompt change. Same engagement-first, views-
    fallback preference as build_style_addendum() (see this module's
    ENGAGEMENT PREFERENCE docstring section), and the same
    CAPTION_INSIGHT_MIN_SAMPLE bar — a show with only 2-4 measured clips
    isn't reliable enough to bias every future caption toward it."""
    if not insight or not canonical_show:
        return None

    engagement = insight.get("engagement")
    if engagement and engagement.get("top_shows"):
        shows, metric_label = engagement["top_shows"], "engagement"
    else:
        shows, metric_label = insight.get("top_shows", []), "views"

    for s in shows:
        if (s["show"] == canonical_show
                and s["vs_overall_avg_pct"] > 0
                and s["sample_size"] >= CAPTION_INSIGHT_MIN_SAMPLE):
            return (
                f"(Note: {canonical_show} clips have been outperforming the "
                f"channel average by {s['vs_overall_avg_pct']:.0f}% on {metric_label} "
                f"— lean into specific {canonical_show} details if the mood allows.)"
            )
    return None


def generate_tiktok(client, user_content, style_addendum="", opener_addendum="", content_key=None):
    system = (TIKTOK_PROMPT + style_addendum + opener_addendum
              + ca.build_example_addendum() + build_anti_repeat_addendum())
    r = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=user_content,
        config=types.GenerateContentConfig(
            system_instruction=system,
            temperature=0.95,
            max_output_tokens=400,
        ),
    )
    ct.log_call("gemini", "text_call", content_key=content_key, content_type="clip")
    return (r.text or "").strip()


def generate_youtube(client, user_content, content_key=None):
    r = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=user_content,
        config=types.GenerateContentConfig(
            system_instruction=YOUTUBE_PROMPT,
            temperature=0.7,
            max_output_tokens=300,
        ),
    )
    ct.log_call("gemini", "text_call", content_key=content_key, content_type="clip")
    return (r.text or "").strip()


def find_clips_needing_captions():
    found = []
    if not os.path.isdir(EDITED_DIR):
        return found
    for channel in os.listdir(EDITED_DIR):
        channel_path = os.path.join(EDITED_DIR, channel)
        if not os.path.isdir(channel_path):
            continue
        for post_id in os.listdir(channel_path):
            post_path = os.path.join(channel_path, post_id)
            raw_path  = os.path.join(post_path, "caption_raw.txt")
            has_video = os.path.isfile(os.path.join(post_path, "video.mp4"))
            if has_video and os.path.isfile(raw_path):
                post = db.get_post(channel, post_id)
                needs_tiktok  = not (post and post.get("caption_tiktok"))
                needs_youtube = not (post and post.get("caption_youtube"))
                if needs_tiktok or needs_youtube:
                    found.append((channel, post_id, post_path, raw_path,
                                  needs_tiktok, needs_youtube))
    return found


def get_visual_hint(channel, post_id, post_path):
    """Prefer pipeline.db (survives finished/ being deleted); fall back to
    the visual_hint.txt file for any post identified before db.py existed."""
    post = db.get_post(channel, post_id)
    hint = post.get("visual_hint") if post else None
    if not hint:
        hint_path = os.path.join(post_path, "visual_hint.txt")
        if os.path.isfile(hint_path):
            with open(hint_path, "r", encoding="utf-8") as f:
                hint = f.read().strip()
    return hint if hint and hint.upper() != "UNKNOWN" else None


def build_user_content(raw_description, visual_hint, performance_note=None):
    if not raw_description.strip():
        raw_description = "(No description — write a generic engaging anime caption.)"
    content = raw_description
    if visual_hint:
        content += (
            f"\n\n(Visual analysis suggests this may be from '{visual_hint}' — "
            f"use if plausible but don't treat as certain.)"
        )
    if performance_note:
        content += f"\n\n{performance_note}"
    return content


def main():
    if not GEMINI_API_KEY:
        print("GEMINI_API_KEY not found. Add it to your .env file first.")
        return

    db.init_db()
    clips = find_clips_needing_captions()
    if not clips:
        print("No clips need captions right now.")
        return

    client = genai.Client(api_key=GEMINI_API_KEY)
    done = 0

    # Loaded once per run, not per clip — cheap, and performance_insights.py
    # only updates this daily-ish anyway. None until that script has run
    # with enough data; every function above already treats None as "no
    # change in behavior".
    insight = db.get_insight("content_strategy")
    style_addendum = build_style_addendum(insight)
    opener_addendum = ca.build_opener_addendum(insight)
    if insight:
        print(f"Using performance insight from {insight['sample_size']} clips "
              f"(computed {insight['computed_at']}).")
        if insight.get("engagement"):
            print("Engagement-ranked data available — style/show notes prefer "
                  "it over views where reliable.")
        if opener_addendum:
            print("Opener-performance addendum ACTIVE this run.")
        print()

    for channel, post_id, post_path, raw_path, need_tiktok, need_youtube in clips:
        with open(raw_path, "r", encoding="utf-8") as f:
            raw = f.read()
        db.set_fields(channel, post_id, caption_raw=raw)
        hint = get_visual_hint(channel, post_id, post_path)
        post_row = db.get_post(channel, post_id)
        canonical_show = post_row.get("canonical_show") if post_row else None
        perf_note = show_performance_note(insight, canonical_show)
        content = build_user_content(raw, hint, perf_note)

        print(f"Captions for {channel}/{post_id}...")
        if hint:
            print(f"  (visual hint: {hint})")
        if perf_note:
            print(f"  (performance note applied: {canonical_show})")

        if need_tiktok:
            # DIVERSITY GUARD (roadmap item T) — a slice of clips are
            # deliberately made with NO performance-based biasing at all
            # (style/opener addenda skipped), so genuinely new winners can
            # still be discovered rather than the system only ever
            # reinforcing what already tested well. Decided once per clip,
            # not per attempt, so a regeneration below stays consistent.
            exploring = dg.should_explore()
            effective_style_addendum = "" if exploring else style_addendum
            effective_opener_addendum = "" if exploring else opener_addendum
            if exploring:
                print("  (diversity guard: exploration slice — skipping style/opener bias this clip)")

            try:
                raw_tiktok = generate_tiktok(client, content, effective_style_addendum,
                                              effective_opener_addendum,
                                              content_key=f"{channel}/{post_id}")
                if raw_tiktok and is_refusal(raw_tiktok):
                    print(f"  TikTok: model returned meta-commentary instead of a "
                          f"caption ({raw_tiktok[:80]!r}) — discarding, will retry next run")
                    raw_tiktok = ""

                lines = [l.strip() for l in raw_tiktok.splitlines() if l.strip()]
                hook = lines[0] if lines else ""
                tiktok = lines[1] if len(lines) > 1 else raw_tiktok

                regenerate_reason = None
                if tiktok and ca.is_generic(tiktok):
                    regenerate_reason = f"generic phrase detected: {tiktok[:60]!r}"
                elif tiktok and not exploring:
                    style_violates, style_pct, style_limit = dg.check_caption_style(
                        classify_caption_style(tiktok))
                    if style_violates:
                        regenerate_reason = (
                            f"caption style would push its share of the last "
                            f"{dg.DIVERSITY_WINDOW} published clips to "
                            f"{style_pct*100:.0f}% (cap {style_limit*100:.0f}%)")

                if regenerate_reason:
                    print(f"  TikTok: regenerating once — {regenerate_reason}")
                    # One regeneration attempt covers BOTH triggers above —
                    # never more than one extra API call per clip, same
                    # bounded-retry posture the original is_generic check
                    # always used.
                    diversity_addendum = (
                        "\n\nDIVERSITY NOTE: the last several captions have "
                        "leaned heavily on one style — deliberately pick a "
                        "DIFFERENT style from TIKTOK_PROMPT's rotation for "
                        "this one." if regenerate_reason.startswith("caption style") else ""
                    )
                    raw_tiktok = generate_tiktok(
                        client, content,
                        effective_style_addendum + diversity_addendum,
                        effective_opener_addendum,
                        content_key=f"{channel}/{post_id}")
                    lines = [l.strip() for l in raw_tiktok.splitlines() if l.strip()]
                    hook = lines[0] if lines else ""
                    tiktok = lines[1] if len(lines) > 1 else raw_tiktok

                if hook and ca.is_generic(hook):
                    hook = ""  # fall back to add_hook_overlay's own extraction

                if tiktok:
                    with open(os.path.join(post_path, "caption_final.txt"), "w", encoding="utf-8") as f:
                        f.write(tiktok)
                    db.set_fields(channel, post_id, caption_tiktok=tiktok, caption_hook=hook or None)
                    print(f"  TikTok: {tiktok[:80]}...")
                    if hook:
                        print(f"  Hook:   {hook}")
                else:
                    if tiktok != "":
                        print("  TikTok: empty response — will retry next run")
            except Exception as e:
                print(f"  TikTok FAILED: {e}")

        if need_youtube:
            try:
                youtube = generate_youtube(client, content, content_key=f"{channel}/{post_id}")
                if youtube and is_refusal(youtube):
                    print(f"  YouTube: model returned meta-commentary instead of "
                          f"metadata ({youtube[:80]!r}) — discarding, will retry next run")
                    youtube = ""
                if youtube:
                    with open(os.path.join(post_path, "caption_youtube.txt"), "w", encoding="utf-8") as f:
                        f.write(youtube)
                    db.set_fields(channel, post_id, caption_youtube=youtube)
                    print(f"  YouTube: {youtube[:80]}...")
                else:
                    if youtube != "":
                        print("  YouTube: empty response — will retry next run")
            except Exception as e:
                print(f"  YouTube FAILED: {e}")

        # Stamp the "captioned" stage once both captions are on record —
        # purely for audit/status display; the per-field checks above are
        # what actually drive skip logic.
        post = db.get_post(channel, post_id)
        if post and post.get("caption_tiktok") and post.get("caption_youtube"):
            db.mark_stage(channel, post_id, "captioned")

        done += 1

    print(f"\nDone. Processed {done} clip(s).")


if __name__ == "__main__":
    main()
