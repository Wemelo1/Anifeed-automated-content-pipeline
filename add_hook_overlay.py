"""
Hook Overlay
-------------
Retention on TikTok/Shorts lives or dies in the first couple seconds.
Burned-in dialogue subtitles (from burn_subtitles.py) only kick in once
someone actually speaks — which can be a beat or two into the clip. This
stage burns a short punchy hook line directly onto the opening ~2.2
seconds as bold on-screen text, so there's something to grab attention
before dialogue subtitles take over.

HOOK SOURCE (updated): generate_captions.py now produces a caption_hook
field distinct from caption_tiktok — previously this stage extracted its
on-screen hook text FROM the TikTok caption itself, which meant the
burned-in hook and the caption sitting below the video in the TikTok app
ended up nearly identical. find_clips_needing_hook() now prefers
caption_hook, falling back to the old caption_tiktok-based extraction only
for clips captioned before this change (backward compatibility — nothing
needs to be re-run for old clips).

WHY THIS IS ITS OWN STAGE, RUNNING WHERE IT DOES:
The hook text comes from caption_final.txt/caption_hook, which don't exist
until generate_captions.py has run — so this can't be folded into
burn_subtitles.py's existing encode pass (which runs earlier). It also
needs to run AFTER generate_thumbnails.py, not before: thumbnail selection
samples frames spread across the clip, and for a short clip an early
candidate frame can land inside the hook window — running this stage last
guarantees the thumbnail is always picked from the clean, non-overlaid
video.

WHY THIS IS A FULL RE-ENCODE, NOT A CHEAPER SPLIT+CONCAT:
The obvious optimization is to only re-encode the ~2s head (with the
overlay) and stream-copy the rest, then concat. Tested that against a
clip produced with this pipeline's own ffmpeg settings (libx264, default
GOP) — the first keyframe after t=0 lands at t=10s (x264's default 250-frame
keyframe interval at 25fps). A stream-copy split at ~2.2s would snap back
to the t=0 keyframe, meaning the "tail" segment would restart from the
beginning instead of picking up at the cut point — not a minor seam, a
broken duplicate of nearly the whole clip. So this does a single full
re-encode with a time-windowed ASS cue instead (the overlay text disappears
on its own once the cue's end-timestamp passes — no splitting needed at
all). Audio is stream-copied (-c:a copy) since it doesn't need touching,
which recovers some of the cost.

COST / OPT-OUT: this is a second full video re-encode on top of
burn_subtitles.py's own pass — a real CPU cost on a phone. Set
HOOK_OVERLAY_ENABLED=0 in .env to skip this stage entirely if it's too
slow on your device; every other stage is unaffected.

SCOPE: clips only, not news digests — digests already have their own
retention hook via the ANIFEED intro title card in build_news_digest.py,
so this isn't a gap, just a deliberate boundary.

Skips anything already marked 'hooked' in pipeline.db (survives finished/
being deleted post-publish, same convention as every other stage here).
Also skips any clip whose caption hasn't been generated yet (need_tiktok
still pending in generate_captions.py) — picked up automatically once it
has, safe to re-run.

LEARN STAGE (added): HOOK_DURATION_SECONDS (the CONFIGURED ceiling) is no
longer a plain os.getenv() constant — it's a learned_config param (see
learned_config.py's PARAM_REGISTRY: "hook_overlay_duration"), same
pattern quality_gate.py already uses for QUALITY_APPROVE_THRESHOLD.
mutator.py does not yet have a rule proposing mutations for it (needs
performance_insights.py's bucketed data first — it now has it, see that
module's RENDER_PARAM_BUCKET_SPECS), so until a rule is added it sits at
its registry default (2.2) — same value this constant always defaulted to.

What actually gets RECORDED into render_params_json per clip is the
EFFECTIVE hook duration — min(HOOK_DURATION_SECONDS, clip_duration), the
same clamp apply_hook_overlay() already applies for short clips — not the
raw configured ceiling. A clip too short to use the full configured
duration genuinely got a shorter overlay, and that's what
performance_insights.py needs to correlate against views, not the
config value that clip never actually got. render_params_json is now
MERGED, not overwritten — this is the LAST stage in a clip's production
(after edit_clips.py's bgm/saturation, burn_subtitles.py's cue length),
so it reads whatever's already accumulated and adds its own key on top.
See db.py's render_params_json docstring for the full picture.

SETUP:
  ffmpeg on PATH

USAGE:
  python add_hook_overlay.py   (run after generate_thumbnails.py, before publishing)
"""

import json
import os
import re
import subprocess

from video_utils import get_duration, ffmpeg_filter_path
from refusal_guard import is_refusal as _looks_like_meta_commentary
import db
import learned_config as lc

lc.init_schema()

# Noto Color Emoji — download once with:
#   curl -L "https://github.com/googlefonts/noto-emoji/raw/main/fonts/NotoColorEmoji.ttf" \
#        -o /storage/emulated/0/tiktok/NotoColorEmoji.ttf
_EMOJI_FONT_PATHS = [
    os.path.join(os.path.abspath("."), "NotoColorEmoji.ttf"),
    "/data/data/com.termux/files/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf",
    "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf",
]
_EMOJI_FONT = next((p for p in _EMOJI_FONT_PATHS if os.path.isfile(p)), None)

FINISHED_DIR = "finished"

HOOK_OVERLAY_ENABLED = os.getenv("HOOK_OVERLAY_ENABLED", "1") != "0"
# LEARNED — was HOOK_DURATION_SECONDS = float(os.getenv("HOOK_DURATION_SECONDS", "2.2")).
# See learned_config.py's "hook_overlay_duration" registry entry and this
# module's LEARN STAGE docstring note above. This is the CONFIGURED
# ceiling; apply_hook_overlay() still clamps it down per-clip for short
# clips (see MIN_CLIP_DURATION below) — that clamped, effective value is
# what gets recorded into render_params_json, not this raw constant.
HOOK_DURATION_SECONDS = float(lc.get_param("hook_overlay_duration"))
HOOK_MAX_CHARS = int(os.getenv("HOOK_MAX_CHARS", "70"))
# Clips shorter than this aren't worth overlaying — the "hook" would cover
# most or all of the clip, which reads as a title card, not a hook.
MIN_CLIP_DURATION = 4.0

OUT_W, OUT_H = 1080, 1920
FONT_SIZE = 62

# Softened from the original solid-red/white-outline look, which read as
# an alert/warning banner rather than a hook — white text with a thin
# black outline instead, matching the yellow-highlight karaoke style
# already used elsewhere in the channel (burn_subtitles.py /
# build_news_digest.py) rather than introducing a third clashing look.
HOOK_STYLE = (
    f"PlayResX={OUT_W},PlayResY={OUT_H},"
    f"FontName=Arial,FontSize={FONT_SIZE},"
    f"PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,"
    f"BorderStyle=1,Outline=3,Shadow=0,Bold=1,Spacing=1,"
    f"Alignment=8,MarginV=180"
)


def find_clips_needing_hook():
    found = []
    if not os.path.isdir(FINISHED_DIR):
        return found
    for channel in os.listdir(FINISHED_DIR):
        if channel == "news":
            continue  # digests are out of scope — see module docstring
        channel_path = os.path.join(FINISHED_DIR, channel)
        if not os.path.isdir(channel_path):
            continue
        for post_id in os.listdir(channel_path):
            post_path = os.path.join(channel_path, post_id)
            video_path = os.path.join(post_path, "video.mp4")
            if not os.path.isfile(video_path):
                continue
            if db.is_stage_done(channel, post_id, "hooked"):
                continue
            post = db.get_post(channel, post_id)
            caption = post.get("caption_hook") if post else None
            if not caption:
                caption = post.get("caption_tiktok") if post else None  # backward-compat fallback
            if not caption:
                continue  # generate_captions.py hasn't produced this yet

            # A/B override (roadmap item L) — a "treatment" variant post
            # created by ab_test.py carries an ab_override_json blob; if
            # it names hook_overlay_duration specifically, use THAT value
            # for this one clip instead of the global learned_config
            # value. Every other clip (ab_override_json NULL, or naming a
            # different param) gets None here, meaning "use the global
            # value" — identical to behavior before this feature existed.
            hook_override = None
            if post and post.get("ab_override_json"):
                try:
                    override_params = json.loads(post["ab_override_json"])
                    hook_override = override_params.get("hook_overlay_duration")
                except (TypeError, ValueError):
                    hook_override = None

            found.append((channel, post_id, video_path, caption, hook_override))
    return found


# Second line of defense against burning meta-commentary onto video, in
# case a bad caption value ever reaches this stage (e.g. rows written
# before generate_captions.py's own refusal filter existed, or any future
# gap in that filter). Deliberately conservative — prefix match only, via
# refusal_guard.is_refusal() — so this never false-positives on a real
# caption that happens to reference these words mid-sentence.


def extract_hook_text(caption_text, max_chars=HOOK_MAX_CHARS):
    """caption_text is either caption_hook (preferred, a short standalone
    line already written to be a hook) or, for older clips, caption_tiktok
    (the full caption blob — 'hook text + hashtags' as one string, see
    generate_captions.py's old TIKTOK_PROMPT). Either way: strips hashtag
    tokens, trims to a length that reads cleanly as bold on-screen text."""
    if not caption_text:
        return None
    words = caption_text.split()
    hook_words = [w for w in words if not w.startswith("#")]
    hook = " ".join(hook_words).strip().strip("\"\u201c\u201d'").strip()
    if not hook:
        return None
    if _looks_like_meta_commentary(hook):
        return None
    if len(hook) > max_chars:
        hook = hook[:max_chars].rsplit(" ", 1)[0]
        # A mid-caption truncation can slice through a quoted phrase and
        # leave a dangling, unmatched quote mark with nothing to close it
        # (e.g. `vibin' "T…`). If the truncated text has an odd number of
        # quote characters, drop back to before the last one so the hook
        # never ends on a half-open quote.
        for qchar in ('"', "\u201c", "\u201d"):
            if hook.count(qchar) % 2 == 1:
                hook = hook.rsplit(qchar, 1)[0]
        hook = hook.rstrip(",.;:—-").strip() + "…"
    return hook or None


def _ass_ts(sec):
    cs = int(round((sec % 1) * 100))
    s = int(sec) % 60
    m = (int(sec) // 60) % 60
    h = int(sec) // 3600
    return f"{h:01d}:{m:02d}:{s:02d}.{cs:02d}"


def _ass_escape(text):
    """ASS treats { and \\ as control characters (override blocks). LLM-
    generated caption text is very unlikely to contain either, but this is
    burned directly into a video file with no further review step, so it's
    worth guarding rather than trusting the input — same defensive posture
    as build_news_digest.py's _strip_markdown()/is_refusal() around its own
    LLM output."""
    return text.replace("\\", "").replace("{", "(").replace("}", ")")


def make_hook_ass(hook_text, duration, font_size=FONT_SIZE):
    header = (
        "[Script Info]\n"
        "ScriptType: v4.00+\n"
        f"PlayResX: {OUT_W}\n"
        f"PlayResY: {OUT_H}\n"
        # WrapStyle 0 = smart/balanced auto-wrap ON, so a long hook wraps
        # onto multiple centered lines instead of running off-screen at
        # this font size.
        "WrapStyle: 0\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        f"Style: Default,Arial,{font_size},&H00FFFFFF,&H00FFFFFF,&H00000000,"
        "&H00000000,1,0,0,0,100,100,1,0,1,3,0,8,60,60,180,1\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, "
        "Effect, Text\n"
    )
    text = _ass_escape(hook_text)
    event = f"Dialogue: 0,{_ass_ts(0)},{_ass_ts(duration)},Default,,0,0,0,,{text}\n"
    return header + event


def apply_hook_overlay(video_path, hook_text, work_dir, hook_duration_override=None):
    """Returns (applied: bool, reason_or_None, effective_hook_duration).
    effective_hook_duration is the ACTUAL on-screen duration used —
    min(HOOK_DURATION_SECONDS, clip duration), or min(override, clip
    duration) when `hook_duration_override` is given — or None when
    `applied` is False (nothing was burned, so there's no effective
    duration to record). Callers should treat effective_hook_duration as
    the value to persist into render_params_json, not the raw
    HOOK_DURATION_SECONDS constant — see this module's LEARN STAGE
    docstring section for why.

    hook_duration_override (roadmap item L, A/B testing) — when given,
    used INSTEAD of the global learned_config value for this one clip.
    This is what lets an ab_test.py "treatment" variant actually render
    differently from its "control" sibling; see find_clips_needing_hook()
    for where this is read from a post's ab_override_json."""
    duration = get_duration(video_path)
    if duration < MIN_CLIP_DURATION:
        return False, f"clip too short ({duration:.1f}s < {MIN_CLIP_DURATION}s) — skipped", None

    configured_duration = (
        hook_duration_override if hook_duration_override is not None
        else HOOK_DURATION_SECONDS
    )
    hook_duration = min(configured_duration, duration)
    ass_path = os.path.join(work_dir, "_hook_overlay.ass")
    with open(ass_path, "w", encoding="utf-8") as f:
        f.write(make_hook_ass(hook_text, hook_duration))
    safe_ass = ffmpeg_filter_path(os.path.abspath(ass_path))

    out_path = video_path + ".hooktmp.mp4"
    fontsdir_arg = f":fontsdir={ffmpeg_filter_path(os.path.dirname(_EMOJI_FONT))}" if _EMOJI_FONT else ""
    vf = f"subtitles={safe_ass}{fontsdir_arg}:force_style='{HOOK_STYLE}'"

    try:
        r = subprocess.run([
            "ffmpeg", "-y",
            "-i", video_path,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "copy",
            out_path,
        ], capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"ffmpeg failed: {r.stderr[-1000:]}")
        os.replace(out_path, video_path)
        return True, None, hook_duration
    finally:
        for p in (ass_path, out_path):
            if os.path.isfile(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


def _merged_render_params(channel, post_id, new_params):
    """Reads this post's EXISTING render_params_json (accumulated by
    edit_clips.py and burn_subtitles.py across the earlier stages of this
    same clip's production), merges in `new_params`, and returns the
    merged dict serialized as JSON. This is the LAST production stage
    before publishing, so this merge is what finalizes the clip's full
    recorded render history. See db.py's render_params_json docstring."""
    post = db.get_post(channel, post_id)
    existing_raw = post.get("render_params_json") if post else None
    try:
        merged = json.loads(existing_raw) if existing_raw else {}
    except (TypeError, ValueError):
        merged = {}
    merged.update(new_params)
    return json.dumps(merged)


def main():
    if not HOOK_OVERLAY_ENABLED:
        print("HOOK_OVERLAY_ENABLED=0 — skipping this stage entirely.")
        return

    db.init_db()
    clips = find_clips_needing_hook()
    if not clips:
        print("No clips need a hook overlay right now.")
        return

    print(f"Applying hook overlay to {len(clips)} clip(s)...")
    done, skipped, failed = 0, 0, 0

    for channel, post_id, video_path, caption, hook_override in clips:
        key = f"{channel}/{post_id}"
        hook_text = extract_hook_text(caption)
        if not hook_text:
            print(f"  {key}: SKIPPED — caption had no non-hashtag text to use as a hook")
            skipped += 1
            continue

        if hook_override is not None:
            print(f"  {key}: A/B override active — hook_overlay_duration={hook_override}s "
                  f"(instead of the global learned value)")

        work_dir = os.path.dirname(video_path)
        print(f"  {key}: \"{hook_text}\"")
        try:
            applied, reason, effective_duration = apply_hook_overlay(
                video_path, hook_text, work_dir, hook_duration_override=hook_override)
        except Exception as e:
            print(f"    FAILED: {e}")
            failed += 1
            continue

        if not applied:
            print(f"    SKIPPED — {reason}")
            skipped += 1
            # Still mark 'hooked' — a clip that's too short to overlay isn't
            # going to grow longer on a future run, so don't retry it forever.
            # No render_params_json write here — nothing was actually
            # burned, so there's no effective hook_overlay_duration to record.
            db.mark_stage(channel, post_id, "hooked")
            continue

        db.mark_stage(
            channel, post_id, "hooked",
            render_params_json=_merged_render_params(
                channel, post_id, {"hook_overlay_duration": effective_duration}
            ),
        )
        done += 1
        print(f"    -> overlay applied ({effective_duration:.1f}s)")

    print(f"\nDone. {done} clip(s) overlaid, {skipped} skipped, {failed} failed.")


if __name__ == "__main__":
    main()
