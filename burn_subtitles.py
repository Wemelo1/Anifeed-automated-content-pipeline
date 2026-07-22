"""
Subtitle Burner
----------------
Takes the already-edited (9:16) clips and adds burned-in English subtitles,
translated directly from the Japanese audio using Groq's Whisper model.
Subtitles use karaoke-style word-by-word highlighting (matching
build_news_digest.py's look) — each word lights up as it's spoken.

Whisper's translation endpoint only returns segment-level timestamps (a
whole phrase at once), not true per-word ones, so per-word timing within
each segment is ESTIMATED by distributing the segment's known duration
proportionally by character length — the same technique
build_news_digest.py uses as its own fallback when edge-tts's real word
timing isn't available. It won't be frame-perfect like true ASR word
alignment, but it's a solid approximation and needs no extra API capability
(Groq's word-level timestamp support isn't confirmed reliable on the
translation endpoint this script uses — see segment_to_words() docstring).

Note: this only catches SPOKEN dialogue. Japanese text that's baked into
the video itself (title cards, on-screen captions in the original clip)
can't be read or translated — only audio gets subtitled.

If a clip has no spoken dialogue (silent, music-only, etc.), it's copied
through untouched rather than failing.

Skips any clip already recorded as subtitled in pipeline.db — the database
is the source of truth, not whether finished/<channel>/<post_id>/video.mp4
still exists, so this stays skip-safe even if you delete finished/ right
after publishing. (Without this, deleting finished/ but leaving edited/ in
place would cause every clip to get re-transcribed and re-burned for
nothing on the next run.)

SUBTITLE-SYNC SIDECAR (for quality_gate.py): whenever real captions are
burned (not the "no dialogue" copy-through path), this writes a small
subtitle_sync.json next to the finished video.mp4 recording the LAST
subtitle cue's end timestamp. This is the exact same "persist the timing
data instead of trying to reverse it out of pixels later" pattern
CAPTION_GENERATION.md already documents for the .timestamps.json sidecar
— by the time quality_gate.py runs, subtitles are burned into the video
frames themselves (not a separate stream), so there's nothing left to
inspect unless the timing is saved at generation time. quality_gate.py
compares last_cue_end against the finished video's actual duration to
catch the desync-class bug: captions stopping well before the audio
actually ends (or, less commonly, extending past it). Missing sidecar
(a clip that predates this feature, or the no-dialogue copy-through path)
is a graceful no-op for quality_gate.py, never a blocker.

LEARN STAGE (added): MAX_WORDS_PER_CUE is no longer a plain module
constant — it's a learned_config param (see learned_config.py's
PARAM_REGISTRY: "words_per_subtitle_cue"), same pattern quality_gate.py
already uses for QUALITY_APPROVE_THRESHOLD. mutator.py does not yet have
a rule proposing mutations for it (that needs performance_insights.py to
have real bucketed data first — it now does, see that module's
RENDER_PARAM_BUCKET_SPECS), so until a rule is added it just sits at its
registry default (4) — same value this constant always defaulted to.

render_params_json is now MERGED, not overwritten: edit_clips.py already
wrote bgm_volume/color_grade_saturation/etc. into this clip's row when it
advanced to 'edited'. This stage reads that existing JSON via
db.get_post(), adds its own words_per_subtitle_cue key, and writes the
merged blob back — so the full production history of a clip accumulates
across stages instead of each later stage erasing the earlier one's
record. See db.py's render_params_json docstring for the full picture
across all stages.

SETUP (one-time):
- ffmpeg must be on your PATH
- pip install groq python-dotenv
- GROQ_API_KEY must be set in your .env

USAGE:
Run `python burn_subtitles.py` any time after edit_clips.py has produced
new edited videos. Output goes to finished/<channel>/<post_id>/video.mp4
"""

import json
import os
import re
import shutil
import subprocess
from dotenv import load_dotenv
from groq import Groq

import db
import learned_config as lc
import cost_tracker as ct
from video_utils import get_duration

# ---------- CONFIG ----------
load_dotenv()
lc.init_schema()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
WHISPER_MODEL = "whisper-large-v3"  # translation endpoint only supports this model, not turbo

EDITED_DIR = "edited"
FINISHED_DIR = "finished"

# Subtitle look — matches build_news_digest.py's SUB_STYLE so clips and news
# digests look visually consistent across the whole channel.
OUT_W, OUT_H = 1080, 1920
FONT_SIZE = 44
MARGIN_FROM_BOTTOM = 260  # pushes text up from the very bottom edge (avoids TikTok UI overlap)
# LEARNED — was a plain module constant (MAX_WORDS_PER_CUE = 4). See
# learned_config.py's "words_per_subtitle_cue" registry entry and this
# module's LEARN STAGE docstring note above.
MAX_WORDS_PER_CUE = int(lc.get_param("words_per_subtitle_cue"))
HIGHLIGHT_COLOR = "&H0000FFFF"  # bright yellow, same as build_news_digest.py
# -----------------------------


def find_clips_needing_subtitles():
    found = []
    if not os.path.isdir(EDITED_DIR):
        return found

    for channel in os.listdir(EDITED_DIR):
        channel_path = os.path.join(EDITED_DIR, channel)
        if not os.path.isdir(channel_path):
            continue
        for post_id in os.listdir(channel_path):
            post_path = os.path.join(channel_path, post_id)
            video_path = os.path.join(post_path, "video.mp4")
            if not os.path.isfile(video_path):
                continue
            if db.is_stage_done(channel, post_id, "subtitled"):
                continue  # already subtitled+published, even if finished/ got deleted since
            found.append((channel, post_id, post_path, video_path))
    return found


def extract_audio(video_path, audio_path):
    """Pull a small, compressed audio-only file for fast/cheap upload to Whisper."""
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000", "-b:a", "64k",
        audio_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Audio extraction failed:\n{result.stderr[-1000:]}")


def transcribe_and_translate(client, audio_path, content_key=None):
    """Returns a list of {start, end, text} segments, translated to English."""
    with open(audio_path, "rb") as f:
        response = client.audio.translations.create(
            model=WHISPER_MODEL,
            file=f,
            response_format="verbose_json",
        )
    # Whisper/transcription pricing is typically per-second of audio, not
    # per-call — units=duration_seconds so cost_tracker's rate table (if
    # ever set to a non-zero $/second) produces a meaningful total.
    # get_duration() failing here (corrupt/odd audio file) must never
    # block the actual transcription result above — logged at units=1 as
    # a degraded fallback rather than raising.
    try:
        units = get_duration(audio_path)
    except Exception:
        units = 1.0
    ct.log_call("groq", "whisper_call", content_key=content_key, content_type="clip", units=units)

    segments = getattr(response, "segments", None) or []
    return [{"start": s["start"], "end": s["end"], "text": s["text"].strip()}
            for s in segments if s["text"].strip()]


def _ass_ts(sec):
    cs = int(round((sec % 1) * 100))
    s = int(sec) % 60
    m = (int(sec) // 60) % 60
    h = int(sec) // 3600
    return f"{h:01d}:{m:02d}:{s:02d}.{cs:02d}"


def segment_to_words(segment):
    """Estimates per-word timing within one Whisper segment by distributing
    the segment's [start,end] duration proportionally by character length.
    Whisper's translation endpoint only returns segment-level timestamps
    (a whole phrase at once), not true word-level ones — this is the same
    proportional-estimate technique build_news_digest.py uses as its own
    fallback when edge-tts's real word timing isn't available. It won't be
    frame-perfect the way true ASR word alignment would be, but it's a
    solid approximation and needs no extra API capability."""
    text = segment["text"].strip()
    raw_words = text.split()
    if not raw_words:
        return []
    start, end = segment["start"], segment["end"]
    dur = max(end - start, 0.01)
    total_chars = sum(len(w) for w in raw_words) or 1
    words, t = [], start
    for w in raw_words:
        wdur = dur * (len(w) / total_chars)
        words.append({"word": w, "start": t, "end": t + wdur})
        t += wdur
    return words


def words_to_chunks(words, max_words=None):
    """max_words defaults to the module-level MAX_WORDS_PER_CUE (now a
    learned_config value) rather than being bound as a default-argument
    value at import time — a default arg would freeze the OLD value if
    this function were ever called after a hypothetical future in-process
    reload of the learned param; reading the module constant fresh inside
    the function body is a cheap way to avoid that footgun, even though
    in practice this script's single process run only ever reads
    MAX_WORDS_PER_CUE once at import anyway."""
    if max_words is None:
        max_words = MAX_WORDS_PER_CUE
    chunks, cur = [], []
    for w in words:
        cur.append(w)
        ends_sentence = re.search(r'[.!?]["\')]*$', w["word"])
        if len(cur) >= max_words or ends_sentence:
            chunks.append(cur)
            cur = []
    if cur:
        chunks.append(cur)
    return chunks


def build_word_chunks(segments):
    """Chunks words for cue display WITHOUT ever merging across a segment
    boundary — each Whisper segment is a natural speech phrase/pause, so
    grouping words from two different segments into one cue would show a
    caption block spanning an actual silence gap. Chunking per-segment
    avoids that."""
    all_chunks = []
    for segment in segments:
        words = segment_to_words(segment)
        if words:
            all_chunks.extend(words_to_chunks(words))
    return all_chunks


def make_ass(chunks, font_size=FONT_SIZE):
    """Builds karaoke-style .ass subtitles: each cue highlights its current
    word in yellow as it's spoken, matching build_news_digest.py's look."""
    if not chunks:
        return ""

    header = (
        "[Script Info]\n"
        "ScriptType: v4.00+\n"
        f"PlayResX: {OUT_W}\n"
        f"PlayResY: {OUT_H}\n"
        "WrapStyle: 2\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        f"Style: Default,Arial,{font_size},&H00FFFFFF,&H00FFFFFF,&H00000000,"
        "&H00000000,1,0,0,0,100,100,0,0,1,2,0,2,40,40,260,1\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, "
        "Effect, Text\n"
    )

    events = []
    for group in chunks:
        full_words = [w["word"] for w in group]
        for i, w in enumerate(group):
            start = w["start"]
            end = group[i + 1]["start"] if i + 1 < len(group) else w["end"]
            if end <= start:
                end = start + 0.05

            parts = []
            for j, ww in enumerate(full_words):
                if j == i:
                    parts.append(f"{{\\c{HIGHLIGHT_COLOR}}}{ww}{{\\r}}")
                else:
                    parts.append(ww)
            text = " ".join(parts)
            events.append(
                f"Dialogue: 0,{_ass_ts(start)},{_ass_ts(end)},Default,,0,0,0,,{text}"
            )

    return header + "\n".join(events) + "\n"


def last_cue_end(chunks):
    """Returns the end timestamp (seconds) of the very last subtitle cue
    across all chunks, or None if there are no chunks."""
    if not chunks:
        return None
    return max(w["end"] for group in chunks for w in group)


def write_subtitle_sync_sidecar(out_dir, chunks):
    """Writes subtitle_sync.json next to video.mp4 recording the last
    subtitle cue's end time, for quality_gate.py's timing-consistency
    check. Deliberately does NOT write anything if `chunks` is empty (the
    no-spoken-dialogue copy-through path) — no sidecar is quality_gate.py's
    signal to skip this check entirely for that clip, same as a clip that
    predates this feature. Never raises: a failure to write this optional
    sidecar must not take down subtitle burning itself."""
    end = last_cue_end(chunks)
    if end is None:
        return
    try:
        with open(os.path.join(out_dir, "subtitle_sync.json"), "w", encoding="utf-8") as f:
            json.dump({"last_cue_end": end, "source": "burn_subtitles"}, f)
    except OSError as e:
        print(f"    NOTE: couldn't write subtitle_sync.json (non-fatal, "
              f"quality_gate.py will just skip the timing check for this "
              f"clip): {e}")


def _merged_render_params(channel, post_id, new_params):
    """Reads this post's EXISTING render_params_json (written by
    edit_clips.py at the 'edited' stage — bgm_volume, color_grade_
    saturation, bgm_file, ...), merges in `new_params`, and returns the
    merged dict serialized as JSON, ready to pass to db.mark_stage(). See
    db.py's render_params_json docstring and this module's LEARN STAGE
    note above for why this MUST be a merge, not an overwrite — a later
    stage blindly writing its own render_params_json would silently erase
    every earlier stage's recorded values for this same clip."""
    post = db.get_post(channel, post_id)
    existing_raw = post.get("render_params_json") if post else None
    try:
        merged = json.loads(existing_raw) if existing_raw else {}
    except (TypeError, ValueError):
        merged = {}
    merged.update(new_params)
    return json.dumps(merged)


def burn_subtitles(video_path, ass_path, output_path):
    # Use forward slashes + relative paths to dodge ffmpeg's filter-string escaping issues on Windows
    safe_ass_path = ass_path.replace("\\", "/")

    # PlayResX/PlayResY set directly in the style — more reliable across ffmpeg builds
    # than the "original_size" filter option, which isn't always honored.
    # This force_style sets the base look (font/size/position); the
    # per-word {\c...}{\r} color-override tags embedded in the .ass file
    # itself still take effect on top of it for the karaoke highlight —
    # same combination build_news_digest.py already uses successfully.
    style = (
        f"PlayResX={OUT_W},PlayResY={OUT_H},"
        f"FontName=Arial,FontSize={FONT_SIZE},"
        f"PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,"
        f"BorderStyle=1,Outline=2,Shadow=0,Bold=1,"
        f"Alignment=2,MarginV={MARGIN_FROM_BOTTOM}"
    )
    vf = f"subtitles={safe_ass_path}:force_style='{style}'"

    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-vf", vf,
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "copy",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Subtitle burn failed:\n{result.stderr[-1500:]}")


def main():
    if not GROQ_API_KEY:
        print("GROQ_API_KEY not found. Add it to your .env file first.")
        return

    db.init_db()
    clips = find_clips_needing_subtitles()
    if not clips:
        print("No clips need subtitles right now. Run edit_clips.py first if you haven't.")
        return

    client = Groq(api_key=GROQ_API_KEY)
    done_count = 0

    for channel, post_id, post_path, video_path in clips:
        out_dir = os.path.join(FINISHED_DIR, channel, post_id)
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, "video.mp4")
        temp_audio = os.path.join(out_dir, "_temp_audio.m4a")
        temp_ass = os.path.join(out_dir, "_temp_subs.ass")

        print(f"Processing {channel}/{post_id}...")
        chunks = []
        try:
            extract_audio(video_path, temp_audio)
            segments = transcribe_and_translate(client, temp_audio,
                                                 content_key=f"{channel}/{post_id}")

            if not segments:
                print("  No spoken dialogue detected — copying clip through with no subtitles.")
                shutil.copy(video_path, output_path)
            else:
                chunks = build_word_chunks(segments)
                ass_content = make_ass(chunks)
                with open(temp_ass, "w", encoding="utf-8") as f:
                    f.write(ass_content)
                burn_subtitles(video_path, temp_ass, output_path)
                write_subtitle_sync_sidecar(out_dir, chunks)
                print(f"  Burned in {len(segments)} subtitle line(s), "
                      f"{sum(len(c) for c in chunks)} word(s) with karaoke highlighting.")

        except RuntimeError as e:
            print(f"  FAILED: {e}")
            continue
        finally:
            for temp_file in (temp_audio, temp_ass):
                if os.path.isfile(temp_file):
                    os.remove(temp_file)

        # Carry the caption text forward too
        raw_caption_path = os.path.join(post_path, "caption_raw.txt")
        raw_caption = None
        if os.path.isfile(raw_caption_path):
            shutil.copy(raw_caption_path, os.path.join(out_dir, "caption_raw.txt"))
            with open(raw_caption_path, "r", encoding="utf-8") as f:
                raw_caption = f.read()

        extra = {"media_type": "video"}
        if raw_caption is not None:
            extra["caption_raw"] = raw_caption

        # Merge words_per_subtitle_cue into the clip's accumulated
        # render_params_json — see _merged_render_params()'s docstring.
        # Recorded even on the no-dialogue copy-through path (chunks==[])
        # since MAX_WORDS_PER_CUE was still the effective config this run
        # was governed by, even though it had no cues to apply it to.
        extra["render_params_json"] = _merged_render_params(
            channel, post_id, {"words_per_subtitle_cue": MAX_WORDS_PER_CUE}
        )

        db.mark_stage(channel, post_id, "subtitled", **extra)

        done_count += 1
        print(f"  Saved to {output_path}")

    print(f"\nDone. Processed {done_count} new clip(s).")


if __name__ == "__main__":
    main()
