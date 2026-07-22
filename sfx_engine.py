"""
SFX Engine
-----------
Scans a story's narration text for trigger keywords ("explosion", "drops",
"finally", ...) and, for each hit, overlays a short sound effect into that
story's voiceover audio at the EXACT timestamp of the triggering word —
using the same word-level timing TTS already returns for subtitle karaoke
highlighting (see build_news_digest.py's tts()), not a guess.

DEGRADES SILENTLY BY DESIGN: if SFX_DIR doesn't exist, or a specific sound
file referenced by SFX_LIBRARY isn't present, this simply skips that cue
and narration continues with plain voiceover — never blocks or fails a
build over a missing asset. Same convention as BGM_PATH/music_engine.

ASSET SETUP:
  Create a folder (default "sfx/", override via SFX_DIR in .env) containing
  any of the files named in SFX_LIBRARY below — .mp3 or .wav, short
  (under ~1s reads best), normalized to a reasonable volume since they're
  layered on top of voiceover, not replacing it:
    whoosh.mp3   boom.mp3   hit.mp3   pop.mp3   click.mp3   bass.mp3   glitch.mp3
  Missing files just mean that specific cue never fires — nothing else
  breaks. Good free sources: freesound.org (filter by CC0 license),
  Pixabay Sound Effects, Mixkit — check the specific track's license
  before use in monetized content either way.

USAGE:
  import sfx_engine
  cues = sfx_engine.find_cues(narration_text, words)
  audio_path = sfx_engine.mix_cues(audio_path, cues, work_dir)
  # returns a NEW path with sfx mixed in, or the original audio_path
  # unchanged if there were no cues / no sfx assets available
"""

import os
import re
import subprocess

SFX_DIR = os.getenv("SFX_DIR", "sfx")

# Keyword -> sfx filename. Checked as whole-word matches against the
# narration, case-insensitive. Order doesn't matter; every match in the
# text fires its cue (a story can trigger more than one sound).
SFX_LIBRARY = {
    "boom.mp3":   ["explosion", "explode", "exploded", "blast"],
    "whoosh.mp3": ["drop", "drops", "dropped", "release", "released", "reveal", "revealed"],
    "hit.mp3":    ["fight", "fights", "battle", "clash", "punch"],
    "pop.mp3":    ["announce", "announced", "announcement", "confirm", "confirmed", "official"],
    "glitch.mp3": ["leak", "leaked", "rumor", "rumored"],
    "bass.mp3":   ["finally", "huge", "massive", "epic"],
    "click.mp3":  ["trailer", "teaser", "pv"],
}

# Max simultaneous SFX cues per story — a narration that happens to hit 4+
# trigger words shouldn't drown the voiceover in overlapping sound effects.
MAX_CUES_PER_STORY = 2

# SFX volume relative to the voiceover track.
# Kept low so SFX never competes with the voice.
SFX_VOLUME = 0.35


def _keyword_pattern(keyword):
    return re.compile(rf"\b{re.escape(keyword)}\b", re.IGNORECASE)


def find_cues(narration_text, words):
    """Returns up to MAX_CUES_PER_STORY (sfx_filename, timestamp_seconds)
    tuples. `words` is the same [{"word","start","end"}, ...] list TTS/
    build_segment already produce — timestamp is the start time of the
    FIRST word in the narration that matches a trigger keyword for that
    sound. If no word-timing is available (empty `words`), no cues are
    generated — better to skip the effect than guess a wrong timestamp."""
    if not narration_text or not words:
        return []

    cues = []
    for sfx_file, keywords in SFX_LIBRARY.items():
        for kw in keywords:
            if not _keyword_pattern(kw).search(narration_text):
                continue
            # Find the matching word's timing — first occurrence only.
            for w in words:
                if _keyword_pattern(kw).search(w["word"]):
                    cues.append((sfx_file, w["start"]))
                    break
            break  # one cue per sfx file max, even with multiple keyword hits
        if len(cues) >= MAX_CUES_PER_STORY:
            break
    return cues[:MAX_CUES_PER_STORY]


def mix_cues(audio_path, cues, work_dir):
    """Overlays each cue's sound effect into `audio_path` at its timestamp.
    Returns a new file path if anything was actually mixed in, or the
    original `audio_path` unchanged if there were no cues, SFX_DIR doesn't
    exist, or none of the referenced files were found on disk."""
    if not cues or not os.path.isdir(SFX_DIR):
        return audio_path

    available = [(f, ts) for f, ts in cues if os.path.isfile(os.path.join(SFX_DIR, f))]
    if not available:
        return audio_path

    out_path = os.path.join(work_dir, os.path.basename(audio_path) + ".sfx.mp3")

    inputs = ["-i", audio_path]
    filter_parts = []
    mix_labels = ["[voice]"]

    # Normalize the voiceover to a consistent loudness level first.
    # loudnorm brings every TTS clip to -16 LUFS (standard for social video)
    # so quiet sentences and loud ones all sit at the same perceived volume.
    filter_parts.append("[0:a]loudnorm=I=-16:TP=-1.5:LRA=11[voice]")

    for i, (sfx_file, ts) in enumerate(available, start=1):
        inputs += ["-i", os.path.join(SFX_DIR, sfx_file)]
        delay_ms = max(int(ts * 1000), 0)
        label = f"[sfx{i}]"
        filter_parts.append(
            f"[{i}:a]adelay={delay_ms}|{delay_ms}{label}"
        )
        mix_labels.append(label)

    # weights="1 0.35 0.35..." keeps the voice at full volume while SFX
    # sit at SFX_VOLUME level — compatible with older ffmpeg that doesn't
    # support normalize_inputs. One weight per input: voice first, then
    # one per SFX cue.
    sfx_weights = " ".join(["1"] + [str(SFX_VOLUME)] * len(available))
    filter_complex = ";".join(filter_parts) + ";" + "".join(mix_labels) + \
        f"amix=inputs={len(mix_labels)}:duration=first:dropout_transition=0:weights='{sfx_weights}'[aout]"

    cmd = [
        "ffmpeg", "-y", *inputs,
        "-filter_complex", filter_complex,
        "-map", "[aout]", out_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        # Non-fatal — voiceover already exists and works fine on its own;
        # an SFX mixing failure should never take a story down with it.
        print(f"    NOTE: SFX mix failed, continuing without sound effects: "
              f"{r.stderr[-300:]}")
        return audio_path
    return out_path
