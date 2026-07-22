"""
Mega Captions
--------------
Replaces build_news_digest.py's old multi-word-per-line karaoke captions
with the modern TikTok "mega caption" look: ONE word fills the middle of
the screen at a time, uppercase, with a quick pop-in scale animation, in
the story's emotion accent color (see motion_engine.EMOTION_PROFILE).

Uses the exact same word-level timing list every TTS engine in
build_news_digest.py already produces ({"word","start","end"}) — no new
timing source needed, just a different renderer for it.

USAGE:
  import mega_captions
  ass_text = mega_captions.make_mega_ass(words, accent_color)
"""

OUT_W, OUT_H = 1080, 1920
FONT_SIZE = 150
MIN_WORD_DISPLAY_SECONDS = 0.18  # floor so fast TTS speech doesn't flash unreadably


def _ass_ts(sec):
    cs = int(round((sec % 1) * 100))
    s = int(sec) % 60
    m = (int(sec) // 60) % 60
    h = int(sec) // 3600
    return f"{h:01d}:{m:02d}:{s:02d}.{cs:02d}"


DEFAULT_FONT = "Bangers"  # comic/impact font — see project README for setup;
                          # libass silently substitutes a default font if
                          # this isn't found, so a missing font file never
                          # breaks a render, it just looks plainer.


def _header(font_size, out_w, out_h, font_name, accent_color="&H00FFFFFF"):
    """Generates the ASS header with accent_color baked into the Style
    definition — this is what actually controls the rendered color.
    Relying on inline \\c tags alone is unreliable across libass builds;
    writing the color into the Style row guarantees it applies."""
    return (
        "[Script Info]\n"
        "ScriptType: v4.00+\n"
        f"PlayResX: {out_w}\n"
        f"PlayResY: {out_h}\n"
        "WrapStyle: 0\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        # PrimaryColour = accent_color, OutlineColour = black, thick outline
        # so words stay readable over ANY background (light or dark image).
        f"Style: Default,{font_name},{font_size},{accent_color},&H00FFFFFF,&H00000000,"
        "&H00000000,1,0,0,0,100,100,2,0,1,5,2,5,40,40,40,1\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, "
        "Effect, Text\n"
    )


def make_mega_ass(words, accent_color="&H0000FFFF", font_size=FONT_SIZE,
                   out_w=OUT_W, out_h=OUT_H, font_name=DEFAULT_FONT,
                   position="center"):
    """One Dialogue event per word, a fast scale-up pop-in
    (60% -> 115% -> 100% over ~220ms) so each word visibly lands instead
    of just appearing, in `accent_color` (an ASS &HBBGGRR string — see
    motion_engine.EMOTION_PROFILE for the per-emotion set).

    `position` controls vertical placement:
      "center"  — dead center (\an5), classic mega-caption look
      "lower"   — lower third (\an2), leaves room for the image up top
      "upper"   — upper third (\an8), good when the main subject is low

    Returns "" if `words` is empty (caller should skip burning subtitles
    entirely in that case, same as the old make_ass())."""
    if not words:
        return ""

    # Alignment tag + margin: \an5=center, \an2=bottom-center, \an8=top-center
    _POS = {
        "center": (r"\an5", 40),
        "lower":  (r"\an2", 120),
        "upper":  (r"\an8", 120),
    }
    an_tag, margin_v = _POS.get(position, _POS["center"])

    events = []
    for i, w in enumerate(words):
        start = w["start"]
        is_last = i + 1 >= len(words)
        end = w["end"] if is_last else words[i + 1]["start"]
        # Minimum-duration padding only applies to the LAST word — padding
        # any other word risks pushing its end past the NEXT word's start,
        # which would render two words on screen at once during the
        # overlap. A mid-sequence word just gets however much time it
        # naturally has before the next one starts, even under the floor.
        if is_last and end - start < MIN_WORD_DISPLAY_SECONDS:
            end = start + MIN_WORD_DISPLAY_SECONDS
        if end <= start:
            end = start + 0.05

        text = w["word"].upper()
        # Pop-in: start at 60% scale, overshoot to 115%, settle to 100%.
        # Color is baked into the Style row (see _header) — no \c tag
        # needed here. \shad3 keeps words readable over bright images.
        tag = (
            rf"{{{an_tag}\fscx60\fscy60\shad3"
            rf"\t(0,90,\fscx115\fscy115)\t(90,220,\fscx100\fscy100)}}"
        )
        events.append(
            f"Dialogue: 0,{_ass_ts(start)},{_ass_ts(end)},Default,,0,0,{margin_v},,{tag}{text}"
        )

    return _header(font_size, out_w, out_h, font_name, accent_color) + "\n".join(events) + "\n"
