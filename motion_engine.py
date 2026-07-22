"""
Motion Engine
--------------
Builds ffmpeg zoompan filter expressions for a small library of camera
motions, and maps a narration EMOTION to the motions/transition/subtitle
color/music mood that suit it — so camera movement matches the news
instead of being random, and instead of every image getting the exact
same slow zoom-in build_news_digest.py used to hard-code.

DESIGN — every motion is expressed in terms of zoompan's `on` (output
frame number, 0..d-1), not the accumulating `zoom+=step` pattern the old
code used. `zoom+0.0015` per frame has no natural ceiling: it drifts
further on a long narration than a short one, so a 20s story ends up
badly over-cropped while an 8s story barely moves. Writing start/end
explicitly as a function of `on` gives a fixed, predictable animation
regardless of how long any given story's narration runs, capped at
MAX_ZOOM.

USAGE:
  import motion_engine as me
  profile = me.profile_for(emotion)              # transition/color/music mood
  motion = me.pick_motion(emotion, previous_motion)
  vf = me.build_motion_vf(motion, frames, out_w, out_h)
"""

import random

MAX_ZOOM = 1.35
# Pan/orbit/parallax motions overscan the frame by this much so panning
# never reveals the canvas edge (which would show as a hard black border).
PAN_OVERSCAN_ZOOM = 1.18


def zoom_in(frames, w, h):
    z = f"min({MAX_ZOOM},1+0.35*on/{frames})"
    return (f"zoompan=z='{z}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
            f"d={frames}:s={w}x{h}")


def zoom_out(frames, w, h):
    z = f"max(1,{MAX_ZOOM}-0.35*on/{frames})"
    return (f"zoompan=z='{z}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
            f"d={frames}:s={w}x{h}")


def pan_left(frames, w, h):
    x = f"(iw-iw/zoom)*(1-on/{frames})"
    return (f"zoompan=z='{PAN_OVERSCAN_ZOOM}':x='{x}':y='ih/2-(ih/zoom/2)':"
            f"d={frames}:s={w}x{h}")


def pan_right(frames, w, h):
    x = f"(iw-iw/zoom)*(on/{frames})"
    return (f"zoompan=z='{PAN_OVERSCAN_ZOOM}':x='{x}':y='ih/2-(ih/zoom/2)':"
            f"d={frames}:s={w}x{h}")


def pan_up(frames, w, h):
    y = f"(ih-ih/zoom)*(1-on/{frames})"
    return (f"zoompan=z='{PAN_OVERSCAN_ZOOM}':x='iw/2-(iw/zoom/2)':y='{y}':"
            f"d={frames}:s={w}x{h}")


def pan_down(frames, w, h):
    y = f"(ih-ih/zoom)*(on/{frames})"
    return (f"zoompan=z='{PAN_OVERSCAN_ZOOM}':x='iw/2-(iw/zoom/2)':y='{y}':"
            f"d={frames}:s={w}x{h}")


def dolly_push(frames, w, h):
    # A faster, eased-in zoom_in — reads as an aggressive push rather than
    # a slow drift. For trailers/fights/high-energy stories.
    z = f"min({MAX_ZOOM},1+0.5*pow(on/{frames}\\,1.4))"
    return (f"zoompan=z='{z}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
            f"d={frames}:s={w}x{h}")


def shake(frames, w, h):
    # Fixed small zoom for headroom; x/y jitter on a fast sine/cosine —
    # reads as impact/tension. For fight scenes and breaking news.
    x = "iw/2-(iw/zoom/2)+10*sin(on/2)"
    y = "ih/2-(ih/zoom/2)+7*cos(on/1.7)"
    return f"zoompan=z='1.1':x='{x}':y='{y}':d={frames}:s={w}x{h}"


def orbit(frames, w, h):
    # Fakes a slow circular drift around the frame center via an
    # elliptical x/y path plus a gentle breathing zoom — no true 3D,
    # just enough motion to not read as a static/dead frame.
    z = f"1.15+0.05*sin(2*3.14159*on/{frames})"
    x = f"iw/2-(iw/zoom/2)+18*cos(2*3.14159*on/{frames})"
    y = f"ih/2-(ih/zoom/2)+18*sin(2*3.14159*on/{frames})"
    return f"zoompan=z='{z}':x='{x}':y='{y}':d={frames}:s={w}x{h}"


def parallax(frames, w, h):
    # APPROXIMATION: real parallax needs separated foreground/background
    # layers, which a single flat source image doesn't have. This fakes
    # depth with a slow pan plus a slower counter-zoom — closer to a
    # "2.5D" drift than true parallax, but reads fine at TikTok viewing
    # speed/distance.
    z = f"1.1+0.08*on/{frames}"
    x = f"(iw-iw/zoom)*(on/{frames})*0.6"
    return f"zoompan=z='{z}':x='{x}':y='ih/2-(ih/zoom/2)':d={frames}:s={w}x{h}"


def drift_up(frames, w, h):
    # Slow upward drift with a gentle zoom — reads as reverent/cinematic.
    # Good for sad or serious stories; different from pan_up's faster sweep.
    z = f"1.08+0.07*on/{frames}"
    y = f"(ih-ih/zoom)*(1-on/{frames})*0.5"
    return f"zoompan=z='{z}':x='iw/2-(iw/zoom/2)':y='{y}':d={frames}:s={w}x{h}"


def whip_pan(frames, w, h):
    # Fast snap pan from left to right — eased in, hits hard in the
    # middle, eases out. Reads as high-energy/dynamic. Good for breaking
    # and hype stories. Uses a sine-curve ease so it accelerates and
    # decelerates instead of moving at a constant rate.
    z = f"{PAN_OVERSCAN_ZOOM}"
    x = f"(iw-iw/zoom)*0.5*(1-cos(3.14159*on/{frames}))"
    return f"zoompan=z='{z}':x='{x}':y='ih/2-(ih/zoom/2)':d={frames}:s={w}x{h}"


def breathe(frames, w, h):
    # Subtle pulsing zoom — in and out like breathing. Reads as tense or
    # mysterious. Low visual noise, works well under heavy subtitle content.
    z = f"1.08+0.07*sin(2*3.14159*on/{frames})"
    return (f"zoompan=z='{z}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
            f"d={frames}:s={w}x{h}")


MOTIONS = {
    "zoom_in": zoom_in,
    "zoom_out": zoom_out,
    "pan_left": pan_left,
    "pan_right": pan_right,
    "pan_up": pan_up,
    "pan_down": pan_down,
    "dolly_push": dolly_push,
    "shake": shake,
    "orbit": orbit,
    "parallax": parallax,
    "drift_up": drift_up,
    "whip_pan": whip_pan,
    "breathe": breathe,
}


def build_motion_vf(motion, frames, w, h):
    """Returns the zoompan filter fragment for `motion`. Falls back to
    zoom_in on an unrecognized name so a typo'd/future motion string never
    breaks a render."""
    fn = MOTIONS.get(motion, zoom_in)
    return fn(max(int(frames), 1), w, h)


# ── Emotion -> creative profile ─────────────────────────────────────────────
# Each emotion lists 2-3 CANDIDATE motions (not one fixed motion), so
# consecutive stories sharing a mood still get variety — pick_motion()
# below also refuses to repeat the immediately previous story's motion.

EMOTIONS = {"excited", "sad", "serious", "funny", "breaking", "hype", "mystery"}

EMOTION_PROFILE = {
    "breaking": {"motions": ["shake", "dolly_push", "whip_pan"],              "transition": "glitch",  "color": "&H000080FF", "music_mood": "breaking"},
    "hype":     {"motions": ["dolly_push", "whip_pan", "orbit", "zoom_in"],   "transition": "whoosh",  "color": "&H0000D7FF", "music_mood": "hype"},
    "excited":  {"motions": ["zoom_in", "dolly_push", "pan_right", "orbit"],  "transition": "whoosh",  "color": "&H0000FFFF", "music_mood": "hype"},
    "serious":  {"motions": ["zoom_in", "pan_left", "breathe", "drift_up"],   "transition": "cut",     "color": "&H00FFFFFF", "music_mood": "dramatic"},
    "sad":      {"motions": ["zoom_out", "pan_down", "drift_up", "breathe"],  "transition": "fade",    "color": "&H00D8D8D8", "music_mood": "sad"},
    "funny":    {"motions": ["shake", "orbit", "pan_right", "whip_pan"],      "transition": "bounce",  "color": "&H0000FFFF", "music_mood": "funny"},
    "mystery":  {"motions": ["parallax", "pan_up", "breathe", "drift_up"],    "transition": "glitch",  "color": "&H00C864C8", "music_mood": "dramatic"},
}

DEFAULT_PROFILE = EMOTION_PROFILE["serious"]


def profile_for(emotion):
    return EMOTION_PROFILE.get(emotion, DEFAULT_PROFILE)


def pick_motion(emotion, previous_motion=None):
    """Chooses one motion from the emotion's candidate list, avoiding
    `previous_motion` when another option exists — this is what stops two
    consecutive stories (even sharing a mood) from feeling identical."""
    candidates = profile_for(emotion)["motions"]
    options = [m for m in candidates if m != previous_motion] or candidates
    return random.choice(options)
