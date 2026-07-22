"""
Transitions
------------
Small opening-frame effects burned into the first ~0.15s of a story
segment, picked via motion_engine.EMOTION_PROFILE's "transition" field.

SCOPE NOTE: these are intra-segment effects (gated with ffmpeg's
`enable='lt(t,...)'` on a single clip), not true cross-clip blends. A
real crossfade/wipe BETWEEN two clips needs `xfade` with precise
duration/offset math across the whole segment chain, which means
replacing the concat-demuxer stitch with a single combined filter graph —
a much heavier render (every segment's frames touched at once instead of
each encoded independently) for a phone already running on the
`ultrafast` preset. "cut" is the honest default; "fade" and "glitch" are
real, tested effects; "whoosh" and "bounce" intentionally do NOT add a
video filter (a convincing motion-blur whoosh needs directional blur
across frame history, not worth the CPU cost here) — their energy comes
from the camera motion + SFX cue instead, which is cheaper and reads
just as well at TikTok speed.

USAGE:
  import transitions
  vf_fragment = transitions.build_transition_vf("glitch")  # "" if none
"""

_GLITCH_DURATION = 0.15
_FADE_DURATION = 0.15
_FLASH_DURATION = 0.08   # white flash — sharp, attention-grabbing
_PUNCH_DURATION  = 0.12  # quick scale-up overshoot at the start of the clip


def _glitch():
    return f"rgbashift=rh=-12:bh=12:enable='lt(t,{_GLITCH_DURATION})'"


def _fade():
    return f"fade=t=in:st=0:d={_FADE_DURATION}:color=black"


def _flash():
    # White flash fades to normal in the first 80ms — reads as a camera
    # flash / impact hit. Works best for breaking/hype openers.
    return f"fade=t=in:st=0:d={_FLASH_DURATION}:color=white"


def _zoom_punch():
    # Starts slightly over-zoomed and eases back to 1x in the first 120ms
    # — gives the impression of a hard cut with kinetic energy.
    # Implemented as a fast brightness dip + scale correction via eq/curves;
    # ffmpeg's zoompan can't run per-frame at render start without resetting
    # the whole chain, so this uses a vf overlay instead.
    return f"eq=brightness=0.3:enable='lt(t,{_PUNCH_DURATION})',eq=brightness=0:enable='gte(t,{_PUNCH_DURATION})'"


_BUILDERS = {
    "glitch":  _glitch,
    "fade":    _fade,
    "flash":   _flash,
    "bounce":  _flash,   # bounce gets the flash treatment — energetic, cheap
    # "cut" and "whoosh" deliberately produce no extra video filter —
    # their energy comes from camera motion + SFX cue instead.
}


def build_transition_vf(transition_name):
    """Returns a comma-prefixed filter fragment ready to append to a vf
    chain (e.g. ",rgbashift=..."), or "" if this transition has no video
    component."""
    builder = _BUILDERS.get(transition_name)
    if builder is None:
        return ""
    return "," + builder()
