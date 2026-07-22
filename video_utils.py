"""
Video Utils
-----------
Small shared ffmpeg/ffprobe helpers used by generate_thumbnails.py and the
publish scripts. Kept separate from the per-stage pipeline scripts so those
scripts (already tested, working) don't need to change their imports.

SETUP:
  ffmpeg + ffprobe on PATH
  pip install numpy
"""

import os
import subprocess
from datetime import datetime, timezone
import numpy as np

# Same convention as every other ffmpeg failure across this pipeline —
# full stderr survives even after failed-build cleanup deletes the actual
# image/video. normalize_image() previously discarded ffmpeg's real error
# and only returned True/False, which made a systemic failure (e.g. an
# entire feed's images failing for the same underlying codec reason)
# indistinguishable from random one-off corruption at the call site. Now
# every normalize_image() failure appends full diagnostic detail here.
FFMPEG_ERROR_LOG = os.getenv("FFMPEG_ERROR_LOG", "ffmpeg_errors.log")


def _log_normalize_failure(path, ffmpeg_result):
    """Appends full diagnostic detail for a normalize_image() failure:
    ffmpeg's actual stderr (not just a bool), plus a raw ffprobe stream
    dump of the file (codec_name, format) so a systemic issue — e.g. an
    entire CDN serving a format this ffmpeg build can't decode (AVIF/WebP
    without libaom/libwebp) — is diagnosable from the log alone, instead
    of needing to reproduce it by hand."""
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "stream=codec_name,codec_long_name,width,height,pix_fmt",
             "-of", "default=noprint_wrappers=0", path],
            capture_output=True, text=True, timeout=15,
        )
        probe_info = (probe.stdout or "").strip() or (probe.stderr or "").strip()
    except Exception as e:
        probe_info = f"(ffprobe diagnostic call itself failed: {e})"

    entry = (
        f"\n[{datetime.now(timezone.utc).isoformat()}] normalize_image FAILED\n"
        f"  path: {path}\n"
        f"  ffmpeg exit code: {ffmpeg_result.returncode}\n"
        f"  ffmpeg stderr (tail):\n{ffmpeg_result.stderr[-1500:]}\n"
        f"  ffprobe stream info:\n{probe_info}\n"
    )
    try:
        with open(FFMPEG_ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(entry)
    except OSError:
        pass  # logging must never be why a caller crashes


def get_duration(path):
    """Duration in seconds for any audio or video file, via ffprobe."""
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", path],
        capture_output=True, text=True,
    )
    if r.returncode != 0 or not r.stdout.strip():
        raise RuntimeError(f"ffprobe failed on {path}: {r.stderr[-400:]}")
    return float(r.stdout.strip())


def get_video_dimensions(path):
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=width,height",
         "-of", "csv=s=x:p=0", path],
        capture_output=True, text=True,
    )
    if r.returncode != 0 or not r.stdout.strip():
        raise RuntimeError(f"ffprobe failed on {path}: {r.stderr[-400:]}")
    w, h = r.stdout.strip().split("x")
    return int(w), int(h)


def extract_frame(video_path, timestamp, out_path):
    """Grab a single JPEG frame at `timestamp` seconds."""
    r = subprocess.run([
        "ffmpeg", "-y", "-ss", str(timestamp), "-i", video_path,
        "-frames:v", "1", "-q:v", "2", out_path,
    ], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"Frame extraction failed on {video_path}@{timestamp}s: {r.stderr[-400:]}")


def normalize_image(path):
    """Re-encodes any downloaded image IN PLACE into a clean, standard
    baseline JPEG via ffmpeg.

    WHY THIS EXISTS: ffprobe-only validation (rss_news_pull.py's and
    fetch_anilist_trending.py's is_valid_image()) confirms a file has a
    readable width/height — it does NOT confirm ffmpeg's image2 demuxer
    (or the relevant decoder) will actually decode the file later when
    build_news_digest.py tries to -loop it as a still-image background.
    ffprobe can often read width/height straight out of a container's
    header metadata WITHOUT fully decoding a frame — so a file in a
    format this ffmpeg build has no decoder for (seen in production:
    AVIF/WebP thumbnails from some CDNs, when the local ffmpeg build
    lacks libaom/libwebp) can pass ffprobe's check and then fail here,
    at the point an actual decode is required.

    Re-encoding immediately after download guarantees every image that
    makes it into the pipeline is a boring, universally-compatible
    baseline JPEG (yuvj420p, single frame) by the time anything else
    touches it — closing off this whole class of bug at the source
    instead of chasing individual downstream failure modes.

    DIAGNOSTICS: a failure here now logs full ffmpeg stderr + an ffprobe
    codec dump to FFMPEG_ERROR_LOG (default ffmpeg_errors.log) via
    _log_normalize_failure(), instead of only returning False. A 100%
    failure rate concentrated on one source (e.g. every image from one
    feed) is the signature of a missing codec, not random corruption —
    check that log for the codec_name it's actually hitting.

    Returns True on success (file at `path` is now a clean JPEG). Returns
    False if ffmpeg couldn't re-encode it at all — callers should treat
    that exactly like a failed is_valid_image() check (discard the post);
    a file ffmpeg itself can't re-encode was never going to survive
    further down the pipeline either."""
    tmp_path = path + ".normalize_tmp.jpg"
    r = subprocess.run([
        "ffmpeg", "-y", "-i", path,
        "-frames:v", "1",
        "-vf", "format=yuvj420p",
        "-q:v", "3",
        tmp_path,
    ], capture_output=True, text=True)
    if r.returncode != 0 or not os.path.isfile(tmp_path) or os.path.getsize(tmp_path) == 0:
        _log_normalize_failure(path, r)
        if os.path.isfile(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        return False
    os.replace(tmp_path, path)
    return True


def _laplacian_variance(gray_2d_array):
    """Shared math: variance-of-Laplacian 'how much detail is here' score
    for a 2D grayscale numpy array. Higher = more edges/texture, lower =
    flatter/emptier (a gradient background, a logo card, empty sky).
    Factored out of laplacian_sharpness() so find_horizontal_focus_x()
    below can reuse the exact same scoring on a cropped region instead of
    a whole frame — same signal, different question ('how sharp is this
    frame' vs 'which part of this image has the actual content')."""
    size = gray_2d_array.shape[0]
    kernel = np.array([[0, 1, 0], [1, -4, 1], [0, 1, 0]], dtype=np.float64)
    padded = np.pad(gray_2d_array, 1, mode="edge")
    conv = np.zeros_like(gray_2d_array)
    for i in range(3):
        for j in range(3):
            if kernel[i, j] == 0:
                continue
            conv += kernel[i, j] * padded[i:i + size, j:j + size]
    return float(conv.var())


def laplacian_sharpness(image_path, size=256):
    """Variance-of-Laplacian sharpness score for a JPEG. Higher = sharper /
    more in-focus. Implemented with ffmpeg (decode + grayscale + downscale
    to a fixed size for consistent scoring) + numpy (Laplacian convolution),
    no OpenCV dependency needed on Termux."""
    raw_path = image_path + ".graybuf"
    try:
        r = subprocess.run([
            "ffmpeg", "-y", "-i", image_path,
            "-vf", f"scale={size}:{size}:flags=lanczos,format=gray",
            "-f", "rawvideo", raw_path,
        ], capture_output=True, text=True)
        if r.returncode != 0 or not os.path.exists(raw_path):
            return 0.0

        with open(raw_path, "rb") as f:
            buf = f.read()
        if len(buf) != size * size:
            return 0.0

        img = np.frombuffer(buf, dtype=np.uint8).reshape(size, size).astype(np.float64)
        return _laplacian_variance(img)
    except Exception:
        return 0.0
    finally:
        if os.path.exists(raw_path):
            os.remove(raw_path)


def find_horizontal_focus_x(image_path, out_w, out_h, num_samples=10, sample_size=128):
    """For a WIDE image that's about to be panned across, finds WHERE along
    the scrollable width the actual content is, so the pan can be built
    around it instead of blindly sweeping the whole image on a fixed timer
    (the 'camera spends half the clip crawling across empty background'
    bug — a promotional banner is very often mostly flat background/
    gradient with the character or art crammed into one region).

    Samples `num_samples` candidate crop windows (each OUT_W wide, spanning
    the same scaled height the pan actually renders at) evenly across the
    scrollable range, scores each with the same variance-of-Laplacian
    'how much detail is here' technique thumbnail selection already uses
    (see laplacian_sharpness/_laplacian_variance above — same signal,
    applied sideways across one image instead of across candidate video
    frames), and returns the position of the busiest one.

    Returns a FRACTION from 0.0 (focus is at the left edge of the
    scrollable range) to 1.0 (right edge) — this is exactly the unit
    build_wide_pan_segment's crop_x formulas already work in, so callers
    can plug it straight in. Returns 0.5 (center — a safe, neutral
    default) on ANY failure: a bad focus guess just means the pan behaves
    like the old center-ish sweep, never a crash or a blocked render."""
    try:
        w0, h0 = get_video_dimensions(image_path)
    except Exception:
        return 0.5
    if h0 <= 0 or w0 <= 0:
        return 0.5

    scaled_w = int(round(w0 * (out_h / h0) / 2) * 2)  # matches ffmpeg's scale=-2:out_h rounding
    max_offset = scaled_w - out_w
    if max_offset <= 0:
        return 0.5  # not actually wider than the output once scaled — nothing to scan

    num_samples = max(2, num_samples)
    offsets = [round(max_offset * i / (num_samples - 1)) for i in range(num_samples)]

    best_offset, best_score = offsets[0], -1.0
    for offset in offsets:
        raw_path = image_path + f".focusbuf_{offset}"
        try:
            r = subprocess.run([
                "ffmpeg", "-y", "-i", image_path,
                "-vf", (f"scale=-2:{out_h},crop={out_w}:{out_h}:{offset}:0,"
                        f"scale={sample_size}:{sample_size}:flags=lanczos,format=gray"),
                "-f", "rawvideo", raw_path,
            ], capture_output=True, text=True, timeout=15)
            if r.returncode != 0 or not os.path.exists(raw_path):
                continue
            with open(raw_path, "rb") as f:
                buf = f.read()
            if len(buf) != sample_size * sample_size:
                continue
            gray = np.frombuffer(buf, dtype=np.uint8).reshape(
                sample_size, sample_size).astype(np.float64)
            score = _laplacian_variance(gray)
            if score > best_score:
                best_score, best_offset = score, offset
        except Exception:
            continue
        finally:
            if os.path.exists(raw_path):
                os.remove(raw_path)

    if best_score < 0:
        return 0.5  # every sample failed — fall back to center rather than guess

    return best_offset / max_offset


def _sample_candidates(video_path, num_candidates, work_dir):
    """Internal: samples `num_candidates` frames spread through video_path
    and scores each for sharpness. Returns a list of
    (frame_path, timestamp_seconds, score) for every frame that was
    successfully extracted, sorted sharpest-first. Ties keep the earlier
    (lower-index/earlier-timestamp) candidate first, matching the tie-
    breaking pick_sharpest_frame has always used. Does NOT delete
    anything — caller owns cleanup of every path returned."""
    work_dir = work_dir or os.path.dirname(video_path) or "."
    duration = get_duration(video_path)

    scored = []
    for i in range(num_candidates):
        ts = duration * (i + 1) / (num_candidates + 1)
        fp = os.path.join(work_dir, f"_thumb_candidate_{i}.jpg")
        try:
            extract_frame(video_path, ts, fp)
        except RuntimeError:
            continue
        scored.append((fp, ts, laplacian_sharpness(fp)))

    scored.sort(key=lambda c: c[2], reverse=True)
    return scored


def pick_sharpest_frame(video_path, num_candidates=6, work_dir=None):
    """Samples `num_candidates` frames spread through the video, scores each
    for sharpness, and returns (best_frame_path, best_timestamp_seconds,
    best_score) — or (None, 0.0, -1.0) if no frame could be extracted.
    Caller owns cleanup of the returned frame file; every other candidate
    is deleted before this returns."""
    scored = _sample_candidates(video_path, num_candidates, work_dir)
    if not scored:
        return None, 0.0, -1.0

    best_path, best_ts, best_score = scored[0]
    for fp, _, _ in scored[1:]:
        if os.path.exists(fp):
            os.remove(fp)
    return best_path, best_ts, best_score


def pick_candidate_frames(video_path, num_candidates=6, work_dir=None):
    """Like pick_sharpest_frame(), but returns EVERY successfully-extracted
    candidate instead of auto-picking the sharpest and deleting the rest:
    a list of (frame_path, timestamp_seconds, score) tuples, sharpest
    first. Caller owns cleanup of ALL returned frame files.

    Use this instead of pick_sharpest_frame() when a caller needs to
    inspect more than just sharpness before deciding which candidate to
    keep — e.g. generate_thumbnails.py classifying candidates by visual
    pattern to bias selection toward whatever's been performing best,
    rather than picking purely by sharpness."""
    return _sample_candidates(video_path, num_candidates, work_dir)


def run_ffmpeg(cmd_args, error_prefix="ffmpeg failed"):
    """Thin wrapper so publish/thumbnail scripts get consistent error text."""
    r = subprocess.run(["ffmpeg", "-y"] + cmd_args, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{error_prefix}: {r.stderr[-1000:]}")


def ffmpeg_filter_path(path):
    """Returns `path` safe to embed in an ffmpeg FILTER option, e.g.
        f"subtitles={ffmpeg_filter_path(ass)}:fontsdir={ffmpeg_filter_path(dir)}"
    Forward slashes, ':' escaped (so 'C:/x' isn't parsed as option 'C'),
    whole value single-quoted (spaces/commas/brackets safe), embedded
    apostrophes escaped across ffmpeg's two parsing levels. It already
    includes the quotes - don't add your own around it. Callers pass an
    absolute path (os.path.abspath) when the filter's cwd isn't guaranteed."""
    p = os.fspath(path).replace("\\", "/")
    p = p.replace("'", "'\\\\\\''")
    p = p.replace(":", "\\:")
    return "'" + p + "'"
