#!/usr/bin/env python3
"""
AniFeed Studio — environment self-check (built for the Windows move, works anywhere).

  python windows_selfcheck.py            # run from the project folder

Prints PASS / WARN / FAIL lines and exits 1 if anything FAILs. Never prints
secret VALUES, only whether keys are present.

The important part: it actually RENDERS the three subtitle command shapes
the pipeline uses (burn_subtitles.py, build_news_digest.py,
add_hook_overlay.py) onto a black frame and checks a glyph really appeared
(max pixel of a raw gray frame). A command that "succeeds" but draws
nothing (missing Arial, fontconfig can't see system fonts) shows up here.
"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(errors="replace")
    except Exception:
        pass

RESULTS = []


def rec(level, name, detail=""):
    RESULTS.append(level)
    print(f"[{level:4s}] {name}" + (f" - {detail}" if detail else ""))


def PASS(n, d=""): rec("PASS", n, d)
def WARN(n, d=""): rec("WARN", n, d)
def FAIL(n, d=""): rec("FAIL", n, d)
def INFO(n, d=""): print(f"[INFO] {n}" + (f" - {d}" if d else ""))


def section(t):
    print(f"\n== {t} ==")


# ── ffmpeg_filter_path: use the project's if installed, else reference copy ─
def _ref_filter_path(path):
    p = os.fspath(path).replace("\\", "/")
    p = p.replace("'", "'\\\\\\''")
    p = p.replace(":", "\\:")
    return "'" + p + "'"


try:
    from video_utils import ffmpeg_filter_path
    _FP_SOURCE = "video_utils.ffmpeg_filter_path"
except Exception:
    ffmpeg_filter_path = _ref_filter_path
    _FP_SOURCE = "built-in reference copy (video_utils.py not patched yet)"


def run(cmd, timeout=60, binary=False):
    return subprocess.run(cmd, capture_output=True, timeout=timeout,
                          text=not binary, errors=None if binary else "replace")


# ── 1. Python / encoding ─────────────────────────────────────────────────
def check_python():
    section("Python & encoding")
    v = sys.version_info
    (PASS if v >= (3, 9) else FAIL)("Python version", sys.version.split()[0] +
                                    ("" if v >= (3, 9) else " (need 3.9+ for zoneinfo)"))
    utf8 = bool(sys.flags.utf8_mode) or os.environ.get("PYTHONUTF8") == "1"
    if utf8:
        PASS("UTF-8 mode", "on")
    else:
        WARN("UTF-8 mode off", "scripts printing arrows/check marks can crash when piped. "
             "Run: setx PYTHONUTF8 1 (then open a NEW terminal). run_all.py sets it for children anyway.")
    INFO("stdout encoding", str(sys.stdout.encoding))
    try:
        from zoneinfo import ZoneInfo
        ZoneInfo("America/Chicago")
        PASS("timezone database (zoneinfo)")
    except Exception as e:
        FAIL("timezone database missing",
             f"{type(e).__name__}. Windows has none: pip install tzdata. Without it "
             f"audience_scheduling.py silently returns no audience hour.")
    free_gb = shutil.disk_usage(ROOT).free / 1e9
    (PASS if free_gb > 20 else WARN)("free disk space", f"{free_gb:.0f} GB")


# ── 2. ffmpeg ────────────────────────────────────────────────────────────
def check_ffmpeg():
    section("ffmpeg")
    ok = True
    for tool in ("ffmpeg", "ffprobe"):
        path = shutil.which(tool)
        if not path:
            FAIL(f"{tool} not on PATH", "winget install Gyan.FFmpeg, then open a NEW terminal")
            ok = False
        else:
            ver = run([tool, "-version"]).stdout.splitlines()[0]
            PASS(tool, ver)
    if not ok:
        return False
    enc = run(["ffmpeg", "-hide_banner", "-encoders"]).stdout
    (PASS if "libx264" in enc else FAIL)("libx264 encoder")
    flt = run(["ffmpeg", "-hide_banner", "-filters"]).stdout
    for f, why in (("subtitles", "libass: burned subtitles/hook"), ("rgbashift", "digest glitch transition"),
                   ("zoompan", "digest camera motion"), ("gblur", "blur backgrounds"),
                   ("silencedetect", "quality gate"), ("blackdetect", "quality gate")):
        has = any(line.split()[1:2] == [f] for line in flt.splitlines())
        (PASS if has else FAIL)(f"filter {f}", why)
    return True


# ── 3. subtitle render tests ─────────────────────────────────────────────
ASS = """[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,{font},90,&H00FFFFFF,&H00FFFFFF,&H00000000,&H00000000,1,0,0,0,100,100,0,0,1,3,0,5,40,40,40,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
Dialogue: 0,0:00:00.00,0:00:02.00,Default,,0,0,0,,{{\\c&H0000FFFF}}HELLO{{\\r}} WORLD
"""


def render_glyph_test(vf):
    """Returns (ok, detail). Renders 1 frame of black with `vf`, reads raw gray."""
    r = run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=black:s=1080x1920:d=2",
             "-vf", vf, "-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "gray", "-"],
            timeout=90, binary=True)
    if r.returncode != 0 or not r.stdout:
        return False, "ffmpeg error: " + (r.stderr or b"").decode("utf-8", "replace").strip()[:200]
    data = r.stdout
    if len(data) != 1080 * 1920:
        return False, f"unexpected frame size {len(data)}"
    bright = sum(1 for b in data[::7] if b > 128)   # sampled
    peak = max(data)
    if peak < 200 or bright < 50:
        return False, f"ffmpeg ran but drew no text (max pixel {peak}) - font not found?"
    return True, f"glyphs drawn (max pixel {peak})"


def check_subtitles():
    section("Subtitle render tests (the 3 command shapes)")
    bangers = (ROOT / "Bangers.ttf").is_file()
    hook_style = ("PlayResX=1080,PlayResY=1920,FontName=Arial,FontSize=62,"
                  "PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,"
                  "Outline=3,Shadow=0,Bold=1,Spacing=1,Alignment=8,MarginV=180")
    burn_style = ("PlayResX=1080,PlayResY=1920,FontName=Arial,FontSize=44,"
                  "PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,"
                  "Outline=2,Shadow=0,Bold=1,Alignment=2,MarginV=260")
    INFO("path helper", _FP_SOURCE)

    tmp = Path(tempfile.mkdtemp(prefix="_selfcheck_", dir=ROOT))
    try:
        rel_ass = tmp / "burn.ass"
        rel_ass.write_text(ASS.format(font="Arial"), encoding="utf-8")
        abs_ass = tmp / "digest.ass"
        abs_ass.write_text(ASS.format(font="Bangers" if bangers else "Arial"), encoding="utf-8")
        hook_ass = tmp / "hook.ass"
        hook_ass.write_text(ASS.format(font="Arial"), encoding="utf-8")

        # A: burn_subtitles.py — RELATIVE path, no fontsdir, FontName=Arial
        rel = os.path.relpath(rel_ass, ROOT).replace("\\", "/")
        okA, dA = render_glyph_test(f"subtitles={rel}:force_style='{burn_style}'")
        (PASS if okA else FAIL)("A burn_subtitles shape (relative path, Arial, no fontsdir)", dA)
        if not okA:
            win_fonts = os.path.join(os.environ.get("WINDIR", r"C:\Windows"), "Fonts")
            if os.path.isdir(win_fonts):
                ok2, d2 = render_glyph_test(
                    f"subtitles={rel}:fontsdir={ffmpeg_filter_path(win_fonts)}:force_style='{burn_style}'")
                (PASS if ok2 else FAIL)("A' same + fontsdir=C:/Windows/Fonts",
                                        d2 + (" -> FIX: add fontsdir to burn_subtitles.py" if ok2 else ""))

        # B: build_news_digest.py — ABSOLUTE path via helper + fontsdir=project root
        okB, dB = render_glyph_test(
            f"subtitles={ffmpeg_filter_path(abs_ass.resolve())}:fontsdir={ffmpeg_filter_path(ROOT)}")
        (PASS if okB else FAIL)("B digest shape (absolute path, fontsdir=project root"
                                + (", Bangers)" if bangers else ", Arial - Bangers.ttf absent)"), dB)

        # C: add_hook_overlay.py — ABSOLUTE path + optional emoji fontsdir + force_style
        emoji = ROOT / "NotoColorEmoji.ttf"
        fd = f":fontsdir={ffmpeg_filter_path(ROOT)}" if emoji.is_file() else ""
        okC, dC = render_glyph_test(
            f"subtitles={ffmpeg_filter_path(hook_ass.resolve())}{fd}:force_style='{hook_style}'")
        (PASS if okC else FAIL)("C hook overlay shape (absolute path, force_style)", dC)

        # D: unfixed absolute path — proves WHY the helper exists (expected to fail on Windows)
        raw = str(hook_ass.resolve()).replace("\\", "/")
        if ":" in raw:
            okD, _ = render_glyph_test(f"subtitles={raw}:force_style='{hook_style}'")
            INFO("D raw absolute path without helper", "works here" if okD else
                 "BREAKS, as expected on Windows (drive-letter colon)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ── 4. database ──────────────────────────────────────────────────────────
def check_db():
    section("Database")
    db_path = Path(os.getenv("PIPELINE_DB_PATH", "pipeline.db"))
    if not db_path.is_file():
        WARN("pipeline.db not found", f"{db_path.resolve()} (expected until you copy it from the phone)")
        return
    for ext in ("-wal", "-shm"):
        side = Path(str(db_path) + ext)
        if side.exists():
            INFO(f"{side.name} present", f"{side.stat().st_size} bytes "
                 f"(fine if you copied it WITH the db; otherwise checkpoint on the phone first)")
    try:
        conn = sqlite3.connect(str(db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        qc = conn.execute("PRAGMA quick_check").fetchone()[0]
        (PASS if qc == "ok" else FAIL)("sqlite quick_check", qc)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        need = {"posts", "digests", "publish_log", "metrics", "learned_config", "canonical_shows"}
        missing = need - tables
        (PASS if not missing else WARN)("core tables", "all present" if not missing else f"missing {sorted(missing)}")
        if "posts" in tables:
            n = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
            PASS("posts table", f"{n} rows")
            rows = conn.execute("SELECT raw_media_path FROM posts WHERE status='downloaded' "
                                "AND raw_media_path IS NOT NULL").fetchall()
            miss = sum(1 for r in rows if not os.path.isfile(r[0]))
            if not rows:
                INFO("downloaded candidates", "none")
            elif miss:
                WARN("candidate files missing on disk", f"{miss} of {len(rows)} 'downloaded' posts point at "
                     f"missing files - did downloads/ copy fully? Do NOT run clear_missing.py until you've checked.")
            else:
                PASS("candidate files present", f"{len(rows)} 'downloaded' posts all resolve")
        conn.close()
    except Exception as e:
        FAIL("database open", f"{type(e).__name__}: {e}")


# ── 5. .env / tokens ─────────────────────────────────────────────────────
def parse_env(path):
    out = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip().removeprefix("export ").strip()] = v.strip().strip("'\"")
    return out


def check_env():
    section(".env & credentials (values never printed)")
    envp = ROOT / ".env"
    if not envp.is_file():
        FAIL(".env missing", "copy it from the phone project folder")
        env = {}
    else:
        try:
            env = parse_env(envp)
            PASS(".env readable", f"{len(env)} keys")
        except Exception as e:
            FAIL(".env unreadable", str(e))
            env = {}
    env = {**env, **{k: v for k, v in os.environ.items() if k in env or k.endswith(("_KEY", "_ID", "_SECRET"))}}

    def have(k): return bool(env.get(k))
    for k, why in (("GROQ_API_KEY", "subtitles/ID/captions-digest"), ("GEMINI_API_KEY", "captions + digest planner")):
        (PASS if have(k) else FAIL)(k, why)
    for k, why in (("REDDIT_CLIENT_ID", "reddit_pull"), ("REDDIT_CLIENT_SECRET", "reddit_pull"),
                   ("YOUTUBE_TRAILER_CHANNELS", "trailer source")):
        (PASS if have(k) else WARN)(k, why + ("" if have(k) else " - that source will be skipped"))
    INFO("optional keys set", ", ".join(k for k in ("FISH_API_KEY", "ELEVENLABS_API_KEY", "YT_DLP_PROXY",
                                                      "TIKTOK_CLIENT_KEY") if have(k)) or "none")

    secrets_f = Path(env.get("YOUTUBE_CLIENT_SECRETS", "client_secret.json"))
    (PASS if secrets_f.is_file() else FAIL)("YouTube client secret file", str(secrets_f))
    tokf = Path(env.get("YOUTUBE_TOKEN_FILE", "youtube_token.json"))
    if not tokf.is_file():
        FAIL("YouTube token file", f"{tokf} missing - copy it, or re-run `python publish_youtube.py` to re-consent")
    else:
        try:
            tok = json.loads(tokf.read_text(encoding="utf-8"))
            (PASS if tok.get("refresh_token") else FAIL)("YouTube token has refresh_token")
            scopes = tok.get("scopes") or []
            (PASS if any("yt-analytics" in s for s in scopes) else WARN)(
                "token has yt-analytics.readonly scope", "retention fetch needs it" )
        except Exception as e:
            FAIL("YouTube token parse", str(e))


# ── 6. python deps ───────────────────────────────────────────────────────
DEPS = [  # (import, pip name, required)
    ("groq", "groq", True), ("dotenv", "python-dotenv", True), ("requests", "requests", True),
    ("google.genai", "google-genai", True), ("edge_tts", "edge-tts", True), ("numpy", "numpy", True),
    ("googleapiclient", "google-api-python-client", True), ("google_auth_oauthlib", "google-auth-oauthlib", True),
    ("google_auth_httplib2", "google-auth-httplib2", True), ("praw", "praw", True), ("yt_dlp", "yt-dlp", True),
    ("fishaudio", "fish-audio-sdk", False),
]


def check_deps():
    section("Python packages")
    import importlib
    missing = []
    for mod, pipname, req in DEPS:
        try:
            importlib.import_module(mod)
            PASS(f"import {mod}")
        except Exception as e:
            (FAIL if req else INFO)(f"import {mod}", f"pip install {pipname}" + ("" if req else " (optional)"))
            if req:
                missing.append(pipname)
    if missing:
        print(f"\n   -> pip install {' '.join(missing)}   (or: pip install -r requirements-windows.txt)")


# ── 7. assets ────────────────────────────────────────────────────────────
def check_assets():
    section("Fonts & media assets")
    (PASS if (ROOT / "Bangers.ttf").is_file() else WARN)(
        "Bangers.ttf", "digest captions fall back to a default font without it")
    (PASS if (ROOT / "NotoColorEmoji.ttf").is_file() else WARN)(
        "NotoColorEmoji.ttf", "hook-overlay emoji won't render without it")
    for d in ("bgm", "music", "sfx"):
        p = ROOT / d
        n = len([f for f in p.iterdir() if f.suffix.lower() in (".mp3", ".wav", ".m4a", ".aac")]) if p.is_dir() else 0
        (PASS if n else WARN)(f"{d}/", f"{n} audio file(s)")
    for d in ("downloads", "edited", "finished"):
        INFO(f"{d}/", "present" if (ROOT / d).is_dir() else "absent (created on first run)")


def main():
    print(f"AniFeed self-check - {ROOT}")
    check_python()
    if check_ffmpeg():
        check_subtitles()
    check_db()
    check_env()
    check_deps()
    check_assets()
    f, w = RESULTS.count("FAIL"), RESULTS.count("WARN")
    print(f"\n{RESULTS.count('PASS')} passed, {w} warning(s), {f} failure(s).")
    if f:
        print("Fix the FAIL lines first, then re-run this script before touching run_all.py.")
    return 1 if f else 0


if __name__ == "__main__":
    sys.exit(main())
