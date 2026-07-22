"""
Pipeline State Database
------------------------
Single SQLite database (pipeline.db) that every stage of the pipeline reads
and writes to. Two different call styles are supported on top of the SAME
underlying tables, because different scripts in this pipeline were written
against different assumed APIs:

  CLASS API  (telegram_pull.py, edit_clips.py, build_news_digest.py,
              fetch_analytics.py):
      from db import PipelineDB
      db = PipelineDB()
      db.insert_post(...); db.posts_with_status(...); db.create_digest(...)

  MODULE-FUNCTION API  (burn_subtitles.py, identify_show.py,
                        generate_captions.py, generate_thumbnails.py,
                        publish_youtube.py, publish_tiktok.py):
      import db
      db.init_db()
      db.mark_stage("channel", "12345", "identified", visual_hint="...")
      db.get_publishable_posts("youtube", min_stage="thumbnailed")

Both APIs operate on the exact same `posts` row for a given post — a video
pulled by telegram_pull.py and later marked "identified" by identify_show.py
is one row, not two. The `key` column ("<channel>/<external_id>") is what
ties the two styles together: the class API builds it from source_id (via
the sources table) + external_id at insert time; the module API builds it
directly from the channel/post_id arguments callers already have on hand
(scripts that scan finished/<channel>/<post_id>/ folders).

WHY A DATABASE AT ALL:
File-existence was the old "have I done this already?" check (e.g. does
finished/<channel>/<post_id>/visual_hint.txt exist). That breaks the moment
those files get deleted after publishing — which is exactly what happens
here once a post goes out. This module is the permanent record: once a
post is marked at a stage, it stays marked, and its generated content
(captions, visual hint, platform post ids) is preserved here even after
downloads/, edited/, and finished/ are wiped.

TABLES:
  sources       one row per (platform, handle) Telegram channel, with a
                pull cursor (last message id seen)
  posts         one row per pulled post (video or image). Has BOTH a
                numeric autoincrement id (class API) and a text key
                "<channel>/<post_id>" (module API) pointing at the same row.
                Carries per-stage timestamps + the actual generated content
                (visual_hint, captions, thumbnail_pattern) so it survives
                file deletion. Also carries render_params_json (see LEARN
                STAGE note below).
  digests       one row per built news digest (finished/news/<digest_id>/).
                Carries an `urgent` flag (see build_news_digest.py's
                _compute_urgency()) that publish_scheduler.py reads to
                decide whether this digest should jump the normal
                publishing queue instead of waiting its turn. Also carries
                render_params_json (see LEARN STAGE note below).
  digest_posts  link table: which posts went into which digest (drives
                is_duplicate_topic's cross-digest checking).
  publish_log   one row per (content, platform) publish attempt. A row
                existing at all — success OR failed — means "don't retry,"
                per the pipeline's terminal-failure convention.
  metrics       append-only analytics snapshots (never overwritten), so
                growth over time is queryable instead of only the latest
                number. Covers BOTH clips (target_type='clip') and
                digests (target_type='digest') — see digests_with_metrics()
                below for why the digest side of this was previously
                write-only.

LEARN STAGE — render_params_json (self-learning loop expansion):
posts.render_params_json / digests.render_params_json store a JSON blob
of the EFFECTIVE learned_config creative-param values actually used to
produce that specific clip/digest. For clips, this is a MERGED blob:
edit_clips.py writes the first keys (bgm_volume, color_grade_saturation,
bgm_file, ...) when a post advances to 'edited', and each LATER stage
that also has a learnable param (burn_subtitles.py's
words_per_subtitle_cue, add_hook_overlay.py's hook_overlay_duration)
reads the existing JSON via get_post(), adds its own key(s), and writes
the merged JSON back — never blindly overwrites, or a later stage would
silently erase an earlier stage's recorded values. For digests, build_
news_digest.py writes render_params_json once, at db.create_digest()
time (news_digest_speed_factor) — digests only ever go through ONE
production pass, so there's no merge concern on that side.

This is a per-item SNAPSHOT, not just "read the current learned_config
value" — learned_config.py's values can change between when an item was
rendered and when performance_insights.py later reports on it, so the
only reliable way to correlate "this saturation value" with "these view
counts" is to record what was actually used AT RENDER TIME, on the row
itself. Written via mark_stage()/advance_stage()/create_digest()'s
existing **extra_fields / kwarg mechanisms — no new write path needed.
NULL for any row rendered before this column existed, which
performance_insights.py's param-bucketing pass simply excludes, same as
it already does for any other missing optional field.

USAGE:
  import db
  db.init_db()
  db.mark_stage("mychannel", "12345", "identified", visual_hint="Chainsaw Man")

  from db import PipelineDB
  pdb = PipelineDB()
  pdb.insert_post(source_id=1, external_id="12345", media_type="video", ...)
"""

import os
import re
import sys
import json
import sqlite3
from datetime import datetime, timezone, timedelta

DB_PATH = os.getenv("PIPELINE_DB_PATH", "pipeline.db")

STAGES = ["downloaded", "edited", "subtitled", "identified",
          "captioned", "thumbnailed", "hooked", "published"]

# QUALITY GATE — see quality_gate.py. Gated at the DB layer (not just in the
# gate script) so every caller of get_publishable_posts/get_publishable_digests
# automatically respects the gate's verdict, including publish_scheduler.py
# (which reads the same two functions to decide what's "ready" — this means
# unapproved content never gets a publish slot computed for it either, not
# just never gets uploaded). Set QUALITY_GATE_ENABLED=0 in .env to fall back
# to pre-gate behavior exactly (no quality_status filtering at all) — same
# opt-out convention as HOOK_OVERLAY_ENABLED in add_hook_overlay.py.
QUALITY_GATE_ENABLED = os.getenv("QUALITY_GATE_ENABLED", "1") != "0"
QUALITY_STATUSES = ("approved", "held", "rejected")

# Columns mark_stage()/set_fields()/advance_stage() are allowed to touch via
# **extra_fields, beyond the stage/status/timestamp bookkeeping they always do.
_EXTRA_COLUMNS = {
    "media_type", "caption_raw", "visual_hint",
    "caption_tiktok", "caption_youtube", "caption_hook", "edited_path",
    "fail_reason", "canonical_show", "thumbnail_pattern",
    "render_params_json",
    "ab_group_id", "ab_variant", "ab_override_json",
}

CROSS_DIGEST_SIMILARITY_THRESHOLD = 0.6
_METRIC_COLUMNS = {"views", "likes", "comments", "shares"}

# Fuzzy-match threshold for show_canon.py deciding whether a new raw title
# is "the same show, differently worded" vs. genuinely new. Same char-ngram
# technique as CROSS_DIGEST_SIMILARITY_THRESHOLD above, tuned separately —
# show titles are short strings where n-gram overlap runs naturally lower
# than full-caption comparisons, so this threshold sits below that one.
CANONICAL_SHOW_SIMILARITY_THRESHOLD = 0.55


def _now():
    return datetime.now(timezone.utc).isoformat()


def _key(channel, post_id):
    return f"{channel}/{post_id}"


def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            platform    TEXT NOT NULL,
            handle      TEXT NOT NULL,
            cursor      TEXT,
            UNIQUE(platform, handle)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            key             TEXT UNIQUE NOT NULL,   -- "<channel>/<post_id>"
            source_id       INTEGER,
            channel         TEXT NOT NULL,
            post_id         TEXT NOT NULL,          -- == external_id
            external_id     TEXT NOT NULL,
            media_type      TEXT,
            status          TEXT NOT NULL DEFAULT 'downloaded',
            caption_raw     TEXT,
            media_id        TEXT,
            thumb_hash      TEXT,
            raw_media_path  TEXT,
            edited_path     TEXT,
            visual_hint     TEXT,
            caption_tiktok  TEXT,
            caption_youtube TEXT,
            fail_reason     TEXT,
            thumbnail_pattern TEXT,
            render_params_json TEXT,
            quality_status     TEXT,
            quality_score      REAL,
            quality_flags      TEXT,
            quality_checked_at TEXT,
            ab_group_id     TEXT,
            ab_variant      TEXT,
            ab_override_json TEXT,
            downloaded_at   TEXT,
            edited_at       TEXT,
            subtitled_at    TEXT,
            identified_at   TEXT,
            captioned_at    TEXT,
            thumbnailed_at  TEXT,
            hooked_at       TEXT,
            published_at    TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_media_id ON posts(media_id)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS digests (
            digest_id       TEXT PRIMARY KEY,
            video_path      TEXT,
            caption_tiktok  TEXT,
            caption_youtube TEXT,
            thumbnail_pattern TEXT,
            render_params_json TEXT,
            urgent          INTEGER DEFAULT 0,
            quality_status     TEXT,
            quality_score      REAL,
            quality_flags      TEXT,
            quality_checked_at TEXT,
            built_at        TEXT,
            thumbnailed_at  TEXT,
            published_at    TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS digest_posts (
            digest_id  TEXT NOT NULL,
            post_id    INTEGER NOT NULL,
            PRIMARY KEY (digest_id, post_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS publish_log (
            content_key         TEXT NOT NULL,
            content_type        TEXT NOT NULL,   -- "clip" | "digest"
            platform            TEXT NOT NULL,
            status              TEXT NOT NULL,   -- "success" | "failed"
            platform_content_id TEXT,
            privacy_level       TEXT,
            error_message       TEXT,
            logged_at           TEXT,
            PRIMARY KEY (content_key, platform)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            target_type TEXT NOT NULL,
            target_id   TEXT NOT NULL,
            platform    TEXT NOT NULL,
            views       INTEGER,
            likes       INTEGER,
            comments    INTEGER,
            shares      INTEGER,
            retention_pct        REAL,
            watch_time_seconds   INTEGER,
            audience_country_top TEXT,
            raw_json    TEXT,
            snapshot_at TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_target "
                 "ON metrics(target_type, target_id, platform)")

    # canonical_shows: merges every raw identify_show.py output ("Chainsaw
    # Man", "chainsaw man s2", "CHAINSAW MAN") into one franchise-level row,
    # so analytics/insights aren't fragmented across string variants of the
    # same show. aliases is a JSON array; every raw string that has ever
    # matched this row (including the canonical_name itself) lives there,
    # which is what future fuzzy-matching compares against.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS canonical_shows (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name  TEXT UNIQUE NOT NULL,
            aliases         TEXT NOT NULL,
            post_count      INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        )
    """)

    # insights: single-row-per-key cache of whatever performance_insights.py
    # last computed (e.g. key="content_strategy"). Read cheaply by
    # generate_captions.py on every run instead of recomputing aggregates
    # from the metrics table on every caption call.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS insights (
            key          TEXT PRIMARY KEY,
            value_json   TEXT NOT NULL,
            computed_at  TEXT NOT NULL
        )
    """)

    # pipeline_runs: one row per run_all.sh execution, written by
    # run_summary.py. Exists so an unattended run that dies partway through
    # (set -e kills run_all.sh on the first failing stage) leaves a
    # permanent, queryable record instead of only a notification that's
    # already been dismissed by the time anyone looks.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at   TEXT NOT NULL,
            finished_at  TEXT,
            status       TEXT,              -- 'success' | 'partial' | 'failed' | 'running'
            exit_code    INTEGER,
            summary_json TEXT
        )
    """)

    # content_removals: tracks published content that fetch_analytics.py
    # confirmed is no longer reachable on the platform (Content ID claim,
    # manual takedown, or self-deletion — the Data API can't tell us which,
    # see fetch_analytics.py's docstring). Exists for three reasons: (1)
    # stop re-querying a known-gone video's stats every single day forever,
    # wasting API quota for zero new signal, (2) preserve the last view
    # count it actually earned instead of losing that data once it
    # disappears, (3) let removal_rate_by_show() surface which shows get
    # claimed most often — actionable content-strategy signal, not just
    # bookkeeping.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS content_removals (
            content_key      TEXT NOT NULL,
            target_type      TEXT NOT NULL,
            platform         TEXT NOT NULL,
            detected_at      TEXT NOT NULL,
            last_known_views INTEGER,
            recheck_after    TEXT,
            PRIMARY KEY (content_key, platform)
        )
    """)

    # publish_schedule: the computed "best time to post this" for every
    # ready clip/digest, per platform — written by publish_scheduler.py.
    # This is deliberately SEPARATE from publish_log: publish_log remains
    # the one source of truth for "did this actually go out" (see its own
    # docstring above); this table only ever tracks scheduling METADATA —
    # a recommended/actual release timestamp and why it was picked. Never
    # duplicate a "published" status in here — schedule_status.py joins
    # against publish_log at display time instead, so there's exactly one
    # place that can say "this is live" and it can't drift out of sync
    # with a second copy of that fact living here.
    #
    # status values:
    #   pending        — a recommended time was computed; nothing has
    #                    acted on it yet (e.g. YouTube scheduling is off,
    #                    so this is informational only — the next
    #                    publish_youtube.py run still uploads immediately)
    #   scheduled      — YouTube actually uploaded with this as publishAt
    #                    (native scheduling engaged, see publish_youtube.py)
    #   manual_pending — TikTok has no auto-publish; this is the
    #                    recommended time for a human to post it manually
    #   cancelled      — the underlying content was reset/removed before
    #                    its slot arrived (reset_stage.py can set this)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS publish_schedule (
            content_key   TEXT NOT NULL,
            content_type  TEXT NOT NULL,   -- "clip" | "digest"
            platform      TEXT NOT NULL,   -- "youtube" | "tiktok"
            scheduled_at  TEXT NOT NULL,   -- ISO 8601 UTC — the computed target release time
            status        TEXT NOT NULL,   -- pending | scheduled | manual_pending | cancelled
            reasoning     TEXT,            -- short human-readable note, e.g. "Evening bucket +23% vs avg (n=12)"
            created_at    TEXT NOT NULL,
            PRIMARY KEY (content_key, platform)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_schedule_platform_time "
                 "ON publish_schedule(platform, scheduled_at)")

    # source_events: append-only log of per-source FAILURE events —
    # image validation failures, normalize_image() failures, video
    # download failures — roadmap item Q (source_health.py). Successful
    # pulls are NOT logged here (they already exist as a posts row);
    # this table exists purely because failed attempts previously left
    # no trace at all, so ">30% image validation failures in the last 7
    # days" had no data to compute from. See source_health.py's
    # module docstring for the full rationale.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id   INTEGER NOT NULL,
            event_type  TEXT NOT NULL,   -- image_invalid | image_normalize_fail | download_failed
            created_at  TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source_events_source "
                 "ON source_events(source_id, created_at)")

    # source_health: the daily classification source_health.py writes —
    # healthy | degraded | blocked, per (platform, handle). Read by
    # rss_news_pull.py / fetch_youtube_trailers.py to skip blocked
    # sources entirely and halve degraded sources' quotas, ON TOP OF
    # source_quality.py's separate soft quota_multiplier (roadmap item
    # N) — the two are deliberately independent signals (N: "is this
    # source's CONTENT good", Q: "is this source's PIPELINE healthy"),
    # not merged into one number.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_health (
            platform      TEXT NOT NULL,
            handle        TEXT NOT NULL,
            status        TEXT NOT NULL,   -- healthy | degraded | blocked
            reasons_json  TEXT,
            computed_at   TEXT NOT NULL,
            PRIMARY KEY (platform, handle)
        )
    """)

    # api_costs: append-only log of billable/quota-consuming API calls —
    # roadmap item S (cost_tracker.py). One row per call, not per clip,
    # so a clip that triggers a retry still gets an accurate total. Cost
    # is computed at RECORD time from cost_tracker.py's rate table (which
    # defaults every known free-tier call to $0.0) rather than stored as
    # a snapshot, so a later rate-table correction can be re-aggregated
    # against the same event log instead of needing new instrumentation.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS api_costs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            content_key   TEXT,
            content_type  TEXT,            -- clip | digest | NULL (not content-attributable)
            provider      TEXT NOT NULL,   -- groq | gemini | youtube | tiktok | edge_tts
            operation     TEXT NOT NULL,   -- vision_call | text_call | whisper_call | upload | ...
            units         REAL NOT NULL DEFAULT 1,
            cost_usd      REAL NOT NULL,
            created_at    TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_api_costs_content "
                 "ON api_costs(content_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_api_costs_created "
                 "ON api_costs(created_at)")

    conn.commit()


def init_db():
    conn = get_connection()
    try:
        _init_schema(conn)
        _migrate_schema(conn)
    finally:
        conn.close()


# Expected columns per table, beyond what's guaranteed by CREATE TABLE IF NOT
# EXISTS on a brand-new db. If a table already exists (e.g. from an older
# version of this schema) but is missing a column the current code needs,
# CREATE TABLE IF NOT EXISTS silently does nothing — this is what actually
# adds the missing column instead of failing at query time with
# "OperationalError: no such column". Safe to run every time: it only adds
# columns that don't already exist, never touches existing data.
_EXPECTED_COLUMNS = {
    "sources": [("platform", "TEXT"), ("handle", "TEXT"), ("cursor", "TEXT")],
    "posts": [
        ("key", "TEXT"), ("source_id", "INTEGER"), ("channel", "TEXT"),
        ("post_id", "TEXT"), ("external_id", "TEXT"), ("media_type", "TEXT"),
        ("status", "TEXT"), ("caption_raw", "TEXT"), ("media_id", "TEXT"),
        ("thumb_hash", "TEXT"), ("raw_media_path", "TEXT"), ("edited_path", "TEXT"),
("visual_hint", "TEXT"), ("caption_tiktok", "TEXT"), ("caption_youtube", "TEXT"),
        ("caption_hook", "TEXT"),        ("fail_reason", "TEXT"), ("downloaded_at", "TEXT"), ("edited_at", "TEXT"),
        ("subtitled_at", "TEXT"), ("identified_at", "TEXT"), ("captioned_at", "TEXT"),
        ("thumbnailed_at", "TEXT"), ("published_at", "TEXT"), ("canonical_show", "TEXT"),
        ("hooked_at", "TEXT"), ("thumbnail_pattern", "TEXT"),
        ("render_params_json", "TEXT"),
        ("quality_status", "TEXT"), ("quality_score", "REAL"),
        ("quality_flags", "TEXT"), ("quality_checked_at", "TEXT"),
        ("ab_group_id", "TEXT"), ("ab_variant", "TEXT"), ("ab_override_json", "TEXT"),
    ],
    "digests": [
        ("video_path", "TEXT"), ("caption_tiktok", "TEXT"), ("caption_youtube", "TEXT"),
        ("built_at", "TEXT"), ("thumbnailed_at", "TEXT"), ("published_at", "TEXT"),
        ("thumbnail_pattern", "TEXT"), ("urgent", "INTEGER"),
        ("render_params_json", "TEXT"),
        ("quality_status", "TEXT"), ("quality_score", "REAL"),
        ("quality_flags", "TEXT"), ("quality_checked_at", "TEXT"),
    ],
    "digest_posts": [("digest_id", "TEXT"), ("post_id", "INTEGER")],
    "publish_log": [
        ("content_key", "TEXT"), ("content_type", "TEXT"), ("platform", "TEXT"),
        ("status", "TEXT"), ("platform_content_id", "TEXT"), ("privacy_level", "TEXT"),
        ("error_message", "TEXT"), ("logged_at", "TEXT"),
    ],
    "metrics": [
        ("target_type", "TEXT"), ("target_id", "TEXT"), ("platform", "TEXT"),
        ("views", "INTEGER"), ("likes", "INTEGER"), ("comments", "INTEGER"),
        ("shares", "INTEGER"), ("raw_json", "TEXT"), ("snapshot_at", "TEXT"),
        ("retention_pct", "REAL"), ("watch_time_seconds", "INTEGER"),
        ("audience_country_top", "TEXT"),
    ],
}


def _migrate_schema(conn):
    existing_tables = {
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    for table, expected_cols in _EXPECTED_COLUMNS.items():
        if table not in existing_tables:
            continue  # _init_schema already created it fresh with every column
        current_cols = {
            row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for col_name, col_type in expected_cols:
            if col_name not in current_cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                # stderr, not stdout: some callers (run_summary.py --start)
                # capture this module's stdout via bash `$(...)` and expect
                # it to contain ONLY the value they asked for — any stray
                # print() on stdout here would silently corrupt that capture.
                print(f"  [db migration] added missing column {table}.{col_name}",
                      file=sys.stderr)
    # These two indexes reference quality_status, which on an existing
    # database only exists once the ALTER TABLE loop above has run (fresh
    # databases get it for free via _init_schema's CREATE TABLE). Creating
    # them here — after migration, not inside _init_schema — is what fixes
    # "no such column: quality_status" on any database that predates the
    # quality gate: _init_schema's CREATE TABLE IF NOT EXISTS is a no-op on
    # a table that already exists, so an index statement sitting in
    # _init_schema would run before this loop ever added the column.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_quality_status ON posts(quality_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_digests_quality_status ON digests(quality_status)")
    conn.commit()


# ── shared helpers used by both APIs ────────────────────────────────────────

def _char_ngrams(text, n=4):
    text = re.sub(r"\s+", "", text.lower())
    return set(text[i:i + n] for i in range(len(text) - n + 1))


def _similarity(a, b):
    na, nb = _char_ngrams(a), _char_ngrams(b)
    if not na or not nb:
        return 0.0
    return len(na & nb) / min(len(na), len(nb))


def _row_to_dict(row):
    return dict(row) if row else None


# ── canonical show matching (used by show_canon.py) ─────────────────────────

def find_matching_canonical_show(raw_title, threshold=CANONICAL_SHOW_SIMILARITY_THRESHOLD):
    """Fuzzy-matches `raw_title` against every alias of every known canonical
    show. Returns the matching row (dict) or None. Exact match (case-
    insensitive) on any alias short-circuits to threshold=1.0 automatically
    since _similarity(a, a) == 1.0. Caller (show_canon.py) decides what to
    do with a None (create a new canonical show) vs a hit (attach as alias)."""
    conn = get_connection()
    rows = conn.execute("SELECT * FROM canonical_shows").fetchall()
    conn.close()

    best_row, best_score = None, 0.0
    for row in rows:
        aliases = json.loads(row["aliases"])
        # Score against the BEST-matching alias for this show, not the
        # average — one strong match (e.g. an exact prior alias) should win
        # even if other aliases for the same show are worded very differently.
        score = max((_similarity(raw_title, a) for a in aliases), default=0.0)
        if score > best_score:
            best_row, best_score = row, score

    return _row_to_dict(best_row) if best_score >= threshold else None


def create_canonical_show(canonical_name, first_alias):
    conn = get_connection()
    now = _now()
    try:
        conn.execute(
            """INSERT INTO canonical_shows
               (canonical_name, aliases, post_count, created_at, updated_at)
               VALUES (?, ?, 1, ?, ?)""",
            (canonical_name, json.dumps([first_alias]), now, now),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # Race: another process created the same canonical_name between our
        # find_matching_canonical_show() miss and this insert. Not fatal —
        # just attach the alias to the row that won the race instead.
        add_show_alias(canonical_name, first_alias)
    conn.close()
    return canonical_name


def add_show_alias(canonical_name, alias):
    """Records a newly-seen raw title as an alias of an existing canonical
    show, and bumps post_count — both feed future fuzzy-matching and the
    per-show counts show_canon.py/performance_insights.py rely on."""
    conn = get_connection()
    row = conn.execute(
        "SELECT aliases, post_count FROM canonical_shows WHERE canonical_name = ?",
        (canonical_name,),
    ).fetchone()
    if row is None:
        conn.close()
        return create_canonical_show(canonical_name, alias)

    aliases = json.loads(row["aliases"])
    if alias not in aliases:
        aliases.append(alias)
    conn.execute(
        "UPDATE canonical_shows SET aliases = ?, post_count = ?, updated_at = ? "
        "WHERE canonical_name = ?",
        (json.dumps(aliases), row["post_count"] + 1, _now(), canonical_name),
    )
    conn.commit()
    conn.close()


def list_canonical_shows():
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM canonical_shows ORDER BY post_count DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── insights cache (used by performance_insights.py / generate_captions.py) ─

def save_insight(key, value_dict):
    conn = get_connection()
    conn.execute(
        """INSERT INTO insights (key, value_json, computed_at) VALUES (?, ?, ?)
           ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
                                           computed_at = excluded.computed_at""",
        (key, json.dumps(value_dict), _now()),
    )
    conn.commit()
    conn.close()


def get_insight(key):
    """Returns the stored dict for `key`, or None if performance_insights.py
    hasn't run yet (or didn't have enough data to produce one)."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM insights WHERE key = ?", (key,)).fetchone()
    conn.close()
    if row is None:
        return None
    d = json.loads(row["value_json"])
    d["computed_at"] = row["computed_at"]
    return d


def posts_with_metrics():
    """Every post that has at least one metrics snapshot, joined to its
    latest snapshot per platform and its canonical_show/captions/
    thumbnail_pattern/render_params_json/published_at. Feeds
    performance_insights.py. metrics.target_id is the posts.key value for
    clips (see publish_log.content_key / record_metrics call sites).

    retention_pct/watch_time_seconds/audience_country_top (roadmap item
    K) and ab_group_id/ab_variant (roadmap item L) are included so
    performance_insights.py and ab_test.py can both read this one query
    rather than each needing their own join against posts+metrics."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT p.key, p.canonical_show, p.caption_tiktok, p.thumbnail_pattern,
                  p.render_params_json, p.published_at, p.ab_group_id, p.ab_variant,
                  m.platform, m.views, m.likes, m.comments, m.shares,
                  m.retention_pct, m.watch_time_seconds, m.audience_country_top
           FROM posts p
           INNER JOIN (
               SELECT target_id, platform, MAX(id) AS max_id
               FROM metrics WHERE target_type = 'clip'
               GROUP BY target_id, platform
           ) latest ON latest.target_id = p.key
           INNER JOIN metrics m ON m.id = latest.max_id
           WHERE m.views IS NOT NULL"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def digests_with_metrics():
    """Digest-side counterpart to posts_with_metrics() — added for the
    self-learning loop expansion (news_digest_speed_factor bucketing).

    BEFORE THIS FUNCTION EXISTED: fetch_analytics.py already recorded
    metrics for digests exactly like clips (metrics.target_type='digest'
    — see successful_publishes()/record_metrics() call sites, which
    handle both content types identically). But nothing ever READ that
    data back for analysis — performance_insights.py's build_insights()
    only ever called posts_with_metrics(), which joins the `posts` table
    and can't see digest rows at all. That meant a digest's
    render_params_json (e.g. build_news_digest.py's
    news_digest_speed_factor) had metrics sitting right next to it in the
    same database with no query path connecting the two. This closes
    that gap — same join shape as posts_with_metrics(), against
    `digests` instead of `posts`."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT d.digest_id AS key, d.render_params_json, d.published_at,
                  m.platform, m.views, m.likes, m.comments, m.shares
           FROM digests d
           INNER JOIN (
               SELECT target_id, platform, MAX(id) AS max_id
               FROM metrics WHERE target_type = 'digest'
               GROUP BY target_id, platform
           ) latest ON latest.target_id = d.digest_id
           INNER JOIN metrics m ON m.id = latest.max_id
           WHERE m.views IS NOT NULL"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def views_by_canonical_show(min_views=1):
    """Total views (summed across platforms, latest snapshot per post — same
    aggregation posts_with_metrics() callers already do by hand) grouped by
    canonical_show, for every published clip that has BOTH a canonical_show
    and at least one metrics snapshot. Feeds trend_signal.py's spike
    detection."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT p.canonical_show AS canonical_show, SUM(m.views) AS total_views
           FROM posts p
           INNER JOIN (
               SELECT target_id, platform, MAX(id) AS max_id
               FROM metrics WHERE target_type = 'clip'
               GROUP BY target_id, platform
           ) latest ON latest.target_id = p.key
           INNER JOIN metrics m ON m.id = latest.max_id
           WHERE m.views IS NOT NULL AND p.canonical_show IS NOT NULL
           GROUP BY p.canonical_show
           HAVING SUM(m.views) >= ?""",
        (min_views,),
    ).fetchall()
    conn.close()
    return {r["canonical_show"]: r["total_views"] for r in rows}


# ── pipeline run tracking (used by run_summary.py) ──────────────────────────

def start_run():
    """Records a new run_all.sh execution starting now. Returns the run id
    — the caller (run_summary.py) is responsible for holding onto it (via a
    temp file, since it's called from bash across two separate invocations
    of the script) and passing it back to finish_run()."""
    conn = get_connection()
    cur = conn.execute(
        "INSERT INTO pipeline_runs (started_at, status) VALUES (?, 'running')",
        (_now(),),
    )
    conn.commit()
    run_id = cur.lastrowid
    conn.close()
    return run_id


def get_run(run_id):
    conn = get_connection()
    row = conn.execute("SELECT * FROM pipeline_runs WHERE id = ?", (run_id,)).fetchone()
    conn.close()
    return _row_to_dict(row)


def close_stale_runs(older_than_hours=3):
    """Finds any run still sitting at status='running' that started more
    than `older_than_hours` ago and closes it as 'interrupted'."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=older_than_hours)).isoformat()

    conn = get_connection()
    stale = conn.execute(
        "SELECT id, started_at FROM pipeline_runs WHERE status = 'running' AND started_at < ?",
        (cutoff,),
    ).fetchall()
    conn.close()

    closed_ids = []
    for row in stale:
        summary = build_run_summary(row["started_at"])
        summary["note"] = (
            f"Run never reported back (no clean exit within {older_than_hours}h) — "
            f"auto-closed as interrupted, most likely the OS killed the process "
            f"(e.g. Termux backgrounded/screen-locked without a wake lock)."
        )
        finish_run(row["id"], "interrupted", None, summary)
        closed_ids.append(row["id"])
    return closed_ids


def finish_run(run_id, status, exit_code, summary_dict):
    conn = get_connection()
    conn.execute(
        "UPDATE pipeline_runs SET finished_at = ?, status = ?, exit_code = ?, "
        "summary_json = ? WHERE id = ?",
        (_now(), status, exit_code, json.dumps(summary_dict), run_id),
    )
    conn.commit()
    conn.close()


def recent_runs(limit=10):
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def build_run_summary(since_iso):
    """Counts everything that happened across the pipeline since `since_iso`
    (a run's started_at), by diffing stage timestamps."""
    conn = get_connection()

    def _count(query, params):
        return conn.execute(query, params).fetchone()[0]

    summary = {
        "posts_pulled": _count(
            "SELECT COUNT(*) FROM posts WHERE downloaded_at >= ?", (since_iso,)),
        "clips_edited": _count(
            "SELECT COUNT(*) FROM posts WHERE edited_at >= ?", (since_iso,)),
        "clips_subtitled": _count(
            "SELECT COUNT(*) FROM posts WHERE subtitled_at >= ?", (since_iso,)),
        "clips_identified": _count(
            "SELECT COUNT(*) FROM posts WHERE identified_at >= ?", (since_iso,)),
        "clips_captioned": _count(
            "SELECT COUNT(*) FROM posts WHERE captioned_at >= ?", (since_iso,)),
        "clips_thumbnailed": _count(
            "SELECT COUNT(*) FROM posts WHERE thumbnailed_at >= ?", (since_iso,)),
        "clips_hooked": _count(
            "SELECT COUNT(*) FROM posts WHERE hooked_at >= ?", (since_iso,)),
        "clips_quality_approved": _count(
            "SELECT COUNT(*) FROM posts WHERE quality_checked_at >= ? AND "
            "quality_status = 'approved'", (since_iso,)),
        "clips_quality_held": _count(
            "SELECT COUNT(*) FROM posts WHERE quality_checked_at >= ? AND "
            "quality_status = 'held'", (since_iso,)),
        "clips_quality_rejected": _count(
            "SELECT COUNT(*) FROM posts WHERE quality_checked_at >= ? AND "
            "quality_status = 'rejected'", (since_iso,)),
        "clips_failed": _count(
            "SELECT COUNT(*) FROM posts WHERE status = 'failed' AND "
            "COALESCE(downloaded_at, '') >= ?", (since_iso,)),
        "digests_built": _count(
            "SELECT COUNT(*) FROM digests WHERE built_at >= ?", (since_iso,)),
        "youtube_published": _count(
            "SELECT COUNT(*) FROM publish_log WHERE platform = 'youtube' "
            "AND status = 'success' AND logged_at >= ?", (since_iso,)),
        "youtube_failed": _count(
            "SELECT COUNT(*) FROM publish_log WHERE platform = 'youtube' "
            "AND status = 'failed' AND logged_at >= ?", (since_iso,)),
        "tiktok_published": _count(
            "SELECT COUNT(*) FROM publish_log WHERE platform = 'tiktok' "
            "AND status = 'success' AND logged_at >= ?", (since_iso,)),
        "tiktok_failed": _count(
            "SELECT COUNT(*) FROM publish_log WHERE platform = 'tiktok' "
            "AND status = 'failed' AND logged_at >= ?", (since_iso,)),
        "new_canonical_shows": _count(
            "SELECT COUNT(*) FROM canonical_shows WHERE created_at >= ?", (since_iso,)),
    }
    conn.close()
    return summary


# ─────────────────────────────────────────────────────────────────────────
# MODULE-LEVEL API — burn_subtitles.py, identify_show.py, generate_captions.py,
# generate_thumbnails.py, publish_youtube.py, publish_tiktok.py
# ─────────────────────────────────────────────────────────────────────────

def get_post(channel, post_id):
    """Returns a dict of the post's row, or None if it's never been tracked."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM posts WHERE key = ?", (_key(channel, post_id),)
    ).fetchone()
    conn.close()
    return _row_to_dict(row)


def is_stage_done(channel, post_id, stage):
    """True if this post already has a timestamp for `stage`."""
    if stage not in STAGES:
        raise ValueError(f"Unknown stage: {stage!r}. Must be one of {STAGES}")
    post = get_post(channel, post_id)
    return bool(post and post.get(f"{stage}_at"))


def _ensure_post_row(conn, channel, post_id, status="downloaded"):
    key = _key(channel, post_id)
    conn.execute(
        """INSERT OR IGNORE INTO posts
           (key, channel, post_id, external_id, status, downloaded_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (key, channel, post_id, post_id, status, _now()),
    )
    return key


def mark_stage(channel, post_id, stage, **extra_fields):
    """Record that `stage` has completed for this post. Creates the row if
    it doesn't exist yet. Any of _EXTRA_COLUMNS can be passed as kwargs to
    store alongside (e.g. visual_hint="Chainsaw Man"). Returns the post's key.
    """
    if stage not in STAGES:
        raise ValueError(f"Unknown stage: {stage!r}. Must be one of {STAGES}")
    unknown = set(extra_fields) - _EXTRA_COLUMNS
    if unknown:
        raise ValueError(f"Unknown column(s) for mark_stage: {unknown}")

    conn = get_connection()
    key = _ensure_post_row(conn, channel, post_id)

    set_clauses = ["status = ?", f"{stage}_at = ?"]
    params = [stage, _now()]
    for col, val in extra_fields.items():
        set_clauses.append(f"{col} = ?")
        params.append(val)
    params.append(key)

    conn.execute(f"UPDATE posts SET {', '.join(set_clauses)} WHERE key = ?", params)
    conn.commit()
    conn.close()
    return key


def set_fields(channel, post_id, **fields):
    """Update columns without advancing/stamping a stage."""
    unknown = set(fields) - _EXTRA_COLUMNS
    if unknown:
        raise ValueError(f"Unknown column(s) for set_fields: {unknown}")
    if not fields:
        return
    conn = get_connection()
    _ensure_post_row(conn, channel, post_id)
    key = _key(channel, post_id)
    set_clauses = [f"{col} = ?" for col in fields]
    params = list(fields.values()) + [key]
    conn.execute(f"UPDATE posts SET {', '.join(set_clauses)} WHERE key = ?", params)
    conn.commit()
    conn.close()


def stats():
    """Count of posts currently sitting at each status — quick health check."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT status, COUNT(*) as n FROM posts GROUP BY status ORDER BY n DESC"
    ).fetchall()
    conn.close()
    return {r["status"]: r["n"] for r in rows}
def recent_captions(limit=8):
    """Last N generated TikTok captions, most recent first."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT caption_tiktok FROM posts WHERE caption_tiktok IS NOT NULL "
        "ORDER BY captioned_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [r["caption_tiktok"] for r in rows if r["caption_tiktok"]]

def sync_posts_from_filesystem(finished_dir="finished"):
    """Scans finished/<channel>/<post_id>/ for thumbnail.jpg and marks the
    'thumbnailed' stage for anything not already marked."""
    if not os.path.isdir(finished_dir):
        return 0
    marked = 0
    for channel in os.listdir(finished_dir):
        if channel == "news":
            continue
        channel_path = os.path.join(finished_dir, channel)
        if not os.path.isdir(channel_path):
            continue
        for post_id in os.listdir(channel_path):
            thumb_path = os.path.join(channel_path, post_id, "thumbnail.jpg")
            if os.path.isfile(thumb_path) and not is_stage_done(channel, post_id, "thumbnailed"):
                mark_stage(channel, post_id, "thumbnailed", media_type="video")
                marked += 1
    return marked


def sync_digests_from_filesystem(finished_dir="finished"):
    """Scans finished/news/<digest_id>/ and keeps the digests table current."""
    news_dir = os.path.join(finished_dir, "news")
    if not os.path.isdir(news_dir):
        return 0

    conn = get_connection()
    touched = 0
    for digest_id in os.listdir(news_dir):
        digest_path = os.path.join(news_dir, digest_id)
        if not os.path.isdir(digest_path):
            continue
        video_path = os.path.join(digest_path, "video.mp4")
        if not os.path.isfile(video_path):
            continue

        thumb_path = os.path.join(digest_path, "thumbnail.jpg")
        tiktok_cap_path = os.path.join(digest_path, "caption_final.txt")
        youtube_cap_path = os.path.join(digest_path, "caption_youtube.txt")

        conn.execute(
            "INSERT OR IGNORE INTO digests (digest_id) VALUES (?)", (digest_id,)
        )
        row = conn.execute(
            "SELECT * FROM digests WHERE digest_id = ?", (digest_id,)
        ).fetchone()

        updates, params = [], []
        if not row["built_at"]:
            updates += ["video_path = ?", "built_at = ?"]
            params += [video_path, _now()]
        if os.path.isfile(thumb_path) and not row["thumbnailed_at"]:
            updates += ["thumbnailed_at = ?"]
            params += [_now()]
        if os.path.isfile(tiktok_cap_path) and not row["caption_tiktok"]:
            with open(tiktok_cap_path, "r", encoding="utf-8") as f:
                updates += ["caption_tiktok = ?"]
                params += [f.read()]
        if os.path.isfile(youtube_cap_path) and not row["caption_youtube"]:
            with open(youtube_cap_path, "r", encoding="utf-8") as f:
                updates += ["caption_youtube = ?"]
                params += [f.read()]

        if updates:
            params.append(digest_id)
            conn.execute(f"UPDATE digests SET {', '.join(updates)} WHERE digest_id = ?", params)
            touched += 1

    conn.commit()
    conn.close()
    return touched


# ── quality gate (used by quality_gate.py) ───────────────────────────────────

def posts_awaiting_quality_gate(min_stage="thumbnailed"):
    conn = get_connection()
    rows = conn.execute(
        f"""SELECT * FROM posts
            WHERE {min_stage}_at IS NOT NULL
              AND media_type = 'video'
              AND quality_status IS NULL
            ORDER BY id ASC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def digests_awaiting_quality_gate(min_stage="thumbnailed"):
    conn = get_connection()
    rows = conn.execute(
        f"""SELECT * FROM digests
            WHERE {min_stage}_at IS NOT NULL
              AND quality_status IS NULL
            ORDER BY digest_id ASC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_quality(content_key, content_type, status, score, flags):
    if content_type not in ("clip", "digest"):
        raise ValueError("content_type must be 'clip' or 'digest'")
    if status not in QUALITY_STATUSES:
        raise ValueError(f"status must be one of {QUALITY_STATUSES}")

    conn = get_connection()
    table = "posts" if content_type == "clip" else "digests"
    key_col = "key" if content_type == "clip" else "digest_id"
    conn.execute(
        f"UPDATE {table} SET quality_status = ?, quality_score = ?, "
        f"quality_flags = ?, quality_checked_at = ? WHERE {key_col} = ?",
        (status, score, json.dumps(flags), _now(), content_key),
    )
    conn.commit()
    conn.close()


def reset_quality(content_key, content_type):
    if content_type not in ("clip", "digest"):
        raise ValueError("content_type must be 'clip' or 'digest'")
    conn = get_connection()
    table = "posts" if content_type == "clip" else "digests"
    key_col = "key" if content_type == "clip" else "digest_id"
    conn.execute(
        f"UPDATE {table} SET quality_status = NULL, quality_score = NULL, "
        f"quality_flags = NULL, quality_checked_at = NULL WHERE {key_col} = ?",
        (content_key,),
    )
    conn.commit()
    conn.close()


def quality_held_items():
    conn = get_connection()
    clips = conn.execute(
        "SELECT key AS content_key, 'clip' AS content_type, quality_score, "
        "quality_flags, quality_checked_at FROM posts WHERE quality_status = 'held'"
    ).fetchall()
    digests = conn.execute(
        "SELECT digest_id AS content_key, 'digest' AS content_type, quality_score, "
        "quality_flags, quality_checked_at FROM digests WHERE quality_status = 'held'"
    ).fetchall()
    conn.close()
    return [dict(r) for r in clips] + [dict(r) for r in digests]


def get_publishable_posts(platform, min_stage="thumbnailed"):
    quality_clause = " AND quality_status = 'approved'" if QUALITY_GATE_ENABLED else ""
    conn = get_connection()
    rows = conn.execute(
        f"""SELECT * FROM posts
            WHERE {min_stage}_at IS NOT NULL
              AND media_type = 'video'
              {quality_clause}
              AND key NOT IN (
                  SELECT content_key FROM publish_log WHERE platform = ?
              )
            ORDER BY id ASC""",
        (platform,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_publishable_digests(platform, min_stage="thumbnailed"):
    quality_clause = " AND quality_status = 'approved'" if QUALITY_GATE_ENABLED else ""
    conn = get_connection()
    rows = conn.execute(
        f"""SELECT * FROM digests
            WHERE {min_stage}_at IS NOT NULL
              {quality_clause}
              AND digest_id NOT IN (
                  SELECT content_key FROM publish_log WHERE platform = ?
              )
            ORDER BY digest_id ASC""",
        (platform,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def log_publish(key, content_type, platform, status,
                 platform_content_id=None, privacy_level=None, error_message=None):
    if content_type not in ("clip", "digest"):
        raise ValueError("content_type must be 'clip' or 'digest'")
    if status not in ("success", "failed"):
        raise ValueError("status must be 'success' or 'failed'")

    conn = get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO publish_log
           (content_key, content_type, platform, status,
            platform_content_id, privacy_level, error_message, logged_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (key, content_type, platform, status,
         platform_content_id, privacy_level, error_message, _now()),
    )
    if status == "success":
        if content_type == "clip":
            conn.execute(
                "UPDATE posts SET published_at = COALESCE(published_at, ?) WHERE key = ?",
                (_now(), key),
            )
        else:
            conn.execute(
                "UPDATE digests SET published_at = COALESCE(published_at, ?) WHERE digest_id = ?",
                (_now(), key),
            )
    conn.commit()
    conn.close()


# ── analytics (module-level, mirrors class API below) ───────────────────────

def successful_publishes(platform):
    conn = get_connection()
    rows = conn.execute(
        """SELECT content_key AS target_id, content_type AS target_type,
                  platform_content_id
           FROM publish_log WHERE platform = ? AND status = 'success'""",
        (platform,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def record_metrics(target_type, target_id, platform,
                    views=None, likes=None, comments=None, shares=None, raw=None,
                    retention_pct=None, watch_time_seconds=None, audience_country_top=None):
    """retention_pct/watch_time_seconds/audience_country_top (roadmap item
    K) are optional and default to None — every existing caller
    (fetch_analytics.py's YouTube/TikTok stats collection) keeps working
    unchanged with no retention data recorded. Only
    fetch_youtube_analytics.py populates these, since TikTok's public API
    doesn't expose an equivalent retention metric."""
    conn = get_connection()
    conn.execute(
        """INSERT INTO metrics
           (target_type, target_id, platform, views, likes, comments, shares,
            retention_pct, watch_time_seconds, audience_country_top,
            raw_json, snapshot_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (target_type, target_id, platform, views, likes, comments, shares,
         retention_pct, watch_time_seconds, audience_country_top,
         json.dumps(raw) if raw is not None else None, _now()),
    )
    conn.commit()
    conn.close()


def top_performers(metric="views", limit=5):
    if metric not in _METRIC_COLUMNS:
        raise ValueError(f"metric must be one of {_METRIC_COLUMNS}")
    conn = get_connection()
    rows = conn.execute(
        f"""SELECT m.* FROM metrics m
            INNER JOIN (
                SELECT target_type, target_id, platform, MAX(id) AS max_id
                FROM metrics GROUP BY target_type, target_id, platform
            ) latest ON m.id = latest.max_id
            WHERE m.{metric} IS NOT NULL
            ORDER BY m.{metric} DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def metrics_history(target_type, target_id, platform=None):
    conn = get_connection()
    query = "SELECT * FROM metrics WHERE target_type = ? AND target_id = ?"
    params = [target_type, target_id]
    if platform:
        query += " AND platform = ?"
        params.append(platform)
    query += " ORDER BY snapshot_at ASC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── content removal tracking (used by fetch_analytics.py) ───────────────────

def mark_removed(content_key, target_type, platform, last_known_views=None,
                  recheck_after_days=14):
    conn = get_connection()
    recheck_after = (datetime.now(timezone.utc) + timedelta(days=recheck_after_days)).isoformat()
    conn.execute(
        """INSERT INTO content_removals
           (content_key, target_type, platform, detected_at, last_known_views, recheck_after)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(content_key, platform) DO UPDATE SET
               last_known_views = excluded.last_known_views,
               recheck_after = excluded.recheck_after""",
        (content_key, target_type, platform, _now(), last_known_views, recheck_after),
    )
    conn.commit()
    conn.close()


def clear_removed(content_key, platform):
    conn = get_connection()
    conn.execute(
        "DELETE FROM content_removals WHERE content_key = ? AND platform = ?",
        (content_key, platform),
    )
    conn.commit()
    conn.close()


def removed_content_map(platform):
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM content_removals WHERE platform = ?", (platform,)
    ).fetchall()
    conn.close()
    return {r["content_key"]: dict(r) for r in rows}


def is_due_for_recheck(removal_row):
    if not removal_row or not removal_row.get("recheck_after"):
        return True
    return _now() >= removal_row["recheck_after"]


def removal_rate_by_show(platform=None):
    conn = get_connection()
    query = """
        SELECT p.canonical_show, COUNT(DISTINCT r.content_key) AS removed_count,
               (SELECT COUNT(*) FROM posts p2
                WHERE p2.canonical_show = p.canonical_show
                  AND p2.published_at IS NOT NULL) AS published_count
        FROM content_removals r
        INNER JOIN posts p ON p.key = r.content_key
        WHERE r.target_type = 'clip' AND p.canonical_show IS NOT NULL
    """
    params = []
    if platform:
        query += " AND r.platform = ?"
        params.append(platform)
    query += " GROUP BY p.canonical_show ORDER BY removed_count DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    result = []
    for r in rows:
        d = dict(r)
        d["removal_rate_pct"] = (
            round(d["removed_count"] / d["published_count"] * 100, 1)
            if d["published_count"] else None
        )
        result.append(d)
    return result


# ── publish scheduling (used by publish_scheduler.py, publish_youtube.py,
# schedule_status.py, list_for_tiktok.py) ───────────────────────────────────

def upsert_schedule(content_key, content_type, platform, scheduled_at, status, reasoning=None):
    if content_type not in ("clip", "digest"):
        raise ValueError("content_type must be 'clip' or 'digest'")
    if platform not in ("youtube", "tiktok"):
        raise ValueError("platform must be 'youtube' or 'tiktok'")
    if status not in ("pending", "scheduled", "manual_pending", "cancelled"):
        raise ValueError("status must be pending/scheduled/manual_pending/cancelled")

    conn = get_connection()
    conn.execute(
        """INSERT INTO publish_schedule
           (content_key, content_type, platform, scheduled_at, status, reasoning, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(content_key, platform) DO UPDATE SET
               scheduled_at = excluded.scheduled_at,
               status = excluded.status,
               reasoning = excluded.reasoning""",
        (content_key, content_type, platform, scheduled_at, status, reasoning, _now()),
    )
    conn.commit()
    conn.close()


def get_schedule_entry(content_key, platform):
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM publish_schedule WHERE content_key = ? AND platform = ?",
        (content_key, platform),
    ).fetchone()
    conn.close()
    return _row_to_dict(row)


def has_schedule_entry(content_key, platform):
    return get_schedule_entry(content_key, platform) is not None


def list_schedule(platform=None, status=None, order="scheduled_at ASC"):
    conn = get_connection()
    query = "SELECT * FROM publish_schedule WHERE 1=1"
    params = []
    if platform:
        query += " AND platform = ?"
        params.append(platform)
    if status:
        query += " AND status = ?"
        params.append(status)
    query += f" ORDER BY {order}"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def reserved_schedule_times(platform, after_iso, statuses=("pending", "scheduled", "manual_pending")):
    conn = get_connection()
    placeholders = ",".join("?" * len(statuses))
    rows = conn.execute(
        f"""SELECT scheduled_at FROM publish_schedule
            WHERE platform = ? AND scheduled_at >= ? AND status IN ({placeholders})""",
        (platform, after_iso, *statuses),
    ).fetchall()
    conn.close()
    return [r["scheduled_at"] for r in rows]


def cancel_schedule(content_key, platform):
    conn = get_connection()
    conn.execute(
        "UPDATE publish_schedule SET status = 'cancelled' WHERE content_key = ? AND platform = ?",
        (content_key, platform),
    )
    conn.commit()
    conn.close()


# ── A/B testing (used by ab_test.py — roadmap item L) ────────────────────
# posts.ab_group_id ties two (or more) variant rows together;
# posts.ab_variant is "control" | "treatment"; posts.ab_override_json
# holds the ONE nudged learned_config param this specific variant should
# render with instead of the current global value (e.g.
# {"hook_overlay_duration": 2.6}) — consumer stages (currently
# add_hook_overlay.py) check this before falling back to
# learned_config.get_param(). See ab_test.py's module docstring for why
# a per-post override column exists beyond the roadmap's literal
# ab_group_id/ab_variant pair: without it, a "treatment" row has no way
# to actually render differently from "control".

def mark_ab_control(channel, post_id, ab_group_id):
    """Tags the ORIGINAL post row as the control variant of a new A/B
    test — no render override, it proceeds with current learned_config
    values exactly as it always would have."""
    set_fields(channel, post_id, ab_group_id=ab_group_id, ab_variant="control")


def create_ab_variant_post(source_channel, source_post_id, variant_post_id,
                            ab_group_id, ab_override_json):
    """Clones the source post's row into a NEW post (new key, same
    channel) representing the "treatment" variant — copies every field a
    downstream stage (add_hook_overlay.py, quality_gate.py,
    publish_scheduler.py, publish_youtube.py) needs to treat it as a
    normal ready-to-hook clip, EXCEPT hooked_at/published_at (this
    variant hasn't gone through those stages yet under its own render).
    Returns the new post's key. Raises if a post already exists at the
    target key (caller should pick a variant_post_id that doesn't
    collide, e.g. f"{source_post_id}_abB")."""
    source = get_post(source_channel, source_post_id)
    if source is None:
        raise ValueError(f"No source post found for {source_channel}/{source_post_id}")

    conn = get_connection()
    new_key = _key(source_channel, variant_post_id)
    try:
        conn.execute(
            """INSERT INTO posts
               (key, source_id, channel, post_id, external_id, media_type,
                status, caption_raw, visual_hint, caption_tiktok,
                caption_youtube, caption_hook, canonical_show,
                render_params_json, quality_status,
                ab_group_id, ab_variant, ab_override_json,
                downloaded_at, edited_at, subtitled_at, identified_at,
                captioned_at, thumbnailed_at)
               VALUES (?, ?, ?, ?, ?, ?, 'thumbnailed', ?, ?, ?, ?, ?, ?, ?, NULL,
                       ?, 'treatment', ?, ?, ?, ?, ?, ?, ?)""",
            (new_key, source.get("source_id"), source_channel, variant_post_id,
             variant_post_id, source.get("media_type"),
             source.get("caption_raw"), source.get("visual_hint"),
             source.get("caption_tiktok"), source.get("caption_youtube"),
             source.get("caption_hook"), source.get("canonical_show"),
             source.get("render_params_json"),
             ab_group_id, ab_override_json,
             source.get("downloaded_at"), source.get("edited_at"),
             source.get("subtitled_at"), source.get("identified_at"),
             source.get("captioned_at"), _now()),
        )
        conn.commit()
    except sqlite3.IntegrityError as e:
        conn.close()
        raise ValueError(f"A post already exists at key {new_key!r}: {e}")
    conn.close()
    return new_key


def ab_groups_with_both_variants():
    """Returns every ab_group_id that has BOTH a 'control' and a
    'treatment' row, each still meeting NOT NULL on published_at — the
    set ab_test.py's report_results() can actually compare. A group
    missing one side (still mid-pipeline, or one variant failed) is
    excluded, not reported as a partial/misleading result."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT ab_group_id,
                  SUM(CASE WHEN ab_variant = 'control' AND published_at IS NOT NULL THEN 1 ELSE 0 END) AS n_control,
                  SUM(CASE WHEN ab_variant = 'treatment' AND published_at IS NOT NULL THEN 1 ELSE 0 END) AS n_treatment
           FROM posts
           WHERE ab_group_id IS NOT NULL
           GROUP BY ab_group_id
           HAVING n_control >= 1 AND n_treatment >= 1"""
    ).fetchall()
    conn.close()
    return [r["ab_group_id"] for r in rows]


def posts_in_ab_group(ab_group_id):
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM posts WHERE ab_group_id = ?", (ab_group_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Source health events (used by acquire-stage scripts + source_health.py
# — roadmap item Q) ──────────────────────────────────────────────────────

def record_source_event(source_id, event_type):
    """Logs one failure event for `source_id` — see source_events'
    CREATE TABLE comment for the three event_type values. Never raises
    on a bad source_id (FK isn't enforced on this table on purpose — an
    acquire script's own error handling shouldn't ALSO have to guard
    this call); a stray row against a since-deleted source is harmless
    noise, not a correctness problem, since source_health.py always
    joins back through sources at read time."""
    conn = get_connection()
    conn.execute(
        "INSERT INTO source_events (source_id, event_type, created_at) VALUES (?, ?, ?)",
        (source_id, event_type, _now()),
    )
    conn.commit()
    conn.close()


def source_event_counts(window_days=7):
    """Returns {(platform, handle): {event_type: count}} for every
    source_events row within the last `window_days` — source_health.py's
    raw input for the image-validation-failure-rate rule."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    conn = get_connection()
    rows = conn.execute(
        """SELECT s.platform, s.handle, e.event_type, COUNT(*) AS n
           FROM source_events e
           INNER JOIN sources s ON s.id = e.source_id
           WHERE e.created_at >= ?
           GROUP BY s.platform, s.handle, e.event_type""",
        (cutoff,),
    ).fetchall()
    conn.close()
    counts = {}
    for r in rows:
        key = (r["platform"], r["handle"])
        counts.setdefault(key, {})[r["event_type"]] = r["n"]
    return counts


def posts_inserted_by_source(window_days=7):
    """Returns {(platform, handle): count} of successfully-inserted posts
    (downloaded_at within the window) — the denominator's other half for
    a validation-failure RATE (failures / (failures + successes))."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    conn = get_connection()
    rows = conn.execute(
        """SELECT s.platform, s.handle, COUNT(*) AS n
           FROM posts p
           INNER JOIN sources s ON s.id = p.source_id
           WHERE p.downloaded_at >= ?
           GROUP BY s.platform, s.handle""",
        (cutoff,),
    ).fetchall()
    conn.close()
    return {(r["platform"], r["handle"]): r["n"] for r in rows}


def upsert_source_health(platform, handle, status, reasons):
    if status not in ("healthy", "degraded", "blocked"):
        raise ValueError("status must be healthy/degraded/blocked")
    conn = get_connection()
    conn.execute(
        """INSERT INTO source_health (platform, handle, status, reasons_json, computed_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(platform, handle) DO UPDATE SET
               status = excluded.status,
               reasons_json = excluded.reasons_json,
               computed_at = excluded.computed_at""",
        (platform, handle, status, json.dumps(reasons), _now()),
    )
    conn.commit()
    conn.close()


def get_source_health(platform, handle):
    """Returns 'healthy' if source_health.py has never run for this
    source (never-computed is not the same as unhealthy — a brand new
    source shouldn't be treated as degraded just because it's never been
    classified yet)."""
    conn = get_connection()
    row = conn.execute(
        "SELECT status FROM source_health WHERE platform = ? AND handle = ?",
        (platform, handle),
    ).fetchone()
    conn.close()
    return row["status"] if row else "healthy"


def all_source_health():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM source_health ORDER BY platform, handle").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── API cost tracking (used by cost_tracker.py — roadmap item S) ────────

def record_api_cost(provider, operation, cost_usd, content_key=None,
                     content_type=None, units=1.0):
    conn = get_connection()
    conn.execute(
        """INSERT INTO api_costs
           (content_key, content_type, provider, operation, units, cost_usd, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (content_key, content_type, provider, operation, units, cost_usd, _now()),
    )
    conn.commit()
    conn.close()


def total_cost_by_content_key(window_days=None):
    """Returns {content_key: total_cost_usd} — cost_tracker.py's
    denominator-free half of cost-per-view; caller joins this against
    posts_with_metrics() views separately (kept as two simple queries
    rather than one large join, matching this project's general
    preference for readable per-purpose queries over one mega-query —
    see check_digest_state.py / dupe_check.py's own convention)."""
    conn = get_connection()
    query = "SELECT content_key, SUM(cost_usd) AS total FROM api_costs WHERE content_key IS NOT NULL"
    params = []
    if window_days is not None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        query += " AND created_at >= ?"
        params.append(cutoff)
    query += " GROUP BY content_key"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return {r["content_key"]: r["total"] for r in rows}


def cost_by_operation(window_days=None):
    """Returns [{provider, operation, total_cost_usd, call_count}, ...]
    sorted by total cost descending — the breakdown cost_tracker.py's
    report reads to answer 'which specific call type is actually
    expensive'."""
    conn = get_connection()
    query = """SELECT provider, operation, SUM(cost_usd) AS total_cost,
                      SUM(units) AS total_units, COUNT(*) AS call_count
               FROM api_costs WHERE 1=1"""
    params = []
    if window_days is not None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        query += " AND created_at >= ?"
        params.append(cutoff)
    query += " GROUP BY provider, operation ORDER BY total_cost DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────
# CLASS API — telegram_pull.py, edit_clips.py, build_news_digest.py,
# fetch_analytics.py
# ─────────────────────────────────────────────────────────────────────────

class PipelineDB:
    """Thin object wrapper over the exact same tables the module-level
    functions above use."""

    def __init__(self, db_path=None):
        global DB_PATH
        if db_path:
            DB_PATH = db_path
        init_db()

    # ── sources / cursors ──
    def get_or_create_source(self, platform, handle):
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO sources (platform, handle) VALUES (?, ?)",
            (platform, handle),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM sources WHERE platform = ? AND handle = ?",
            (platform, handle),
        ).fetchone()
        conn.close()
        return row["id"]

    def get_source(self, source_id):
        conn = get_connection()
        row = conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
        conn.close()
        return _row_to_dict(row)

    def get_cursor(self, platform, handle):
        conn = get_connection()
        row = conn.execute(
            "SELECT cursor FROM sources WHERE platform = ? AND handle = ?",
            (platform, handle),
        ).fetchone()
        conn.close()
        return row["cursor"] if row else None

    def set_cursor(self, platform, handle, value):
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO sources (platform, handle) VALUES (?, ?)",
            (platform, handle),
        )
        conn.execute(
            "UPDATE sources SET cursor = ? WHERE platform = ? AND handle = ?",
            (str(value), platform, handle),
        )
        conn.commit()
        conn.close()

    # ── dedup (telegram_pull.py) ──
    def media_id_seen(self, media_id):
        if not media_id:
            return False
        conn = get_connection()
        row = conn.execute("SELECT 1 FROM posts WHERE media_id = ?", (media_id,)).fetchone()
        conn.close()
        return row is not None

    def find_close_thumb_match(self, thumb_hash, threshold=8):
        if not thumb_hash:
            return False
        try:
            target = int(thumb_hash, 16)
        except ValueError:
            return False
        conn = get_connection()
        rows = conn.execute(
            "SELECT thumb_hash FROM posts WHERE thumb_hash IS NOT NULL"
        ).fetchall()
        conn.close()
        for row in rows:
            try:
                other = int(row["thumb_hash"], 16)
            except (ValueError, TypeError):
                continue
            if bin(target ^ other).count("1") <= threshold:
                return True
        return False

    # ── posts (telegram_pull.py / edit_clips.py) ──
    def insert_post(self, source_id, external_id, media_type, caption_raw,
                     media_id=None, thumb_hash=None, raw_media_path=None):
        source = self.get_source(source_id)
        channel = source["handle"] if source else str(source_id)
        key = _key(channel, external_id)

        conn = get_connection()
        try:
            cur = conn.execute(
                """INSERT INTO posts
                   (key, source_id, channel, post_id, external_id, media_type,
                    caption_raw, media_id, thumb_hash, raw_media_path,
                    status, downloaded_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'downloaded', ?)""",
                (key, source_id, channel, external_id, external_id, media_type,
                 caption_raw, media_id, thumb_hash, raw_media_path, _now()),
            )
            conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return None  # already recorded — race/rerun, not fatal
        finally:
            conn.close()

    def posts_with_status(self, status, media_type=None, limit=50):
        conn = get_connection()
        query = "SELECT * FROM posts WHERE status = ?"
        params = [status]
        if media_type:
            query += " AND media_type = ?"
            params.append(media_type)
        query += " ORDER BY id ASC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def mark_failed(self, post_id, reason):
        conn = get_connection()
        conn.execute(
            "UPDATE posts SET status = 'failed', fail_reason = ? WHERE id = ?",
            (reason, post_id),
        )
        conn.commit()
        conn.close()

    def advance_stage(self, post_id, stage, **extra_fields):
        if stage not in STAGES:
            raise ValueError(f"Unknown stage: {stage!r}. Must be one of {STAGES}")
        unknown = set(extra_fields) - _EXTRA_COLUMNS
        if unknown:
            raise ValueError(f"Unknown column(s) for advance_stage: {unknown}")

        set_clauses = ["status = ?", f"{stage}_at = ?"]
        params = [stage, _now()]
        for col, val in extra_fields.items():
            set_clauses.append(f"{col} = ?")
            params.append(val)
        params.append(post_id)

        conn = get_connection()
        conn.execute(f"UPDATE posts SET {', '.join(set_clauses)} WHERE id = ?", params)
        conn.commit()
        conn.close()

    # ── news digests (build_news_digest.py) ──
    def is_duplicate_topic(self, caption, threshold=CROSS_DIGEST_SIMILARITY_THRESHOLD):
        conn = get_connection()
        rows = conn.execute(
            """SELECT p.caption_raw FROM posts p
               INNER JOIN digest_posts dp ON dp.post_id = p.id
               WHERE p.caption_raw IS NOT NULL"""
        ).fetchall()
        conn.close()
        return any(_similarity(caption, r["caption_raw"]) >= threshold for r in rows)

    def create_digest(self, video_path, caption_tiktok, caption_youtube, post_ids,
                       urgent=False, render_params_json=None):
        """Atomically records the digest, links its stories, and flips every
        used post to status='used_in_digest'.

        render_params_json (self-learning loop expansion): JSON string of
        the effective creative-param values (e.g. news_digest_speed_factor)
        actually used to render this digest — see db.py's docstring. None
        is a normal, expected value for anything built before
        build_news_digest.py started populating it."""
        digest_id = os.path.basename(os.path.dirname(video_path))

        conn = get_connection()
        try:
            conn.execute("BEGIN")
            conn.execute(
                """INSERT INTO digests (digest_id, video_path, caption_tiktok,
                                         caption_youtube, built_at, urgent,
                                         render_params_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (digest_id, video_path, caption_tiktok, caption_youtube, _now(),
                 1 if urgent else 0, render_params_json),
            )
            for post_id in post_ids:
                conn.execute(
                    "INSERT OR IGNORE INTO digest_posts (digest_id, post_id) VALUES (?, ?)",
                    (digest_id, post_id),
                )
                conn.execute(
                    "UPDATE posts SET status = 'used_in_digest' WHERE id = ?",
                    (post_id,),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return digest_id

    # ── analytics (fetch_analytics.py) — same tables as the module functions ──
    def successful_publishes(self, platform):
        return successful_publishes(platform)

    def record_metrics(self, target_type, target_id, platform,
                        views=None, likes=None, comments=None, shares=None, raw=None,
                        retention_pct=None, watch_time_seconds=None, audience_country_top=None):
        return record_metrics(target_type, target_id, platform,
                               views=views, likes=likes, comments=comments,
                               shares=shares, raw=raw,
                               retention_pct=retention_pct,
                               watch_time_seconds=watch_time_seconds,
                               audience_country_top=audience_country_top)

    def top_performers(self, metric="views", limit=5):
        return top_performers(metric=metric, limit=limit)

    def metrics_history(self, target_type, target_id, platform=None):
        return metrics_history(target_type, target_id, platform=platform)

    def mark_removed(self, content_key, target_type, platform,
                      last_known_views=None, recheck_after_days=14):
        return mark_removed(content_key, target_type, platform,
                             last_known_views=last_known_views,
                             recheck_after_days=recheck_after_days)

    def clear_removed(self, content_key, platform):
        return clear_removed(content_key, platform)

    def removed_content_map(self, platform):
        return removed_content_map(platform)

    def is_due_for_recheck(self, removal_row):
        return is_due_for_recheck(removal_row)

    def removal_rate_by_show(self, platform=None):
        return removal_rate_by_show(platform=platform)

    # ── source health (roadmap item Q) — wrapped so acquire scripts that
    # take a PipelineDB INSTANCE as their `db` parameter (rss_news_pull.py's
    # process_feed, fetch_youtube_trailers.py's process_channel) can call
    # these the same way they call every other db.<method>() on that
    # instance, rather than needing a second `import db as db_module`
    # just for these two calls. ──
    def record_source_event(self, source_id, event_type):
        return record_source_event(source_id, event_type)

    def get_source_health(self, platform, handle):
        return get_source_health(platform, handle)


if __name__ == "__main__":
    init_db()
    counts = stats()
    if not counts:
        print(f"{DB_PATH}: no posts tracked yet.")
    else:
        print(f"{DB_PATH} — posts by current status:")
        for status, n in counts.items():
            print(f"  {status:12s} {n}")
