"""
Learned Config Store
----------------------
Backing store for every parameter the Learn stage (mutator.py) is allowed
to tune automatically, plus the append-only audit trail and insight-
snapshot history trend-based mutation rules need to exist at all.

WHY A NEW MODULE, NOT A config/*.yaml FILE:
This pipeline has no config/*.yaml + load_yaml() layer today — every
threshold is an os.getenv()-backed module constant living next to the
one consumer that reads it (generate_captions.py's
CAPTION_INSIGHT_MIN_SAMPLE, quality_gate.py's QUALITY_APPROVE_THRESHOLD,
generate_thumbnails.py's THUMBNAIL_BIAS_TOP_N, publish_scheduler.py's
PUBLISH_MAX_PER_DAY, etc.). Introducing a YAML config layer AND a
DB-backed mutable layer in the same change would mean two different
places a value could live with no obvious precedence rule between them.
db.py's `insights` table is already this project's "last known good
value survives process boundaries" pattern (see db.save_insight /
db.get_insight) — this module reuses the exact same pipeline.db file
(via db.get_connection()) for the same reason, just with its own two
tables, so there's still exactly ONE source of truth project-wide, not
two competing ones.

WHY A SEPARATE MODULE FROM db.py, NOT ADDED DIRECTLY TO IT:
db.py already owns pipeline STAGE state (posts/digests/publish_log/
metrics). Learned tuning state is a distinct concern per PROJECT_RULES.md's
"One Job Per Module" — same reasoning show_canon.py and
thumbnail_strategy.py already follow (their own tables/logic, db.py's
connection helper). Nothing here ever touches posts/digests/news_items —
see mutator.py's docstring for why that boundary is load-bearing, not
incidental.

TABLES:
  learned_config      current value of every mutable param. One row per
                       param_name, last-write-wins — exactly like
                       db.py's `insights` table.
  learned_config_log   append-only audit trail. EVERY mutation attempt
                       is recorded here — applied, clamped, skipped for
                       cooldown, skipped for insufficient sample, or
                       proposed-pending-manual-approval — never just the
                       ones that took effect. This is the permanent
                       answer to "why does this threshold say what it
                       says right now," the same way publish_log answers
                       "did this actually go out."
  insight_snapshots    append-only copy of performance_insights.py's
                       cached "content_strategy" insight, taken once per
                       mutator.py run. Exists because db.save_insight()/
                       db.get_insight() only ever keep the LATEST
                       computed value (an upsert, not a log) — a trend
                       rule ("is average performance moving up or down
                       across the last few computations") has nothing to
                       diff against without this. Same "append-only so
                       growth over time is queryable" principle
                       fetch_analytics.py's `metrics` table already
                       documents for view counts, applied to the insight
                       itself instead of raw platform stats.

USAGE:
  import learned_config as lc
  lc.init_schema()
  n = lc.get_param("thumbnail_bias_top_n")
  lc.propose_mutation(
      "thumbnail_bias_top_n", new_value=4,
      reasoning="face_forward beating avg by 22% at n=14",
      sample_size=14, insight_computed_at="2026-08-10T00:00:00+00:00",
  )

CREATIVE PARAMS (added — see PROJECT_notes/self-learning roadmap):
edit_color_grade_saturation, bgm_volume, hook_overlay_duration,
news_digest_speed_factor, and words_per_subtitle_cue are registered here
so mutator.py can eventually tune HOW content is produced, not just
WHEN/WHICH gets published. Registering a param here does NOT make any
consumer script use it — each consumer still needs its own small
refactor (hardcoded constant -> lc.get_param() call) before the
registry entry actually does anything. `consumer` on each entry tracks
which scripts have and haven't been migrated yet. edit_clips.py is the
first to be wired up (edit_color_grade_saturation, bgm_volume) — the
other three remain registered-but-unconsumed until their own scripts
are migrated.
"""

import json
from datetime import datetime, timezone, timedelta

import db  # reuses db.get_connection() — same pipeline.db, two new tables


def _now():
    return datetime.now(timezone.utc).isoformat()


# ── Mutable parameter registry ──────────────────────────────────────────────
# Every param mutator.py may touch is declared here, with the guardrails
# that ALWAYS apply no matter what a mutation rule computes. A rule
# proposing a value outside [floor, ceiling], or a step bigger than
# max_delta_up/max_delta_down, is CLAMPED, never rejected outright — a
# rule that's directionally right but too aggressive still makes bounded
# forward progress instead of being thrown away entirely.
#
# min_sample_to_loosen / auto_apply_loosen only matter for params flagged
# asymmetric — currently just quality_approve_threshold. See that entry's
# comment and mutator.py's rule for why loosening a safety gate gets a
# harder bar than tightening one.
#
# `consumer` documents which script currently reads this as a hardcoded
# os.getenv() constant and needs a small refactor to call get_param()
# instead — this module doesn't import or touch those scripts itself.
PARAM_REGISTRY = {
    "thumbnail_bias_top_n": {
        "default": 3, "floor": 2, "ceiling": 6,
        "max_delta_up": 1, "max_delta_down": 1,
        "min_sample_to_mutate": 8,
        "cooldown_days": 3,
        "consumer": "generate_thumbnails.py (THUMBNAIL_BIAS_TOP_N)",
        "description": "How many of the sharpest thumbnail candidates get "
                        "a Groq vision classification call per clip. Purely "
                        "a resource-spend knob, not a safety/quality gate — "
                        "lowest-risk param in the registry.",
    },
    "publish_max_per_day": {
        "default": 3, "floor": 1, "ceiling": 5,
        "max_delta_up": 1, "max_delta_down": 1,
        "min_sample_to_mutate": 15,
        "cooldown_days": 7,
        "consumer": "publish_scheduler.py (PUBLISH_MAX_PER_DAY)",
        "description": "Max non-urgent publish slots per platform per day. "
                        "Bounded to ±1/run specifically because a bad nudge "
                        "here changes the channel's actual output cadence, "
                        "not just an internal knob.",
    },
    "quality_approve_threshold": {
        "default": 75, "floor": 60, "ceiling": 90,
        "max_delta_up": 5,      # tightening — cheap, low evidence bar
        "max_delta_down": 2,    # loosening — small step even when it applies
        "min_sample_to_mutate": 15,
        "min_sample_to_loosen": 30,   # extra bar, LOOSENING direction only
        "cooldown_days": 7,
        "auto_apply_loosen": False,   # see mutator.py — never auto-applied
        "consumer": "quality_gate.py (QUALITY_APPROVE_THRESHOLD)",
        "description": "Score at/above which a rendered clip/digest is "
                        "auto-approved. Tightening is fully automatic; "
                        "loosening is only ever PROPOSED and logged — a "
                        "human applies it via apply_proposal() after "
                        "reviewing the reasoning, same manual-confirmation "
                        "posture reset_stage.py uses for destructive ops.",
    },
    # ── Creative-production params (self-learning loop expansion) ──────────
    # These feed HOW a clip/digest is rendered, not WHEN/WHICH gets
    # published/selected. Wider default_delta tolerances than the
    # scheduling params above since a wrong creative value degrades
    # aesthetics, not output cadence or safety — but still bounded, since
    # a runaway saturation/volume value would be visibly bad.
    "edit_color_grade_saturation": {
        "default": 1.35, "floor": 1.0, "ceiling": 1.8,
        "max_delta_up": 0.15, "max_delta_down": 0.15,
        "min_sample_to_mutate": 20,
        "cooldown_days": 7,
        "consumer": "edit_clips.py (color_grade saturation)",
        "description": "Saturation multiplier in edit_clips.py's cinematic "
                        "color grade eq= filter. Tuned once "
                        "performance_insights.py can bucket avg views by "
                        "the saturation value baked into each clip's "
                        "render_params_json.",
    },
    "bgm_volume": {
        "default": 0.18, "floor": 0.05, "ceiling": 0.35,
        "max_delta_up": 0.05, "max_delta_down": 0.05,
        "min_sample_to_mutate": 20,
        "cooldown_days": 7,
        "consumer": "edit_clips.py (BGM_VOLUME)",
        "description": "Background music volume relative to original clip "
                        "audio, mixed in edit_clips.py's amix filter. "
                        "Separate from build_news_digest.py's own "
                        "BGM_VOLUME env var (digest BGM is mixed post-"
                        "speedup, a different filter chain) — this param "
                        "only governs clips.",
    },
    "hook_overlay_duration": {
        "default": 2.2, "floor": 1.0, "ceiling": 4.0,
        "max_delta_up": 0.4, "max_delta_down": 0.4,
        "min_sample_to_mutate": 20,
        "cooldown_days": 7,
        "consumer": "add_hook_overlay.py (HOOK_DURATION_SECONDS) — "
                     "NOT YET MIGRATED, registered ahead of the refactor",
        "description": "How many seconds the burned-in hook text overlay "
                        "stays on screen at the start of a clip.",
    },
    "news_digest_speed_factor": {
        "default": 1.25, "floor": 1.0, "ceiling": 1.6,
        "max_delta_up": 0.1, "max_delta_down": 0.1,
        "min_sample_to_mutate": 15,
        "cooldown_days": 7,
        "consumer": "build_news_digest.py (DIGEST_SPEED_FACTOR) — "
                     "NOT YET MIGRATED, registered ahead of the refactor",
        "description": "Global narration/video speedup applied to the "
                        "whole stitched news digest in speed_up_video().",
    },
    "words_per_subtitle_cue": {
        "default": 4, "floor": 2, "ceiling": 7,
        "max_delta_up": 1, "max_delta_down": 1,
        "min_sample_to_mutate": 15,
        "cooldown_days": 7,
        "consumer": "burn_subtitles.py (MAX_WORDS_PER_CUE) — "
                     "NOT YET MIGRATED, registered ahead of the refactor",
        "description": "Max words grouped into one on-screen subtitle cue "
                        "for clip captions.",
    },
}


def init_schema():
    conn = db.get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS learned_config (
            param_name   TEXT PRIMARY KEY,
            value_json   TEXT NOT NULL,
            updated_at   TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS learned_config_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            param_name          TEXT NOT NULL,
            old_value_json      TEXT,
            proposed_value_json TEXT NOT NULL,
            applied_value_json  TEXT,           -- NULL unless status='applied'
            status              TEXT NOT NULL,  -- applied | clamped_and_applied |
                                                 -- skipped_cooldown |
                                                 -- skipped_insufficient_sample |
                                                 -- skipped_no_change |
                                                 -- proposed_pending_approval |
                                                 -- approved_manually
            reasoning           TEXT,
            sample_size         INTEGER,
            insight_computed_at TEXT,
            created_at          TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_learned_log_param "
                 "ON learned_config_log(param_name, created_at)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS insight_snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            insight_key    TEXT NOT NULL,       -- e.g. "content_strategy"
            value_json     TEXT NOT NULL,       -- full insight dict, as-read
            computed_at    TEXT,                -- performance_insights.py's own timestamp
            snapshotted_at TEXT NOT NULL        -- when mutator.py took this copy
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_insight_snapshots_key "
                 "ON insight_snapshots(insight_key, snapshotted_at)")
    conn.commit()
    conn.close()


# ── current values ───────────────────────────────────────────────────────

def get_param(name):
    """Returns the current value for `name` — the last applied mutation,
    or PARAM_REGISTRY's default if nothing's ever been applied yet.
    Raises KeyError for an unregistered name (fail loudly here — a typo'd
    param name silently falling back to some unrelated default is worse
    than crashing the one consumer that mistyped it)."""
    if name not in PARAM_REGISTRY:
        raise KeyError(f"'{name}' is not a registered learned-config param. "
                        f"Known params: {sorted(PARAM_REGISTRY)}")
    conn = db.get_connection()
    row = conn.execute(
        "SELECT value_json FROM learned_config WHERE param_name = ?", (name,)
    ).fetchone()
    conn.close()
    if row is None:
        return PARAM_REGISTRY[name]["default"]
    return json.loads(row["value_json"])


def all_current_values():
    """Every registered param's current effective value — for a status
    view / debugging, not used by mutator.py itself."""
    return {name: get_param(name) for name in PARAM_REGISTRY}


# ── audit trail ──────────────────────────────────────────────────────────

def _log(param_name, old_value, proposed_value, applied_value, status,
         reasoning, sample_size, insight_computed_at):
    conn = db.get_connection()
    conn.execute(
        """INSERT INTO learned_config_log
           (param_name, old_value_json, proposed_value_json, applied_value_json,
            status, reasoning, sample_size, insight_computed_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (param_name, json.dumps(old_value), json.dumps(proposed_value),
         json.dumps(applied_value) if applied_value is not None else None,
         status, reasoning, sample_size, insight_computed_at, _now()),
    )
    conn.commit()
    conn.close()


def _last_applied_at(param_name):
    conn = db.get_connection()
    row = conn.execute(
        """SELECT created_at FROM learned_config_log
           WHERE param_name = ? AND status IN ('applied', 'clamped_and_applied',
                                                'approved_manually')
           ORDER BY created_at DESC LIMIT 1""",
        (param_name,),
    ).fetchone()
    conn.close()
    return row["created_at"] if row else None


def _clamp(name, spec, current, proposed):
    """Applies floor/ceiling and max_delta_up/max_delta_down, in that
    order. Returns (clamped_value, was_clamped: bool)."""
    delta = proposed - current
    if delta > 0:
        delta = min(delta, spec["max_delta_up"])
    elif delta < 0:
        delta = -min(-delta, spec["max_delta_down"])
    clamped = current + delta
    clamped = max(spec["floor"], min(spec["ceiling"], clamped))
    return clamped, (clamped != proposed)


def propose_mutation(name, new_value, reasoning, sample_size, insight_computed_at):
    """The single entry point mutator.py's rules call — every guardrail in
    the registry is enforced HERE, not left to the caller's rule logic, so
    a rule can propose anything and this function is what actually decides
    whether/how it takes effect. Always writes a learned_config_log row,
    whatever the outcome — see that table's docstring.

    Returns the status string (see learned_config_log.status for the full
    list) so mutator.py can print a one-line summary per param."""
    if name not in PARAM_REGISTRY:
        raise KeyError(f"'{name}' is not a registered learned-config param.")
    spec = PARAM_REGISTRY[name]
    current = get_param(name)

    if new_value == current:
        _log(name, current, new_value, None, "skipped_no_change",
             reasoning, sample_size, insight_computed_at)
        return "skipped_no_change"

    is_loosening = spec.get("auto_apply_loosen") is False and (
        (new_value < current) if "min_sample_to_loosen" in spec else False
    )

    min_sample_needed = spec["min_sample_to_mutate"]
    if is_loosening:
        min_sample_needed = spec.get("min_sample_to_loosen", min_sample_needed)

    if sample_size is None or sample_size < min_sample_needed:
        _log(name, current, new_value, None, "skipped_insufficient_sample",
             f"{reasoning} (needed n>={min_sample_needed}, had n={sample_size})",
             sample_size, insight_computed_at)
        return "skipped_insufficient_sample"

    last_applied = _last_applied_at(name)
    if last_applied:
        cooldown_until = datetime.fromisoformat(last_applied) + timedelta(days=spec["cooldown_days"])
        if datetime.now(timezone.utc) < cooldown_until:
            _log(name, current, new_value, None, "skipped_cooldown",
                 f"{reasoning} (cooldown until {cooldown_until.isoformat()})",
                 sample_size, insight_computed_at)
            return "skipped_cooldown"

    clamped_value, was_clamped = _clamp(name, spec, current, new_value)

    if is_loosening:
        # Never auto-applied — logged as a standing proposal for a human
        # to review and apply via apply_proposal(). Still respects the
        # clamp above so a proposed value is never outside registry bounds
        # even before a human looks at it.
        _log(name, current, clamped_value, None, "proposed_pending_approval",
             reasoning, sample_size, insight_computed_at)
        return "proposed_pending_approval"

    conn = db.get_connection()
    conn.execute(
        """INSERT INTO learned_config (param_name, value_json, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(param_name) DO UPDATE SET value_json = excluded.value_json,
                                                  updated_at = excluded.updated_at""",
        (name, json.dumps(clamped_value), _now()),
    )
    conn.commit()
    conn.close()

    status = "clamped_and_applied" if was_clamped else "applied"
    _log(name, current, new_value, clamped_value, status,
         reasoning, sample_size, insight_computed_at)
    return status


def pending_proposals():
    """Every 'proposed_pending_approval' row that hasn't since been
    approved or superseded by a newer proposal for the same param —
    the human-review queue, same role db.quality_held_items() plays for
    the quality gate."""
    conn = db.get_connection()
    rows = conn.execute(
        """SELECT * FROM learned_config_log
           WHERE status = 'proposed_pending_approval'
           AND id IN (
               SELECT MAX(id) FROM learned_config_log
               WHERE status = 'proposed_pending_approval'
               GROUP BY param_name
           )
           ORDER BY created_at DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def apply_proposal(log_id):
    """Manually applies a pending proposal by its learned_config_log id —
    the human-approval step for anything registered as auto_apply_loosen=
    False. Mirrors reset_stage.py's explicit-confirmation posture rather
    than taking a string param name (an id is unambiguous; a param name
    could have more than one historical proposal)."""
    conn = db.get_connection()
    row = conn.execute(
        "SELECT * FROM learned_config_log WHERE id = ? AND status = 'proposed_pending_approval'",
        (log_id,),
    ).fetchone()
    if row is None:
        conn.close()
        print(f"No pending proposal with id {log_id} (already applied, "
              f"superseded, or never existed).")
        return False

    name = row["param_name"]
    value = json.loads(row["proposed_value_json"])
    conn.execute(
        """INSERT INTO learned_config (param_name, value_json, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(param_name) DO UPDATE SET value_json = excluded.value_json,
                                                  updated_at = excluded.updated_at""",
        (name, json.dumps(value), _now()),
    )
    conn.commit()
    conn.close()

    _log(name, json.loads(row["old_value_json"]) if row["old_value_json"] else None,
         value, value, "approved_manually",
         f"manually approved (was log id {log_id}): {row['reasoning']}",
         row["sample_size"], row["insight_computed_at"])
    print(f"Applied {name} = {value} (approved from proposal id {log_id}).")
    return True


def log_for_param(name, limit=10):
    conn = db.get_connection()
    rows = conn.execute(
        "SELECT * FROM learned_config_log WHERE param_name = ? "
        "ORDER BY created_at DESC LIMIT ?", (name, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── insight history ──────────────────────────────────────────────────────

def snapshot_insight(insight_key, insight_dict):
    """Appends a copy of the current insight to insight_snapshots. Called
    once per mutator.py run, BEFORE any rule reads history — so a rule
    comparing 'current vs prior' is always comparing against a snapshot
    from a STRICTLY EARLIER run, never the one it's about to take itself."""
    if not insight_dict:
        return
    conn = db.get_connection()
    conn.execute(
        """INSERT INTO insight_snapshots
           (insight_key, value_json, computed_at, snapshotted_at)
           VALUES (?, ?, ?, ?)""",
        (insight_key, json.dumps(insight_dict),
         insight_dict.get("computed_at"), _now()),
    )
    conn.commit()
    conn.close()


def prior_snapshot(insight_key, before_snapshotted_at=None):
    """Returns the most recent insight_snapshots row for `insight_key`
    STRICTLY BEFORE `before_snapshotted_at` (defaults to now, i.e. 'the
    most recent one that already existed'), or None if there isn't one
    yet (first-ever run — trend rules should treat this as 'no signal',
    not as 'flat trend', since there's nothing to compare against)."""
    before = before_snapshotted_at or _now()
    conn = db.get_connection()
    row = conn.execute(
        """SELECT * FROM insight_snapshots
           WHERE insight_key = ? AND snapshotted_at < ?
           ORDER BY snapshotted_at DESC LIMIT 1""",
        (insight_key, before),
    ).fetchone()
    conn.close()
    if row is None:
        return None
    d = dict(row)
    d["value"] = json.loads(d["value_json"])
    return d