"""
YouTube Publisher
------------------
Publishes finished clips and news digests to YouTube as Shorts.

SAFETY DEFAULT: uploads are PRIVACY_STATUS (default "unlisted") — never
public — until you've verified the pipeline end-to-end and choose to
change it. See PRIVACY_STATUS below.

NATIVE SCHEDULING (opt-in, off by default — see YOUTUBE_USE_SCHEDULING):
publish_scheduler.py computes a recommended release time for every ready
item and records it in db.py's publish_schedule table, but this script
does NOT act on it unless YOUTUBE_USE_SCHEDULING=1 is explicitly set.
This is deliberate, not an oversight: YouTube's API only supports
scheduling by uploading as privacyStatus="private" with a future
status.publishAt, which YouTube then auto-flips to PUBLIC (not to
whatever PRIVACY_STATUS says) at that time. Enabling this means scheduled
videos WILL go public automatically — a real change from today's "stays
unlisted forever" safe default. Leave this off until you've verified the
scheduler's recommended times are actually sane (see schedule_status.py)
and you're ready to let the pipeline go public on its own schedule.

When scheduling is off (default): behavior is 100% unchanged from before
this feature existed — immediate upload at PRIVACY_STATUS, no publishAt.
The scheduler's recommended time is still recorded and visible via
schedule_status.py, purely as information, and publish_schedule's status
for that item stays 'pending' rather than flipping to 'scheduled'.

Reads from db.py's ledger (synced from the filesystem by generate_thumbnails.py
or `python db.py sync`) rather than re-scanning finished/ itself, so it only
ever sees content that's reached the 'thumbnailed' stage.

Every attempt — success or failure — is written to publish_log via db.py.
A FAILED upload is terminal: it will not be retried on the next run. This
is intentional (prevents infinite retry loops on a permanently broken
asset); if the failure was transient, clear the publish_log row manually
and re-run.

If the API reports quota exhaustion, the run stops immediately rather than
burning further attempts that will also fail — remaining items are picked
up automatically on the next run once quota resets.

SETUP:
  pip install google-api-python-client google-auth-oauthlib google-auth-httplib2
  1. Create OAuth 2.0 credentials (Desktop app type) in Google Cloud Console,
     enable the YouTube Data API v3, download as client_secret.json.
  2. Put client_secret.json in this directory (path overridable via
     YOUTUBE_CLIENT_SECRETS env var).
  3. First run opens a browser (or prints a URL on headless/Termux) for
     one-time authorization; a token.json is cached after that.

USAGE:
  python publish_youtube.py
"""

import os
import time
import socket
import ssl

# --- ADDED: Increase global socket timeout to 5 minutes ---
# This prevents Python from dropping the connection during 
# extended periods of slow upstream bandwidth.
socket.setdefaulttimeout(300)
# ----------------------------------------------------------

from dotenv import load_dotenv
from refusal_guard import contains_refusal
import db

load_dotenv()

CLIENT_SECRETS_FILE = os.getenv("YOUTUBE_CLIENT_SECRETS", "client_secret.json")
TOKEN_FILE = os.getenv("YOUTUBE_TOKEN_FILE", "youtube_token.json")
SCOPES = ["https://www.googleapis.com/auth/youtube.upload",
          "https://www.googleapis.com/auth/youtube",
          # Read-only YouTube Analytics access (roadmap item K —
          # fetch_youtube_analytics.py). Added here, not to a separate
          # OAuth flow, so retention collection reuses this module's
          # existing token/refresh handling rather than needing its own.
          # Requires: (1) the YouTube Analytics API enabled in the same
          # Google Cloud project as this OAuth client, (2) deleting the
          # existing YOUTUBE_TOKEN_FILE once after this scope is added,
          # so the next run re-prompts for consent covering the new
          # scope — an old token minted before this line existed will
          # NOT silently gain the new permission.
          "https://www.googleapis.com/auth/yt-analytics.readonly"]

# SAFE DEFAULT — do not change to "public" until live-tested.
PRIVACY_STATUS = os.getenv("YOUTUBE_PRIVACY_STATUS", "unlisted")

# OFF BY DEFAULT — see the module docstring's NATIVE SCHEDULING section.
# Flipping this to "1" means videos with a computed schedule slot will be
# uploaded private+publishAt and go PUBLIC automatically at that time —
# a real behavior change from the PRIVACY_STATUS-forever default above.
YOUTUBE_USE_SCHEDULING = os.getenv("YOUTUBE_USE_SCHEDULING", "0") == "1"

CATEGORY_ID = "1"  # Film & Animation

# --- CHANGED: Reduced chunk size from 4MB to 1MB ---
# Smaller chunks mean less data needs to be retransmitted 
# if a network blip occurs during the upload.
CHUNK_SIZE_BYTES = 1 * 1024 * 1024  
# ---------------------------------------------------

# ── Connectivity retry (OAuth token refresh) ────────────────────────────────
# creds.refresh() below talks to oauth2.googleapis.com over HTTPS BEFORE
# any upload logic runs — a transient TLS/network blip here (seen in
# production: SSLEOFError mid-handshake on a mobile connection) used to
# crash the entire publish stage with a raw traceback and zero retry,
# even though upload_video()'s own chunked upload already has proper
# connectivity-retry handling (see ConnectivityError below). This brings
# the token refresh step up to the same standard.
_TOKEN_REFRESH_MAX_RETRIES = int(os.getenv("YOUTUBE_TOKEN_REFRESH_RETRIES", "3"))
_TOKEN_REFRESH_BACKOFF_SECONDS = float(os.getenv("YOUTUBE_TOKEN_REFRESH_BACKOFF", "2"))

_CONNECTIVITY_KEYWORDS = (
    "connection", "network", "handshake", "ssl", "tls", "timed out",
    "timeout", "eof occurred", "unreachable", "getaddrinfo",
    "name resolution", "reset by peer",
)


def _is_connectivity_error(e):
    """Broad, text-based check rather than a fixed isinstance() tuple —
    the actual exception this needs to catch (google.auth.exceptions.
    TransportError wrapping a chained urllib3/ssl error) isn't a stable
    importable type across library versions, but its message reliably
    mentions the underlying network/TLS failure."""
    if isinstance(e, (TimeoutError, ConnectionError, socket.timeout,
                       socket.gaierror, ssl.SSLError)):
        return True
    text = str(e).lower()
    return any(kw in text for kw in _CONNECTIVITY_KEYWORDS)


def _refresh_with_retry(creds):
    """Refreshes an expired OAuth token, retrying on transient network/TLS
    failures with linear backoff instead of letting a single blip crash
    the whole publish stage. Raises RuntimeError with a clear message if
    every attempt fails — main()'s existing 'except RuntimeError' handler
    already prints that cleanly instead of a raw traceback, no other
    change needed there."""
    from google.auth.transport.requests import Request

    last_error = None
    for attempt in range(1, _TOKEN_REFRESH_MAX_RETRIES + 1):
        try:
            creds.refresh(Request())
            return
        except Exception as e:
            if not _is_connectivity_error(e):
                raise  # a genuinely revoked/invalid token — no point retrying
            last_error = e
            if attempt < _TOKEN_REFRESH_MAX_RETRIES:
                wait = _TOKEN_REFRESH_BACKOFF_SECONDS * attempt
                print(f"    Token refresh attempt {attempt}/{_TOKEN_REFRESH_MAX_RETRIES} "
                      f"failed (network error: {e}); retrying in {wait:.0f}s...")
                time.sleep(wait)
    raise RuntimeError(
        f"Couldn't refresh YouTube OAuth token after {_TOKEN_REFRESH_MAX_RETRIES} "
        f"attempts due to network errors — try again next run. ({last_error})"
    )
# -----------------------------


def get_authenticated_service():
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            _refresh_with_retry(creds)
        else:
            if not os.path.exists(CLIENT_SECRETS_FILE):
                raise RuntimeError(
                    f"{CLIENT_SECRETS_FILE} not found. Download OAuth client "
                    f"credentials from Google Cloud Console first."
                )
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            # On a normal desktop, run_local_server(port=0) auto-opens a
            # browser via Python's webbrowser module. On Termux there's no
            # browser Python can launch that way, so that call raises
            # webbrowser.Error — the fix is NOT run_console() (Google
            # removed the out-of-band/copy-paste flow it depended on back
            # in google-auth-oauthlib 1.0, so that method doesn't exist on
            # any current install and can't be restored). The real headless
            # path is the same run_local_server, just with open_browser=False:
            # it still starts the local listener, but prints the auth URL
            # instead of trying to auto-launch a browser. Open that URL
            # manually in Chrome on the same device — Termux and Chrome
            # share the phone's loopback network, so the redirect back to
            # localhost still reaches this script.
            try:
                creds = flow.run_local_server(port=0)
            except Exception:
                print("\nCouldn't auto-launch a browser (expected on Termux).")
                print("Open the URL below in Chrome on this device, approve access,")
                print("then come back here — it'll pick up automatically.\n")
                creds = flow.run_local_server(port=0, open_browser=False)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return build("youtube", "v3", credentials=creds)


def get_authenticated_analytics_service():
    """Roadmap item K — builds a YouTube Analytics Data API v2 client off
    the SAME token file/refresh handling get_authenticated_service()
    already uses, rather than duplicating credential logic (same
    convention fetch_analytics.py's own docstring already describes for
    reusing this module's auth). Requires the token to have been minted
    (or refreshed) AFTER the yt-analytics.readonly scope was added to
    SCOPES above — if it raises an HttpError mentioning insufficient
    permissions, delete YOUTUBE_TOKEN_FILE and re-run this module (or
    fetch_youtube_analytics.py, which calls this same function) once to
    re-consent."""
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build as _build

    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            _refresh_with_retry(creds)
        else:
            if not os.path.exists(CLIENT_SECRETS_FILE):
                raise RuntimeError(
                    f"{CLIENT_SECRETS_FILE} not found. Download OAuth client "
                    f"credentials from Google Cloud Console first."
                )
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            try:
                creds = flow.run_local_server(port=0)
            except Exception:
                print("\nCouldn't auto-launch a browser (expected on Termux).")
                print("Open the URL below in Chrome on this device, approve access,")
                print("then come back here — it'll pick up automatically.\n")
                creds = flow.run_local_server(port=0, open_browser=False)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return _build("youtubeAnalytics", "v2", credentials=creds)


def parse_youtube_caption(caption_path):
    """caption_youtube.txt is title \\n description \\n hashtags"""
    with open(caption_path, "r", encoding="utf-8") as f:
        lines = [l for l in f.read().splitlines() if l.strip()]
    title = lines[0] if len(lines) > 0 else "Anime Short"
    description = lines[1] if len(lines) > 1 else ""
    hashtags = lines[2] if len(lines) > 2 else ""
    full_description = f"{description}\n\n{hashtags}".strip()
    return title[:100], full_description[:4900]  # YouTube hard limits


class ConnectivityError(Exception):
    """Raised when the upload can't complete due to a network-level failure
    (timeout, connection reset, DNS blip, etc.) rather than a genuine
    rejection from YouTube's API. This distinction matters: a real rejection
    (bad request, quota, etc.) is safe to log as a terminal failure, but a
    network blip should NOT be — the upload should just be retried on the
    next run, not permanently written off. Chunked resumable uploads are
    specifically designed to survive this by resuming from the last
    successfully uploaded byte, but only if the caller retries the
    connection-level exceptions too, not just HttpError."""
    pass


_CONNECTIVITY_EXCEPTION_TYPES = (
    TimeoutError,
    ConnectionError,
    socket.timeout,
    socket.gaierror,
    ssl.SSLError,
)


def upload_video(youtube, video_path, title, description, thumbnail_path=None, publish_at=None):
    """Chunked resumable upload. Returns the video ID.
    Raises QuotaExceededError-equivalent (RuntimeError tagged) on quota errors
    so the caller can stop the whole run rather than attempt further uploads
    that would also fail against the same exhausted quota. Raises
    ConnectivityError on network-level failures after retries are
    exhausted, so the caller does NOT log this as a terminal failure.

    publish_at, if given, is an ISO 8601 UTC datetime string — when set,
    the upload goes out as privacyStatus="private" with status.publishAt
    set to that time, which YouTube auto-flips to PUBLIC at that moment
    (not to PRIVACY_STATUS — see this module's NATIVE SCHEDULING docstring
    section for why that distinction matters). When None (the default),
    behavior is unchanged: immediate upload at PRIVACY_STATUS."""
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError

    status_block = {"selfDeclaredMadeForKids": False}
    if publish_at:
        status_block["privacyStatus"] = "private"
        status_block["publishAt"] = publish_at
    else:
        status_block["privacyStatus"] = PRIVACY_STATUS

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": CATEGORY_ID,
        },
        "status": status_block,
    }

    media = MediaFileUpload(video_path, chunksize=CHUNK_SIZE_BYTES, resumable=True, mimetype="video/mp4")
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    response = None
    retries = 0
    MAX_RETRIES = 5
    while response is None:
        try:
            status, response = request.next_chunk()
        except HttpError as e:
            if e.resp.status in (403,) and "quotaExceeded" in str(e):
                raise RuntimeError("QUOTA_EXCEEDED: " + str(e))
            if e.resp.status in (500, 502, 503, 504) and retries < MAX_RETRIES:
                retries += 1
                print(f"    Server error ({e.resp.status}), retrying "
                      f"({retries}/{MAX_RETRIES})...")
                time.sleep(2 ** retries)
                continue
            raise RuntimeError(f"Upload failed: {e}")
        except _CONNECTIVITY_EXCEPTION_TYPES as e:
            # NOT an HttpError — this is a raw network failure (e.g. socket
            # timeout deep inside httplib2, before any HTTP response came
            # back at all). Resumable uploads are built to survive exactly
            # this by resuming from the last uploaded byte, as long as we
            # retry instead of letting it crash the whole script.
            if retries < MAX_RETRIES:
                retries += 1
                print(f"    Network error ({e}), retrying "
                      f"({retries}/{MAX_RETRIES})...")
                time.sleep(2 ** retries)
                continue
            raise ConnectivityError(
                f"Upload failed after {MAX_RETRIES} retries due to network "
                f"errors: {e}"
            ) from e

    video_id = response["id"]

    if thumbnail_path and os.path.isfile(thumbnail_path):
        try:
            youtube.thumbnails().set(
                videoId=video_id,
                media_body=MediaFileUpload(thumbnail_path, mimetype="image/jpeg"),
            ).execute()
        except HttpError as e:
            # Thumbnail failure shouldn't fail the whole upload — video is
            # already live (unlisted). Log and move on.
            print(f"    WARNING: thumbnail set failed: {e}")
        except _CONNECTIVITY_EXCEPTION_TYPES as e:
            print(f"    WARNING: thumbnail set failed (network error): {e}")

    return video_id


def _resolve_publish_at(content_key):
    """Returns an ISO 8601 UTC string to use as publishAt, or None to
    upload immediately — the single gate everything in main() goes
    through, so the "off by default" behavior is enforced in one place
    rather than duplicated per call site. Returns None (immediate upload)
    unless ALL of: YOUTUBE_USE_SCHEDULING is on, a publish_schedule row
    exists for this content on youtube, that row's status is still
    'pending' (not already 'scheduled' or 'cancelled'), and its
    scheduled_at is actually in the future (a slot computed hours ago that
    the pipeline is only now getting around to uploading shouldn't be
    backdated into an invalid past publishAt — just upload it now instead)."""
    if not YOUTUBE_USE_SCHEDULING:
        return None
    entry = db.get_schedule_entry(content_key, "youtube")
    if not entry or entry["status"] != "pending":
        return None
    from datetime import datetime, timezone
    try:
        scheduled_dt = datetime.fromisoformat(entry["scheduled_at"])
    except ValueError:
        return None
    if scheduled_dt <= datetime.now(timezone.utc):
        return None
    return entry["scheduled_at"]


def main():
    db.init_db()

    try:
        youtube = get_authenticated_service()
    except RuntimeError as e:
        print(f"Cannot authenticate: {e}")
        return
    except ImportError:
        print("Missing dependencies. Run: pip install google-api-python-client "
              "google-auth-oauthlib google-auth-httplib2")
        return

    posts = db.get_publishable_posts("youtube", min_stage="thumbnailed")
    digests = db.get_publishable_digests("youtube", min_stage="thumbnailed")

    if not posts and not digests:
        print("Nothing new to publish to YouTube.")
        return

    print(f"Publishing {len(posts)} clip(s) and {len(digests)} digest(s) to YouTube "
          f"(privacy={PRIVACY_STATUS}"
          f"{', native scheduling ON — scheduled items will go PUBLIC automatically' if YOUTUBE_USE_SCHEDULING else ''})...\n")

    quota_hit = False

    for post in posts:
        if quota_hit:
            break
        key = post["key"]
        post_dir = os.path.join("finished", post["channel"], post["post_id"])
        video_path = os.path.join(post_dir, "video.mp4")
        caption_path = os.path.join(post_dir, "caption_youtube.txt")
        thumb_path = os.path.join(post_dir, "thumbnail.jpg")

        if not os.path.isfile(video_path) or not os.path.isfile(caption_path):
            print(f"  {key}: SKIPPED — missing video or caption file")
            continue

        title, description = parse_youtube_caption(caption_path)
        if contains_refusal(title) or contains_refusal(description):
            print(f"  {key}: SKIPPED — caption looks like an LLM refusal, not "
                  f"real content ({title[:60]!r}). Not logged as failed — fix "
                  f"with reset_stage.py stage <key> captioned, then re-run.")
            continue
        publish_at = _resolve_publish_at(key)
        print(f"  {key}: uploading \"{title}\"..."
              + (f" [scheduled for {publish_at}, will go PUBLIC then]" if publish_at else ""))
        try:
            video_id = upload_video(youtube, video_path, title, description, thumb_path,
                                     publish_at=publish_at)
            effective_privacy = "private (scheduled)" if publish_at else PRIVACY_STATUS
            db.log_publish(key, "clip", "youtube", "success",
                            platform_content_id=video_id, privacy_level=effective_privacy)
            if publish_at:
                entry = db.get_schedule_entry(key, "youtube")
                db.upsert_schedule(key, "clip", "youtube", publish_at, "scheduled",
                                    entry["reasoning"] if entry else None)
            print(f"    -> https://youtube.com/watch?v={video_id} ({effective_privacy})")
        except ConnectivityError as e:
            print(f"    NETWORK ERROR — not marked failed, will retry next run. ({e})")
        except RuntimeError as e:
            if str(e).startswith("QUOTA_EXCEEDED"):
                print(f"    QUOTA EXCEEDED — stopping this run. Remaining items will "
                      f"be picked up next run once quota resets.")
                quota_hit = True
                break
            print(f"    FAILED (terminal, will not retry): {e}")
            db.log_publish(key, "clip", "youtube", "failed", error_message=str(e))

    for digest in digests:
        if quota_hit:
            break
        key = digest["digest_id"]
        digest_dir = os.path.join("finished", "news", key)
        video_path = os.path.join(digest_dir, "video.mp4")
        caption_path = os.path.join(digest_dir, "caption_youtube.txt")
        thumb_path = os.path.join(digest_dir, "thumbnail.jpg")

        if not os.path.isfile(video_path) or not os.path.isfile(caption_path):
            print(f"  {key}: SKIPPED — missing video or caption file")
            continue

        title, description = parse_youtube_caption(caption_path)
        if contains_refusal(title) or contains_refusal(description):
            print(f"  {key}: SKIPPED — caption looks like an LLM refusal, not "
                  f"real content ({title[:60]!r}). Not logged as failed — fix "
                  f"with reset_stage.py stage <key> captioned, then re-run.")
            continue
        publish_at = _resolve_publish_at(key)
        print(f"  {key}: uploading \"{title}\"..."
              + (f" [scheduled for {publish_at}, will go PUBLIC then]" if publish_at else ""))
        try:
            video_id = upload_video(youtube, video_path, title, description, thumb_path,
                                     publish_at=publish_at)
            effective_privacy = "private (scheduled)" if publish_at else PRIVACY_STATUS
            db.log_publish(key, "digest", "youtube", "success",
                            platform_content_id=video_id, privacy_level=effective_privacy)
            if publish_at:
                entry = db.get_schedule_entry(key, "youtube")
                db.upsert_schedule(key, "digest", "youtube", publish_at, "scheduled",
                                    entry["reasoning"] if entry else None)
            print(f"    -> https://youtube.com/watch?v={video_id} ({effective_privacy})")
        except ConnectivityError as e:
            print(f"    NETWORK ERROR — not marked failed, will retry next run. ({e})")
        except RuntimeError as e:
            if str(e).startswith("QUOTA_EXCEEDED"):
                print(f"    QUOTA EXCEEDED — stopping this run.")
                quota_hit = True
                break
            print(f"    FAILED (terminal, will not retry): {e}")
            db.log_publish(key, "digest", "youtube", "failed", error_message=str(e))

    print("\nDone.")


if __name__ == "__main__":
    main()