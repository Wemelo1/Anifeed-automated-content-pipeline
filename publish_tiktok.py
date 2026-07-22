"""
TikTok Publisher
------------------
Publishes finished clips and news digests to TikTok via the Content Posting API
(https://developers.tiktok.com/doc/content-posting-api-reference-upload-video).

SAFETY DEFAULT: privacy_level defaults to SELF_ONLY. Note this is ALSO
enforced server-side by TikTok for apps that haven't passed audit — so even
if you change PRIVACY_LEVEL below, an unaudited app will be forced back to
SELF_ONLY by TikTok regardless. Don't rely on this script alone for privacy
guarantees once your app is audited; re-check this default deliberately at
that point.

Uses the FILE_UPLOAD source type with TikTok's chunked upload protocol
(PUT requests with Content-Range headers). video_cover_timestamp_ms is
pulled from thumbnail_timestamp_ms.txt (written by generate_thumbnails.py)
so the cover frame matches what was picked for YouTube's static thumbnail.

Reads from db.py's ledger — only content at 'thumbnailed' stage is eligible.
Every attempt is logged via db.py; a FAILED attempt is terminal (no retry
loop) — EXCEPT a 429 rate-limit response, which is never logged as a
failure at all (the content is fine, TikTok just needs a breather) and
instead stops the run early so remaining/rate-limited items are picked up
automatically on the next run. See publish_youtube.py's QUOTA_EXCEEDED
handling for the same pattern.

SETUP:
  pip install requests python-dotenv
  1. Register an app at https://developers.tiktok.com, add the
     "Content Posting API" product, get a client_key/client_secret.
  2. Complete the OAuth flow once to get a refresh_token, store it as
     TIKTOK_REFRESH_TOKEN in .env (access tokens are refreshed automatically
     from there — TikTok access tokens are short-lived).

USAGE:
  python publish_tiktok.py
"""

import os
import time
import requests
from dotenv import load_dotenv
import db
from refusal_guard import contains_refusal

load_dotenv()

CLIENT_KEY = os.getenv("TIKTOK_CLIENT_KEY")
CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("TIKTOK_REFRESH_TOKEN")

# SAFE DEFAULT — also enforced server-side for unaudited apps regardless of this value.
PRIVACY_LEVEL = os.getenv("TIKTOK_PRIVACY_LEVEL", "SELF_ONLY")

TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"
INIT_URL = "https://open.tiktokapis.com/v2/post/publish/video/init/"
STATUS_URL = "https://open.tiktokapis.com/v2/post/publish/status/fetch/"

CHUNK_SIZE_BYTES = 10 * 1024 * 1024  # TikTok minimum recommended chunk is 5MB; use 10MB
MAX_STATUS_POLLS = 10
STATUS_POLL_INTERVAL_SECONDS = 3

# TikTok limits each user access token to 6 requests/minute (Content Posting
# API). A short pause between items keeps a normal run well under that even
# when several items fail fast (a failed init call returns almost instantly,
# so back-to-back failures can burn through the limit far quicker than
# back-to-back successes, which are naturally paced by the upload+poll time).
DELAY_BETWEEN_ITEMS_SECONDS = 8


class RateLimitError(Exception):
    """Raised on HTTP 429 specifically — distinct from a genuine failure.
    This must NEVER be logged as a terminal failure in db.py: the content
    itself is fine, TikTok just needs a breather. Treated like
    publish_youtube.py's QUOTA_EXCEEDED handling — stop the run, leave the
    item unlogged so it's picked up again automatically next run."""
    pass


def _check_response(resp, context):
    """Raises with TikTok's actual JSON error body included, instead of the
    generic 'N Client Error' message raise_for_status() would give — that
    message alone was hiding exactly what TikTok objected to."""
    if resp.status_code == 429:
        raise RateLimitError(f"{context}: 429 rate limited — {resp.text[:500]}")
    if resp.status_code >= 400:
        raise RuntimeError(f"{context}: {resp.status_code} — {resp.text[:500]}")
    return resp


def get_access_token():
    if not (CLIENT_KEY and CLIENT_SECRET and REFRESH_TOKEN):
        raise RuntimeError(
            "TIKTOK_CLIENT_KEY / TIKTOK_CLIENT_SECRET / TIKTOK_REFRESH_TOKEN "
            "must be set in .env. Complete the OAuth flow once to get a refresh token."
        )
    resp = requests.post(TOKEN_URL, data={
        "client_key": CLIENT_KEY,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
    }, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"Token refresh failed: {data}")
    return data["access_token"]


def parse_tiktok_caption(caption_path):
    """caption_final.txt is a single blob: hook text + hashtags."""
    with open(caption_path, "r", encoding="utf-8") as f:
        return f.read().strip()[:2200]  # TikTok caption limit


def get_cover_timestamp_ms(content_dir):
    ts_path = os.path.join(content_dir, "thumbnail_timestamp_ms.txt")
    if os.path.isfile(ts_path):
        with open(ts_path, "r") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 1000
    return 1000


def upload_video(access_token, video_path, caption, cover_timestamp_ms):
    """Init a FILE_UPLOAD post, PUT the video in chunks, poll for completion.
    Returns the publish_id."""
    file_size = os.path.getsize(video_path)

    # TikTok requires chunk_size to exactly equal video_size whenever
    # total_chunk_count is 1 — which can happen both when the file is
    # smaller than CHUNK_SIZE_BYTES AND in the edge case where it's only
    # slightly larger (floor division below still rounds to 1). Handling
    # both together avoids sending a chunk_size that doesn't match
    # file_size when there's only a single chunk.
    total_chunks = max(1, file_size // CHUNK_SIZE_BYTES)
    if total_chunks == 1:
        chunk_size = file_size
    else:
        chunk_size = CHUNK_SIZE_BYTES
        # TikTok requires FLOOR division here, not ceiling: the final chunk
        # is allowed to be LARGER than chunk_size (up to 128MB) to absorb
        # whatever bytes are left over, rather than there being one extra,
        # smaller trailing chunk. TikTok's own documented example is
        # video_size=50000123 / chunk_size=10000000 -> total_chunk_count=5,
        # not 6 — ceiling division here is exactly what was producing
        # "invalid_params: The total chunk count is invalid".
        total_chunks = file_size // chunk_size

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=UTF-8",
    }

    init_body = {
        "post_info": {
            "title": caption,
            "privacy_level": PRIVACY_LEVEL,
            "disable_duet": False,
            "disable_comment": False,
            "disable_stitch": False,
            "video_cover_timestamp_ms": cover_timestamp_ms,
        },
        "source_info": {
            "source": "FILE_UPLOAD",
            "video_size": file_size,
            "chunk_size": chunk_size,
            "total_chunk_count": total_chunks,
        },
    }

    r = requests.post(INIT_URL, headers=headers, json=init_body, timeout=20)
    _check_response(r, "Init")
    init_data = r.json()
    if init_data.get("error", {}).get("code") not in (None, "ok"):
        raise RuntimeError(f"Init failed: {init_data}")

    publish_id = init_data["data"]["publish_id"]
    upload_url = init_data["data"]["upload_url"]

    with open(video_path, "rb") as f:
        offset = 0
        for chunk_index in range(total_chunks):
            is_last = (chunk_index == total_chunks - 1)
            # Last chunk reads everything remaining (chunk_size + any
            # leftover bytes), matching the total_chunk_count math above —
            # every other chunk reads exactly chunk_size.
            read_size = (file_size - offset) if is_last else chunk_size
            chunk = f.read(read_size)
            if not chunk:
                break
            start = offset
            end = offset + len(chunk) - 1
            put_headers = {
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Content-Type": "video/mp4",
            }
            pr = requests.put(upload_url, headers=put_headers, data=chunk, timeout=120)
            if pr.status_code == 429:
                raise RateLimitError(f"Chunk {chunk_index} upload: 429 rate limited — {pr.text[:500]}")
            if pr.status_code not in (200, 201, 206):
                raise RuntimeError(f"Chunk {chunk_index} upload failed: {pr.status_code} {pr.text[:300]}")
            offset += len(chunk)

    # Poll publish status — catches init-succeeded-but-processing-failed cases
    for _ in range(MAX_STATUS_POLLS):
        time.sleep(STATUS_POLL_INTERVAL_SECONDS)
        sr = requests.post(STATUS_URL, headers=headers,
                            json={"publish_id": publish_id}, timeout=20)
        if sr.status_code == 429:
            raise RateLimitError(f"Status poll: 429 rate limited — {sr.text[:500]}")
        if sr.status_code != 200:
            continue
        status_data = sr.json().get("data", {})
        status = status_data.get("status")
        if status == "PUBLISH_COMPLETE":
            return publish_id
        if status in ("FAILED",):
            raise RuntimeError(f"Publish failed: {status_data}")

    # Didn't reach a terminal state in time — not necessarily a failure
    # (TikTok processing can be slow), but we can't confirm success either.
    raise RuntimeError(f"Publish status unconfirmed after polling (publish_id={publish_id}); "
                        f"check TikTok Studio manually before assuming failure")


def main():
    db.init_db()

    try:
        access_token = get_access_token()
    except RuntimeError as e:
        print(f"Cannot authenticate: {e}")
        return

    posts = db.get_publishable_posts("tiktok", min_stage="thumbnailed")
    digests = db.get_publishable_digests("tiktok", min_stage="thumbnailed")

    if not posts and not digests:
        print("Nothing new to publish to TikTok.")
        return

    print(f"Publishing {len(posts)} clip(s) and {len(digests)} digest(s) to TikTok "
          f"(privacy={PRIVACY_LEVEL})...\n")

    rate_limited = False

    for post in posts:
        if rate_limited:
            break
        key = post["key"]
        post_dir = os.path.join("finished", post["channel"], post["post_id"])
        video_path = os.path.join(post_dir, "video.mp4")
        caption_path = os.path.join(post_dir, "caption_final.txt")

        if not os.path.isfile(video_path) or not os.path.isfile(caption_path):
            print(f"  {key}: SKIPPED — missing video or caption file")
            continue

        caption = parse_tiktok_caption(caption_path)
        if contains_refusal(caption):
            print(f"  {key}: SKIPPED — caption looks like an LLM refusal. "
                  f"Fix with reset_stage.py stage <key> captioned, then re-run.")
            continue
        cover_ms = get_cover_timestamp_ms(post_dir)
        print(f"  {key}: uploading...")
        try:
            publish_id = upload_video(access_token, video_path, caption, cover_ms)
            db.log_publish(key, "clip", "tiktok", "success",
                            platform_content_id=publish_id, privacy_level=PRIVACY_LEVEL)
            print(f"    -> published (publish_id={publish_id}, {PRIVACY_LEVEL})")
        except RateLimitError as e:
            print(f"    RATE LIMITED — stopping this run. NOT marked failed, "
                  f"will retry automatically next run. ({e})")
            rate_limited = True
            break
        except (RuntimeError, requests.RequestException) as e:
            print(f"    FAILED (terminal, will not retry): {e}")
            db.log_publish(key, "clip", "tiktok", "failed", error_message=str(e))

        time.sleep(DELAY_BETWEEN_ITEMS_SECONDS)

    for digest in digests:
        if rate_limited:
            break
        key = digest["digest_id"]
        digest_dir = os.path.join("finished", "news", key)
        video_path = os.path.join(digest_dir, "video.mp4")
        caption_path = os.path.join(digest_dir, "caption_final.txt")

        if not os.path.isfile(video_path) or not os.path.isfile(caption_path):
            print(f"  {key}: SKIPPED — missing video or caption file")
            continue

        caption = parse_tiktok_caption(caption_path)
        if contains_refusal(caption):
            print(f"  {key}: SKIPPED — caption looks like an LLM refusal. "
                  f"Fix with reset_stage.py stage <key> captioned, then re-run.")
            continue
        cover_ms = get_cover_timestamp_ms(post_dir)
        print(f"  {key}: uploading...")
        try:
            publish_id = upload_video(access_token, video_path, caption, cover_ms)
            db.log_publish(key, "digest", "tiktok", "success",
                            platform_content_id=publish_id, privacy_level=PRIVACY_LEVEL)
            print(f"    -> published (publish_id={publish_id}, {PRIVACY_LEVEL})")
        except RateLimitError as e:
            print(f"    RATE LIMITED — stopping this run. NOT marked failed, "
                  f"will retry automatically next run. ({e})")
            rate_limited = True
            break
        except (RuntimeError, requests.RequestException) as e:
            print(f"    FAILED (terminal, will not retry): {e}")
            db.log_publish(key, "digest", "tiktok", "failed", error_message=str(e))

        time.sleep(DELAY_BETWEEN_ITEMS_SECONDS)

    print("\nDone.")


if __name__ == "__main__":
    main()
