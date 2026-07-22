"""
TikTok OAuth Authorization (run once, or again whenever the refresh token expires)
------------------------------------------------------------------------------------
Mints a fresh TIKTOK_REFRESH_TOKEN by walking through TikTok's OAuth 2.0
authorization_code flow manually. Two things make this different from the
YouTube/Google flow:

  1. Termux has no browser Python can drive automatically — same as Google.
  2. UNLIKE Google, TikTok's redirect_uri must be a real HTTPS address and
     explicitly REJECTS localhost/127.0.0.1/*.local outright. There's no
     "start a local listener" trick available here.

So the process is: open the printed authorize URL in Chrome, log in and
approve, and TikTok redirects to whatever TIKTOK_REDIRECT_URI is registered
for this app in the TikTok Developer Portal. That page will likely fail to
load (unless something is actually running there) — that's fine and
expected. The `code` parameter is still visible in Chrome's address bar on
that failed page. Copy the FULL url from the address bar and paste it back
into this script when prompted.

SETUP:
  .env must already have TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET.
  .env must also have TIKTOK_REDIRECT_URI, and it must EXACTLY match a
  Redirect URI already registered for this app in the TikTok Developer
  Portal (Manage apps -> your app -> Login Kit / Redirect URI settings) —
  TikTok will reject the request otherwise. If you don't already have one
  registered, add an HTTPS URL you control (or even one you don't — the
  redirect only needs to reach Chrome's address bar, not actually load
  successfully) to the portal first, then put the same value in .env.

USAGE:
  python tiktok_authorize.py
"""

import os
import secrets
from urllib.parse import urlencode, urlparse, parse_qs
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_KEY = os.getenv("TIKTOK_CLIENT_KEY")
CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET")
REDIRECT_URI = os.getenv("TIKTOK_REDIRECT_URI")
SCOPE = "video.publish"

AUTH_URL = "https://www.tiktok.com/v2/auth/authorize/"
TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"


def main():
    if not (CLIENT_KEY and CLIENT_SECRET and REDIRECT_URI):
        print("TIKTOK_CLIENT_KEY, TIKTOK_CLIENT_SECRET, and TIKTOK_REDIRECT_URI "
              "must all be set in .env first.")
        print("TIKTOK_REDIRECT_URI must exactly match a Redirect URI already "
              "registered for this app in the TikTok Developer Portal — it "
              "must be HTTPS; TikTok rejects localhost/127.0.0.1 outright.")
        return

    state = secrets.token_urlsafe(16)
    params = {
        "client_key": CLIENT_KEY,
        "response_type": "code",
        "scope": SCOPE,
        "redirect_uri": REDIRECT_URI,
        "state": state,
    }
    url = f"{AUTH_URL}?{urlencode(params)}"

    print("=" * 70)
    print("Open this URL in Chrome on this device, log in, and approve access:")
    print("=" * 70)
    print(f"\n{url}\n")
    print("After approving, TikTok redirects to your registered redirect URI.")
    print("The page may fail to load — that's expected. Copy the FULL URL")
    print("from Chrome's address bar at that point (it will contain a 'code='")
    print("parameter) and paste it below.\n")

    pasted = input("Paste the full redirect URL here: ").strip()

    parsed = urlparse(pasted)
    qs = parse_qs(parsed.query)
    code = qs.get("code", [None])[0]
    returned_state = qs.get("state", [None])[0]

    if not code:
        print("\nCouldn't find a 'code' parameter in that URL. Make sure you "
              "pasted the URL you were redirected TO after approving, not the "
              "authorize URL printed above.")
        return

    if returned_state != state:
        print("WARNING: state mismatch — this redirect doesn't match the "
              "request this script just made. Proceeding anyway, but double "
              "check you pasted the right URL if the exchange below fails.")

    resp = requests.post(
        TOKEN_URL,
        data={
            "client_key": CLIENT_KEY,
            "client_secret": CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": REDIRECT_URI,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=20,
    )

    try:
        data = resp.json()
    except ValueError:
        print(f"\nToken exchange failed — non-JSON response ({resp.status_code}): {resp.text[:500]}")
        return

    if "refresh_token" not in data:
        print(f"\nToken exchange failed: {data}")
        return

    print("\n" + "=" * 70)
    print("SUCCESS. Update your .env — replace the old value with:")
    print("=" * 70)
    print(f"\nTIKTOK_REFRESH_TOKEN={data['refresh_token']}\n")
    refresh_days = int(data.get("refresh_expires_in", 0) / 86400)
    print(f"(access token expires in {data.get('expires_in')}s; "
          f"this new refresh token is valid for ~{refresh_days} days)")


if __name__ == "__main__":
    main()
