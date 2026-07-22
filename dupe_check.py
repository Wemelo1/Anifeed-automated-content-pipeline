import db, json

conn = db.get_connection()
rows = [dict(r) for r in conn.execute(
    """SELECT p.key, p.channel, p.post_id, p.caption_raw, p.canonical_show, p.media_id, p.thumb_hash
       FROM posts p WHERE p.published_at IS NOT NULL"""
).fetchall()]

print(f"{len(rows)} published clip(s) total.\n")

print("=== Exact media_id matches (should be impossible — telegram_pull.py Layer 1 dedup) ===")
seen_media = {}
for r in rows:
    if r["media_id"]:
        seen_media.setdefault(r["media_id"], []).append(r["key"])
for mid, keys in seen_media.items():
    if len(keys) > 1:
        print(f"  media_id={mid}: {keys}")

print("\n=== Same canonical_show, flagged for manual caption comparison ===")
by_show = {}
for r in rows:
    if r["canonical_show"]:
        by_show.setdefault(r["canonical_show"], []).append(r)
for show, items in by_show.items():
    if len(items) > 1:
        print(f"  {show}:")
        for it in items:
            print(f"    {it['key']}  caption: {(it['caption_raw'] or '')[:70]}")

print("\n=== Caption similarity > 0.5 among all published clips ===")
from db import _similarity
flagged = 0
for i in range(len(rows)):
    for j in range(i + 1, len(rows)):
        a, b = rows[i]["caption_raw"] or "", rows[j]["caption_raw"] or ""
        if not a or not b:
            continue
        s = _similarity(a, b)
        if s > 0.5:
            flagged += 1
            print(f"  {s:.2f}  {rows[i]['key']}  <->  {rows[j]['key']}")
            print(f"        \"{a[:60]}\"")
            print(f"        \"{b[:60]}\"")
if flagged == 0:
    print("  none found")

# Also check for duplicate publish_log entries per platform (same content published twice)
print("\n=== publish_log: any content_key logged more than once for the same platform? ===")
pub_rows = conn.execute(
    "SELECT content_key, platform, COUNT(*) as n FROM publish_log GROUP BY content_key, platform HAVING n > 1"
).fetchall()
if not pub_rows:
    print("  none — publish_log's PRIMARY KEY (content_key, platform) should make this structurally impossible")
for pr in pub_rows:
    print(f"  {pr['content_key']} / {pr['platform']}: logged {pr['n']} times")

conn.close()
