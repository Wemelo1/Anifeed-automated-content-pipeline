# clear_missing.py
import db, os

conn = db.get_connection()
posts = conn.execute(
    "SELECT id, raw_media_path FROM posts "
    "WHERE media_type='image' AND status='downloaded'"
).fetchall()

missing_ids = [
    p["id"] for p in posts
    if not p["raw_media_path"] or not os.path.isfile(p["raw_media_path"])
]

print(f"Marking {len(missing_ids)} posts as failed...")
conn.execute(
    f"UPDATE posts SET status='failed', fail_reason='file_missing_on_disk' "
    f"WHERE id IN ({','.join(['?']*len(missing_ids))})",
    missing_ids
)
conn.commit()
conn.close()
print("Done.")