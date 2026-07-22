"""
Digest State Diagnostic
-------------------------
Answers "why does my candidate count look stuck" by showing exactly what's
in pipeline.db, rather than trusting any one script's printed summary.

USAGE:
  python check_digest_state.py   (run from the same directory as pipeline.db)
"""

import db


def main():
    db.init_db()
    conn = db.get_connection()

    print("=== Post status breakdown (image posts only) ===")
    rows = conn.execute(
        "SELECT status, COUNT(*) as n FROM posts WHERE media_type='image' "
        "GROUP BY status ORDER BY n DESC"
    ).fetchall()
    total_images = 0
    for r in rows:
        print(f"  {r['status']:20s} {r['n']}")
        total_images += r["n"]
    print(f"  {'TOTAL':20s} {total_images}")

    print("\n=== Digests built ===")
    digests = conn.execute(
        "SELECT digest_id, built_at FROM digests ORDER BY built_at"
    ).fetchall()
    for d in digests:
        n = conn.execute(
            "SELECT COUNT(*) as n FROM digest_posts WHERE digest_id=?",
            (d["digest_id"],),
        ).fetchone()["n"]
        print(f"  {d['digest_id']}  built={d['built_at']}  posts_linked={n}")

    print(f"\n  Total digests: {len(digests)}")
    total_linked = conn.execute("SELECT COUNT(*) as n FROM digest_posts").fetchone()["n"]
    print(f"  Total posts linked across all digests: {total_linked}")

    print("\n=== 'downloaded' image posts (the actual current candidate pool) ===")
    downloaded = conn.execute(
        "SELECT key, caption_raw FROM posts WHERE media_type='image' AND status='downloaded' "
        "ORDER BY id"
    ).fetchall()
    print(f"  Count: {len(downloaded)}")

    print("\n=== Of those, how many get filtered out by is_duplicate_topic() "
          "right now (cross-digest caption similarity)? ===")
    pdb = db.PipelineDB()
    dup_count = 0
    for r in downloaded:
        caption = (r["caption_raw"] or "").strip()
        if caption and pdb.is_duplicate_topic(caption):
            dup_count += 1
    print(f"  Currently flagged as duplicate-of-something-already-used: {dup_count}")
    print(f"  Actually eligible as a fresh candidate right now: {len(downloaded) - dup_count}")

    conn.close()


if __name__ == "__main__":
    main()
