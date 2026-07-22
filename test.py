# run from your tiktok/ directory
import db, os
pdb = db.PipelineDB()
posts = pdb.posts_with_status("downloaded", media_type="image", limit=300)
valid = [p for p in posts if p["raw_media_path"] and os.path.isfile(p["raw_media_path"])]
missing = [p for p in posts if not p["raw_media_path"] or not os.path.isfile(p["raw_media_path"])]
print(f"Valid (file exists): {len(valid)}")
print(f"Missing file: {len(missing)}")
if missing:
    print("Sample missing paths:")
    for p in missing[:5]:
        print(f"  {p['raw_media_path']}")