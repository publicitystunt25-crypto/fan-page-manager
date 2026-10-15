"""Render worker: runs in GitHub Actions (see .github/workflows/render.yml).

For every job in jobs.json marked  "render": "remote"  that has no manifest yet, this
  1. downloads the clip once,
  2. renders each variant (a few at a time),
  3. uploads the videos to Cloudflare R2 under  videos/<job id>/<n>.mp4,
  4. writes  manifests/<job id>.json  listing each video's public URL, caption and target pages.
The bot (app.py) waits for that manifest and then does the posting. Old videos are deleted on every run.

Environment (GitHub secrets):  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
For a local test set  LOCAL_STORE=<folder>  instead; files are then written there and nothing is uploaded.
"""
import os
import sys
import json
import time
import shutil
import traceback
from concurrent.futures import ProcessPoolExecutor

import clip_render as cr

HERE = os.path.dirname(os.path.abspath(__file__))
JOBS_FILE = os.path.join(HERE, "jobs.json")
CONFIG_FILE = os.path.join(HERE, "render_config.json")
VIDEO_MAX_AGE_HOURS = 36


class Store:
    """R2 (S3 API) or a local folder, with the few operations the worker needs."""

    def __init__(self):
        self.local = os.environ.get("LOCAL_STORE")
        if self.local:
            os.makedirs(self.local, exist_ok=True)
            return
        import boto3
        self.bucket = os.environ["R2_BUCKET"]
        self.s3 = boto3.client(
            "s3", region_name="auto",
            endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])

    def exists(self, key):
        if self.local:
            return os.path.exists(os.path.join(self.local, key))
        from botocore.exceptions import ClientError
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def put_file(self, key, path, content_type):
        if self.local:
            dst = os.path.join(self.local, key)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copyfile(path, dst)
        else:
            self.s3.upload_file(path, self.bucket, key, ExtraArgs={"ContentType": content_type})

    def put_json(self, key, obj):
        data = json.dumps(obj, indent=2).encode("utf-8")
        if self.local:
            dst = os.path.join(self.local, key)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            open(dst, "wb").write(data)
        else:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType="application/json",
                               CacheControl="no-store")

    def delete_key(self, key):
        if self.local:
            p = os.path.join(self.local, key)
            if os.path.exists(p):
                os.remove(p)
        else:
            self.s3.delete_object(Bucket=self.bucket, Key=key)

    def delete_old(self, prefix, max_age_hours):
        """Delete objects under prefix older than max_age_hours. Returns how many were removed."""
        cutoff = time.time() - max_age_hours * 3600
        removed = 0
        if self.local:
            base = os.path.join(self.local, prefix)
            for root, _, files in os.walk(base):
                for f in files:
                    p = os.path.join(root, f)
                    if os.path.getmtime(p) < cutoff:
                        os.remove(p)
                        removed += 1
            return removed
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["LastModified"].timestamp() < cutoff:
                    self.s3.delete_object(Bucket=self.bucket, Key=obj["Key"])
                    removed += 1
        return removed


def render_one(args):
    """Runs in a worker process. Returns the rendered file's path."""
    src, slug, platform, v, strip = args
    name = cr.render_clip(
        src, slug, platform, cr.clean_hook(v.get("text", "")),
        hook_top=v.get("hook_top"), hook_size=v.get("hook_size", 76), hook_seconds=0,
        crop_factor=v.get("crop", 1.0), crop_x=v.get("crop_x", 0.5), crop_y=v.get("crop_y", 0.5),
        strip_lift=v.get("strip_lift"), strip_pos=v.get("strip_pos", "bottom"),
        trim_start=v.get("trim_start", 0.0), tone=v.get("tone"), strip=strip)
    return os.path.join(cr.CLIP_DIR, name)


def process_job(job, store, public_base, workers):
    jid = job["id"]
    url = job["url"]
    print(f"[render] {jid}: downloading {url}", flush=True)
    src, info = cr.download_clip(url)
    platform = "kick" if "kick.com" in url else "twitch" if "twitch.tv" in url else "custom"
    slug = cr.channel_slug_from(url, info, job.get("channel")) if platform != "custom" else ""
    footer = (job.get("footer") or "").strip()
    variants = job["variants"]
    # strip for the bottom bar: None = platform strip (Kick/Twitch), "none", or {"label","text","color"}.
    # A direct video file has no platform, so it defaults to no strip unless the job asks for one.
    job_strip = job.get("strip", "none" if platform == "custom" else None)
    print(f"[render] {jid}: rendering {len(variants)} variant(s) as {slug}/{platform}", flush=True)

    with ProcessPoolExecutor(max_workers=max(1, min(workers, len(variants)))) as pool:
        paths = list(pool.map(render_one, [(src, slug, platform, v, v.get("strip", job_strip)) for v in variants]))

    items = []
    for i, (v, path) in enumerate(zip(variants, paths)):
        key = f"videos/{jid}/{i}.mp4"
        store.put_file(key, path, "video/mp4")
        text = (v.get("text") or "").strip()
        caption = (v.get("caption") or text).strip() + (f"\n\n{footer}" if footer else "")
        items.append({"page_ids": [int(p) for p in v["page_ids"]], "caption": caption,
                      "url": f"{public_base}/{key}",
                      "tags": [str(t).lstrip("@") for t in v.get("tags", [])][:20],
                      "collaborators": [str(c).lstrip("@") for c in v.get("collaborators", [])][:3]})
        os.remove(path)
        print(f"[render] {jid}: uploaded variant {i + 1}/{len(variants)}", flush=True)
    os.remove(src)
    # the manifest goes up last: the bot starts posting as soon as it appears
    store.put_json(f"manifests/{jid}.json", {"job": jid, "created": int(time.time()), "items": items})
    # an original that was uploaded to our own bucket is no longer needed once every version is rendered
    if job.get("delete_source", True) and url.startswith(public_base + "/"):
        try:
            store.delete_key(url[len(public_base) + 1:])
            print(f"[render] {jid}: removed the uploaded original", flush=True)
        except Exception as e:
            print(f"[render] {jid}: could not remove the original: {type(e).__name__}", flush=True)
    print(f"[render] {jid}: done", flush=True)


def main():
    try:
        jobs = json.load(open(JOBS_FILE, encoding="utf-8"))
    except FileNotFoundError:
        print("no jobs.json")
        return 0
    cfg = json.load(open(CONFIG_FILE, encoding="utf-8")) if os.path.exists(CONFIG_FILE) else {}
    public_base = (os.environ.get("R2_PUBLIC_BASE") or cfg.get("public_base") or "").rstrip("/")
    if not public_base:
        print("render_config.json has no public_base yet - nothing to do")
        return 0
    store = Store()
    removed = store.delete_old("videos/", VIDEO_MAX_AGE_HOURS)
    print(f"[render] cleanup: removed {removed} old video(s)", flush=True)

    workers = int(os.environ.get("RENDER_WORKERS", "3"))
    failed = 0
    for job in jobs:
        if job.get("render") != "remote" or not job.get("id") or not job.get("variants"):
            continue
        if store.exists(f"manifests/{job['id']}.json"):
            continue
        try:
            process_job(job, store, public_base, workers)
        except Exception:
            failed += 1
            print(f"[render] {job['id']} FAILED:\n{traceback.format_exc()}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
