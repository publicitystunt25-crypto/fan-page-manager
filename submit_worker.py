"""Submit worker: runs in GitHub Actions (see .github/workflows/submit.yml).

For every  submissions/<id>.json  in the bucket that has no manifest yet (written by the bot's /submit page) it renders
one version of the uploaded video per chosen page, with the same layout rules as every other clip (vertical: strip
across the bottom, text higher; widescreen: blurred top and bottom, text just above the video), the Kick/Twitch strip
the form asked for, and the caption, tags and collab invites. It then puts the manifest in R2 and adds the job to
auto/queue.json, where the bot picks it up and posts it (the bot enforces the daily limit per page).
If a job fails, submissions/<id>.status.json says so and the page shows it.

Environment: the R2 secrets (or LOCAL_STORE for a test).
"""
import os
import re
import sys
import json
import time
import traceback

import render_worker as rw
import auto_feed as af

HANDLE_RE = re.compile(r"^[A-Za-z0-9._]{1,30}$")
CHANNEL_RE = re.compile(r"^[A-Za-z0-9_]{2,40}$")


def strip_for(platform, channel):
    """The strip the page asked for, in the form the renderer takes: 'none' or {label, text, color}."""
    if platform == "kick":
        return {"label": "KICK", "text": f"KICK.COM/{channel}".upper(), "color": "#53FC18"}
    if platform == "twitch":
        return {"label": "TWITCH", "text": f"TWITCH.TV/{channel}".upper(), "color": "#9146FF"}
    return "none"


def build_job(spec, public_base):
    """Validate the stored spec again (never trust it blindly) and turn it into a render job."""
    if not str(spec.get("url", "")).startswith(public_base + "/uploads/"):
        raise ValueError("video is not from this bucket's uploads")
    platform = spec.get("platform", "none")
    channel = str(spec.get("channel", "")).lstrip("@")
    if platform not in ("kick", "twitch", "none") or (platform != "none" and not CHANNEL_RE.match(channel)):
        raise ValueError("bad strip settings")
    tags = [str(t).lstrip("@") for t in spec.get("tags", [])][:20]
    collabs = [str(c).lstrip("@") for c in spec.get("collaborators", [])][:3]
    if not all(HANDLE_RE.match(h) for h in tags + collabs):
        raise ValueError("bad username in tags or collabs")
    pages = []
    for p in spec.get("page_ids", []):                 # keep the order they were ticked in
        if int(p) not in pages:
            pages.append(int(p))
    if not pages:
        raise ValueError("no pages")
    gap = float(spec.get("gap_hours") or 0)
    if gap < 0 or gap > 24 or (0 < gap < 0.25):
        raise ValueError("bad gap")
    caption = str(spec.get("caption", "")).strip()[:2200]
    overlay = str(spec.get("overlay", "")).strip()[:80]
    copy = [{"overlay": overlay, "caption": caption}] * len(pages)
    variants = af.make_variants({"id": spec["id"]}, pages, copy)   # small per-page crop/tone differences
    for v in variants:
        v["tags"], v["collaborators"] = tags, collabs
    return {"id": spec["id"], "url": spec["url"], "render": "remote", "footer": "", "gap_hours": gap,
            "strip": strip_for(platform, channel), "variants": variants}


def main():
    store = rw.Store()
    cfg = json.load(open(rw.CONFIG_FILE, encoding="utf-8"))
    public_base = (os.environ.get("R2_PUBLIC_BASE") or cfg.get("public_base") or "").rstrip("/")
    if not public_base:
        print("no public base configured")
        return 0
    todo = []
    for key in store.list_keys("submissions/"):
        name = os.path.basename(key)
        if not name.endswith(".json") or name.endswith(".status.json"):
            continue
        sid = name[:-5]
        if store.exists(f"manifests/{sid}.json") or store.exists(f"submissions/{sid}.status.json"):
            continue
        todo.append(sid)
    if not todo:
        print("[submit] nothing waiting")
        return 0
    failed = 0
    for sid in sorted(todo):
        try:
            spec = store.get_json(f"submissions/{sid}.json")
            job = build_job(spec, public_base)
            print(f"[submit] {sid}: rendering for pages {[v['page_ids'][0] for v in job['variants']]}", flush=True)
            rw.process_job(job, store, public_base, int(os.environ.get("RENDER_WORKERS", "3")))
            queue = store.get_json("auto/queue.json", []) or []
            queue.append({"id": sid, "created": int(time.time()), "stagger_minutes": [1, 3],
                          "gap_hours": job.get("gap_hours", 0),
                          "title": spec.get("caption", "")[:60], "channel": spec.get("channel", "")})
            store.put_json("auto/queue.json", queue[-60:])
            print(f"[submit] {sid}: queued for the bot", flush=True)
        except Exception as e:
            failed += 1
            print(f"[submit] {sid} FAILED:\n{traceback.format_exc()}", flush=True)
            store.put_json(f"submissions/{sid}.status.json", {"state": "failed", "error": f"{type(e).__name__}: {str(e)[:200]}"})
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
