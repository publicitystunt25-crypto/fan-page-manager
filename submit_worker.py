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


NEUTRAL = {"crop": 1.0, "crop_x": 0.5, "crop_y": 0.5, "strip_pos": "bottom", "trim_start": 0.0,
           "tone": {"brightness": 0.0, "contrast": 1.0, "saturation": 1.0}}
TOKEN_RE = re.compile(r"[@#][A-Za-z0-9_][A-Za-z0-9_.]*")


def keeps_tokens(original, new):
    """True when every @mention and #hashtag of the original is still in the new text."""
    low = new.lower()
    return all(t.lower() in low for t in TOKEN_RE.findall(original))


def reword(caption, overlay, n, key):
    """n different wordings of the caption (and of the on-screen text, if there is one) from Claude, or None when it
    cannot be done. Meaning, tone, @mentions and #hashtags are kept; nothing new is added."""
    if not key or n <= 0:
        return None
    try:
        import anthropic
        want_overlay = bool(overlay)
        msg = anthropic.Anthropic(api_key=key).messages.create(
            model="claude-haiku-5-5", max_tokens=4000, messages=[{"role": "user", "content":
                f"Rewrite this Instagram caption {n} different ways, for {n} different pages posting the same video.\n"
                f"Caption: {caption}\n" + (f"On-screen text (a short line over the video): {overlay}\n" if want_overlay else "") +
                "Rules: keep the same meaning and tone; do not add facts, names, claims, links or new hashtags; keep every "
                "@mention and #hashtag exactly as written; keep a similar length; at most one emoji; every version must "
                "read clearly differently from the others and from the original (different word order, phrasing or "
                "opening), not just a synonym swap.\n"
                'Output only JSON: {"captions": [...]' + (', "overlays": [...]' if want_overlay else "") + "}, "
                f"each list with exactly {n} items."}])
        raw = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
        data = json.loads(raw[raw.index("{"): raw.rindex("}") + 1])
        caps = [str(c).strip() for c in data.get("captions", [])]
        ovs = [str(o).strip()[:80] for o in data.get("overlays", [])] if want_overlay else [overlay] * n
        if len(caps) < n or len(ovs) < n:
            return None
        out = []
        for i in range(n):          # a version that lost a mention or hashtag falls back to the original wording
            c = caps[i] if caps[i] and keeps_tokens(caption, caps[i]) else caption
            o = ovs[i] if ovs[i] and keeps_tokens(overlay, ovs[i]) else overlay
            out.append((c[:2200], o))
        return out
    except Exception as e:
        print(f"[submit] could not reword: {type(e).__name__}: {str(e)[:120]}")
        return None


def unique_variants(spec_id, pages, caption, overlay, unique, key):
    """First page: exactly what was uploaded. Other pages (when `unique`): own caption wording, own on-screen text, and
    own crop / trim / tone / strip position, the same kind of per-page variation the stream clips get."""
    copy = [{"overlay": overlay, "caption": caption} for _ in pages]
    variants = af.make_variants({"id": spec_id}, pages, copy)
    if not unique:
        for v in variants:
            v.update(NEUTRAL)
        return variants, "all identical"
    variants[0].update(NEUTRAL)
    words = reword(caption, overlay, len(pages) - 1, key)
    if words:
        for v, (c, o) in zip(variants[1:], words):
            v["caption"], v["text"] = c, o
    return variants, ("first as uploaded, others reworded and reframed" if words else
                      "first as uploaded, others reframed (caption not reworded)")


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
    variants, how = unique_variants(spec["id"], pages, caption, overlay, bool(spec.get("unique", True)),
                                    os.environ.get("ANTHROPIC_API_KEY"))
    print(f"[submit] {spec['id']}: {how}", flush=True)
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
