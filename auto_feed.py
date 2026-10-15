"""Auto feed: runs hourly in GitHub Actions (see .github/workflows/auto_feed.yml).

Picks the newest decent clip from the channels in auto_feed.json (one per run, rotating through the channels),
transcribes it, writes a caption and a different overlay line for each page, renders every version with
render_worker.process_job and puts the manifest in R2. It then appends the job to  auto/queue.json  - the bot
(app.py, _auto_feed_loop) picks new entries up from there and posts them, staggered across the pages.
State (clips already used) lives in  auto/seen.json  in R2.

Environment: the R2 secrets (or LOCAL_STORE for a test), optional ANTHROPIC_API_KEY (without it the Kick title is used).
Flags: --dry-run  only prints what it would pick, renders nothing.
"""
import os
import sys
import json
import time
import random
import traceback
from datetime import datetime, timezone

import requests

import clip_render as cr
import render_worker as rw
import transcribe_worker as tw

CONFIG = os.path.join(rw.HERE, "auto_feed.json")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def list_clips(slug):
    url = f"https://kick.com/api/v2/channels/{slug}/clips"
    params = {"sort": "date", "time": "all"}
    r = requests.get(url, params=params, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=30)
    if r.status_code != 200:
        try:                      # Kick sits behind Cloudflare: retry the way a real browser would talk
            from curl_cffi import requests as cffi
            r = cffi.get(url, params=params, impersonate="chrome", timeout=30)
        except Exception as e:
            print(f"[auto] {slug}: HTTP {r.status_code}, browser retry unavailable ({type(e).__name__})")
    if r.status_code != 200:
        print(f"[auto] {slug}: clip list not readable (HTTP {r.status_code})")
        return []
    return r.json().get("clips", [])


def pick_clip(cfg, seen, hour):
    channels = cfg["channels"]
    order = channels[hour % len(channels):] + channels[:hour % len(channels)]   # rotate so each channel gets turns
    now = datetime.now(timezone.utc)
    for slug in order:
        for c in list_clips(slug):                       # newest first
            try:
                age_h = (now - datetime.fromisoformat(c["created_at"].replace("Z", "+00:00"))).total_seconds() / 3600
            except Exception:
                continue
            title = (c.get("title") or "").strip()
            dur = c.get("duration") or 0
            why = None
            if age_h > cfg.get("max_age_hours", 6):
                break                                    # newest-first: everything after this is older still
            if c["id"] in seen:
                why = "already used"
            elif len(title) < cfg.get("min_title_chars", 15):
                why = "title too short"
            elif not (cfg.get("min_seconds", 10) <= dur <= cfg.get("max_seconds", 90)):
                why = f"length {dur}s"
            elif (c.get("view_count") or c.get("views") or 0) < cfg.get("min_views", 0):
                why = "too few views"
            elif any(w in title.lower() for w in cfg.get("skip_words", []) + cfg.get("skip_title_words", [])):
                why = "blocked word in title"
            if why:
                continue
            c["_slug"] = slug
            return c
    return None


def write_copy(clip, transcript, n):
    """n different overlay lines and captions. Claude if ANTHROPIC_API_KEY is set, else the Kick title."""
    title = clip["title"].strip()
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        try:
            import anthropic
            msg = anthropic.Anthropic(api_key=key).messages.create(
                model="claude-haiku-5-5", max_tokens=1500,
                messages=[{"role": "user", "content":
                    f"Instagram Reels from a Kick clip of the streamer '{clip['_slug']}'.\n"
                    f"Clip title: {title}\nTranscript (speech-to-text, may contain errors): {transcript[:1500]}\n\n"
                    f"Write {n} different versions. Each has an 'overlay' (on-screen text, 4 to 9 words, plain text) and a "
                    "'caption' (one or two short sentences, at most one emoji).\n"
                    "Rules: say only what the title or transcript supports, never invent facts or names, do not name anyone "
                    "that is not in the title or the streamer's own name, no @mentions, no hashtags, no quotes around the "
                    "text, no insults or claims about a real person that the clip does not show. Every version must read "
                    "differently from the others.\n"
                    'Output only JSON: {"versions":[{"overlay":"...","caption":"..."}]}'}])
            raw = msg.content[0].text
            data = json.loads(raw[raw.index("{"): raw.rindex("}") + 1])["versions"]
            data = [v for v in data if v.get("overlay") and v.get("caption")]
            if len(data) >= n:
                return data[:n]
            print(f"[auto] copywriter returned {len(data)} of {n} versions, using the title")
        except Exception as e:
            print(f"[auto] copywriter failed ({type(e).__name__}), using the title")
    base = cr.clean_hook(title) or title[:60]
    return [{"overlay": base, "caption": title} for _ in range(n)]


def make_variants(clip, cfg, copy):
    rnd = random.Random(clip["id"])
    variants = []
    for pid, c in zip(cfg["page_ids"], copy):
        variants.append({
            "page_ids": [int(pid)], "text": c["overlay"], "caption": c["caption"],
            "crop": round(rnd.uniform(0.9, 1.0), 2), "crop_x": round(rnd.uniform(0.3, 0.7), 2),
            "crop_y": round(rnd.uniform(0.4, 0.6), 2), "strip_pos": rnd.choice(["bottom", "bottom", "top"]),
            "trim_start": rnd.choice([0.0, 0.0, 0.2, 0.4]),
            "tone": {"brightness": round(rnd.uniform(0.0, 0.03), 2), "contrast": round(rnd.uniform(1.0, 1.05), 2),
                     "saturation": round(rnd.uniform(1.0, 1.06), 2)}})
    return variants


def main():
    dry = "--dry-run" in sys.argv
    cfg = json.load(open(CONFIG, encoding="utf-8"))
    if not cfg.get("enabled", True):
        print("[auto] disabled in auto_feed.json")
        return 0
    store = rw.Store()
    seen = set(store.get_json("auto/seen.json", []) or [])
    clip = pick_clip(cfg, seen, datetime.now(timezone.utc).hour)
    if not clip:
        print("[auto] no suitable new clip this hour")
        return 0
    url = f"https://kick.com/{clip['_slug']}/clips/{clip['id']}"
    print(f"[auto] picked {url}  {clip.get('duration')}s  {clip['title']!r}", flush=True)
    if dry:
        return 0
    seen.add(clip["id"])                       # mark first: a clip that fails to render is not retried every hour
    store.put_json("auto/seen.json", sorted(seen)[-500:])

    cfg_pb = json.load(open(rw.CONFIG_FILE, encoding="utf-8"))
    public_base = (os.environ.get("R2_PUBLIC_BASE") or cfg_pb.get("public_base") or "").rstrip("/")
    jid = f"auto_{clip['id']}"
    try:
        from faster_whisper import WhisperModel
        src, _ = cr.download_clip(url)
        wav = src + ".wav"
        tw.extract_audio(src, wav)
        tr = tw.transcribe(wav, WhisperModel(tw.MODEL, device="cpu", compute_type="int8"))
        for p in (src, wav):
            if os.path.exists(p):
                os.remove(p)
        text = (clip["title"] + " " + tr["text"]).lower()
        hit = [w for w in cfg.get("skip_words", []) if w in text]
        if hit:
            print(f"[auto] skipped: blocked word in the speech ({hit[0]!r})")
            return 0
        store.put_json(f"transcripts/{jid}.json", {**tr, "job": jid, "url": url, "title": clip["title"]})
        copy = write_copy(clip, tr["text"], len(cfg["page_ids"]))
        job = {"id": jid, "url": url, "render": "remote", "footer": "", "variants": make_variants(clip, cfg, copy)}
        rw.process_job(job, store, public_base, int(os.environ.get("RENDER_WORKERS", "3")))
    except Exception:
        print(f"[auto] {jid} FAILED:\n{traceback.format_exc()}", flush=True)
        return 1
    queue = store.get_json("auto/queue.json", []) or []
    queue.append({"id": jid, "created": int(time.time()), "stagger_minutes": cfg.get("stagger_minutes", [3, 6]),
                  "title": clip["title"], "channel": clip["_slug"]})
    store.put_json("auto/queue.json", queue[-60:])
    print(f"[auto] queued {jid} for the bot", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
