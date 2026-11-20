"""Twitch feed: runs once a day in GitHub Actions (see .github/workflows/twitch_feed.yml).

For the Twitch channels in twitch_feed.json it lists the clips of the last 24 hours (yt-dlp), keeps the ones that
Claude sees are GTA 6 gameplay in the frames, and gives each page in page_ids its OWN clip (the best-viewed clips that
pass the checks, one per page). Each clip is transcribed and looked at (three frames, judged by Claude), and gets a caption
and an on-screen line written from what is said and shown. The versions are rendered with render_worker.process_job and
appended to auto/queue.json, where the bot picks them up and posts them.
State: twitch/seen.json (clips already used or rejected) and twitch/last_run.json (what happened; readable at <public base>/twitch/last_run.json).

Environment: the R2 secrets and ANTHROPIC_API_KEY. No Twitch keys are needed (yt-dlp reads the public clips page). Flags: --dry-run (choose and write copy, render and queue nothing), --force (run again today).
"""
import os
import re
import sys
import json
import time
import base64
import subprocess
import traceback
from datetime import datetime, timezone, timedelta


import clip_render as cr
import render_worker as rw
import transcribe_worker as tw
import auto_feed as af

CONFIG = os.environ.get("TWITCH_FEED_CONFIG") or os.path.join(rw.HERE, "twitch_feed.json")
ID_URL = "https://id.twitch.tv/oauth2/token"
HELIX = "https://api.twitch.tv/helix"
HASHTAGS = "#GTA6 #grandtheftauto6 #gtavi"  # on every GTA 6 post


def list_clips(login, hours):
    """Clips of the last 24 hours of one channel, best-viewed first (yt-dlp reads the public clips page, no keys needed)."""
    import yt_dlp
    rng = "24hr" if hours <= 24 else "7d"
    opts = {"quiet": True, "no_warnings": True, "extract_flat": True, "playlistend": 40}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(f"https://www.twitch.tv/{login}/clips?range={rng}", download=False)
    out = []
    for e in (info or {}).get("entries") or []:
        url = e.get("url") or ""
        if "/clip/" not in url:
            continue
        out.append({"id": url.rsplit("/clip/", 1)[1], "url": url, "title": e.get("title") or "", "duration": float(e.get("duration") or 0),
                    "views": int(e.get("view_count") or 0), "game": "", "channel": login})
    return sorted(out, key=lambda c: -c["views"])


def candidates(cfg, seen):
    out = []
    for login in cfg["channels"]:
        try:
            clips = list_clips(login, cfg.get("hours_back", 24))
        except Exception as e:
            print(f"[twitch] {login}: could not list clips ({type(e).__name__}: {str(e)[:100]})")
            continue
        kept = []
        for c in clips:
            if c["id"] in seen:
                continue
            if not (cfg.get("min_seconds", 10) <= c["duration"] <= cfg.get("max_seconds", 90)):
                continue
            if c["views"] < cfg.get("min_views", 0):
                continue
            kept.append(c)
        kept = kept[: int(cfg.get("max_checked_per_channel", 12))]
        print(f"[twitch] {login}: {len(clips)} clips in the last {cfg.get('hours_back', 24)}h, {len(kept)} to check")
        out += kept
    return sorted(out, key=lambda c: -c["views"])


def frames_of(src, duration):
    paths = []
    d = float(duration or 30)
    for i, frac in enumerate((0.15, 0.5, 0.85)):
        out = f"{src}.f{i}.jpg"
        subprocess.run([cr._ffmpeg_exe(), "-y", "-ss", str(round(d * frac, 1)), "-i", src, "-frames:v", "1",
                        "-vf", "scale=640:-2", "-q:v", "4", out], capture_output=True, timeout=60)
        if os.path.exists(out):
            paths.append(out)
    return paths


def judge_and_write(clip, transcript, frames, streamer, key):
    """One Claude call that looks at the frames and reads the transcript: (safe, reason, overlay, caption)."""
    import anthropic
    content = [{"type": "image", "source": {"type": "base64", "media_type": "image/jpeg",
                                            "data": base64.b64encode(open(p, "rb").read()).decode()}} for p in frames]
    content.append({"type": "text", "text":
        f"These are three frames from a short Twitch clip of the streamer {streamer} playing a game (maybe GTA 6).\n"
        f"Clip title: {clip['title']}\nTranscript (speech-to-text, may contain errors): {transcript[:1800]}\n\n"
        "0) Decide if the footage is clearly Grand Theft Auto VI (GTA 6) gameplay (set \"gta6\" true only if you can see it is GTA 6 / Vice City style GTA gameplay, not another game or just the streamer talking).\n"
        "1) Decide if it is safe for a general-audience Instagram page. NOT safe: nudity, sexual content, graphic gore or "
        "injury, slurs or hate speech, drugs, a real child, or anything that identifies a private person.\n"
        "2) Write an Instagram Reel caption that makes people want to WATCH the clip. It must be about what is actually on "
        "screen and said (the setting, vehicles, characters, the action, the streamer's reaction), name a concrete detail, "
        "hint at what happens without giving it away, be one or two short sentences with at most two emoji, and contain the "
        "exact words 'GTA 6'. You may name the streamer as " + streamer + ". Never invent facts, outcomes or names; do not "
        "identify anyone from their face; no profanity; no @mentions or hashtags; do not say 'watch till the end', "
        "'part 2' or 'the full clip'.\n"
        "3) Write an on-screen line of 3 to 8 words in plain text, different from the caption.\n"
        'Output only JSON: {"gta6": true or false, "safe": true or false, "reason": "short", "overlay": "...", "caption": "..."}'})
    msg = anthropic.Anthropic(api_key=key).messages.create(
        model="claude-sonnet-5-5", max_tokens=1500, messages=[{"role": "user", "content": content}])
    raw = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
    d = json.loads(raw[raw.index("{"): raw.rindex("}") + 1])
    caption = str(d.get("caption", "")).strip()
    if caption and "gta 6" not in caption.lower():
        caption = "GTA 6: " + caption
    if d.get("gta6") is not True:
        return None, "not GTA 6", "", ""
    return bool(d.get("safe")) is True, str(d.get("reason", "")), str(d.get("overlay", "")).strip()[:80], caption[:600]


def main():
    dry, force = "--dry-run" in sys.argv, "--force" in sys.argv
    cfg = json.load(open(CONFIG, encoding="utf-8"))
    if not cfg.get("enabled", True):
        print("[twitch] disabled in twitch_feed.json")
        return 0
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        print("[twitch] ANTHROPIC_API_KEY is not set - captions cannot be written")
        return 1
    store = rw.Store()
    # nothing is kept: rendered videos from earlier days are deleted once they have had time to post
    try:
        print(f"[twitch] cleanup: removed {store.delete_old('videos/', rw.VIDEO_MAX_AGE_HOURS)} old video(s)", flush=True)
    except Exception as e:
        print(f"[twitch] cleanup skipped ({type(e).__name__})", flush=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    last = store.get_json("twitch/last_run.json", {}) or {}
    if not dry and not force and last.get("day") == today and last.get("status") == "queued":
        print("[twitch] already posted today")
        return 0
    seen = set(store.get_json("twitch/seen.json", []) or [])
    cands = candidates(cfg, seen)
    print(f"[twitch] {len(cands)} usable clip(s) across the channels", flush=True)
    pages = list(cfg["page_ids"])
    cfg_pb = json.load(open(rw.CONFIG_FILE, encoding="utf-8"))
    public_base = (os.environ.get("R2_PUBLIC_BASE") or cfg_pb.get("public_base") or "").rstrip("/")
    from faster_whisper import WhisperModel
    model = WhisperModel(tw.MODEL, device="cpu", compute_type="int8")

    chosen, report = [], []
    for clip in cands:
        if len(chosen) >= len(pages):
            break
        src = wav = None
        frames = []
        try:
            src, _ = cr.download_clip(clip["url"])
            wav = src + ".wav"
            tw.extract_audio(src, wav)
            tr = tw.transcribe(wav, model)
            hit = [w for w in cfg["skip_words"] if w in (clip["title"] + " " + tr["text"]).lower()]
            if hit:
                report.append({"clip": clip["id"], "title": clip["title"], "result": f"blocked word ({hit[0]!r})"})
                seen.add(clip["id"])
                continue
            if len(tr["text"].split()) < int(cfg.get("min_speech_words", 0)):
                report.append({"clip": clip["id"], "title": clip["title"], "result": "too little speech"})
                seen.add(clip["id"])
                continue
            frames = frames_of(src, clip["duration"])
            if len(frames) < 2:
                report.append({"clip": clip["id"], "title": clip["title"], "result": "could not read frames"})
                continue
            streamer = (cfg.get("display_names") or {}).get(clip["channel"], clip["channel"])
            safe, reason, overlay, caption = judge_and_write(clip, tr["text"], frames, streamer, key)
            if safe is None:
                report.append({"clip": clip["id"], "title": clip["title"], "result": "not GTA 6"})
                seen.add(clip["id"])
                continue
            if not safe or not caption or not overlay:
                report.append({"clip": clip["id"], "title": clip["title"], "result": f"rejected: {reason or 'no copy'}"})
                seen.add(clip["id"])
                continue
            chosen.append({**clip, "overlay": overlay, "caption": caption + "\n\n" + HASHTAGS, "transcript": tr["text"][:300]})
            report.append({"clip": clip["id"], "title": clip["title"], "result": "chosen", "overlay": overlay, "caption": caption})
        except Exception:
            print(f"[twitch] {clip['id']} could not be read:\n{traceback.format_exc()}", flush=True)
            report.append({"clip": clip["id"], "title": clip["title"], "result": "could not be read"})
        finally:
            for p in [src, wav] + frames:
                if p and os.path.exists(p):
                    os.remove(p)

    for r in report:
        print(f"[twitch] {r['clip']} {r['title']!r}: {r['result']}", flush=True)
    status = "dry run" if dry else ("queued" if chosen else "nothing worth posting")
    if dry or not chosen:
        store.put_json("twitch/last_run.json", {"day": today, "status": status, "pages": pages, "report": report, "at": int(time.time())})
        if not dry:
            store.put_json("twitch/seen.json", sorted(seen)[-500:])
        return 0

    queued = []
    for clip, page in zip(chosen, pages):
        jid = f"tw_{clip['id']}"[:80]
        try:
            job = {"id": jid, "url": clip["url"], "render": "remote", "footer": "", "channel": clip["channel"],
                   "variants": af.make_variants({"id": jid}, [page], [{"overlay": clip["overlay"], "caption": clip["caption"]}])}
            rw.process_job(job, store, public_base, 1)
        except Exception:
            print(f"[twitch] {jid} FAILED:\n{traceback.format_exc()}", flush=True)
            continue
        queue = store.get_json("auto/queue.json", []) or []
        queue.append({"id": jid, "created": int(time.time()), "stagger_minutes": cfg.get("stagger_minutes", [1, 3]),
                      "title": clip["title"], "channel": clip["channel"]})
        store.put_json("auto/queue.json", queue[-60:])
        seen.add(clip["id"])
        queued.append({"clip": clip["id"], "page": page, "title": clip["title"]})
        print(f"[twitch] queued {jid} for page {page}", flush=True)
    store.put_json("twitch/seen.json", sorted(seen)[-500:])
    store.put_json("twitch/last_run.json", {"day": today, "status": "queued" if queued else "failed", "queued": queued,
                                             "report": report, "at": int(time.time())})
    return 0 if queued else 1


if __name__ == "__main__":
    sys.exit(main())
