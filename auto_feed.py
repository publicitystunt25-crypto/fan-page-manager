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


def chan(cfg, slug):
    """Global settings with the channel's own overrides (cfg["channel_settings"][slug]) applied on top."""
    own = (cfg.get("channel_settings") or {}).get(slug, {})
    s = {**cfg, **own}
    s["skip_words"] = cfg.get("skip_words", []) + own.get("skip_words_extra", [])
    return s


def list_clips(slug, sort="date", period="all"):
    url = f"https://kick.com/api/v2/channels/{slug}/clips"
    params = {"sort": sort, "time": period}
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


def pick_clips(cfg, seen, hour, limit=5):
    """Up to `limit` suitable clips, rotating through the channels. Each channel can have its own rules
    (cfg["channel_settings"]): e.g. title_optional, api_sort/api_time (best-viewed instead of newest),
    min_age_hours / max_age_hours, min_speech_words, frame_check, skip_words_extra."""
    found = []
    channels = cfg["channels"]
    order = channels[hour % len(channels):] + channels[:hour % len(channels)]   # rotate so each channel gets turns
    now = datetime.now(timezone.utc)
    for slug in order:
        s = chan(cfg, slug)
        newest_first = s.get("api_sort", "date") == "date"
        for c in list_clips(slug, s.get("api_sort", "date"), s.get("api_time", "all")):
            try:
                age_h = (now - datetime.fromisoformat(c["created_at"].replace("Z", "+00:00"))).total_seconds() / 3600
            except Exception:
                continue
            title = (c.get("title") or "").strip()
            dur = c.get("duration") or 0
            why = None
            if age_h > s.get("max_age_hours", 6):
                if newest_first:
                    break                                # newest-first: everything after this is older still
                continue
            if age_h < s.get("min_age_hours", 0):
                why = "too new (letting it collect views)"
            elif c["id"] in seen:
                why = "already used"
            elif not s.get("title_optional") and (len(title) < s.get("min_title_chars", 15) or len(title.split()) < 3):
                why = "title too short"
            elif not (s.get("min_seconds", 10) <= dur <= s.get("max_seconds", 90)):
                why = f"length {dur}s"
            elif (c.get("view_count") or c.get("views") or 0) < s.get("min_views", 0):
                why = "too few views"
            elif any(w in title.lower() for w in s["skip_words"] + s.get("skip_title_words", [])):
                why = "blocked word in title"
            if why:
                continue
            c["_slug"] = slug
            c["_settings"] = s
            found.append(c)
            if len(found) >= limit:
                return found
    return found


def check_text(clip, transcript, key):
    """(safe, reason): is the title + speech fit to repost on general-audience pages? Claude judges, not a word list.
    No key = allowed (the title fallback is in use anyway); any error = not safe."""
    if not key:
        return True, "no API key, text not checked"
    try:
        import anthropic
        msg = anthropic.Anthropic(api_key=key).messages.create(
            model="claude-haiku-5-5", max_tokens=1000, messages=[{"role": "user", "content":
                f"A livestream clip. Title: {clip['title']}\nSpeech-to-text transcript (may contain errors): {transcript[:1800]}\n\n"
                "Decide whether it is suitable to repost on general-audience Instagram clip pages. It is NOT suitable if "
                "the title or speech: is sexual or sexualizes someone, uses slurs or hate, insults or demeans a real person "
                "(including insulting slang for a person), mocks someone's race, body or sexuality, shows violence, drugs or "
                "self-harm, shares private information, or involves a child. Ordinary swearing and trash talk in a "
                "comedic or competitive setting are fine.\n"
                'Output only JSON: {"safe": true or false, "reason": "short reason"}'}])
        raw = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
        data = json.loads(raw[raw.index("{"): raw.rindex("}") + 1])
        return bool(data.get("safe")) is True, str(data.get("reason", ""))
    except Exception as e:
        return False, f"text check failed: {type(e).__name__}: {str(e)[:120]}"


def check_frames(src, duration, key):
    """(safe, scene, reason) from four frames of the clip, judged by Claude. Fails closed: no key or any error = not safe."""
    import base64
    import subprocess
    if not key:
        return False, "", "no API key for the frame check"
    frames = []
    try:
        d = float(duration or 30)
        for i, frac in enumerate((0.1, 0.35, 0.6, 0.85)):
            out = f"{src}.f{i}.jpg"
            subprocess.run([cr._ffmpeg_exe(), "-y", "-ss", str(round(d * frac, 1)), "-i", src, "-frames:v", "1",
                            "-vf", "scale=512:-2", "-q:v", "4", out], capture_output=True, timeout=60)
            if os.path.exists(out):
                frames.append(out)
        if len(frames) < 2:
            return False, "", "could not read frames"
        content = [{"type": "image", "source": {"type": "base64", "media_type": "image/jpeg",
                                                "data": base64.b64encode(open(p, "rb").read()).decode()}} for p in frames]
        content.append({"type": "text", "text":
            "These are four frames from a short clip of a livestream. Describe what is happening in one or two plain "
            "sentences (setting, actions, any on-screen text). Do NOT identify anyone by their face or appearance.\n"
            "Then decide whether the clip is safe to post on a general-audience Instagram page. It is NOT safe if any "
            "frame shows: nudity or partial nudity, sexual content, undressing or getting dressed, underwear or towels, "
            "a bathroom, bedroom or other private space, violence, injury or gore, drugs, a weapon pointed at someone, a "
            "child, or someone who seems unaware they are being filmed or who is in distress.\n"
            'Output only JSON: {"scene": "...", "safe": true or false, "reason": "short reason"}'})
        import anthropic
        msg = anthropic.Anthropic(api_key=key).messages.create(
            model="claude-haiku-5-5", max_tokens=2000, messages=[{"role": "user", "content": content}])
        raw = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
        data = json.loads(raw[raw.index("{"): raw.rindex("}") + 1])
        return bool(data.get("safe")) is True, str(data.get("scene", "")), str(data.get("reason", ""))
    except Exception as e:
        return False, "", f"frame check failed: {type(e).__name__}: {str(e)[:120]}"
    finally:
        for p in frames:
            if os.path.exists(p):
                os.remove(p)


class CopyFailed(Exception):
    pass


def write_copy(clip, transcript, n):
    """n different overlay lines and captions. Claude if ANTHROPIC_API_KEY is set, else the Kick title."""
    title = clip["title"].strip()
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        try:
            import anthropic
            msg = anthropic.Anthropic(api_key=key).messages.create(
                model="claude-haiku-5-5", max_tokens=4000,
                messages=[{"role": "user", "content":
                    f"Instagram Reels from a Kick clip of the streamer '{clip['_slug']}'.\n"
                    f"Clip title: {title}" + (" (written by a viewer, probably meaningless - ignore it if so)" if len(title.split()) < 3 else "") + "\n"
                    f"Transcript (speech-to-text, may contain errors): {transcript[:1800]}\n"
                    + (f"Scene (described from the video frames): {clip['_scene']}\n" if clip.get("_scene") else "") + "\n"
                    f"Write {n} different versions. Each has an 'overlay' (on-screen text, 4 to 9 words, plain text) and a "
                    "'caption' (one or two short sentences, at most one emoji).\n"
                    "Make each one SPECIFIC: use a concrete detail from the transcript or title - what was said, what the "
                    "argument or moment is about, a short quote of up to six words, who is involved if the title names them. "
                    "Vary the style across versions: some a question, some a plain statement, some a short quote, some a "
                    "reaction. A viewer who has not seen the clip should know what it is about.\n"
                    "Rules: say only what the title or transcript supports; never invent facts, names or outcomes. Do NOT "
                    "mention the stream, Kick, 'the full clip', 'watch till the end' or 'part 2'. Do not use filler like "
                    "'chaos', 'no notes', 'you have to see this', 'things got heated' unless the clip clearly shows it. "
                    "If the transcript is thin or unclear, stick to what the title and the scene say. Do not name anyone who is not "
                    "in the title or the streamer's own name, no @mentions, no hashtags, no quotes around the whole text, "
                    "no insults or claims about a real person that the clip does not show, no profanity. Every version must "
                    "read differently from the others.\n"
                    'Output only JSON: {"versions":[{"overlay":"...","caption":"..."}]}'}])
            raw = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")   # skip any thinking block
            data = json.loads(raw[raw.index("{"): raw.rindex("}") + 1])["versions"]
            data = [v for v in data if v.get("overlay") and v.get("caption")]
            if len(data) >= n:
                return data[:n]
            raise CopyFailed(f"copywriter returned {len(data)} of {n} versions")
        except CopyFailed:
            raise
        except Exception as e:
            raise CopyFailed(f"{type(e).__name__}: {str(e)[:200]}")
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


def note_run(store, clip, status, detail=""):
    """Last run's outcome, readable at <public base>/auto/last_run.json."""
    try:
        store.put_json("auto/last_run.json", {"at": int(time.time()), "clip": clip["id"], "title": clip["title"],
                                              "status": status, "detail": detail})
    except Exception:
        pass


def main():
    dry = "--dry-run" in sys.argv
    cfg = json.load(open(CONFIG, encoding="utf-8"))
    if not cfg.get("enabled", True):
        print("[auto] disabled in auto_feed.json")
        return 0
    store = rw.Store()
    seen = set(store.get_json("auto/seen.json", []) or [])
    candidates = pick_clips(cfg, seen, datetime.now(timezone.utc).hour)
    if not candidates:
        print("[auto] no suitable new clip this hour")
        return 0
    if dry:
        for c in candidates:
            print(f"[auto] candidate https://kick.com/{c['_slug']}/clips/{c['id']}  {c.get('duration')}s  {c['title']!r}")
        return 0

    cfg_pb = json.load(open(rw.CONFIG_FILE, encoding="utf-8"))
    public_base = (os.environ.get("R2_PUBLIC_BASE") or cfg_pb.get("public_base") or "").rstrip("/")
    from faster_whisper import WhisperModel
    model = WhisperModel(tw.MODEL, device="cpu", compute_type="int8")
    clip = tr = jid = url = None
    for cand in candidates:
        url = f"https://kick.com/{cand['_slug']}/clips/{cand['id']}"
        s = cand["_settings"]
        print(f"[auto] checking {url}  {cand.get('duration')}s  {cand['title']!r}", flush=True)
        seen.add(cand["id"])                   # mark first: a clip that fails is not retried every hour
        store.put_json("auto/seen.json", sorted(seen)[-500:])
        src = wav = None
        tr = None
        try:
            src, _ = cr.download_clip(url)
            wav = src + ".wav"
            tw.extract_audio(src, wav)
            tr = tw.transcribe(wav, model)
            text = (cand["title"] + " " + tr["text"]).lower()
            hit = [w for w in s["skip_words"] if w in text]
            if hit:
                print(f"[auto] skipped: blocked word in the speech ({hit[0]!r})")
                tr = None
            elif len(tr["text"].split()) < int(s.get("min_speech_words", 15)):
                print(f"[auto] skipped: too little speech ({len(tr['text'].split())} words) to write a real caption")
                tr = None
            elif not (verdict := check_text(cand, tr["text"], os.environ.get("ANTHROPIC_API_KEY")))[0]:
                print(f"[auto] skipped by the content check: {verdict[1]!r}")
                tr = None
            elif s.get("frame_check"):
                safe, scene, reason = check_frames(src, cand.get("duration"), os.environ.get("ANTHROPIC_API_KEY"))
                print(f"[auto] frame check: safe={safe}  {reason!r}  scene={scene!r}", flush=True)
                if safe:
                    cand["_scene"] = scene
                else:
                    tr = None
        except Exception:
            print(f"[auto] {cand['id']} could not be read:\n{traceback.format_exc()}", flush=True)
            note_run(store, cand, "failed", traceback.format_exc()[-400:])
            tr = None
        finally:
            for p in (src, wav):
                if p and os.path.exists(p):
                    os.remove(p)
        if tr is None:
            continue
        clip = cand
        break
    if not clip:
        note_run(store, candidates[0], "skipped: nothing worth posting", f"{len(candidates)} candidates checked")
        print("[auto] none of the candidates was worth posting")
        return 0

    jid = f"auto_{clip['id']}"
    try:
        store.put_json(f"transcripts/{jid}.json", {**tr, "job": jid, "url": url, "title": clip["title"]})
        copy = write_copy(clip, tr["text"], len(cfg["page_ids"]))
        job = {"id": jid, "url": url, "render": "remote", "footer": "", "variants": make_variants(clip, cfg, copy)}
        rw.process_job(job, store, public_base, int(os.environ.get("RENDER_WORKERS", "3")))
    except CopyFailed as e:
        seen.discard(clip["id"])               # not the clip's fault: let the next hour try it again
        store.put_json("auto/seen.json", sorted(seen)[-500:])
        note_run(store, clip, "skipped: no copy", str(e))
        print(f"[auto] {jid} skipped, the copywriter failed: {e}", flush=True)
        return 1
    except Exception:
        print(f"[auto] {jid} FAILED:\n{traceback.format_exc()}", flush=True)
        note_run(store, clip, "failed", traceback.format_exc()[-400:])
        return 1
    queue = store.get_json("auto/queue.json", []) or []
    queue.append({"id": jid, "created": int(time.time()), "stagger_minutes": cfg.get("stagger_minutes", [3, 6]),
                  "title": clip["title"], "channel": clip["_slug"]})
    store.put_json("auto/queue.json", queue[-60:])
    note_run(store, clip, "queued", "copy by " + ("claude" if os.environ.get("ANTHROPIC_API_KEY") else "title (no key)"))
    print(f"[auto] queued {jid} for the bot", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
