"""Transcribe worker: runs in GitHub Actions (see .github/workflows/transcribe.yml).

For every entry in transcribe.json ({"id", "url"}) with no transcript yet it downloads the clip, extracts the audio,
runs a small speech-to-text model (faster-whisper) and uploads  transcripts/<id>.json  to R2 (public, same base URL as
the rendered videos).  Same environment as render_worker.py (or LOCAL_STORE for a local test).
"""
import os
import sys
import json
import time
import subprocess
import traceback

import clip_render as cr
from render_worker import Store, HERE, CONFIG_FILE

JOBS_FILE = os.path.join(HERE, "transcribe.json")
MODEL = os.environ.get("WHISPER_MODEL", "small")


def extract_audio(src, wav):
    subprocess.run([cr._ffmpeg_exe(), "-y", "-i", src, "-vn", "-ac", "1", "-ar", "16000", wav],
                   check=True, capture_output=True, timeout=300)


def save_frames(src, jid, duration, store):
    """Three still frames (20%, 50%, 80% in) uploaded as frames/<id>_1..3.jpg, to see what is on screen."""
    d = float(duration or 30)
    for n, frac in enumerate((0.2, 0.5, 0.8), 1):
        jpg = f"{src}.f{n}.jpg"
        subprocess.run([cr._ffmpeg_exe(), "-y", "-ss", str(round(d * frac, 1)), "-i", src, "-frames:v", "1",
                        "-vf", "scale=640:-2", "-q:v", "4", jpg], check=True, capture_output=True, timeout=120)
        store.put_file(f"frames/{jid}_{n}.jpg", jpg, "image/jpeg")
        os.remove(jpg)


def transcribe(wav, model):
    segments, info = model.transcribe(wav, beam_size=5, vad_filter=True)
    segs = [{"start": round(s.start, 1), "end": round(s.end, 1), "text": s.text.strip()} for s in segments]
    return {"language": info.language, "text": " ".join(s["text"] for s in segs).strip(), "segments": segs}


def main():
    try:
        jobs = json.load(open(JOBS_FILE, encoding="utf-8"))
    except FileNotFoundError:
        print("no transcribe.json")
        return 0
    store = Store()
    todo = [j for j in jobs if j.get("id") and j.get("url") and (
        not store.exists(f"transcripts/{j['id']}.json") or (j.get("frames") and not store.exists(f"frames/{j['id']}_1.jpg")))]
    if not todo:
        print("nothing to transcribe")
        return 0
    from faster_whisper import WhisperModel
    model = WhisperModel(MODEL, device="cpu", compute_type="int8")
    failed = 0
    for job in todo:
        jid = job["id"]
        try:
            print(f"[transcribe] {jid}: downloading {job['url']}", flush=True)
            src, info = cr.download_clip(job["url"])
            wav = src + ".wav"
            if not store.exists(f"transcripts/{jid}.json"):
                extract_audio(src, wav)
                out = transcribe(wav, model)
                out.update({"job": jid, "url": job["url"], "title": (info or {}).get("title"),
                            "duration": (info or {}).get("duration"), "created": int(time.time())})
                store.put_json(f"transcripts/{jid}.json", out)
            if job.get("frames"):
                save_frames(src, jid, (info or {}).get("duration"), store)
            for p in (src, wav):
                if os.path.exists(p):
                    os.remove(p)
            print(f"[transcribe] {jid}: done ({len(out['segments'])} segments)", flush=True)
        except Exception:
            failed += 1
            print(f"[transcribe] {jid} FAILED:\n{traceback.format_exc()}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
