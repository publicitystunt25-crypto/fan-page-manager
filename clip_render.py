"""Clip rendering: download a Twitch/Kick clip, brand it and burn in an overlay.

Used by the bot (app.py) and by render_worker.py (GitHub Actions). It has no database or Flask
dependencies, so it runs anywhere ffmpeg and the packages in requirements-render.txt are available.
"""
import os
import re
import uuid
import tempfile
import textwrap
from PIL import Image, ImageDraw, ImageFont

CLIP_DIR = os.environ.get("CLIP_DIR") or os.path.join(tempfile.gettempdir(), "clips")
os.makedirs(CLIP_DIR, exist_ok=True)

def _ffmpeg_exe():
    import imageio_ffmpeg
    return imageio_ffmpeg.get_ffmpeg_exe()

def download_clip(url):
    """Download a Twitch/Kick clip. Returns (path, info dict)."""
    import yt_dlp
    out = os.path.join(CLIP_DIR, uuid.uuid4().hex)
    opts = {"outtmpl": out + ".%(ext)s", "format": "best[ext=mp4]/best",
            "quiet": True, "noprogress": True, "noplaylist": True, "ffmpeg_location": _ffmpeg_exe()}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        path = ydl.prepare_filename(info)
    return path, info

BAR_H = 160
BAR_LIFT = 330   # px above the bottom on portrait clips: clears IG's caption/profile area
SIDE_SAFE = 150  # px kept free on both sides: IG's like/comment/share buttons sit on the right
# Output size kept modest: the Render instance is small and 1080x1920 encodes ran it out of memory
OUT_W = int(os.environ.get("CLIP_OUT_W", "720"))
OUT_H = OUT_W * 16 // 9 // 2 * 2

def _bold_font(size):
    for fp in ["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
               "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
               "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
               "C:/Windows/Fonts/arialbd.ttf"]:
        if os.path.exists(fp):
            return ImageFont.truetype(fp, size)
    return ImageFont.load_default()

def make_brand_bar(slug, platform):
    """1080xBAR_H black bar, centered: platform wordmark on top, PLATFORM.COM/CHANNEL underneath."""
    W = 1080
    bar = Image.new("RGB", (W, BAR_H), (0, 0, 0))
    d = ImageDraw.Draw(bar)
    if platform == "kick":
        word, color, domain = "KICK", (83, 252, 24), "KICK.COM"
    else:
        word, color, domain = "TWITCH", (145, 70, 255), "TWITCH.TV"
    max_w = W - 2 * SIDE_SAFE

    def fit(text, size, min_size):
        while True:
            f = _bold_font(size)
            box = d.textbbox((0, 0), text, font=f)
            if box[2] - box[0] <= max_w or size <= min_size:
                return f, box
            size -= 2

    lf, lb = fit(word, 76, 40)
    uf, ub = fit(f"{domain}/{slug}".upper(), 42, 20)
    lh, uh = lb[3] - lb[1], ub[3] - ub[1]
    gap = 14
    y = (BAR_H - (lh + gap + uh)) // 2
    d.text(((W - (lb[2] - lb[0])) // 2 - lb[0], y - lb[1]), word, font=lf, fill=color)
    d.text(((W - (ub[2] - ub[0])) // 2 - ub[0], y + lh + gap - ub[1]),
           f"{domain}/{slug}".upper(), font=uf, fill=(255, 255, 255))
    path = os.path.join(CLIP_DIR, f"{uuid.uuid4().hex}_bar.png")
    bar.save(path)
    return path

def detect_crop(src_path):
    """Find the real picture area (strips pillarbox/letterbox bars). Returns 'W:H:X:Y' or None."""
    import re, subprocess
    r = subprocess.run([_ffmpeg_exe(), "-ss", "1", "-t", "4", "-i", src_path,
                        "-vf", "cropdetect=24:2:0", "-an", "-f", "null", "-"],
                       capture_output=True, text=True, timeout=120)
    best = None
    for w, h, x, y in re.findall(r"crop=(\d+):(\d+):(\d+):(\d+)", r.stderr):
        w, h = int(w), int(h)
        if w >= 64 and h >= 64 and (best is None or w * h > best[0] * best[1]):
            best = (w, h, int(x), int(y))
    return f"{best[0]}:{best[1]}:{best[2]}:{best[3]}" if best else None

HOOK_SECONDS = 3
HOOK_TOP = 340      # px from the top: below IG's Reels header
HOOK_SIDE = 100     # px side margin

def clean_hook(text):
    """Keep only characters the bundled fonts can draw (no emoji), collapse whitespace."""
    text = re.sub(r"[^\x20-\x7E -ɏ‘’“”–—…]", "", text or "")
    return re.sub(r"\s+", " ", text).strip()[:80]

def generate_clip_hook(title, channel_name, hint=""):
    """Short on-screen hook for the first seconds. Returns '' when there is nothing good to show."""
    fallback = clean_hook(title) if title and len(title) <= 45 else ""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return fallback
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key)
        msg = client.messages.create(
            model="claude-haiku-5-5", max_tokens=60,
            messages=[{"role": "user", "content":
                f"Write the on-screen text hook shown in the first 3 seconds of a short vertical clip of "
                f"streamer {channel_name}. Clip title: {title}. {('Context: ' + hint) if hint else ''}\n"
                "Rules: 2 to 6 words, curiosity or tension (e.g. 'Wait for the ending'), plain text only, "
                "no emoji, no hashtags, no quotes. Output only the hook."}])
        return clean_hook(msg.content[0].text) or fallback
    except Exception as e:
        print(f"[hook] {e}")
        return fallback

def make_hook_overlay(text, top=None, max_size=92):
    """Transparent 1080x1920 PNG with the hook text: white, black outline, centered, wrapped."""
    W, H = 1080, 1920
    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    max_w = W - 2 * HOOK_SIDE
    size = max_size
    while True:
        f = _bold_font(size)
        words, lines, cur = text.upper().split(), [], ""
        for w in words:
            trial = f"{cur} {w}".strip()
            if d.textbbox((0, 0), trial, font=f, stroke_width=6)[2] <= max_w or not cur:
                cur = trial
            else:
                lines.append(cur)
                cur = w
        lines.append(cur)
        widest = max(d.textbbox((0, 0), l, font=f, stroke_width=6)[2] for l in lines)
        if (widest <= max_w and len(lines) <= 3) or size <= 48:
            break
        size -= 4
    y = HOOK_TOP if top is None else top
    for line in lines:
        box = d.textbbox((0, 0), line, font=f, stroke_width=6)
        d.text(((W - (box[2] - box[0])) // 2 - box[0], y - box[1]), line, font=f,
               fill=(255, 255, 255, 255), stroke_width=6, stroke_fill=(0, 0, 0, 255))
        y += int(size * 1.2)
    path = os.path.join(CLIP_DIR, f"{uuid.uuid4().hex}_hook.png")
    img.save(path)
    return path

def render_clip(src_path, slug, platform, hook="", hook_top=None, hook_size=92, hook_seconds=None,
                crop_factor=1.0, crop_x=0.5, crop_y=0.5, strip_lift=None, strip_pos="bottom"):
    """1080x1920 output with a branded bar. Portrait picture fills the frame (bar overlaid above the
    caption area); landscape picture is centered over a blurred copy with the bar attached underneath.
    If `hook` is given it is shown as text over the first HOOK_SECONDS seconds."""
    bar = make_brand_bar(slug, platform)
    hook_png = make_hook_overlay(hook, hook_top, hook_size) if hook else None
    filename = f"{uuid.uuid4().hex}.mp4"
    dst = os.path.join(CLIP_DIR, filename)
    crop = detect_crop(src_path)
    pre = f"crop={crop}," if crop else ""
    if crop_factor < 1.0:   # slight extra crop; crop_x / crop_y (0..1) shift where it is taken from
        f = max(0.8, float(crop_factor))
        pre += f"crop=iw*{f}:ih*{f}:(iw-iw*{f})*{float(crop_x)}:(ih-ih*{f})*{float(crop_y)},"
    lift = BAR_LIFT if strip_lift is None else int(strip_lift)
    portrait = False
    if crop:
        cw, ch = (int(v) for v in crop.split(":")[:2])
        portrait = ch / cw > 1.2
    last = "[vb]" if hook_png else "[v]"
    if portrait:
        fc = (f"[0:v]{pre}scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[base];"
              f"[base][1:v]overlay=0:H-{BAR_H + lift}{last}")
    else:
        fc = (f"[0:v]{pre}scale='min(iw,1280)':-2,split[a][b];"
              "[a]scale=270:480:force_original_aspect_ratio=increase,crop=270:480,boxblur=8:2,scale=1080:1920[bg];"
              + (f"[b]scale=1080:-2,pad=1080:ih+{BAR_H}:0:{BAR_H}:black[blk0];"
                 "[blk0][1:v]overlay=0:0[blk];" if strip_pos == "top" else
                 f"[b]scale=1080:-2,pad=1080:ih+{BAR_H}:0:0:black[blk0];"
                 f"[blk0][1:v]overlay=0:H-{BAR_H}[blk];") +
              f"[bg][blk]overlay=(W-w)/2:(H-h)/2{last}")
    cmd = [_ffmpeg_exe(), "-y", "-i", src_path, "-i", bar]
    if hook_png:
        window = HOOK_SECONDS if hook_seconds is None else hook_seconds
        fc += (f";[vb][2:v]overlay=0:0:enable='lt(t,{window})'[v]" if window else ";[vb][2:v]overlay=0:0[v]")
        cmd += ["-i", hook_png]
    cmd += ["-filter_complex", fc,
            "-map", "[v]", "-map", "0:a?", "-s", f"{OUT_W}x{OUT_H}", "-threads", "1",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-t", "90", dst]
    import subprocess
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)
    finally:
        for p in (bar, hook_png):
            if p and os.path.exists(p):
                os.remove(p)
    if r.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {r.stderr[-400:]}")
    return filename

def channel_slug_from(url, info, override):
    """Channel handle for the bar: override > url path > extractor metadata."""
    if override:
        return override.strip().lstrip("@").replace(" ", "")
    import re
    m = re.search(r"(?:twitch\.tv|kick\.com)/([^/?#]+)", url)
    if m and m.group(1).lower() not in ("clip", "clips", "video", "videos"):
        return m.group(1)
    return (info.get("uploader_id") or info.get("uploader") or "streamer").replace(" ", "")

def generate_clip_caption(title, channel_name, platform, handle_tag, hint=""):
    """AI-written caption; falls back to a plain template."""
    fallback = f"{title}\n\n{handle_tag} 🔥"
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return fallback
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key)
        msg = client.messages.create(
            model="claude-haiku-5-5", max_tokens=300,
            messages=[{"role": "user", "content":
                f"Write an Instagram Reels caption for a {platform} clip of streamer {channel_name}. "
                f"Clip title: {title}. {('Extra context: ' + hint) if hint else ''}\n"
                "One punchy hook line, optionally one short follow-up line, then 4-6 relevant hashtags. "
                "No quotes around it, no @mentions. Output only the caption."}])
        text = msg.content[0].text.strip()
        return f"{text}\n\n{handle_tag}"
    except Exception as e:
        print(f"[caption] {e}")
        return fallback

