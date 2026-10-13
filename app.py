import os
import time
import threading
import uuid
import textwrap
import requests
from flask import Flask, jsonify, request, send_file
from psycopg2.extras import RealDictCursor
import psycopg2
from PIL import Image, ImageDraw, ImageFont

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
USER_ACCESS_TOKEN = os.environ.get("FB_USER_TOKEN")
GRAPH = "https://graph.facebook.com/v19.0"

# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10,
                            options="-c statement_timeout=8000")

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fan_pages (
            id SERIAL PRIMARY KEY,
            fb_page_id TEXT UNIQUE NOT NULL,
            ig_account_id TEXT,
            page_name TEXT,
            page_access_token TEXT,
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS post_log (
            id SERIAL PRIMARY KEY,
            fan_page_id INTEGER REFERENCES fan_pages(id),
            ig_post_id TEXT,
            caption TEXT,
            media_url TEXT,
            posted_at TIMESTAMPTZ DEFAULT NOW(),
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS quote_queue (
            id SERIAL PRIMARY KEY,
            quote TEXT NOT NULL,
            caption TEXT,
            scheduled_at TIMESTAMPTZ NOT NULL,
            status TEXT DEFAULT 'pending',
            posted_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS analytics_snapshots (
            id SERIAL PRIMARY KEY,
            fan_page_id INTEGER REFERENCES fan_pages(id),
            ig_media_id TEXT,
            pulled_at TIMESTAMPTZ DEFAULT NOW(),
            impressions INTEGER,
            reach INTEGER,
            likes INTEGER,
            comments INTEGER,
            saves INTEGER
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# ── INSTAGRAM HELPERS ─────────────────────────────────────────────────────────

def get_ig_for_page(page_access_token, fb_page_id):
    r = requests.get(f"{GRAPH}/{fb_page_id}",
                     params={"fields": "instagram_business_account", "access_token": page_access_token},
                     timeout=10)
    data = r.json()
    return data.get("instagram_business_account", {}).get("id")

def post_to_ig(ig_account_id, page_token, media_url, caption, media_type="IMAGE"):
    """Post a photo or video to one IG account."""
    if media_type == "VIDEO":
        payload = {"video_url": media_url, "media_type": "REELS", "caption": caption,
                   "access_token": page_token}
    else:
        payload = {"image_url": media_url, "caption": caption, "access_token": page_token}

    r = requests.post(f"{GRAPH}/{ig_account_id}/media", data=payload, timeout=30)
    result = r.json()
    creation_id = result.get("id")
    if not creation_id:
        return None, result.get("error", {}).get("message", str(result))

    pub = requests.post(f"{GRAPH}/{ig_account_id}/media_publish",
                        data={"creation_id": creation_id, "access_token": page_token},
                        timeout=30)
    pub_result = pub.json()
    post_id = pub_result.get("id")
    if not post_id:
        return None, pub_result.get("error", {}).get("message", str(pub_result))

    return post_id, None

def _post_to_all_pages(media_url, caption, media_type="IMAGE"):
    """Background thread — posts to all active fan pages with staggered delays."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, ig_account_id, page_access_token, page_name FROM fan_pages WHERE active=TRUE AND ig_account_id IS NOT NULL")
        pages = cur.fetchall()
        cur.close()
        conn.close()

        for page in pages:
            post_id, err = post_to_ig(page["ig_account_id"], page["page_access_token"],
                                      media_url, caption, media_type)
            conn2 = get_conn()
            cur2 = conn2.cursor()
            cur2.execute("""
                INSERT INTO post_log (fan_page_id, ig_post_id, caption, media_url, error)
                VALUES (%s, %s, %s, %s, %s)
            """, (page["id"], post_id, caption, media_url, err))
            conn2.commit()
            cur2.close()
            conn2.close()
            if err:
                print(f"[post] {page['page_name']} error: {err}")
            else:
                print(f"[post] {page['page_name']} → {post_id}")
            time.sleep(3)
    except Exception as e:
        print(f"[post-all] {e}")

def _quote_scheduler_loop():
    time.sleep(10)
    while True:
        try:
            conn = get_conn()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT id, quote, caption FROM quote_queue
                WHERE status = 'pending' AND scheduled_at <= NOW()
                ORDER BY scheduled_at ASC LIMIT 1
            """)
            job = cur.fetchone()
            cur.close()
            conn.close()

            if job:
                filename = make_quote_card(job["quote"])
                base_url = os.environ.get("BASE_URL", "https://fan-page-manager.onrender.com")
                media_url = f"{base_url}/quote-image/{filename}"
                _post_to_all_pages(media_url, job["caption"], "IMAGE")

                conn2 = get_conn()
                cur2 = conn2.cursor()
                cur2.execute("UPDATE quote_queue SET status='done', posted_at=NOW() WHERE id=%s", (job["id"],))
                conn2.commit()
                cur2.close()
                conn2.close()
                print(f"[quote-scheduler] posted quote id {job['id']}")
        except Exception as e:
            print(f"[quote-scheduler] {e}")
        time.sleep(30)

def _analytics_loop():
    time.sleep(30)
    while True:
        try:
            conn = get_conn()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT id, ig_account_id, page_access_token FROM fan_pages WHERE active=TRUE AND ig_account_id IS NOT NULL")
            pages = cur.fetchall()
            cur.close()
            conn.close()

            for page in pages:
                try:
                    r = requests.get(f"{GRAPH}/{page['ig_account_id']}/media",
                                     params={"fields": "id,like_count,comments_count,insights.metric(impressions,reach,saved)",
                                             "limit": 10, "access_token": page["page_access_token"]},
                                     timeout=15)
                    posts = r.json().get("data", [])
                    for post in posts:
                        insights = {i["name"]: i["values"][0]["value"]
                                    for i in post.get("insights", {}).get("data", [])}
                        conn2 = get_conn()
                        cur2 = conn2.cursor()
                        cur2.execute("""
                            INSERT INTO analytics_snapshots
                              (fan_page_id, ig_media_id, impressions, reach, likes, comments, saves)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (page["id"], post["id"],
                              insights.get("impressions", 0),
                              insights.get("reach", 0),
                              post.get("like_count", 0),
                              post.get("comments_count", 0),
                              insights.get("saved", 0)))
                        conn2.commit()
                        cur2.close()
                        conn2.close()
                    time.sleep(2)
                except Exception as pe:
                    print(f"[analytics] page {page['id']}: {pe}")
        except Exception as e:
            print(f"[analytics-loop] {e}")
        time.sleep(3600)

# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({"status": "fan-page-manager running"})

@app.route("/list-pages", methods=["GET"])
def list_available_pages():
    """Show all FB pages on your account without adding them to DB — use to pick which ones to sync."""
    try:
        r = requests.get(f"{GRAPH}/me/accounts",
                         params={"fields": "id,name,instagram_business_account",
                                 "access_token": USER_ACCESS_TOKEN, "limit": 100},
                         timeout=15)
        pages = r.json().get("data", [])
        result = [{"fb_page_id": p["id"], "name": p.get("name"),
                   "ig_account_id": p.get("instagram_business_account", {}).get("id")}
                  for p in pages]
        return jsonify({"available": result, "total": len(result)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/sync-pages", methods=["POST", "GET"])
def sync_pages():
    """Pull FB pages and save to DB.
    Optional body: { "page_ids": ["123", "456"] } to only add specific pages.
    Without page_ids, adds all pages on your account.
    """
    try:
        data = (request.get_json(silent=True) or {}) if request.method == "POST" else {}
        allowed_ids = data.get("page_ids")  # optional filter

        r = requests.get(f"{GRAPH}/me/accounts",
                         params={"fields": "id,name,access_token,instagram_business_account",
                                 "access_token": USER_ACCESS_TOKEN, "limit": 100},
                         timeout=15)
        pages = r.json().get("data", [])
        if not pages:
            return jsonify({"error": "no pages returned", "raw": r.json()}), 400

        if allowed_ids:
            pages = [p for p in pages if p["id"] in allowed_ids]

        added, updated, skipped = 0, 0, 0
        for page in pages:
            fb_page_id = page["id"]
            page_name = page.get("name", "")
            page_token = page.get("access_token", "")
            ig_id = page.get("instagram_business_account", {}).get("id")

            if not ig_id:
                ig_id = get_ig_for_page(page_token, fb_page_id)

            if not ig_id:
                skipped += 1
                continue

            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO fan_pages (fb_page_id, ig_account_id, page_name, page_access_token)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fb_page_id) DO UPDATE
                  SET ig_account_id = EXCLUDED.ig_account_id,
                      page_name = EXCLUDED.page_name,
                      page_access_token = EXCLUDED.page_access_token
                RETURNING (xmax = 0) as inserted
            """, (fb_page_id, ig_id, page_name, page_token))
            row = cur.fetchone()
            if row and row[0]:
                added += 1
            else:
                updated += 1
            conn.commit()
            cur.close()
            conn.close()

        return jsonify({"added": added, "updated": updated, "skipped_no_ig": skipped})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

QUOTE_IMG_DIR = "/tmp/quote_images"
os.makedirs(QUOTE_IMG_DIR, exist_ok=True)

def make_quote_card(quote_text):
    """Generate a 1080x1080 purple gradient quote card, return file path."""
    W, H = 1080, 1080
    img = Image.new("RGB", (W, H))
    draw = ImageDraw.Draw(img)

    # Purple gradient background
    for y in range(H):
        ratio = y / H
        r = int(60 + ratio * 20)
        g = int(0 + ratio * 10)
        b = int(120 + ratio * 60)
        draw.line([(0, y), (W, y)], fill=(r, g, b))

    # Decorative accent line
    draw.rectangle([80, 80, 90, H - 80], fill=(255, 255, 255, 120))

    # Font
    font_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
    ]
    font = None
    small_font = None
    for fp in font_paths:
        if os.path.exists(fp):
            font = ImageFont.truetype(fp, 72)
            small_font = ImageFont.truetype(fp, 42)
            break
    if not font:
        font = ImageFont.load_default()
        small_font = font

    # Wrap quote text
    lines = textwrap.wrap(quote_text, width=22)
    total_h = len(lines) * 90
    y_start = (H - total_h) // 2 - 40

    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        tw = bbox[2] - bbox[0]
        x = (W - tw) // 2
        # Shadow
        draw.text((x + 3, y_start + 3), line, font=font, fill=(0, 0, 0, 100))
        draw.text((x, y_start), line, font=font, fill=(255, 255, 255))
        y_start += 90

    # Decorative divider
    draw.rectangle([(W // 2 - 60, y_start + 20), (W // 2 + 60, y_start + 24)], fill=(255, 255, 255))

    # Handle on card
    handle = ".....TheRealMiaSnow"
    hbbox = draw.textbbox((0, 0), handle, font=small_font)
    hw = hbbox[2] - hbbox[0]
    draw.text(((W - hw) // 2, y_start + 50), handle, font=small_font, fill=(220, 180, 255))

    filename = f"{uuid.uuid4().hex}.jpg"
    path = os.path.join(QUOTE_IMG_DIR, filename)
    img.save(path, "JPEG", quality=95)
    return filename

@app.route("/quote-image/<filename>")
def serve_quote_image(filename):
    path = os.path.join(QUOTE_IMG_DIR, filename)
    if not os.path.exists(path):
        return "not found", 404
    return send_file(path, mimetype="image/jpeg")

@app.route("/post-quote", methods=["POST"])
def post_quote():
    """Generate a quote card and post to all active fan pages.
    Body: { "quote": "your quote text", "caption": "optional caption override" }
    """
    data = request.get_json(silent=True) or {}
    quote = data.get("quote", "").strip()
    if not quote:
        return jsonify({"error": "quote required"}), 400

    caption = data.get("caption") or f"{quote}\n\n.....TheRealMiaSnow"

    try:
        filename = make_quote_card(quote)
    except Exception as e:
        return jsonify({"error": f"image generation failed: {e}"}), 500

    base_url = os.environ.get("BASE_URL", "https://fan-page-manager.onrender.com")
    media_url = f"{base_url}/quote-image/{filename}"

    threading.Thread(target=_post_to_all_pages,
                     args=(media_url, caption, "IMAGE"), daemon=True).start()
    return jsonify({"status": "posting", "media_url": media_url, "caption": caption})

@app.route("/post", methods=["POST"])
def post_content():
    """Post content to all active fan pages.
    Body: { media_url, caption, media_type (optional, IMAGE or VIDEO) }
    """
    data = request.json or {}
    media_url = data.get("media_url")
    caption = data.get("caption", "")
    media_type = data.get("media_type", "IMAGE").upper()

    if not media_url:
        return jsonify({"error": "media_url required"}), 400

    threading.Thread(target=_post_to_all_pages,
                     args=(media_url, caption, media_type), daemon=True).start()
    return jsonify({"status": "posting to all active pages", "media_url": media_url})

@app.route("/pages", methods=["GET"])
def list_pages():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, fb_page_id, ig_account_id, page_name, active, created_at FROM fan_pages ORDER BY created_at DESC")
        pages = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"pages": pages, "total": len(pages)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/pages/<int:page_id>/toggle", methods=["POST"])
def toggle_page(page_id):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE fan_pages SET active = NOT active WHERE id = %s RETURNING active", (page_id,))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        if not row:
            return jsonify({"error": "page not found"}), 404
        return jsonify({"page_id": page_id, "active": row[0]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/post-log", methods=["GET"])
def post_log():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT pl.id, fp.page_name, pl.ig_post_id, pl.caption,
                   pl.posted_at, pl.error
            FROM post_log pl
            JOIN fan_pages fp ON fp.id = pl.fan_page_id
            ORDER BY pl.posted_at DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"posts": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analytics", methods=["GET"])
def analytics():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT fp.page_name,
                   SUM(a.impressions) as impressions,
                   SUM(a.reach) as reach,
                   SUM(a.likes) as likes,
                   SUM(a.comments) as comments,
                   SUM(a.saves) as saves,
                   COUNT(DISTINCT a.ig_media_id) as posts_tracked
            FROM analytics_snapshots a
            JOIN fan_pages fp ON fp.id = a.fan_page_id
            WHERE a.pulled_at >= NOW() - INTERVAL '7 days'
            GROUP BY fp.page_name
            ORDER BY impressions DESC
        """)
        rows = cur.fetchall()
        cur.execute("""
            SELECT SUM(impressions) as impressions, SUM(reach) as reach,
                   SUM(likes) as likes, SUM(comments) as comments, SUM(saves) as saves
            FROM analytics_snapshots
            WHERE pulled_at >= NOW() - INTERVAL '7 days'
        """)
        totals = cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({"by_page": rows, "totals_7d": totals})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/followers", methods=["GET"])
def followers():
    """Get follower count for all active fan pages."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, ig_account_id, page_name, page_access_token FROM fan_pages WHERE active=TRUE AND ig_account_id IS NOT NULL")
        pages = cur.fetchall()
        cur.close()
        conn.close()

        results = []
        for page in pages:
            r = requests.get(f"{GRAPH}/{page['ig_account_id']}",
                             params={"fields": "followers_count,username,media_count",
                                     "access_token": page["page_access_token"]},
                             timeout=10)
            data = r.json()
            err = data.get("error", {})
            err_code = err.get("code")

            if err_code:
                status = "flagged/restricted" if err_code in (200, 368) else "token_expired" if err_code == 190 else f"error_{err_code}"
                results.append({
                    "page_name": page["page_name"],
                    "status": status,
                    "error": err.get("message")
                })
            else:
                results.append({
                    "page_name": page["page_name"],
                    "status": "ok",
                    "username": data.get("username"),
                    "followers": data.get("followers_count"),
                    "posts": data.get("media_count")
                })
        return jsonify({"pages": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analytics/today", methods=["GET"])
def analytics_today():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT fp.page_name,
                   SUM(a.impressions) as impressions,
                   SUM(a.reach) as reach,
                   SUM(a.likes) as likes,
                   SUM(a.saves) as saves
            FROM analytics_snapshots a
            JOIN fan_pages fp ON fp.id = a.fan_page_id
            WHERE a.pulled_at >= (NOW() AT TIME ZONE 'America/New_York')::date
            GROUP BY fp.page_name
            ORDER BY impressions DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"today": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/schedule-quotes", methods=["POST"])
def schedule_quotes():
    """Schedule a list of quotes.
    Body: { "quotes": ["q1","q2",...], "first_delay_minutes": 0, "interval_hours": 4 }
    First quote posts after first_delay_minutes, then every interval_hours after that.
    """
    data = request.get_json(silent=True) or {}
    quotes = data.get("quotes", [])
    first_delay = int(data.get("first_delay_minutes", 0))
    interval_hours = float(data.get("interval_hours", 4))

    if not quotes:
        return jsonify({"error": "quotes list required"}), 400

    import datetime as _dt
    now = _dt.datetime.utcnow()
    scheduled = []

    for i, quote in enumerate(quotes):
        if i == 0:
            due = now + _dt.timedelta(minutes=first_delay)
        else:
            due = now + _dt.timedelta(minutes=first_delay) + _dt.timedelta(hours=interval_hours * i)

        caption = f"{quote}\n\n.....TheRealMiaSnow"
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO quote_queue (quote, caption, scheduled_at) VALUES (%s, %s, %s) RETURNING id",
                    (quote, caption, due))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        scheduled.append({"id": row[0], "quote": quote[:40], "scheduled_at": due.isoformat()})

    return jsonify({"scheduled": len(scheduled), "quotes": scheduled})

@app.route("/delete-post", methods=["POST"])
def delete_post():
    """Delete an IG post from a specific fan page.
    Body: { "ig_post_id": "123456", "fan_page_id": 1 }
    """
    data = request.get_json(silent=True) or {}
    ig_post_id = data.get("ig_post_id")
    fan_page_id = data.get("fan_page_id")

    if not ig_post_id or not fan_page_id:
        return jsonify({"error": "ig_post_id and fan_page_id required"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT page_access_token FROM fan_pages WHERE id=%s", (fan_page_id,))
        page = cur.fetchone()
        cur.close()
        conn.close()

        if not page:
            return jsonify({"error": "page not found"}), 404

        r = requests.delete(f"{GRAPH}/{ig_post_id}",
                            params={"access_token": page["page_access_token"]},
                            timeout=10)
        result = r.json()
        if result.get("success"):
            return jsonify({"status": "deleted", "ig_post_id": ig_post_id})
        return jsonify({"error": result.get("error", {}).get("message", str(result))}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/migrate", methods=["GET"])
def migrate():
    init_db()
    return jsonify({"status": "ok"})

# ── STARTUP ───────────────────────────────────────────────────────────────────

init_db()
threading.Thread(target=_analytics_loop, daemon=True).start()
threading.Thread(target=_quote_scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
