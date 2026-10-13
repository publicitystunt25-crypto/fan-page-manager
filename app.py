import os
import time
import threading
import datetime as _dt
import requests
from flask import Flask, jsonify, request
from psycopg2.extras import RealDictCursor
import psycopg2

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
USER_ACCESS_TOKEN = os.environ.get("FB_USER_TOKEN")   # long-lived user token
MIA_IG_ID = os.environ.get("MIA_IG_ID")               # Mia's IG business account ID
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
        CREATE TABLE IF NOT EXISTS repost_queue (
            id SERIAL PRIMARY KEY,
            source_ig_media_id TEXT NOT NULL,
            fan_page_id INTEGER REFERENCES fan_pages(id),
            status TEXT DEFAULT 'pending',
            scheduled_at TIMESTAMPTZ DEFAULT NOW(),
            posted_at TIMESTAMPTZ,
            ig_creation_id TEXT,
            error TEXT,
            UNIQUE(source_ig_media_id, fan_page_id)
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
    """Get the IG business account linked to a FB page."""
    r = requests.get(f"{GRAPH}/{fb_page_id}",
                     params={"fields": "instagram_business_account", "access_token": page_access_token},
                     timeout=10)
    data = r.json()
    return data.get("instagram_business_account", {}).get("id")

def get_media_info(ig_media_id, token):
    """Fetch media details from Mia's IG."""
    r = requests.get(f"{GRAPH}/{ig_media_id}",
                     params={"fields": "media_type,media_url,thumbnail_url,caption,permalink",
                             "access_token": token},
                     timeout=10)
    return r.json()

def repost_to_page(ig_account_id, page_token, media_info):
    """Repost a single media item to one fan page IG account."""
    media_type = media_info.get("media_type", "IMAGE")
    caption = media_info.get("caption", "")
    media_url = media_info.get("media_url") or media_info.get("thumbnail_url")

    if not media_url:
        return None, "no media_url"

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

    # publish
    pub = requests.post(f"{GRAPH}/{ig_account_id}/media_publish",
                        data={"creation_id": creation_id, "access_token": page_token},
                        timeout=30)
    pub_result = pub.json()
    post_id = pub_result.get("id")
    if not post_id:
        return None, pub_result.get("error", {}).get("message", str(pub_result))

    return post_id, None

# ── BACKGROUND LOOPS ──────────────────────────────────────────────────────────

_repost_lock = threading.Lock()

def _fetch_mia_recent_media():
    """Pull Mia's last 20 posts."""
    r = requests.get(f"{GRAPH}/{MIA_IG_ID}/media",
                     params={"fields": "id,media_type,media_url,thumbnail_url,caption,timestamp",
                             "limit": 20, "access_token": USER_ACCESS_TOKEN},
                     timeout=15)
    return r.json().get("data", [])

def _queue_new_posts():
    """Find Mia's posts not yet queued, insert into repost_queue for all active pages."""
    try:
        posts = _fetch_mia_recent_media()
        if not posts:
            return
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id FROM fan_pages WHERE active = TRUE AND ig_account_id IS NOT NULL")
        pages = cur.fetchall()
        for post in posts:
            mid = post["id"]
            for page in pages:
                cur.execute("""
                    INSERT INTO repost_queue (source_ig_media_id, fan_page_id)
                    VALUES (%s, %s)
                    ON CONFLICT (source_ig_media_id, fan_page_id) DO NOTHING
                """, (mid, page["id"]))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[queue-new-posts] {e}")

def _process_repost_queue():
    """Process pending reposts, staggered to avoid rate limits."""
    with _repost_lock:
        try:
            conn = get_conn()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT rq.id, rq.source_ig_media_id, rq.fan_page_id,
                       fp.ig_account_id, fp.page_access_token
                FROM repost_queue rq
                JOIN fan_pages fp ON fp.id = rq.fan_page_id
                WHERE rq.status = 'pending'
                  AND fp.active = TRUE
                ORDER BY rq.scheduled_at ASC
                LIMIT 20
            """)
            jobs = cur.fetchall()
            cur.close()
            conn.close()

            for job in jobs:
                media_info = get_media_info(job["source_ig_media_id"], USER_ACCESS_TOKEN)
                post_id, err = repost_to_page(job["ig_account_id"],
                                              job["page_access_token"], media_info)
                conn2 = get_conn()
                cur2 = conn2.cursor()
                if post_id:
                    cur2.execute("""
                        UPDATE repost_queue
                        SET status='done', posted_at=NOW(), ig_creation_id=%s
                        WHERE id=%s
                    """, (post_id, job["id"]))
                    print(f"[repost] queued job {job['id']} → posted {post_id}")
                else:
                    cur2.execute("""
                        UPDATE repost_queue SET status='error', error=%s WHERE id=%s
                    """, (err, job["id"]))
                    print(f"[repost] job {job['id']} error: {err}")
                conn2.commit()
                cur2.close()
                conn2.close()
                time.sleep(3)  # stagger between pages
        except Exception as e:
            print(f"[process-queue] {e}")

def _repost_loop():
    time.sleep(15)
    while True:
        _queue_new_posts()
        _process_repost_queue()
        time.sleep(300)  # check every 5 minutes

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
        time.sleep(3600)  # pull analytics every hour

# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({"status": "fan-page-manager running"})

@app.route("/sync-pages", methods=["POST", "GET"])
def sync_pages():
    """Pull all FB pages from /me/accounts, find their linked IG accounts, save to DB."""
    try:
        r = requests.get(f"{GRAPH}/me/accounts",
                         params={"access_token": USER_ACCESS_TOKEN, "limit": 100},
                         timeout=15)
        pages = r.json().get("data", [])
        if not pages:
            return jsonify({"error": "no pages returned", "raw": r.json()}), 400

        added, updated, skipped = 0, 0, 0
        for page in pages:
            fb_page_id = page["id"]
            page_name = page.get("name", "")
            page_token = page.get("access_token", "")

            ig_id = get_ig_for_page(page_token, fb_page_id)

            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO fan_pages (fb_page_id, ig_account_id, page_name, page_access_token)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fb_page_id) DO UPDATE
                  SET ig_account_id = EXCLUDED.ig_account_id,
                      page_name = EXCLUDED.page_name,
                      page_access_token = EXCLUDED.page_access_token
            """, (fb_page_id, ig_id, page_name, page_token))
            if cur.rowcount == 1:
                added += 1
            else:
                updated += 1
            conn.commit()
            cur.close()
            conn.close()

        return jsonify({"synced": len(pages), "added": added, "updated": updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/pages", methods=["GET"])
def list_pages():
    """List all fan pages."""
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
    """Enable or disable a fan page."""
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

@app.route("/repost-now", methods=["POST"])
def repost_now():
    """Manually trigger a repost cycle."""
    threading.Thread(target=_queue_new_posts, daemon=True).start()
    threading.Thread(target=_process_repost_queue, daemon=True).start()
    return jsonify({"status": "repost cycle triggered"})

@app.route("/queue", methods=["GET"])
def view_queue():
    """View repost queue status."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT status, COUNT(*) as count
            FROM repost_queue
            GROUP BY status
        """)
        summary = cur.fetchall()
        cur.execute("""
            SELECT rq.id, rq.source_ig_media_id, fp.page_name, rq.status,
                   rq.scheduled_at, rq.posted_at, rq.error
            FROM repost_queue rq
            JOIN fan_pages fp ON fp.id = rq.fan_page_id
            ORDER BY rq.scheduled_at DESC
            LIMIT 50
        """)
        recent = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"summary": summary, "recent": recent})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analytics", methods=["GET"])
def analytics():
    """Analytics summary across all fan pages."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT fp.page_name, fp.ig_account_id,
                   SUM(a.impressions) as total_impressions,
                   SUM(a.reach) as total_reach,
                   SUM(a.likes) as total_likes,
                   SUM(a.comments) as total_comments,
                   SUM(a.saves) as total_saves,
                   COUNT(DISTINCT a.ig_media_id) as posts_tracked
            FROM analytics_snapshots a
            JOIN fan_pages fp ON fp.id = a.fan_page_id
            WHERE a.pulled_at >= NOW() - INTERVAL '7 days'
            GROUP BY fp.id, fp.page_name, fp.ig_account_id
            ORDER BY total_impressions DESC
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

@app.route("/analytics/today", methods=["GET"])
def analytics_today():
    """Today's analytics across all pages."""
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

@app.route("/migrate", methods=["GET"])
def migrate():
    init_db()
    return jsonify({"status": "ok"})

# ── STARTUP ───────────────────────────────────────────────────────────────────

init_db()
threading.Thread(target=_repost_loop, daemon=True).start()
threading.Thread(target=_analytics_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
