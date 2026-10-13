import os
import time
import threading
import requests
from flask import Flask, jsonify, request
from psycopg2.extras import RealDictCursor
import psycopg2

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

@app.route("/sync-pages", methods=["POST", "GET"])
def sync_pages():
    """Pull all FB pages, find linked IG accounts, save to DB."""
    try:
        r = requests.get(f"{GRAPH}/me/accounts",
                         params={"fields": "id,name,access_token,instagram_business_account",
                                 "access_token": USER_ACCESS_TOKEN, "limit": 100},
                         timeout=15)
        pages = r.json().get("data", [])
        if not pages:
            return jsonify({"error": "no pages returned", "raw": r.json()}), 400

        added, updated = 0, 0
        for page in pages:
            fb_page_id = page["id"]
            page_name = page.get("name", "")
            page_token = page.get("access_token", "")
            ig_id = page.get("instagram_business_account", {}).get("id")

            if not ig_id:
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

        return jsonify({"synced": len(pages), "added": added, "updated": updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

@app.route("/migrate", methods=["GET"])
def migrate():
    init_db()
    return jsonify({"status": "ok"})

# ── STARTUP ───────────────────────────────────────────────────────────────────

init_db()
threading.Thread(target=_analytics_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
