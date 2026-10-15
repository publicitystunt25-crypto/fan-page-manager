import os
import re
import json
import time
import random
import threading
import uuid
import textwrap
import requests
from flask import Flask, jsonify, request, send_file, Response
from psycopg2.extras import RealDictCursor
import psycopg2
from PIL import Image, ImageDraw, ImageFont

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
USER_ACCESS_TOKEN = os.environ.get("FB_USER_TOKEN")
GRAPH = "https://graph.facebook.com/" + os.environ.get("FB_GRAPH_VERSION", "v23.0")
IG_LOGIN_GRAPH = "https://graph.instagram.com/" + os.environ.get("FB_GRAPH_VERSION", "v23.0")

def _graph_for(auth_type):
    """Facebook-linked pages use graph.facebook.com; accounts connected with Instagram Login use graph.instagram.com."""
    return IG_LOGIN_GRAPH if auth_type == "instagram" else GRAPH

PAGE_GROUPS = {"promo", "clipper"}

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
        ALTER TABLE fan_pages ADD COLUMN IF NOT EXISTS page_group TEXT DEFAULT 'promo';
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS clip_submissions (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            submitter_name TEXT,
            status TEXT DEFAULT 'pending',
            error TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            decided_at TIMESTAMPTZ,
            posted_at TIMESTAMPTZ
        );
        ALTER TABLE clip_submissions ADD COLUMN IF NOT EXISTS variants TEXT;
        ALTER TABLE post_log ADD COLUMN IF NOT EXISTS permalink TEXT;
        ALTER TABLE fan_pages ADD COLUMN IF NOT EXISTS name_locked BOOLEAN DEFAULT FALSE;
        ALTER TABLE fan_pages ADD COLUMN IF NOT EXISTS auth_type TEXT DEFAULT 'facebook';
        ALTER TABLE fan_pages ADD COLUMN IF NOT EXISTS token_expires_at TIMESTAMPTZ;
        ALTER TABLE fan_pages ADD COLUMN IF NOT EXISTS analytics_error TEXT;
        ALTER TABLE fan_pages ADD COLUMN IF NOT EXISTS analytics_pulled_at TIMESTAMPTZ;
        CREATE TABLE IF NOT EXISTS post_metrics (
            ig_media_id TEXT PRIMARY KEY,
            fan_page_id INTEGER REFERENCES fan_pages(id),
            permalink TEXT,
            media_type TEXT,
            caption TEXT,
            posted_at TIMESTAMPTZ,
            views INTEGER DEFAULT 0,
            reach INTEGER DEFAULT 0,
            likes INTEGER DEFAULT 0,
            comments INTEGER DEFAULT 0,
            saves INTEGER DEFAULT 0,
            shares INTEGER DEFAULT 0,
            prev_views INTEGER,
            prev_likes INTEGER,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        ALTER TABLE post_metrics ADD COLUMN IF NOT EXISTS reposts INTEGER DEFAULT 0;
        ALTER TABLE post_metrics ADD COLUMN IF NOT EXISTS follows INTEGER DEFAULT 0;
        ALTER TABLE post_metrics ADD COLUMN IF NOT EXISTS profile_visits INTEGER DEFAULT 0;
        CREATE TABLE IF NOT EXISTS page_daily (
            fan_page_id INTEGER REFERENCES fan_pages(id),
            day DATE,
            followers INTEGER,
            media_count INTEGER,
            new_followers INTEGER,
            profile_views INTEGER,
            reach INTEGER,
            PRIMARY KEY (fan_page_id, day)
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

def ig_create_container(ig_account_id, token, media_url, caption, media_type="IMAGE", graph=None):
    """Step 1: hand the media to Instagram. Returns (creation_id, error)."""
    g = graph or GRAPH
    if media_type == "VIDEO":
        payload = {"video_url": media_url, "media_type": "REELS", "caption": caption, "access_token": token}
    else:
        payload = {"image_url": media_url, "caption": caption, "access_token": token}
    result = requests.post(f"{g}/{ig_account_id}/media", data=payload, timeout=30).json()
    creation_id = result.get("id")
    if not creation_id:
        return None, result.get("error", {}).get("message", str(result))
    return creation_id, None

def ig_create_container_tagged(ig_account_id, token, media_url, caption, media_type="IMAGE", graph=None,
                               tags=None, collaborators=None):
    """Like ig_create_container, but also tags users and/or invites collaborators. If Instagram rejects the
    combination it falls back step by step (tag + collab, collab only, tag only, neither) so the post still goes out.
    Returns (creation_id, error, note)."""
    g = graph or GRAPH
    tags = [str(u).lstrip("@") for u in (tags or [])]
    collabs = [str(u).lstrip("@") for u in (collaborators or [])]
    if not (tags or collabs):
        cid, err = ig_create_container(ig_account_id, token, media_url, caption, media_type, graph)
        return cid, err, ""
    if media_type == "VIDEO":
        base = {"video_url": media_url, "media_type": "REELS", "caption": caption, "access_token": token}
        tag_json = json.dumps([{"username": u} for u in tags])      # Reels reject x/y coordinates
    else:
        base = {"image_url": media_url, "caption": caption, "access_token": token}
        tag_json = json.dumps([{"username": u, "x": 0.5, "y": 0.5} for u in tags])
    attempts = []
    if tags and collabs:
        attempts.append(("tag + collab invite", {"user_tags": tag_json, "collaborators": json.dumps(collabs)}))
    if collabs:
        attempts.append(("collab invite only", {"collaborators": json.dumps(collabs)}))
    if tags:
        attempts.append(("tag only", {"user_tags": tag_json}))
    errors = []
    for label, extra in attempts:
        result = requests.post(f"{g}/{ig_account_id}/media", data={**base, **extra}, timeout=30).json()
        if result.get("id"):
            note = f"attached: {label}" + (f" (dropped: {'; '.join(errors)[:120]})" if errors else "")
            return result["id"], None, note
        errors.append(f"{label}: {result.get('error', {}).get('message', str(result))[:70]}")
    cid, err = ig_create_container(ig_account_id, token, media_url, caption, media_type, graph)
    return cid, err, (f"posted WITHOUT tag/collab: {'; '.join(errors)[:160]}" if cid else "")

def ig_wait_ready(creation_id, token, media_type="IMAGE", graph=None):
    """Step 2: Instagram processes the media asynchronously; publishing early gives 'Media ID is not available'.
    Returns an error string, or None when it is ready."""
    g = graph or GRAPH
    deadline = time.time() + (300 if media_type == "VIDEO" else 45)
    while time.time() < deadline:
        st = requests.get(f"{g}/{creation_id}", params={"fields": "status_code", "access_token": token},
                          timeout=15).json().get("status_code")
        if st == "FINISHED":
            return None
        if st in ("ERROR", "EXPIRED"):
            return f"media processing {st}"
        time.sleep(3)
    return "media processing timed out"

def ig_publish(ig_account_id, token, creation_id, graph=None):
    """Step 3: publish the processed media. Returns (post_id, error)."""
    g = graph or GRAPH
    pub = requests.post(f"{g}/{ig_account_id}/media_publish",
                        data={"creation_id": creation_id, "access_token": token}, timeout=30).json()
    post_id = pub.get("id")
    if not post_id:
        return None, pub.get("error", {}).get("message", str(pub))
    return post_id, None

def post_to_ig(ig_account_id, page_token, media_url, caption, media_type="IMAGE", graph=None):
    """Post a photo or video to one IG account (create, wait, publish)."""
    creation_id, err = ig_create_container(ig_account_id, page_token, media_url, caption, media_type, graph)
    if err:
        return None, err
    err = ig_wait_ready(creation_id, page_token, media_type, graph)
    if err:
        return None, err
    return ig_publish(ig_account_id, page_token, creation_id, graph)

def _fetch_permalink(graph, post_id, token):
    """The public instagram.com link for a just-published post (None if Instagram will not say)."""
    try:
        return requests.get(f"{graph}/{post_id}", params={"fields": "permalink", "access_token": token},
                            timeout=15).json().get("permalink")
    except Exception:
        return None

def _log_post(page_id, post_id, caption, media_url, err, permalink=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""INSERT INTO post_log (fan_page_id, ig_post_id, caption, media_url, error, permalink)
                   VALUES (%s, %s, %s, %s, %s, %s)""", (page_id, post_id, caption, media_url, err, permalink))
    conn.commit()
    cur.close()
    conn.close()

PUBLISH_GAP = (30, 60)   # seconds between publishes: close together, but never in the same minute

def _post_pipelined(items, gap=PUBLISH_GAP):
    """items: [{"page_ids": [..], "caption", "url"}]. All videos are handed to Instagram at once so they
    process in parallel, then published one at a time with a random gap. Returns [(page_name, post_id, error)]."""
    from concurrent.futures import ThreadPoolExecutor
    wanted = sorted({int(p) for it in items for p in it["page_ids"]})
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""SELECT id, ig_account_id, page_access_token, page_name, auth_type FROM fan_pages
                   WHERE active=TRUE AND ig_account_id IS NOT NULL AND page_group='clipper' AND id = ANY(%s)""", (wanted,))
    pages = {r["id"]: r for r in cur.fetchall()}
    cur.close()
    conn.close()

    tasks = []   # (page row, item) in the order the items were listed
    for it in items:
        for pid in it["page_ids"]:
            if int(pid) in pages:
                tasks.append((pages[int(pid)], it))

    def prepare(task):
        page, it = task
        g = _graph_for(page.get("auth_type"))
        cid, err, note = ig_create_container_tagged(page["ig_account_id"], page["page_access_token"], it["url"],
                                                    it["caption"], "VIDEO", g, it.get("tags"), it.get("collaborators"))
        if err:
            return None, err, note
        return cid, ig_wait_ready(cid, page["page_access_token"], "VIDEO", g), note

    with ThreadPoolExecutor(max_workers=max(1, min(6, len(tasks)))) as pool:
        prepared = list(pool.map(prepare, tasks))

    results = []
    for n, ((page, it), (cid, err, note)) in enumerate(zip(tasks, prepared)):
        post_id = None
        if not err:
            if n:
                time.sleep(random.uniform(*gap))
            post_id, err = ig_publish(page["ig_account_id"], page["page_access_token"], cid, _graph_for(page.get("auth_type")))
        link = _fetch_permalink(_graph_for(page.get("auth_type")), post_id, page["page_access_token"]) if post_id else None
        try:
            _log_post(page["id"], post_id, it["caption"], it["url"], err, link)
        except Exception as e:
            print(f"[post] could not log: {type(e).__name__}")
        shown = f"{post_id} ({note})" if (post_id and note) else post_id
        results.append((page["page_name"], shown, err))
        print(f"[post] {page['page_name']}: {'posted ' + str(shown) if not err else 'error ' + str(err)}")
    return results

def upload_to_imgbb(image_path):
    """Upload image to imgbb and return public URL."""
    imgbb_key = os.environ.get("IMGBB_API_KEY")
    if not imgbb_key:
        return None
    with open(image_path, "rb") as f:
        import base64
        b64 = base64.b64encode(f.read()).decode("utf-8")
    r = requests.post("https://api.imgbb.com/1/upload",
                      data={"key": imgbb_key, "image": b64},
                      timeout=30)
    result = r.json()
    return result.get("data", {}).get("url")

def _post_to_all_pages(media_url, caption, media_type="IMAGE", delay=(3, 3), group="promo", page_ids=None):
    """Posts to all active fan pages in `group` (promo / clipper), sleeping a random delay[0]..delay[1] seconds between pages.
    Returns a list of (page_name, ig_post_id, error)."""
    results = []
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        sql = "SELECT id, ig_account_id, page_access_token, page_name, auth_type FROM fan_pages WHERE active=TRUE AND ig_account_id IS NOT NULL AND page_group=%s"
        args = [group]
        if page_ids is not None:   # only these fan_pages ids
            sql += " AND id = ANY(%s)"
            args.append(list(page_ids))
        cur.execute(sql, args)
        pages = cur.fetchall()
        cur.close()
        conn.close()

        for n, page in enumerate(pages):
            if n:
                time.sleep(random.uniform(*delay))
            post_id, err = post_to_ig(page["ig_account_id"], page["page_access_token"],
                                      media_url, caption, media_type, graph=_graph_for(page.get("auth_type")))
            conn2 = get_conn()
            cur2 = conn2.cursor()
            cur2.execute("""
                INSERT INTO post_log (fan_page_id, ig_post_id, caption, media_url, error)
                VALUES (%s, %s, %s, %s, %s)
            """, (page["id"], post_id, caption, media_url, err))
            conn2.commit()
            cur2.close()
            conn2.close()
            results.append((page["page_name"], post_id, err))
            if err:
                print(f"[post] {page['page_name']} error: {err}")
            else:
                print(f"[post] {page['page_name']} -> {post_id}")
    except Exception as e:
        print(f"[post-all] {e}")
    return results

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
                image_path = os.path.join(QUOTE_IMG_DIR, filename)
                media_url = upload_to_imgbb(image_path)
                if not media_url:
                    base_url = os.environ.get("BASE_URL", "https://fan-page-manager.onrender.com")
                    media_url = f"{base_url}/quote-image/{filename}"
                    time.sleep(5)
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

_pull_lock = threading.Lock()
METRIC_STATE = {"reposts": "untested", "follows": "untested", "profile_visits": "untested"}   # untested / supported / rejected
# metric sets to try, richest first (availability differs by media type and account)
INSIGHT_SETS = ("views,reach,saved,shares,reposts,follows,profile_visits,total_interactions",
                "views,reach,saved,shares,reposts,total_interactions",
                "views,reach,saved,shares,total_interactions", "views,reach,saved", "reach,saved,shares", "reach,saved")

def _media_insights(media_id, token, graph=None):
    """Latest insights for one post -> (dict, error message). The dict includes '__set': index of the metric set used."""
    err = ""
    for idx, metrics in enumerate(INSIGHT_SETS):
        try:
            r = requests.get(f"{graph or GRAPH}/{media_id}/insights",
                             params={"metric": metrics, "access_token": token}, timeout=20).json()
        except Exception as e:
            return {}, type(e).__name__
        if "data" in r:
            out = {}
            for item in r["data"]:
                vals = item.get("values") or [{}]
                v = vals[0].get("value", (item.get("total_value") or {}).get("value"))
                if isinstance(v, (int, float)):
                    out[item["name"]] = int(v)
            out["__set"] = idx
            return out, ""
        err = (r.get("error") or {}).get("message", "")[:200]
    return {}, err

def _note_metric_support(ins):
    """Remember which optional metrics Instagram accepts (persisted, so every process reports the same answer)."""
    idx = ins.pop("__set", None)
    if idx is None:
        return
    asked = {"reposts": idx <= 1, "follows": idx == 0, "profile_visits": idx == 0}
    changed = False
    for key in METRIC_STATE:
        before = METRIC_STATE[key]
        if key in ins:
            METRIC_STATE[key] = "supported"
        elif asked[key] is False and before != "supported":
            METRIC_STATE[key] = "rejected"
        changed = changed or METRIC_STATE[key] != before
    if changed:
        try:
            _set_setting("metric_support", json.dumps(METRIC_STATE))
        except Exception as e:
            print(f"[analytics] could not store metric support: {type(e).__name__}")

def _pull_page_metrics(page):
    """Refresh the latest numbers for a page's recent posts. Returns an error string ('' when fine)."""
    r = requests.get(f"{_graph_for(page.get('auth_type'))}/{page['ig_account_id']}/media",
                     params={"fields": "id,caption,media_type,media_product_type,permalink,timestamp,like_count,comments_count",
                             "limit": 25, "access_token": page["page_access_token"]}, timeout=30).json()
    if "data" not in r:
        return (r.get("error") or {}).get("message", "no data returned")[:300]
    last_err, got_any = "", False
    conn = get_conn()
    cur = conn.cursor()
    for post in r["data"]:
        ins, err = _media_insights(post["id"], page["page_access_token"], _graph_for(page.get("auth_type")))
        _note_metric_support(ins)
        last_err = err or last_err
        got_any = got_any or bool(ins)
        cur.execute("""
            INSERT INTO post_metrics (ig_media_id, fan_page_id, permalink, media_type, caption, posted_at,
                                      views, reach, likes, comments, saves, shares, reposts, follows, profile_visits)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ig_media_id) DO UPDATE SET
                prev_views = post_metrics.views, prev_likes = post_metrics.likes,
                permalink = EXCLUDED.permalink, caption = EXCLUDED.caption,
                views = EXCLUDED.views, reach = EXCLUDED.reach, likes = EXCLUDED.likes,
                comments = EXCLUDED.comments, saves = EXCLUDED.saves, shares = EXCLUDED.shares, reposts = EXCLUDED.reposts,
                follows = EXCLUDED.follows, profile_visits = EXCLUDED.profile_visits,
                updated_at = NOW()""",
            (post["id"], page["id"], post.get("permalink"),
             post.get("media_product_type") or post.get("media_type"), (post.get("caption") or "")[:300],
             post.get("timestamp"), ins.get("views", 0), ins.get("reach", 0),
             post.get("like_count", 0), post.get("comments_count", 0), ins.get("saved", 0), ins.get("shares", 0),
             ins.get("reposts", 0), ins.get("follows", 0), ins.get("profile_visits", 0)))
        time.sleep(0.2)
    conn.commit()
    cur.close()
    conn.close()
    if not r["data"]:
        return "no posts yet"
    return "" if got_any else last_err   # some old posts lack a metric; only warn when nothing came back

def _account_total(g, ig_id, token, metric, since, until):
    """Total of an account-level metric over [since, until) -> int or None."""
    r = requests.get(f"{g}/{ig_id}/insights",
                     params={"metric": metric, "period": "day", "metric_type": "total_value",
                             "since": int(since), "until": int(until), "access_token": token}, timeout=20).json()
    for item in r.get("data", []):
        v = (item.get("total_value") or {}).get("value")
        if isinstance(v, (int, float)):
            return int(v)
    return None

def _pull_account_daily(page):
    """Followers snapshot for today, plus Instagram's own daily new-followers, profile visits and reach.
    Days are UTC dates. Safe to call repeatedly: it upserts, and only asks for days it is missing."""
    from datetime import datetime, timedelta, timezone
    g = _graph_for(page.get("auth_type"))
    ig, tok, pid = page["ig_account_id"], page["page_access_token"], page["id"]
    prof = requests.get(f"{g}/{ig}", params={"fields": "followers_count,media_count", "access_token": tok}, timeout=20).json()
    if "followers_count" not in prof:
        return
    today = datetime.now(timezone.utc).date()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""INSERT INTO page_daily (fan_page_id, day, followers, media_count) VALUES (%s, %s, %s, %s)
                   ON CONFLICT (fan_page_id, day) DO UPDATE SET followers=EXCLUDED.followers, media_count=EXCLUDED.media_count""",
                (pid, today, prof.get("followers_count"), prof.get("media_count")))
    conn.commit()

    # new followers per day (time series, last 7 days). end_time marks the END of the day it describes.
    try:
        now = datetime.now(timezone.utc)
        r = requests.get(f"{g}/{ig}/insights",
                         params={"metric": "follower_count", "period": "day",
                                 "since": int((now - timedelta(days=7)).timestamp()), "until": int(now.timestamp()),
                                 "access_token": tok}, timeout=20).json()
        for item in r.get("data", []):
            for v in item.get("values", []):
                if isinstance(v.get("value"), (int, float)) and v.get("end_time"):
                    end = datetime.strptime(v["end_time"][:19], "%Y-%m-%dT%H:%M:%S").date()
                    day = end - timedelta(days=1)
                    cur.execute("""INSERT INTO page_daily (fan_page_id, day, new_followers) VALUES (%s, %s, %s)
                                   ON CONFLICT (fan_page_id, day) DO UPDATE SET new_followers=EXCLUDED.new_followers""",
                                (pid, day, int(v["value"])))
        conn.commit()
    except Exception as e:
        print(f"[daily] new followers: {type(e).__name__}")

    # profile visits and reach for the last 3 completed days, only the ones we do not have yet
    cur.execute("SELECT day, profile_views, reach FROM page_daily WHERE fan_page_id=%s AND day >= %s", (pid, today - timedelta(days=3)))
    have = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    for back in (1, 2, 3):
        day = today - timedelta(days=back)
        if have.get(day, (None, None))[0] is not None:
            continue
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()
        try:
            pv = _account_total(g, ig, tok, "profile_views", start, start + 86400)
            rc = _account_total(g, ig, tok, "reach", start, start + 86400)
            if pv is not None or rc is not None:
                cur.execute("""INSERT INTO page_daily (fan_page_id, day, profile_views, reach) VALUES (%s, %s, %s, %s)
                               ON CONFLICT (fan_page_id, day) DO UPDATE SET profile_views=EXCLUDED.profile_views, reach=EXCLUDED.reach""",
                            (pid, day, pv, rc))
        except Exception as e:
            print(f"[daily] day insights: {type(e).__name__}")
            break
        time.sleep(0.2)
    conn.commit()
    cur.close()
    conn.close()

def _pull_analytics_once():
    """One pass over every active page. Skips if a pass is already running."""
    if not _pull_lock.acquire(blocking=False):
        return False
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, ig_account_id, page_access_token, auth_type FROM fan_pages WHERE active=TRUE AND ig_account_id IS NOT NULL")
        pages = cur.fetchall()
        cur.close()
        conn.close()
        for page in pages:
            try:
                err = _pull_page_metrics(page)
            except Exception as e:
                err = type(e).__name__
                print(f"[analytics] page {page['id']}: {err}")
            try:
                _pull_account_daily(page)
            except Exception as e:
                print(f"[analytics] daily {page['id']}: {type(e).__name__}")
            try:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("UPDATE fan_pages SET analytics_error=%s, analytics_pulled_at=NOW() WHERE id=%s",
                            (err or None, page["id"]))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print(f"[analytics] status write: {type(e).__name__}")
            time.sleep(1)
    except Exception as e:
        print(f"[analytics-loop] {type(e).__name__}")
    finally:
        _pull_lock.release()
    return True

def _analytics_loop():
    time.sleep(45)
    while True:
        _pull_analytics_once()
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
                                 "access_token": get_user_token(), "limit": 100},
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
    Optional "group": "promo" (default) or "clipper" for newly added pages; existing pages keep their group.
    """
    try:
        data = (request.get_json(silent=True) or {}) if request.method == "POST" else {}
        allowed_ids = data.get("page_ids")  # optional filter
        group = data.get("group", "promo")
        if group not in PAGE_GROUPS:
            return jsonify({"error": f"group must be one of {sorted(PAGE_GROUPS)}"}), 400

        r = requests.get(f"{GRAPH}/me/accounts",
                         params={"fields": "id,name,access_token,instagram_business_account",
                                 "access_token": get_user_token(), "limit": 100},
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
                INSERT INTO fan_pages (fb_page_id, ig_account_id, page_name, page_access_token, page_group)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fb_page_id) DO UPDATE
                  SET ig_account_id = EXCLUDED.ig_account_id,
                      page_name = CASE WHEN fan_pages.name_locked THEN fan_pages.page_name ELSE EXCLUDED.page_name END,
                      page_access_token = EXCLUDED.page_access_token
                RETURNING (xmax = 0) as inserted
            """, (fb_page_id, ig_id, page_name, page_token, group))
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

# ── CLIPS ─────────────────────────────────────────────────────────────────────

from clip_render import (CLIP_DIR, download_clip, detect_crop, render_clip, clean_hook,
                         generate_clip_hook, generate_clip_caption, channel_slug_from)

CLIP_URL_RE = re.compile(
    r"^https://(?:(?:www|m|clips)\.)?(?:twitch\.tv|kick\.com)/[A-Za-z0-9_\-./?=&%#]+$")

def valid_clip_url(url):
    """Strict check: https, host is exactly twitch.tv / kick.com (or their clips/www/m subdomains)."""
    return bool(url) and len(url) <= 300 and bool(CLIP_URL_RE.match(url))

def process_clip(url, channel_override=None, hint="", caption_override=None, delay=(3, 3), hook=None, variants=None):
    """Download, brand, caption and post one clip to all active pages.
    hook: None = AI-written on-screen hook, "" = no hook, any other text = use it.
    variants: [{"page_ids": [..], "text": overlay text for the whole clip, "caption": ...}] renders one
    video per variant and posts each only to its own pages.
    Raises on download/render failure. Returns the per-page results list."""
    src = out = None
    try:
        print(f"[clip] downloading {url}")
        src, info = download_clip(url)
        print(f"[clip] downloaded {os.path.getsize(src)//1024} KB")
        platform = "kick" if "kick.com" in url else "twitch"
        slug = channel_slug_from(url, info, channel_override)
        channel = channel_override or slug
        title = info.get("title") or "Clip"
        handle_tag = f"{channel} on {platform.capitalize()}"
        if variants:
            results = []
            base_url = os.environ.get("BASE_URL", "https://fan-page-manager.onrender.com")
            for i, v in enumerate(variants):
                if i:
                    time.sleep(random.uniform(*delay))
                print(f"[clip] variant {i + 1}/{len(variants)} for pages {v['page_ids']}")
                fname = render_clip(src, slug, platform, clean_hook(v.get("text", "")),
                                    hook_top=230, hook_size=62, hook_seconds=0,
                                    crop_factor=v.get("crop", 1.0), crop_x=v.get("crop_x", 0.5),
                                    crop_y=v.get("crop_y", 0.5), strip_lift=v.get("strip_lift"),
                                    strip_pos=v.get("strip_pos", "bottom"))
                vout = os.path.join(CLIP_DIR, fname)
                try:
                    results += _post_to_all_pages(f"{base_url}/clip-video/{fname}", v["caption"], "VIDEO",
                                                  delay=delay, group="clipper", page_ids=v["page_ids"])
                finally:
                    if os.path.exists(vout):
                        os.remove(vout)
            return results
        print(f"[clip] rendering as {slug}/{platform}")
        hook_text = generate_clip_hook(title, channel, hint) if hook is None else clean_hook(hook)
        print(f"[clip] hook: {hook_text!r}")
        filename = render_clip(src, slug, platform, hook_text)
        out = os.path.join(CLIP_DIR, filename)
        print(f"[clip] rendered {filename} ({os.path.getsize(out)//1024} KB)")
        os.remove(src)
        src = None
        caption = caption_override or generate_clip_caption(title, channel, platform, handle_tag, hint)
        base_url = os.environ.get("BASE_URL", "https://fan-page-manager.onrender.com")
        media_url = f"{base_url}/clip-video/{filename}"
        print(f"[clip] posting {channel} -> {media_url}")
        return _post_to_all_pages(media_url, caption, "VIDEO", delay=delay, group="clipper")
    finally:
        for p in (src, out):
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

def _process_and_post_clip(url, channel_override, hint, caption_override, hook=None):
    try:
        process_clip(url, channel_override, hint, caption_override, hook=hook)
    except Exception as e:
        print(f"[clip] failed: {e}")

@app.route("/clip-video/<filename>")
def serve_clip_video(filename):
    path = os.path.join(CLIP_DIR, os.path.basename(filename))
    if not os.path.exists(path):
        return "not found", 404
    return send_file(path, mimetype="video/mp4")

@app.route("/post-clip", methods=["POST"])
def post_clip():
    """Take a Twitch/Kick clip URL, brand it, caption it and post as a Reel to all active pages.
    Body: { "url": "...", "channel": "optional name override", "hint": "optional caption context",
            "caption": "optional full caption override",
            "hook": "optional on-screen text for the first 3s; omit for AI-written, \"\" for none" }
    """
    if ADMIN_KEY and not _admin_ok():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not valid_clip_url(url):
        return jsonify({"error": "a twitch.tv or kick.com clip url is required"}), 400
    threading.Thread(target=_process_and_post_clip,
                     args=(url, data.get("channel"), data.get("hint", ""), data.get("caption"),
                           (data.get("hook") or "") if "hook" in data else None),
                     daemon=True).start()
    return jsonify({"status": "processing", "url": url})

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
        cur.execute("SELECT id, fb_page_id, ig_account_id, page_name, page_group, active, created_at FROM fan_pages ORDER BY created_at DESC")
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

@app.route("/pages/<int:page_id>/group", methods=["POST"])
def set_page_group(page_id):
    """Body: { "group": "promo" | "clipper" } - which kind of content this page receives."""
    group = (request.get_json(silent=True) or {}).get("group")
    if group not in PAGE_GROUPS:
        return jsonify({"error": f"group must be one of {sorted(PAGE_GROUPS)}"}), 400
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE fan_pages SET page_group=%s WHERE id=%s RETURNING id", (group, page_id))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        if not row:
            return jsonify({"error": "page not found"}), 404
        return jsonify({"page_id": page_id, "group": group})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/post-log", methods=["GET"])
def post_log():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT pl.id, fp.page_name, pl.ig_post_id, pl.caption,
                   pl.posted_at, pl.error, COALESCE(pl.permalink, m.permalink) AS permalink
            FROM post_log pl
            JOIN fan_pages fp ON fp.id = pl.fan_page_id
            LEFT JOIN post_metrics m ON m.ig_media_id = pl.ig_post_id
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
    """7-day totals from each post's latest numbers (not summed snapshots)."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT fp.page_name, COUNT(*) AS posts_tracked,
                   SUM(m.views) AS views, SUM(m.reach) AS reach, SUM(m.likes) AS likes,
                   SUM(m.comments) AS comments, SUM(m.saves) AS saves, SUM(m.shares) AS shares, SUM(m.reposts) AS reposts
            FROM post_metrics m JOIN fan_pages fp ON fp.id = m.fan_page_id
            WHERE m.posted_at >= NOW() - INTERVAL '7 days'
            GROUP BY fp.page_name ORDER BY views DESC NULLS LAST""")
        rows = cur.fetchall()
        cur.execute("""
            SELECT COUNT(*) AS posts, SUM(views) AS views, SUM(reach) AS reach, SUM(likes) AS likes,
                   SUM(comments) AS comments, SUM(saves) AS saves, SUM(shares) AS shares, SUM(reposts) AS reposts
            FROM post_metrics WHERE posted_at >= NOW() - INTERVAL '7 days'""")
        totals = cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({"by_page": rows, "totals_7d": totals})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analytics-status", methods=["GET"])
def analytics_status():
    """Per page: when metrics were last pulled, how many posts are tracked, and the last error."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT p.page_name, p.page_group, p.analytics_error,
                   to_char(p.analytics_pulled_at, 'YYYY-MM-DD HH24:MI "UTC"') AS pulled_at,
                   (SELECT COUNT(*) FROM post_metrics m WHERE m.fan_page_id = p.id) AS posts_tracked,
                   (SELECT COALESCE(SUM(views), 0) FROM post_metrics m WHERE m.fan_page_id = p.id) AS views
            FROM fan_pages p ORDER BY p.id""")
        rows = cur.fetchall()
        cur.execute("SELECT value FROM app_settings WHERE key='metric_support'")
        saved = cur.fetchone()
        cur.execute("SELECT COUNT(*) AS n, COUNT(DISTINCT fan_page_id) AS pages, MAX(day)::text AS last_day FROM page_daily")
        daily = cur.fetchone()
        cur.close()
        conn.close()
        support = dict(METRIC_STATE)
        try:
            support.update(json.loads(saved["value"])) if saved else None
        except Exception:
            pass
        return jsonify({"pages": rows, "reposts_metric": support.get("reposts"), "metric_support": support,
                        "daily_rows": daily})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analytics/today", methods=["GET"])
def analytics_today():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT fp.page_name, COUNT(*) AS posts, SUM(m.views) AS views, SUM(m.reach) AS reach,
                   SUM(m.likes) AS likes, SUM(m.saves) AS saves
            FROM post_metrics m JOIN fan_pages fp ON fp.id = m.fan_page_id
            WHERE m.posted_at >= (NOW() AT TIME ZONE 'America/New_York')::date AT TIME ZONE 'America/New_York'
            GROUP BY fp.page_name ORDER BY views DESC NULLS LAST""")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"today": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/followers", methods=["GET"])
def followers():
    """Get follower count for all active fan pages."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, ig_account_id, page_name, page_access_token, auth_type FROM fan_pages WHERE active=TRUE AND ig_account_id IS NOT NULL")
        pages = cur.fetchall()
        cur.close()
        conn.close()

        results = []
        for page in pages:
            r = requests.get(f"{_graph_for(page.get('auth_type'))}/{page['ig_account_id']}",
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
        cur.execute("SELECT page_access_token, auth_type FROM fan_pages WHERE id=%s", (fan_page_id,))
        page = cur.fetchone()
        cur.close()
        conn.close()

        if not page:
            return jsonify({"error": "page not found"}), 404

        r = requests.delete(f"{_graph_for(page.get('auth_type'))}/{ig_post_id}",
                            params={"access_token": page["page_access_token"]},
                            timeout=10)
        result = r.json()
        if result.get("success"):
            return jsonify({"status": "deleted", "ig_post_id": ig_post_id})
        return jsonify({"error": result.get("error", {}).get("message", str(result))}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cancel-pending-quotes", methods=["POST", "GET"])
def cancel_pending_quotes():
    """Cancel all pending quotes in the queue."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE quote_queue SET status='cancelled' WHERE status='pending'")
        count = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"cancelled": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/migrate", methods=["GET"])
def migrate():
    init_db()
    return jsonify({"status": "ok"})

# ── PUBLIC CLIP SUBMISSIONS + APPROVAL QUEUE ──────────────────────────────────

ADMIN_KEY = os.environ.get("ADMIN_KEY")
# The review dashboard (/admin) and the public submit form (/submit) are off unless ENABLE_ADMIN=1,
# so nothing can be submitted into a queue nobody is reviewing.
ADMIN_DASHBOARD = os.environ.get("ENABLE_ADMIN") == "1"

@app.before_request
def _dashboard_switch():
    if ADMIN_DASHBOARD:
        return None
    path = request.path.rstrip("/")
    if path in ("/admin", "/submit") or path.startswith("/admin/"):
        return "not found", 404

SUBMIT_LIMIT, SUBMIT_WINDOW = 5, 3600     # per IP: 5 submissions / hour
MAX_PENDING = 200
_submit_hits = {}                         # ip -> [timestamps]
_submit_lock = threading.Lock()

def _client_ip():
    fwd = request.headers.get("X-Forwarded-For", "")
    return (fwd.split(",")[0].strip() if fwd else request.remote_addr) or "unknown"

def _rate_limited(ip):
    now = time.time()
    with _submit_lock:
        hits = [t for t in _submit_hits.get(ip, []) if now - t < SUBMIT_WINDOW]
        if len(hits) >= SUBMIT_LIMIT:
            _submit_hits[ip] = hits
            return True
        hits.append(now)
        _submit_hits[ip] = hits
        if len(_submit_hits) > 5000:      # keep memory bounded
            for k in [k for k, v in _submit_hits.items() if not v or now - v[-1] > SUBMIT_WINDOW]:
                _submit_hits.pop(k, None)
        return False

def _admin_ok():
    import hmac
    if not ADMIN_KEY:
        return False
    return hmac.compare_digest(request.headers.get("X-Admin-Key", ""), ADMIN_KEY)

SUBMIT_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Submit a Clip</title>
<style>
:root{--bg:#f6f5fb;--card:#fff;--fg:#1c1b22;--muted:#6b6a78;--accent:#7c3aed;--line:#dcdae8}
@media(prefers-color-scheme:dark){:root{--bg:#0f0e14;--card:#1a1923;--fg:#f1f0f7;--muted:#9a98aa;--line:#2e2c3b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.5 system-ui,sans-serif;
display:grid;place-items:center;min-height:100vh;padding:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:28px;width:100%;max-width:460px}
h1{margin:0 0 6px;font-size:22px}p{margin:0 0 18px;color:var(--muted);font-size:14px}
label{display:block;font-size:13px;margin:14px 0 6px}
input{width:100%;padding:12px;border-radius:10px;border:1px solid var(--line);background:transparent;color:var(--fg);font:inherit}
button{margin-top:20px;width:100%;padding:13px;border:0;border-radius:10px;background:var(--accent);color:#fff;font:inherit;font-weight:600;cursor:pointer}
button:disabled{opacity:.6}#msg{margin-top:14px;font-size:14px;min-height:20px}.ok{color:#16a34a}.err{color:#dc2626}
.hp{position:absolute;left:-9999px}
</style></head><body><form class="card" id="f">
<h1>Submit your clip</h1>
<p>Paste a Twitch or Kick clip link. Approved clips are posted to our clip pages.</p>
<label for="url">Clip link</label>
<input id="url" name="url" type="url" required placeholder="https://kick.com/channel/clips/clip_..." autocomplete="off">
<label for="name">Your name (optional)</label>
<input id="name" name="name" maxlength="40" autocomplete="off">
<input class="hp" name="website" tabindex="-1" autocomplete="off" aria-hidden="true">
<button id="b" type="submit">Submit</button><div id="msg" role="status"></div></form>
<script>
const f=document.getElementById('f'),m=document.getElementById('msg'),b=document.getElementById('b');
f.addEventListener('submit',async e=>{e.preventDefault();b.disabled=true;m.className='';m.textContent='Sending...';
try{const r=await fetch('/submit',{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify(Object.fromEntries(new FormData(f)))});const j=await r.json();
if(r.ok){m.className='ok';m.textContent=j.message||'Submitted!';f.reset();}else{m.className='err';m.textContent=j.error||'Something went wrong.';}
}catch(x){m.className='err';m.textContent='Network error, try again.';}b.disabled=false;});
</script></body></html>"""

ADMIN_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Clip Queue</title>
<style>
:root{--bg:#f6f5fb;--card:#fff;--fg:#1c1b22;--muted:#6b6a78;--accent:#7c3aed;--line:#dcdae8}
@media(prefers-color-scheme:dark){:root{--bg:#0f0e14;--card:#1a1923;--fg:#f1f0f7;--muted:#9a98aa;--line:#2e2c3b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif;padding:16px;max-width:760px;margin-inline:auto}
h1{font-size:20px}.tabs{display:flex;gap:8px;margin:12px 0;flex-wrap:wrap}
.tabs button{padding:7px 14px;border-radius:99px;border:1px solid var(--line);background:var(--card);color:var(--fg);cursor:pointer}
.tabs button.on{background:var(--accent);color:#fff;border-color:var(--accent)}
.item{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px;margin:10px 0}
.item input,.item textarea,.item select{width:100%;margin:4px 0;padding:8px;border-radius:8px;border:1px solid var(--line);background:transparent;color:var(--fg);font:inherit}details{margin:12px 0}summary{cursor:pointer;font-weight:600}
.item a{color:var(--accent);word-break:break-all}.meta{color:var(--muted);font-size:13px;margin-top:4px}
.row{display:flex;gap:8px;margin-top:10px}.row button{padding:8px 16px;border-radius:8px;border:0;cursor:pointer;font-weight:600;color:#fff}
.yes{background:#16a34a}.no{background:#dc2626}.empty{color:var(--muted);padding:20px 0}
</style></head><body><h1>Clip queue</h1>
<details><summary>New custom post</summary><div class="item">
<input id="cpurl" placeholder="Clip link (Twitch or Kick)" autocomplete="off">
<input id="cpfoot" placeholder="Caption footer, e.g. @nathanielonline" autocomplete="off">
<div id="cprows"></div>
<div class="row"><button class="no" style="background:#6b6a78" id="cpadd">+ Add variant</button><button class="yes" id="cpgo">Queue post</button></div>
<div class="meta" id="cpmsg"></div></div></details>
<div class="tabs" id="tabs"></div><div id="list"></div>
<script>
let key=localStorage.getItem('adminKey')||'';let st='pending';
const S=['pending','approved','posting','posted','failed','rejected'];
const tabs=document.getElementById('tabs'),list=document.getElementById('list');
function hdr(){return {'X-Admin-Key':key}}
async function load(){
 tabs.replaceChildren(...S.map(s=>{const b=document.createElement('button');b.textContent=s;b.className=s===st?'on':'';b.onclick=()=>{st=s;load()};return b}));
 const r=await fetch('/admin/submissions?status='+st,{headers:hdr()});
 if(r.status===401||r.status===503){const msg=r.status===503?'ADMIN_KEY is not set on the server.':'Enter admin key:';
   if(r.status===503){list.textContent=msg;return}
   key=prompt(msg)||'';localStorage.setItem('adminKey',key);if(key)return load();list.textContent='Not authorized.';return}
 const j=await r.json();list.replaceChildren();
 if(!j.items.length){const d=document.createElement('div');d.className='empty';d.textContent='Nothing here.';list.append(d);return}
 for(const it of j.items){const d=document.createElement('div');d.className='item';
  const a=document.createElement('a');a.href=it.url;a.target='_blank';a.rel='noopener noreferrer';a.textContent=it.url;
  const m=document.createElement('div');m.className='meta';
  m.textContent='#'+it.id+' - '+(it.submitter_name||'anonymous')+' - '+it.created_at+(it.error?' - '+it.error:'');
  d.append(a,m);
  if(st==='pending'||st==='failed'){const row=document.createElement('div');row.className='row';
   const y=document.createElement('button');y.className='yes';y.textContent=st==='failed'?'Retry':'Approve';y.onclick=()=>act(it.id,'approve');
   const n=document.createElement('button');n.className='no';n.textContent='Reject';n.onclick=()=>act(it.id,'reject');
   row.append(y,n);d.append(row)}
  list.append(d)}
}
async function act(id,a){await fetch('/admin/submissions/'+id+'/'+a,{method:'POST',headers:hdr()});load()}
let pagesList=[];
fetch('/pages').then(r=>r.json()).then(j=>{pagesList=(j.pages||[]).filter(p=>p.page_group==='clipper');addRow();addRow()});
function addRow(){const d=document.createElement('div');d.className='cprow';
 const sel=document.createElement('select');for(const p of pagesList){const o=document.createElement('option');o.value=p.id;o.textContent=p.page_name;sel.append(o)}
 const t=document.createElement('textarea');t.placeholder='Overlay text shown on the video';t.rows=2;
 const c=document.createElement('textarea');c.placeholder='Instagram caption (optional, defaults to the overlay text)';c.rows=2;
 d.append(sel,t,c);document.getElementById('cprows').append(d)}
document.getElementById('cpadd').onclick=addRow;
document.getElementById('cpgo').onclick=async()=>{const m=document.getElementById('cpmsg');m.textContent='Queueing...';
 const variants=[...document.querySelectorAll('.cprow')].map(d=>({page_id:d.querySelector('select').value,text:d.querySelectorAll('textarea')[0].value,caption:d.querySelectorAll('textarea')[1].value})).filter(v=>v.text.trim());
 const r=await fetch('/admin/submissions/create',{method:'POST',headers:{...hdr(),'Content-Type':'application/json'},body:JSON.stringify({url:document.getElementById('cpurl').value,footer:document.getElementById('cpfoot').value,variants})});
 const j=await r.json();m.textContent=r.ok?'Queued as #'+j.id+' - it posts automatically. Check the "approved" and "posted" tabs.':(j.error||'Something went wrong');if(r.ok){st='approved';load()}};
load();
</script></body></html>"""

@app.route("/submit", methods=["GET"])
def submit_page():
    return Response(SUBMIT_HTML, mimetype="text/html")

@app.route("/submit", methods=["POST"])
def submit_clip():
    data = request.get_json(silent=True) or request.form or {}
    if data.get("website"):                       # honeypot: bots fill hidden fields
        return jsonify({"message": "Submitted! It will be reviewed shortly."})
    url = (data.get("url") or "").strip()
    name = (data.get("name") or "").strip()[:40]
    if not valid_clip_url(url):
        return jsonify({"error": "Please paste a Twitch or Kick clip link (https://...)."}), 400
    if _rate_limited(_client_ip()):
        return jsonify({"error": "Too many submissions. Please try again later."}), 429
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM clip_submissions WHERE status='pending'")
        if cur.fetchone()[0] >= MAX_PENDING:
            cur.close(); conn.close()
            return jsonify({"error": "The queue is full right now. Please try again later."}), 503
        cur.execute("SELECT 1 FROM clip_submissions WHERE url=%s AND status IN ('pending','approved','posting','posted')", (url,))
        if cur.fetchone():
            cur.close(); conn.close()
            return jsonify({"message": "That clip was already submitted. Thanks!"})
        cur.execute("INSERT INTO clip_submissions (url, submitter_name) VALUES (%s, %s)", (url, name or None))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"message": "Submitted! It will be reviewed shortly."})
    except Exception as e:
        print(f"[submit] {e}")
        return jsonify({"error": "Could not save your submission. Please try again."}), 500

@app.route("/admin", methods=["GET"])
def admin_page():
    return Response(ADMIN_HTML, mimetype="text/html")

def _admin_guard():
    if not ADMIN_KEY:
        return jsonify({"error": "ADMIN_KEY is not configured"}), 503
    if not _admin_ok():
        return jsonify({"error": "unauthorized"}), 401
    return None

@app.route("/admin/submissions", methods=["GET"])
def admin_list_submissions():
    denied = _admin_guard()
    if denied:
        return denied
    status = request.args.get("status", "pending")
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT id, url, submitter_name, status, error,
                              to_char(created_at AT TIME ZONE 'America/New_York', 'Mon DD HH12:MI AM') AS created_at
                       FROM clip_submissions WHERE status=%s ORDER BY id DESC LIMIT 100""", (status,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"items": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/admin/submissions/create", methods=["POST"])
def admin_create_submission():
    """Queue an already-approved custom post.
    Body: { "url", "footer": "text added under every caption",
            "variants": [ {"page_id": <fan_pages.id>, "text": "overlay text", "caption": "optional"} ] }"""
    denied = _admin_guard()
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    footer = (data.get("footer") or "").strip()[:200]
    if not valid_clip_url(url):
        return jsonify({"error": "paste a Twitch or Kick clip link"}), 400
    variants = []
    for v in (data.get("variants") or [])[:10]:
        try:
            pid = int(v.get("page_id"))
        except (TypeError, ValueError):
            return jsonify({"error": "each variant needs a page"}), 400
        text = (v.get("text") or "").strip()[:140]
        if not text:
            return jsonify({"error": "each variant needs overlay text"}), 400
        caption = (v.get("caption") or "").strip()[:1800] or text
        if footer:
            caption += "\n\n" + footer
        variants.append({"page_ids": [pid], "text": text, "caption": caption})
    if not variants:
        return jsonify({"error": "add at least one variant"}), 400
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""INSERT INTO clip_submissions (url, submitter_name, status, decided_at, variants)
                       VALUES (%s, 'admin', 'approved', NOW(), %s) RETURNING id""", (url, json.dumps(variants)))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"id": new_id, "status": "approved", "variants": len(variants)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/admin/submissions/<int:sub_id>/<action>", methods=["POST"])
def admin_decide_submission(sub_id, action):
    denied = _admin_guard()
    if denied:
        return denied
    if action not in ("approve", "reject"):
        return jsonify({"error": "bad action"}), 400
    new_status = "approved" if action == "approve" else "rejected"
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""UPDATE clip_submissions SET status=%s, decided_at=NOW(), error=NULL
                       WHERE id=%s AND status IN ('pending','failed') RETURNING id""", (new_status, sub_id))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        if not row:
            return jsonify({"error": "not found or already decided"}), 404
        return jsonify({"id": sub_id, "status": new_status})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def _recover_interrupted_clips():
    """A restart mid-post could leave a row stuck in 'posting'. Mark it failed rather than re-post
    (pages may already have it); it can be retried from /admin."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE clip_submissions SET status='failed', error='interrupted by restart' WHERE status='posting'")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[clip-queue] recover: {e}")

def _clip_queue_loop():
    """Posts approved submissions one at a time (rendering is memory-heavy on a small instance)."""
    time.sleep(15)
    while True:
        job = None
        try:
            conn = get_conn()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                UPDATE clip_submissions SET status='posting'
                WHERE id = (SELECT id FROM clip_submissions WHERE status='approved'
                            ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)
                RETURNING id, url, variants""")
            job = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"[clip-queue] {e}")

        if not job:
            time.sleep(10)
            continue

        status, error = "posted", None
        try:
            # 30-90s between pages so ten accounts don't all publish the same video at once
            results = process_clip(job["url"], delay=(30, 90),
                                   variants=json.loads(job["variants"]) if job.get("variants") else None)
            ok = [r for r in results if not r[2]]
            if not results:
                status, error = "failed", "no active pages to post to"
            elif not ok:
                status, error = "failed", "; ".join(f"{n}: {e}" for n, _, e in results)[:500]
            elif len(ok) < len(results):
                error = f"posted to {len(ok)}/{len(results)} pages"
        except Exception as e:
            status, error = "failed", str(e)[:500]
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("UPDATE clip_submissions SET status=%s, error=%s, posted_at=NOW() WHERE id=%s",
                        (status, error, job["id"]))
            conn.commit()
            cur.close()
            conn.close()
            print(f"[clip-queue] #{job['id']} {status} {error or ''}")
        except Exception as e:
            print(f"[clip-queue] could not record result for #{job['id']}: {e}")

# ── CONNECT PAGES (Facebook login -> long-lived user token -> non-expiring page tokens) ──────────

FB_APP_ID = os.environ.get("FB_APP_ID")
FB_APP_SECRET = os.environ.get("FB_APP_SECRET")
FB_SCOPES = os.environ.get("FB_SCOPES", ",".join([
    "pages_show_list", "pages_read_engagement", "pages_manage_posts",
    "instagram_basic", "instagram_content_publish", "instagram_manage_insights",
    "business_management"]))

def _connect_redirect_uri():
    return os.environ.get("BASE_URL", "https://fan-page-manager.onrender.com").rstrip("/") + "/connect/callback"

def _sign_state(group):
    """Stateless, tamper-proof OAuth state: nonce.group.expiry.signature (keyed with ADMIN_KEY)."""
    import hmac, hashlib
    body = f"{uuid.uuid4().hex[:16]}.{group}.{int(time.time()) + 600}"
    sig = hmac.new(ADMIN_KEY.encode(), body.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{body}.{sig}"

def _check_state(state):
    """Returns the page group if the state is genuine and unexpired, else None."""
    import hmac, hashlib
    try:
        nonce, group, exp, sig = (state or "").split(".")
        good = hmac.new(ADMIN_KEY.encode(), f"{nonce}.{group}.{exp}".encode(), hashlib.sha256).hexdigest()[:32]
        if hmac.compare_digest(sig, good) and int(exp) >= time.time() and group in PAGE_GROUPS:
            return group
    except Exception:
        pass
    return None

def get_user_token():
    """User token saved by the connect flow, falling back to the FB_USER_TOKEN env var."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_settings WHERE key='fb_user_token'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row[0]:
            return row[0]
    except Exception as e:
        print(f"[token] {e}")
    return USER_ACCESS_TOKEN

def _save_user_token(token):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""INSERT INTO app_settings (key, value) VALUES ('fb_user_token', %s)
                   ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""", (token,))
    conn.commit()
    cur.close()
    conn.close()

def _token_never_expires(token):
    """True/False from Facebook's debug_token; None if it could not be checked."""
    try:
        d = requests.get(f"{GRAPH}/debug_token",
                         params={"input_token": token, "access_token": f"{FB_APP_ID}|{FB_APP_SECRET}"},
                         timeout=15).json().get("data", {})
        if "expires_at" in d:
            return d["expires_at"] == 0
    except Exception as e:
        print(f"[debug_token] {type(e).__name__}")
    return None

CONNECT_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Connect Pages</title>
<style>
:root{--bg:#f6f5fb;--card:#fff;--fg:#1c1b22;--muted:#6b6a78;--accent:#1877f2;--line:#dcdae8}
@media(prefers-color-scheme:dark){:root{--bg:#0f0e14;--card:#1a1923;--fg:#f1f0f7;--muted:#9a98aa;--line:#2e2c3b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.5 system-ui,sans-serif;display:grid;place-items:center;min-height:100vh;padding:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:28px;width:100%;max-width:460px}
h1{margin:0 0 6px;font-size:22px}p{margin:0 0 16px;color:var(--muted);font-size:14px}
label{display:block;font-size:13px;margin:14px 0 6px}select{width:100%;padding:12px;border-radius:10px;border:1px solid var(--line);background:transparent;color:var(--fg);font:inherit}
button{margin-top:20px;width:100%;padding:13px;border:0;border-radius:10px;background:var(--accent);color:#fff;font:inherit;font-weight:600;cursor:pointer}
#msg{margin-top:14px;font-size:14px;color:#dc2626;min-height:20px}
</style></head><body><div class="card"><h1>Connect pages</h1>
<p>Log in with Facebook and tick the pages to add. Tokens are saved automatically and don't expire.</p>
<label for="g">These pages will post:</label>
<select id="g"><option value="clipper">Clips (clipper pages)</option><option value="promo">Mia promo content</option></select>
<button id="b">Connect with Facebook</button><div id="msg" role="status"></div></div>
<script>
const b=document.getElementById('b'),m=document.getElementById('msg');
b.onclick=async()=>{let key=localStorage.getItem('adminKey')||'';if(!key){key=prompt('Enter admin key:')||'';localStorage.setItem('adminKey',key)}
m.textContent='';b.disabled=true;
try{const r=await fetch('/connect/start',{method:'POST',headers:{'X-Admin-Key':key,'Content-Type':'application/json'},body:JSON.stringify({group:document.getElementById('g').value})});
const j=await r.json();if(r.ok&&j.url){location.href=j.url;return}
if(r.status===401)localStorage.removeItem('adminKey');m.textContent=j.error||'Something went wrong.';}
catch(e){m.textContent='Network error.'}b.disabled=false};
</script></body></html>"""

def _result_page(title, lines, ok=True):
    import html
    items = "".join(f"<li>{html.escape(l)}</li>" for l in lines)
    color = "#16a34a" if ok else "#dc2626"
    return Response(f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Connect Pages</title>
<style>body{{margin:0;background:#f6f5fb;color:#1c1b22;font:16px/1.5 system-ui,sans-serif;padding:24px;max-width:560px;margin-inline:auto}}
@media(prefers-color-scheme:dark){{body{{background:#0f0e14;color:#f1f0f7}}}}
h1{{font-size:22px;color:{color}}}li{{margin:6px 0}}a{{color:#1877f2}}</style></head>
<body><h1>{html.escape(title)}</h1><ul>{items}</ul><p><a href="/connect">Connect more pages</a></p></body></html>""",
                    mimetype="text/html")

@app.route("/connect", methods=["GET"])
def connect_page():
    return Response(CONNECT_HTML, mimetype="text/html")

@app.route("/connect/start", methods=["POST"])
def connect_start():
    denied = _admin_guard()
    if denied:
        return denied
    if not (FB_APP_ID and FB_APP_SECRET):
        return jsonify({"error": "FB_APP_ID and FB_APP_SECRET are not set on the server"}), 503
    group = (request.get_json(silent=True) or {}).get("group", "clipper")
    if group not in PAGE_GROUPS:
        return jsonify({"error": "bad group"}), 400
    from urllib.parse import urlencode
    url = "https://www.facebook.com/v19.0/dialog/oauth?" + urlencode({
        "client_id": FB_APP_ID, "redirect_uri": _connect_redirect_uri(),
        "state": _sign_state(group), "scope": FB_SCOPES, "response_type": "code"})
    return jsonify({"url": url})

PAGE_FIELDS = "id,name,access_token,instagram_business_account{id,username},connected_instagram_account{id,username}"

def _fetch_pages(user_token, extra_ids=()):
    """Pages the token can see: /me/accounts plus pages owned by or shared with the user's Business
    accounts (business pages are not in /me/accounts). Returns (pages, error_message)."""
    found, err = {}, ""
    r = requests.get(f"{GRAPH}/me/accounts",
                     params={"fields": PAGE_FIELDS, "access_token": user_token, "limit": 100}, timeout=30).json()
    err = r.get("error", {}).get("message", "")
    for p in r.get("data", []):
        found[p["id"]] = p
    try:
        biz = requests.get(f"{GRAPH}/me/businesses",
                           params={"fields": "id,name", "access_token": user_token, "limit": 50},
                           timeout=30).json().get("data", [])
        for b in biz:
            for edge in ("owned_pages", "client_pages"):
                rr = requests.get(f"{GRAPH}/{b['id']}/{edge}",
                                  params={"fields": PAGE_FIELDS, "access_token": user_token, "limit": 100},
                                  timeout=30).json()
                for p in rr.get("data", []):
                    found.setdefault(p["id"], p)
    except Exception as e:
        print(f"[fetch-pages] business lookup: {type(e).__name__}")
    for pid in extra_ids:   # pages entered by id: fetch directly, no list needed
        if pid in found:
            continue
        for fields in (PAGE_FIELDS, "id,name,access_token"):
            d = requests.get(f"{GRAPH}/{pid}", params={"fields": fields, "access_token": user_token},
                             timeout=30).json()
            if d.get("id"):
                found[pid] = d
                break
            err = d.get("error", {}).get("message", err)
    return list(found.values()), err

def _page_ig(p, user_token):
    """Instagram account for a page -> (ig dict or {}, note). Tries the fields on the page object, then
    asks the page directly (instagram_business_account / connected_instagram_account), then the
    page's instagram_accounts edge. `note` carries Facebook's error text when nothing is found."""
    ig = p.get("instagram_business_account") or p.get("connected_instagram_account")
    if ig and ig.get("id"):
        return ig, ""
    note = ""
    tok = p.get("access_token")
    if not tok:   # business-owned pages come back without a token; fetch the page's own token first
        try:
            d = requests.get(f"{GRAPH}/{p['id']}", params={"fields": "access_token", "access_token": user_token},
                             timeout=20).json()
            tok = d.get("access_token")
            if not tok and d.get("error"):
                note = "no page token: " + d["error"].get("message", "")[:300]
        except Exception as e:
            print(f"[page-ig] {type(e).__name__}")
    tok = tok or user_token
    for path, params in (
        (p["id"], {"fields": "instagram_business_account{id,username},connected_instagram_account{id,username}"}),
        (f"{p['id']}/instagram_accounts", {"fields": "id,username"}),
    ):
        try:
            d = requests.get(f"{GRAPH}/{path}", params={**params, "access_token": tok}, timeout=20).json()
        except Exception as e:
            print(f"[page-ig] {type(e).__name__}")
            continue
        if d.get("error"):
            note = d["error"].get("message", "")[:400]
            continue
        found = d.get("instagram_business_account") or d.get("connected_instagram_account")
        if not found and d.get("data"):
            found = d["data"][0]
        if found and found.get("id"):
            return found, ""
    return {}, note or "Facebook reports no Instagram account on this page"

def _preview_pages(user_token, extra_ids=()):
    """Nothing is saved: list the pages so the admin can pick which ones to add."""
    pages, err = _fetch_pages(user_token, extra_ids)
    existing = {}
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT fb_page_id, page_group FROM fan_pages")
    for fb_id, grp in cur.fetchall():
        existing[fb_id] = grp
    cur.close()
    conn.close()
    out = []
    for p in pages:
        ig, note = _page_ig(p, user_token)
        out.append({"id": p["id"], "name": p.get("name", p["id"]), "ig": ig.get("username") or ig.get("id"),
                    "has_ig": bool(ig.get("id")), "group": existing.get(p["id"]), "note": note})
    return out, err

def _connect_with_token(short_token, group, only, exchange=True, extra_ids=()):
    """Exchange a user token for a long-lived one (skipped for system user / saved tokens), save it,
    then add or refresh ONLY the pages whose ids are in `only`. Returns (ok, title, lines)."""
    base = {"client_id": FB_APP_ID, "client_secret": FB_APP_SECRET}
    long_ = {"access_token": short_token} if not exchange else requests.get(
        f"{GRAPH}/oauth/access_token",
        params={**base, "grant_type": "fb_exchange_token", "fb_exchange_token": short_token}, timeout=20).json()
    user_token = long_.get("access_token")
    if not user_token:
        return False, "Token exchange failed", [long_.get("error", {}).get("message", "no token returned")]
    _save_user_token(user_token)

    pages, err = _fetch_pages(user_token, extra_ids)   # re-read with the long-lived token: its page tokens never expire
    wanted = set(only)
    lines, added = [], 0
    for p in pages:
        if p["id"] not in wanted:
            continue
        name, token = p.get("name", p["id"]), p.get("access_token")
        ig, _note = _page_ig(p, user_token)
        if not ig.get("id"):
            lines.append(f"{name}: skipped - no Instagram account linked to this Facebook Page")
            continue
        if not token:   # business-owned pages may not expose a token in the list; ask for it directly
            token = requests.get(f"{GRAPH}/{p['id']}", params={"fields": "access_token", "access_token": user_token},
                                 timeout=20).json().get("access_token")
        if not token:
            lines.append(f"{name}: skipped - Facebook gave no page token (the token's user needs full control of this page)")
            continue
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO fan_pages (fb_page_id, ig_account_id, page_name, page_access_token, page_group)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (fb_page_id) DO UPDATE
              SET ig_account_id=EXCLUDED.ig_account_id,
                  page_name=CASE WHEN fan_pages.name_locked THEN fan_pages.page_name ELSE EXCLUDED.page_name END,
                  page_access_token=EXCLUDED.page_access_token
            RETURNING (xmax = 0) AS inserted""", (p["id"], ig["id"], name, token, group))
        inserted = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        forever = _token_never_expires(token)
        tok = "token never expires" if forever else ("token expires - check app settings" if forever is False else "token saved")
        who = f"@{ig['username']}" if ig.get("username") else "Instagram linked"
        lines.append(f"{name} ({who}): {'added to ' + group if inserted else 'updated (group unchanged)'} - {tok}")
        added += 1
    if not added and not lines:
        lines.append("None of the selected pages were visible to the token." + (f" {err}" if err else ""))
    return added > 0, f"Connected {added} page(s)", lines

@app.route("/connect/callback", methods=["GET"])
def connect_callback():
    """Facebook login lands here. Saves a long-lived user token only; pages are chosen on /connect/token."""
    if not (ADMIN_KEY and FB_APP_ID and FB_APP_SECRET):
        return _result_page("Not configured", ["ADMIN_KEY, FB_APP_ID and FB_APP_SECRET must be set."], ok=False), 503
    if not _check_state(request.args.get("state")):
        return _result_page("Link expired", ["Start again from the Connect page."], ok=False), 400
    if request.args.get("error") or not request.args.get("code"):
        return _result_page("Not connected", [request.args.get("error_description", "Facebook login was cancelled.")], ok=False), 400
    try:
        base = {"client_id": FB_APP_ID, "client_secret": FB_APP_SECRET}
        short = requests.get(f"{GRAPH}/oauth/access_token",
                             params={**base, "redirect_uri": _connect_redirect_uri(),
                                     "code": request.args["code"]}, timeout=20).json()
        if "access_token" not in short:
            return _result_page("Facebook login failed", [short.get("error", {}).get("message", "no token returned")], ok=False), 400
        long_ = requests.get(f"{GRAPH}/oauth/access_token",
                             params={**base, "grant_type": "fb_exchange_token",
                                     "fb_exchange_token": short["access_token"]}, timeout=20).json()
        if "access_token" not in long_:
            return _result_page("Token exchange failed", [long_.get("error", {}).get("message", "no token returned")], ok=False), 400
        _save_user_token(long_["access_token"])
        return _result_page("Login saved", ["Now open /connect/token, leave the token box empty, click Find pages and choose which pages to add."])
    except Exception as e:
        print(f"[connect] {type(e).__name__}")   # not str(e): request errors can contain the URL with tokens
        return _result_page("Something went wrong", ["Check the server logs and try again."], ok=False), 500

CONNECT_TOKEN_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Connect with a Token</title>
<style>
:root{--bg:#f6f5fb;--card:#fff;--fg:#1c1b22;--muted:#6b6a78;--accent:#1877f2;--line:#dcdae8}
@media(prefers-color-scheme:dark){:root{--bg:#0f0e14;--card:#1a1923;--fg:#f1f0f7;--muted:#9a98aa;--line:#2e2c3b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.5 system-ui,sans-serif;display:grid;place-items:center;min-height:100vh;padding:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:28px;width:100%;max-width:520px}
h1{margin:0 0 6px;font-size:22px}p,ol{margin:0 0 14px;color:var(--muted);font-size:14px}ol{padding-left:20px}
label{display:block;font-size:13px;margin:14px 0 6px}select,textarea{width:100%;padding:12px;border-radius:10px;border:1px solid var(--line);background:transparent;color:var(--fg);font:inherit}
textarea{min-height:90px;font:13px monospace}
button{margin-top:18px;width:100%;padding:13px;border:0;border-radius:10px;background:var(--accent);color:#fff;font:inherit;font-weight:600;cursor:pointer}
button:disabled{opacity:.6}ul{padding-left:18px;font-size:14px}.ok{color:#16a34a}.err{color:#dc2626}
.pg{display:flex;gap:10px;align-items:center;padding:10px 0;border-top:1px solid var(--line);font-size:15px}.pg small{color:var(--muted)}
</style></head><body><div class="card"><h1>Connect pages</h1>
<ol><li>Paste a token (Graph API Explorer user token, or a system user token). Leave it empty to reuse the saved one.</li>
<li>Click <b>Find pages</b>, tick only the pages you want, then <b>Add selected pages</b>.</li></ol>
<label for="g">New pages will post:</label>
<select id="g"><option value="clipper">Clips (clipper pages)</option><option value="promo">Mia promo content</option></select>
<label for="t">Access token (optional)</label><textarea id="t" autocomplete="off" spellcheck="false"></textarea>
<label for="pid">Page ID (optional - adds that page directly if it is not listed)</label><input id="pid" inputmode="numeric" placeholder="e.g. 1424718177383290" autocomplete="off" style="width:100%;padding:12px;border-radius:10px;border:1px solid var(--line);background:transparent;color:var(--fg);font:inherit">
<label style="display:flex;gap:8px;align-items:center"><input type="checkbox" id="sys"> System user token (never expires)</label>
<button id="find">Find pages</button><div id="o" role="status"></div></div>
<script>
const o=document.getElementById('o'),find=document.getElementById('find');let tokenUsed=false;
function say(cls,text){const h=document.createElement('p');h.className=cls;h.textContent=text;o.append(h)}
async function post(payload){let key=localStorage.getItem('adminKey')||'';if(!key){key=prompt('Enter admin key:')||'';localStorage.setItem('adminKey',key)}
const r=await fetch('/connect/token',{method:'POST',headers:{'X-Admin-Key':key,'Content-Type':'application/json'},body:JSON.stringify(payload)});
if(r.status===401)localStorage.removeItem('adminKey');return r.json()}
function base(){const p=document.getElementById('pid').value.trim();return {token:document.getElementById('t').value.trim(),group:document.getElementById('g').value,system:document.getElementById('sys').checked,page_ids:p?[p]:[]}}
find.onclick=async()=>{o.replaceChildren();find.disabled=true;
try{const j=await post({...base(),preview:true});
if(!j.pages){say('err',j.error||j.title||'Something went wrong.');find.disabled=false;return}
if(!j.pages.length){say('err','No pages found for this token.');find.disabled=false;return}
const boxes=[];for(const p of j.pages){const row=document.createElement('label');row.className='pg';
const cb=document.createElement('input');cb.type='checkbox';cb.disabled=!p.has_ig;cb.value=p.id;boxes.push(cb);
const t=document.createElement('span');t.textContent=p.name+' ';const s=document.createElement('small');
s.textContent=!p.has_ig?'(no Instagram linked'+(p.note?': '+p.note:'')+')':'@'+p.ig+(p.group?' - already added as '+p.group:'');t.append(s);row.append(cb,t);o.append(row)}
const add=document.createElement('button');add.textContent='Add selected pages';
add.onclick=async()=>{const ids=boxes.filter(b=>b.checked).map(b=>b.value);if(!ids.length){alert('Tick at least one page.');return}
add.disabled=true;const k=await post({...base(),only:ids});o.replaceChildren();say(k.ok?'ok':'err',k.title||k.error||'Something went wrong.');
if(k.lines){const u=document.createElement('ul');for(const l of k.lines){const li=document.createElement('li');li.textContent=l;u.append(li)}o.append(u)}
if(k.ok)document.getElementById('t').value=''};o.append(add)
}catch(e){say('err','Network error.')}find.disabled=false};
</script></body></html>"""

@app.route("/connect/token", methods=["GET"])
def connect_token_page():
    return Response(CONNECT_TOKEN_HTML, mimetype="text/html")

@app.route("/connect/token", methods=["POST"])
def connect_token_submit():
    """preview=true lists pages (saves nothing). Adding requires an explicit `only` list of page ids."""
    denied = _admin_guard()
    if denied:
        return denied
    if not (FB_APP_ID and FB_APP_SECRET):
        return jsonify({"error": "FB_APP_ID and FB_APP_SECRET are not set on the server"}), 503
    data = request.get_json(silent=True) or {}
    token, group = (data.get("token") or "").strip(), data.get("group", "clipper")
    if group not in PAGE_GROUPS:
        return jsonify({"error": "choose a group"}), 400
    system = bool(data.get("system"))
    exchange = not system
    extra_ids = [str(x).strip() for x in (data.get("page_ids") or []) if str(x).strip().isdigit()][:20]
    if not token:                       # reuse the token saved by an earlier connect
        token, exchange = get_user_token(), False
    if not token or len(token) < 20:
        return jsonify({"error": "paste an access token first"}), 400
    try:
        if data.get("preview"):
            pages, err = _preview_pages(token, extra_ids)
            if not pages:
                return jsonify({"error": err or "No pages came back for this token."}), 400
            return jsonify({"pages": pages})
        only = data.get("only")
        if not isinstance(only, list) or not only:
            return jsonify({"error": "choose at least one page to add"}), 400
        ok, title, lines = _connect_with_token(token, group, [str(i) for i in only], exchange=exchange, extra_ids=extra_ids)
        return jsonify({"ok": ok, "title": title, "lines": lines}), (200 if ok else 400)
    except Exception as e:
        print(f"[connect-token] {type(e).__name__}")
        return jsonify({"ok": False, "title": "Something went wrong", "lines": ["Check the server logs and try again."]}), 500

# ── LEGAL PAGES (required by Meta for app settings) ───────────────────────────────

BRAND_NAME = os.environ.get("BRAND_NAME", "Fan Page Manager")
CONTACT_EMAIL = os.environ.get("CONTACT_EMAIL")

def _legal_page(title, sections):
    import html
    contact = (f'<a href="mailto:{html.escape(CONTACT_EMAIL)}">{html.escape(CONTACT_EMAIL)}</a>'
               if CONTACT_EMAIL else "a direct message to any of our Instagram pages")
    body = "".join(f"<h2>{html.escape(h)}</h2>" + "".join(f"<p>{p.replace('{contact}', contact)}</p>" for p in ps)
                   for h, ps in sections)
    return Response(f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>{html.escape(title)} - {html.escape(BRAND_NAME)}</title>
<style>:root{{--bg:#fff;--fg:#1c1b22;--muted:#6b6a78}}@media(prefers-color-scheme:dark){{:root{{--bg:#0f0e14;--fg:#f1f0f7;--muted:#9a98aa}}}}
body{{margin:0;background:var(--bg);color:var(--fg);font:16px/1.65 system-ui,sans-serif;padding:24px 16px;max-width:680px;margin-inline:auto}}
h1{{font-size:26px;margin:0 0 4px}}h2{{font-size:18px;margin:28px 0 6px}}p{{margin:0 0 10px}}.m{{color:var(--muted);font-size:14px}}a{{color:#7c3aed}}</style></head>
<body><h1>{html.escape(title)}</h1><p class="m">{html.escape(BRAND_NAME)} &middot; Last updated October 2026</p>{body}</body></html>""",
                    mimetype="text/html")

@app.route("/privacy", methods=["GET"])
def privacy_page():
    return _legal_page("Privacy Policy", [
        ("Who we are", [f"{BRAND_NAME} operates social media pages that share short video clips and quote graphics, and a website where people can submit clip links for us to consider posting."]),
        ("What we collect", [
            "Clip submissions: the clip link you submit, the optional name you type, and the time of submission. Your IP address is used briefly to limit spam and is not stored with your submission.",
            "Facebook and Instagram account connection: when an administrator connects a page, we receive and store the page's name, ID, linked Instagram account ID and an access token so we can publish posts to that page. We do not collect personal profile information, friends lists or private messages.",
            "Post analytics: public engagement numbers for posts we publish (likes, comments, reach, saves) through Meta's tools."]),
        ("How we use it", [
            "We use this information only to review and publish clips, to publish and measure content on our own pages, and to prevent abuse. We do not sell your information and we do not use it for advertising."]),
        ("Sharing", [
            "We share data only with the services needed to run the pages: Meta (to publish posts), our hosting and database providers, and image or video hosting services used to deliver posts. We may disclose information if required by law."]),
        ("Retention and deletion", [
            "Submissions are kept until reviewed and then for a limited period for moderation records. Page access tokens are kept while a page is connected and deleted when it is disconnected. To request deletion of anything we hold about you, contact us via {contact} or see our data deletion page at /data-deletion."]),
        ("Contact", ["Questions about this policy: {contact}."]),
    ])

@app.route("/terms", methods=["GET"])
def terms_page():
    return _legal_page("Terms of Service", [
        ("About this service", [f"{BRAND_NAME} runs social media pages featuring clips from live streams and a form to submit clip links. By using the website you agree to these terms."]),
        ("Submitting clips", [
            "Only submit links to clips that you have the right to share or that are publicly available on Twitch or Kick. Submissions are reviewed, and we may decline, edit, or remove any clip at our discretion. Submitting a link does not guarantee it will be posted.",
            "Do not submit content that is illegal, hateful, harassing, sexually explicit involving minors, or that violates anyone's privacy or rights."]),
        ("Credit and takedowns", [
            "Clips we post credit the original channel. All clips remain the property of their creators and the streamers shown. We are an unofficial clip page and are not affiliated with any streamer, Twitch, Kick, Instagram or Meta. If you are a creator and want a clip credited differently or removed, contact us via {contact} and we will act promptly."]),
        ("Acceptable use", ["Do not abuse the submission form, attempt to bypass rate limits, or interfere with the service."]),
        ("Disclaimer and changes", [
            "The service is provided as is, without warranties. We may change or discontinue it, and update these terms, at any time. Continued use means you accept the updated terms."]),
        ("Contact", ["Questions: {contact}."]),
    ])

@app.route("/data-deletion", methods=["GET", "POST"])
def data_deletion_page():
    if request.method == "POST":   # Meta's deauthorize / data-deletion callback: acknowledge it
        base = os.environ.get("BASE_URL", "https://fan-page-manager.onrender.com").rstrip("/")
        return jsonify({"url": f"{base}/data-deletion", "confirmation_code": uuid.uuid4().hex[:12]})
    return _legal_page("Data Deletion", [
        ("How to request deletion", [
            "To have information we hold about you deleted, contact us via {contact} with the clip link or page name involved. We will remove it within 30 days and confirm once it is done.",
            "If you connected a page to this service, you can also remove access at any time in your Facebook settings under Business Integrations (or Apps and Websites) by removing this app. Doing so revokes our access token."]),
    ])

# ── JOBS FILE (jobs.json in the repo: posts the owner has approved; each id runs once ever) ──────────

JOBS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jobs.json")

def _set_setting(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""INSERT INTO app_settings (key, value) VALUES (%s, %s)
                   ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""", (key, value))
    conn.commit()
    cur.close()
    conn.close()

RENDER_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "render_config.json")

def _render_public_base():
    """Public base URL of the R2 bucket the render worker uploads to (env R2_PUBLIC_BASE or render_config.json)."""
    base = os.environ.get("R2_PUBLIC_BASE")
    if not base:
        try:
            with open(RENDER_CONFIG_FILE, encoding="utf-8") as f:
                base = json.load(f).get("public_base", "")
        except Exception:
            base = ""
    return (base or "").rstrip("/")

def _claim_setting(key, value="1"):
    """True only for the first caller (atomic), so exactly one process ever does the thing."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""INSERT INTO app_settings (key, value) VALUES (%s, %s)
                   ON CONFLICT (key) DO NOTHING RETURNING key""", (key, value))
    first = cur.fetchone() is not None
    conn.commit()
    cur.close()
    conn.close()
    return first

def _summarize(results):
    return "; ".join(f"{n}: {'posted ' + str(pid) if not err else 'ERROR ' + str(err)[:120]}"
                     for n, pid, err in results) or "no pages posted"

def _run_remote_job(job, delay=(30, 90), wait_hours=3):
    """Rendering happens elsewhere (render_worker.py on GitHub Actions). Wait for its manifest,
    then post each finished video to its pages. Returns a summary string."""
    base = _render_public_base()
    if not base:
        return "failed: render_config.json has no public_base yet"
    url = f"{base}/manifests/{job.get('from_manifest') or job['id']}.json"   # from_manifest: reuse an earlier job's finished videos
    deadline = time.time() + wait_hours * 3600
    manifest = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=20, headers={"Cache-Control": "no-cache"})
            if r.status_code == 200:
                manifest = r.json()
                break
        except Exception as e:
            print(f"[jobs] manifest check: {type(e).__name__}")
        time.sleep(60)
    if not manifest:
        return f"failed: rendered videos did not arrive within {wait_hours} hours"
    # past this point a restart must never re-post; the claim is atomic so overlapping copies can't both post
    if not _claim_setting(f"job_{job['id']}_posting"):
        return "skipped: another process is already posting this job"
    stagger = job.get("stagger_minutes")      # e.g. [5, 10]: minutes between pages, to spread a clip over a while
    gap = (float(stagger[0]) * 60, float(stagger[1]) * 60) if stagger else PUBLISH_GAP
    items = manifest.get("items", [])
    only = job.get("only_pages")                 # post just these pages from the earlier manifest
    if only:
        keep = {int(x) for x in only}
        items = [it for it in items if keep & {int(p) for p in it["page_ids"]}]
    return _summarize(_post_pipelined(items, gap=gap))

def _jobs_worker(jobs):
    for job in jobs:
        key = f"job_{job['id']}"
        footer = (job.get("footer") or "").strip()
        variants = []
        for v in job["variants"]:
            text = (v.get("text") or "").strip()
            caption = (v.get("caption") or text).strip()
            variants.append({"page_ids": [int(p) for p in v["page_ids"]], "text": text,
                             "caption": caption + (f"\n\n{footer}" if footer else ""),
                             **{k: v[k] for k in ("crop", "crop_x", "crop_y", "strip_lift", "strip_pos") if k in v}})
        try:
            if job.get("render") == "remote":
                summary = _run_remote_job(job)
            else:
                results = process_clip(job["url"], delay=(30, 90), variants=variants)
                summary = _summarize(results)
        except Exception as e:
            summary = f"failed: {type(e).__name__}: {str(e)[:200]}"
        print(f"[jobs] {job['id']}: {summary}")
        try:
            _set_setting(key + "_result", summary)
        except Exception as e:
            print(f"[jobs] could not store result: {type(e).__name__}")

def _probe_worker(job):
    """Ask Instagram to prepare a video with different tag formats and NEVER publish it, to learn which
    format it accepts. Unpublished containers are invisible on the profile and expire on their own."""
    p = job["probe"]
    out = []
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT ig_account_id, page_access_token, auth_type FROM fan_pages WHERE id=%s", (int(p["page_id"]),))
        page = cur.fetchone()
        cur.close()
        conn.close()
        if not page:
            raise RuntimeError("page not found")
        g = _graph_for(page.get("auth_type"))
        user = str(p["username"]).lstrip("@")
        base = {"video_url": p["media_url"], "media_type": "REELS", "caption": "probe - never published",
                "access_token": page["page_access_token"]}
        tests = [
            ("user_tags with x,y", {"user_tags": json.dumps([{"username": user, "x": 0.5, "y": 0.5}])}),
            ("user_tags username only", {"user_tags": json.dumps([{"username": user}])}),
            ("user_tags plain list", {"user_tags": json.dumps([user])}),
            ("collaborators", {"collaborators": json.dumps([user])}),
            ("control, no tags", {}),
        ]
        for label, extra in tests:
            r = requests.post(f"{g}/{page['ig_account_id']}/media", data={**base, **extra}, timeout=30).json()
            out.append(f"{label}: " + ("ACCEPTED" if r.get("id") else (r.get("error") or {}).get("message", str(r))[:130]))
        summary = " | ".join(out)
    except Exception as e:
        summary = f"failed: {type(e).__name__}"
    print(f"[probe] {job['id']}: {summary}")
    try:
        _set_setting(f"job_{job['id']}_result", summary)
    except Exception as e:
        print(f"[probe] could not store result: {type(e).__name__}")

def _audit_worker(job):
    """READ-ONLY. For each page, list the posts that were NOT published by the bot (not in post_log), so the owner
    can decide what to delete. Also re-checks whether posts we already deleted are really gone.
    job["audit"] = {"page_ids": [..], "check_ids": {"<fan_page_id>": ["<media id>", ...]}}"""
    spec = job["audit"]
    report = {"pages": {}, "deleted_check": {}}
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT ig_post_id FROM post_log WHERE ig_post_id IS NOT NULL")
        bot_posts = {r["ig_post_id"] for r in cur.fetchall()}
        cur.execute("SELECT id, page_name, ig_account_id, page_access_token, auth_type FROM fan_pages WHERE id = ANY(%s)",
                    (sorted({int(p) for p in spec.get("page_ids", [])} | {int(p) for p in spec.get("check_ids", {})}),))
        pages = {r["id"]: r for r in cur.fetchall()}
        cur.close()
        conn.close()
        for pid in spec.get("page_ids", []):
            page = pages.get(int(pid))
            if not page:
                continue
            g = _graph_for(page.get("auth_type"))
            url = f"{g}/{page['ig_account_id']}/media"
            params = {"fields": "id,timestamp,caption,permalink,media_product_type", "limit": 50,
                      "access_token": page["page_access_token"]}
            rows, total, guard = [], 0, 0
            while url and guard < 20:
                r = requests.get(url, params=params, timeout=30).json()
                guard += 1
                if "data" not in r:
                    rows.append({"error": (r.get("error") or {}).get("message", "no data")[:120]})
                    break
                for m in r["data"]:
                    total += 1
                    if m["id"] not in bot_posts:
                        rows.append({"id": m["id"], "date": (m.get("timestamp") or "")[:10],
                                     "type": m.get("media_product_type"), "caption": (m.get("caption") or "")[:50],
                                     "link": m.get("permalink")})
                url, params = (r.get("paging") or {}).get("next"), None
            report["pages"][page["page_name"]] = {"id": int(pid), "total_posts": total, "not_from_bot": rows}
        for pid, ids in (spec.get("check_ids") or {}).items():
            page = pages.get(int(pid))
            if not page:
                continue
            g = _graph_for(page.get("auth_type"))
            for mid in ids:
                r = requests.get(f"{g}/{mid}", params={"fields": "id", "access_token": page["page_access_token"]}, timeout=20).json()
                report["deleted_check"][mid] = "STILL EXISTS" if r.get("id") else "gone: " + (r.get("error") or {}).get("message", "")[:80]
        summary = json.dumps(report, ensure_ascii=False, separators=(",", ":"))
    except Exception as e:
        summary = json.dumps({"failed": type(e).__name__})
    print(f"[audit] {job['id']}: {len(summary)} chars")
    try:
        _set_setting(f"job_{job['id']}_result", summary)
    except Exception as e:
        print(f"[audit] could not store result: {type(e).__name__}")

def _delete_old_worker(job):
    """Deletes exactly the posts an earlier audit job listed as 'not from the bot', on the named pages only.
    Every id is re-checked against post_log first, so a post the bot made can never be deleted here.
    job["delete_old"] = {"from_audit": "<audit job id>", "page_ids": [..], "max": 200}"""
    spec = job["delete_old"]
    summary = {}
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT value FROM app_settings WHERE key=%s", (f"job_{spec['from_audit']}_result",))
        row = cur.fetchone()
        if not row or not row["value"]:
            raise RuntimeError("audit result not found")
        audit = json.loads(row["value"])
        cur.execute("SELECT ig_post_id FROM post_log WHERE ig_post_id IS NOT NULL")
        bot_posts = {r["ig_post_id"] for r in cur.fetchall()}
        wanted = {int(p) for p in spec["page_ids"]}
        cur.execute("SELECT id, page_name, page_access_token, auth_type FROM fan_pages WHERE id = ANY(%s)", (sorted(wanted),))
        pages = {r["id"]: r for r in cur.fetchall()}
        cur.close()
        conn.close()

        budget = int(spec.get("max", 200))
        deleted_ids, failed = [], []
        per_page = {}
        for name, info in audit.get("pages", {}).items():
            pid = int(info["id"])
            page = pages.get(pid)
            if pid not in wanted or not page:
                continue
            g = _graph_for(page.get("auth_type"))
            n_ok = 0
            for item in info.get("not_from_bot", []):
                mid = item.get("id")
                if not mid or mid in bot_posts or budget <= 0:
                    continue
                budget -= 1
                r = requests.delete(f"{g}/{mid}", params={"access_token": page["page_access_token"]}, timeout=30).json()
                if r.get("success"):
                    n_ok += 1
                    deleted_ids.append(mid)
                else:
                    failed.append(f"{mid}: {(r.get('error') or {}).get('message', str(r))[:80]}")
                time.sleep(random.uniform(3, 6))
            per_page[name] = n_ok
        if deleted_ids:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM post_metrics WHERE ig_media_id = ANY(%s)", (deleted_ids,))
            conn.commit()
            cur.close()
            conn.close()
        summary = {"deleted": len(deleted_ids), "per_page": per_page, "failed": failed[:10], "failed_count": len(failed)}
    except Exception as e:
        summary = {"failed": type(e).__name__}
    text = json.dumps(summary, ensure_ascii=False, separators=(",", ":"))
    print(f"[delete-old] {job['id']}: {text[:300]}")
    try:
        _set_setting(f"job_{job['id']}_result", text)
    except Exception as e:
        print(f"[delete-old] could not store result: {type(e).__name__}")

LIMIT_HINTS = ("too many actions", "rate limit", "try again later", "temporarily blocked", "action blocked")

def _wipe_worker(job):
    """Slowly deletes EVERY post older than job["wipe"]["before"] on the named pages (bot posts included),
    in small batches so Instagram's action limits are not hit. If Instagram says 'too many actions' it stops
    deleting for a long cool-down. Re-lists what is left each pass, so it is safe to resume after a restart.
    job["wipe"] = {"page_ids": [..], "before": "2026-10-14T23:50:00+0000", "per_batch": 12,
                   "delay": [20, 50], "batch_gap_minutes": 40, "cooldown_minutes": 120, "start_after_minutes": 90}"""
    from datetime import datetime
    spec = job["wipe"]
    per_batch = int(spec.get("per_batch", 12))
    dmin, dmax = (spec.get("delay") or [20, 50])[:2]
    gap = float(spec.get("batch_gap_minutes", 40)) * 60
    cooldown = float(spec.get("cooldown_minutes", 120)) * 60
    max_total = int(spec["max_total"]) if spec.get("max_total") else None   # optional: stop after this many deletions
    total_deleted, limit_hits, passes = 0, 0, 0
    per_page_done = {}
    errors = []
    try:
        cutoff = datetime.strptime(spec["before"], "%Y-%m-%dT%H:%M:%S%z")
        time.sleep(float(spec.get("start_after_minutes", 0)) * 60)
        while passes < 80:
            passes += 1
            conn = get_conn()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT id, page_name, ig_account_id, page_access_token, auth_type FROM fan_pages WHERE id = ANY(%s)",
                        ([int(p) for p in spec["page_ids"]],))
            pages = cur.fetchall()
            cur.close()
            conn.close()
            remaining, hit_limit = 0, False
            for page in pages:
                g = _graph_for(page.get("auth_type"))
                url = f"{g}/{page['ig_account_id']}/media"
                params = {"fields": "id,timestamp", "limit": 50, "access_token": page["page_access_token"]}
                todo, guard = [], 0
                while url and guard < 30:
                    r = requests.get(url, params=params, timeout=30).json()
                    guard += 1
                    for m in r.get("data", []):
                        ts = datetime.strptime(m["timestamp"], "%Y-%m-%dT%H:%M:%S%z") if m.get("timestamp") else None
                        if ts and ts < cutoff:
                            todo.append(m["id"])
                    url, params = (r.get("paging") or {}).get("next"), None
                remaining += len(todo)
                per_page_done.setdefault(page["page_name"], 0)
                if hit_limit:
                    continue
                gone = []
                allowed = per_batch if max_total is None else min(per_batch, max_total - total_deleted)
                for mid in todo[:max(allowed, 0)]:
                    d = requests.delete(f"{g}/{mid}", params={"access_token": page["page_access_token"]}, timeout=30).json()
                    if d.get("success"):
                        gone.append(mid)
                    else:
                        msg = str((d.get("error") or {}).get("message", d)).lower()
                        if len(errors) < 5:
                            errors.append(f"{mid}: {msg[:120]}")
                        if any(h in msg for h in LIMIT_HINTS):
                            hit_limit = True
                            limit_hits += 1
                            break
                    time.sleep(random.uniform(float(dmin), float(dmax)))
                remaining -= len(gone)
                per_page_done[page["page_name"]] += len(gone)
                total_deleted += len(gone)
                if gone:
                    conn = get_conn()
                    cur = conn.cursor()
                    cur.execute("DELETE FROM post_metrics WHERE ig_media_id = ANY(%s)", (gone,))
                    conn.commit()
                    cur.close()
                    conn.close()
            try:
                _set_setting(f"job_{job['id']}_progress", json.dumps(
                    {"pass": passes, "deleted": total_deleted, "remaining": remaining, "limit_hits": limit_hits,
                     "per_page": per_page_done}, separators=(",", ":")))
            except Exception:
                pass
            if remaining <= 0 or (max_total is not None and total_deleted >= max_total):
                break
            time.sleep(cooldown if hit_limit else gap)
        summary = {"deleted": total_deleted, "remaining": remaining, "passes": passes, "limit_hits": limit_hits,
                   "per_page": per_page_done, "errors": errors}
    except Exception as e:
        summary = {"failed": type(e).__name__, "deleted": total_deleted}
    text = json.dumps(summary, ensure_ascii=False, separators=(",", ":"))
    print(f"[wipe] {job['id']}: {text[:300]}")
    try:
        _set_setting(f"job_{job['id']}_result", text)
    except Exception as e:
        print(f"[wipe] could not store result: {type(e).__name__}")

def _demographics_worker(job):
    """READ-ONLY. Country breakdown of each page's followers and of the people its posts reached.
    job["demographics"] = {"page_ids": [..], "breakdown": "country"}"""
    spec = job["demographics"]
    breakdown = spec.get("breakdown", "country")
    out = {}
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, page_name, ig_account_id, page_access_token, auth_type FROM fan_pages WHERE id = ANY(%s)",
                    ([int(p) for p in spec["page_ids"]],))
        pages = cur.fetchall()
        cur.close()
        conn.close()
        for page in pages:
            g = _graph_for(page.get("auth_type"))
            entry = {}
            for label, metric, extra in (("followers", "follower_demographics", {"period": "lifetime"}),
                                         ("reached", "reached_audience_demographics", {"period": "lifetime"})):
                try:
                    r = requests.get(f"{g}/{page['ig_account_id']}/insights",
                                     params={"metric": metric, "metric_type": "total_value", "breakdown": breakdown,
                                             "access_token": page["page_access_token"], **extra}, timeout=30).json()
                except Exception as e:
                    entry[label] = f"error: {type(e).__name__}"
                    continue
                results = []
                for item in r.get("data", []):
                    for bd in ((item.get("total_value") or {}).get("breakdowns") or []):
                        results += bd.get("results", [])
                if not results:
                    entry[label] = "unavailable: " + str((r.get("error") or {}).get("message", "no data"))[:90]
                    continue
                total = sum(int(x.get("value", 0)) for x in results) or 1
                top = sorted(results, key=lambda x: -int(x.get("value", 0)))[:6]
                entry[label] = [{"c": (x.get("dimension_values") or ["?"])[0], "n": int(x.get("value", 0)),
                                 "pct": round(100 * int(x.get("value", 0)) / total)} for x in top]
                entry[label + "_total"] = total
            out[page["page_name"]] = entry
            time.sleep(1)
        summary = json.dumps(out, ensure_ascii=False, separators=(",", ":"))
    except Exception as e:
        summary = json.dumps({"failed": type(e).__name__})
    print(f"[demographics] {job['id']}: {len(summary)} chars")
    try:
        _set_setting(f"job_{job['id']}_result", summary)
    except Exception as e:
        print(f"[demographics] could not store result: {type(e).__name__}")

def _report_worker(job):
    """READ-ONLY, database only (no Instagram calls). Lists a page's recent posts with their latest numbers and says
    whether each one was a clip posted through the render pipeline or an older bot post (quote cards etc.).
    job["report"] = {"page_ids": [..], "days": 8}"""
    spec = job["report"]
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT p.page_name, to_char(m.posted_at AT TIME ZONE 'UTC', 'MM-DD HH24:MI') AS posted_utc,
                   m.views, m.reach, m.likes, m.comments, m.saves, m.shares, m.reposts, m.follows, m.profile_visits,
                   LEFT(COALESCE(m.caption, ''), 45) AS caption,
                   CASE WHEN l.media_url LIKE '%%/videos/%%' OR l.media_url LIKE '%%/clip-video/%%' THEN 'clip'
                        WHEN l.ig_post_id IS NOT NULL THEN 'older bot post'
                        ELSE 'not from bot' END AS kind
            FROM post_metrics m
            JOIN fan_pages p ON p.id = m.fan_page_id
            LEFT JOIN post_log l ON l.ig_post_id = m.ig_media_id
            WHERE m.fan_page_id = ANY(%s) AND m.posted_at >= NOW() - (%s || ' days')::interval
            ORDER BY p.page_name, m.posted_at DESC""", ([int(x) for x in spec["page_ids"]], str(int(spec.get("days", 8)))))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        summary = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    except Exception as e:
        summary = json.dumps({"failed": type(e).__name__})
    print(f"[report] {job['id']}: {len(summary)} chars")
    try:
        _set_setting(f"job_{job['id']}_result", summary)
    except Exception as e:
        print(f"[report] could not store result: {type(e).__name__}")

AUTO_FEED_STALE_HOURS = 3


def _auto_feed_loop():
    """Posts what the hourly auto feed (auto_feed.py on GitHub Actions) has rendered. Every couple of minutes it reads
    auto/queue.json from the bucket; each new entry is claimed once (job_<id> in app_settings) and posted to its pages,
    staggered. Entries older than AUTO_FEED_STALE_HOURS are skipped, so a bot that was asleep never dumps a backlog."""
    time.sleep(60)
    while True:
        try:
            base = _render_public_base()
            if base:
                r = requests.get(f"{base}/auto/queue.json", timeout=20, headers={"Cache-Control": "no-cache"})
                queue = r.json() if r.status_code == 200 else []
                for entry in sorted(queue, key=lambda e: e.get("created", 0)):
                    jid = entry.get("id")
                    if not isinstance(jid, str) or not jid.startswith("auto_"):
                        continue
                    stale = time.time() - entry.get("created", 0) > AUTO_FEED_STALE_HOURS * 3600
                    if not _claim_setting(f"job_{jid}", "skipped: too old" if stale else "started"):
                        continue
                    if stale:
                        print(f"[auto-feed] {jid}: skipped, queued over {AUTO_FEED_STALE_HOURS}h ago")
                        continue
                    try:
                        summary = _run_remote_job({"id": jid, "stagger_minutes": entry.get("stagger_minutes", [3, 6])},
                                                  wait_hours=1)
                    except Exception as e:
                        summary = f"failed: {type(e).__name__}: {str(e)[:200]}"
                    print(f"[auto-feed] {jid}: {summary}")
                    try:
                        _set_setting(f"job_{jid}_result", summary)
                    except Exception as e:
                        print(f"[auto-feed] could not store result: {type(e).__name__}")
        except Exception as e:
            print(f"[auto-feed] {type(e).__name__}")
        time.sleep(120)


def _run_jobs_once():
    """Claim every job in jobs.json that has never started (the INSERT only succeeds once per id)."""
    try:
        with open(JOBS_FILE, encoding="utf-8") as f:
            jobs = json.load(f)
    except FileNotFoundError:
        return
    except Exception as e:
        print(f"[jobs] could not read jobs.json: {type(e).__name__}")
        return
    todo = []
    try:
        conn = get_conn()
        cur = conn.cursor()
        for job in jobs:
            if isinstance(job.get("report"), dict) and isinstance(job.get("id"), str):
                cur.execute("""INSERT INTO app_settings (key, value) VALUES (%s, 'started')
                               ON CONFLICT (key) DO NOTHING RETURNING key""", (f"job_{job['id']}",))
                if cur.fetchone():
                    threading.Thread(target=_report_worker, args=(job,), daemon=True).start()
                continue
            if isinstance(job.get("demographics"), dict) and isinstance(job.get("id"), str):
                cur.execute("""INSERT INTO app_settings (key, value) VALUES (%s, 'started')
                               ON CONFLICT (key) DO NOTHING RETURNING key""", (f"job_{job['id']}",))
                if cur.fetchone():
                    threading.Thread(target=_demographics_worker, args=(job,), daemon=True).start()
                continue
            if isinstance(job.get("wipe"), dict) and isinstance(job.get("id"), str):
                cur.execute("""INSERT INTO app_settings (key, value) VALUES (%s, 'started')
                               ON CONFLICT (key) DO NOTHING RETURNING key""", (f"job_{job['id']}",))
                start = bool(cur.fetchone())
                if not start and job["wipe"].get("resume"):
                    cur.execute("SELECT 1 FROM app_settings WHERE key=%s", (f"job_{job['id']}_result",))
                    start = not cur.fetchall()      # claimed earlier but never finished (a restart interrupted it)
                if start:
                    threading.Thread(target=_wipe_worker, args=(job,), daemon=True).start()
                continue
            if isinstance(job.get("delete_old"), dict) and isinstance(job.get("id"), str):
                cur.execute("""INSERT INTO app_settings (key, value) VALUES (%s, 'started')
                               ON CONFLICT (key) DO NOTHING RETURNING key""", (f"job_{job['id']}",))
                if cur.fetchone():
                    threading.Thread(target=_delete_old_worker, args=(job,), daemon=True).start()
                continue
            if isinstance(job.get("audit"), dict) and isinstance(job.get("id"), str):
                cur.execute("""INSERT INTO app_settings (key, value) VALUES (%s, 'started')
                               ON CONFLICT (key) DO NOTHING RETURNING key""", (f"job_{job['id']}",))
                if cur.fetchone():
                    threading.Thread(target=_audit_worker, args=(job,), daemon=True).start()
                continue
            if isinstance(job.get("probe"), dict) and isinstance(job.get("id"), str):
                cur.execute("""INSERT INTO app_settings (key, value) VALUES (%s, 'started')
                               ON CONFLICT (key) DO NOTHING RETURNING key""", (f"job_{job['id']}",))
                if cur.fetchone():
                    threading.Thread(target=_probe_worker, args=(job,), daemon=True).start()
                continue
            url_ok = valid_clip_url(job.get("url", "")) or (job.get("render") == "remote" and str(job.get("url", "")).startswith("https://"))
            if not (isinstance(job.get("id"), str) and url_ok and job.get("variants")):
                print(f"[jobs] skipping malformed job {job.get('id')!r}")
                continue
            cur.execute("""INSERT INTO app_settings (key, value) VALUES (%s, 'started')
                           ON CONFLICT (key) DO NOTHING RETURNING key""", (f"job_{job['id']}",))
            if cur.fetchone():
                todo.append(job)
            elif job.get("render") == "remote":
                # already claimed: resume only if it is still waiting for its videos (no result, not posting)
                cur.execute("SELECT 1 FROM app_settings WHERE key IN (%s, %s)",
                            (f"job_{job['id']}_result", f"job_{job['id']}_posting"))
                if not cur.fetchall():
                    todo.append(job)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[jobs] {type(e).__name__}")
        return
    if todo:
        print(f"[jobs] starting {[j['id'] for j in todo]}")
        threading.Thread(target=_jobs_worker, args=(todo,), daemon=True).start()

@app.route("/jobs-status", methods=["GET"])
def jobs_status():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT key, value FROM app_settings WHERE key LIKE 'job\\_%' ORDER BY key")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        out = {}
        for key, value in rows:
            jid = key[4:]
            if jid.endswith("_result"):
                out.setdefault(jid[:-7], {})["result"] = value
            else:
                out.setdefault(jid, {})["state"] = value
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": type(e).__name__}), 500

# ── ANALYTICS DASHBOARD ──────────────────────────────────────────────────────────────────

DASHBOARD_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Analytics</title>
<style>
:root{--bg:#f6f5fb;--card:#fff;--fg:#1c1b22;--muted:#6b6a78;--accent:#7c3aed;--line:#dcdae8;--good:#16a34a;--bad:#dc2626}
@media(prefers-color-scheme:dark){:root{--bg:#0f0e14;--card:#1a1923;--fg:#f1f0f7;--muted:#9a98aa;--line:#2e2c3b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif;padding:16px;max-width:1200px;margin-inline:auto}
h1{font-size:20px;margin:0 0 4px}.sub{color:var(--muted);margin-bottom:14px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin:12px 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px}.card b{display:block;font-size:22px}.card span{color:var(--muted);font-size:12px}
.bar{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}.bar select,.bar input,.bar button{padding:8px 10px;border-radius:8px;border:1px solid var(--line);background:var(--card);color:var(--fg);font:inherit}
.bar button{background:var(--accent);color:#fff;border:0;cursor:pointer;font-weight:600}
.pages{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}.pg{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:8px 10px;font-size:13px}
.pg.err{border-color:var(--bad)}.pg small{display:block;color:var(--muted)}.pg .e{color:var(--bad)}
.wrap{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:12px}
table{border-collapse:collapse;width:100%;min-width:860px}th,td{padding:8px 10px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap}
th{cursor:pointer;color:var(--muted);font-weight:600;user-select:none}td.n,th.n{text-align:right}td.cap{white-space:normal;min-width:220px;max-width:340px}
td a{color:var(--accent)}.up{color:var(--good)}.empty{padding:24px;color:var(--muted)}
</style></head><body><h1>Post analytics</h1><div class="sub" id="sub">Loading...</div>
<div class="cards" id="cards"></div>
<div class="bar"><select id="fpage"><option value="">All pages</option></select>
<select id="fgroup"><option value="">All groups</option><option value="clipper">Clipper</option><option value="promo">Promo</option></select>
<input id="fq" placeholder="Search captions"><button id="refresh">Refresh now</button></div>
<div class="pages" id="pages"></div>
<h2 style="font-size:16px;margin:18px 0 8px">Page growth</h2>
<div class="wrap"><table style="min-width:760px"><thead id="ghead"></thead><tbody id="gbody"></tbody></table></div>
<h2 style="font-size:16px;margin:18px 0 8px">Day by day <span style="color:var(--muted);font-weight:400;font-size:13px">(UTC days)</span></h2>
<div class="wrap"><table style="min-width:640px"><thead id="dhead"></thead><tbody id="dbody"></tbody></table></div>
<h2 style="font-size:16px;margin:18px 0 8px">Posts</h2>
<div class="wrap"><table><thead id="head"></thead><tbody id="body"></tbody></table></div>
<script>
const COLS=[['page','Page'],['posted','Posted'],['caption','Caption'],['views','Views',1],['reach','Reach',1],['likes','Likes',1],['comments','Comments',1],['saves','Saves',1],['shares','Shares',1],['reposts','Reposts',1],['follows','Follows',1],['profile_visits','Visits',1],['views_delta','+Views',1]];
let rows=[],pages=[],growth=[],daily=[],sortKey='ts',asc=false;
const $=id=>document.getElementById(id);
const fmt=n=>(n==null?0:n).toLocaleString();
function el(t,c,x){const e=document.createElement(t);if(c)e.className=c;if(x!=null)e.textContent=x;return e}
async function api(url,opt={}){let key=localStorage.getItem('adminKey')||'';if(!key){key=prompt('Enter admin key:')||'';localStorage.setItem('adminKey',key)}
 const r=await fetch(url,{...opt,headers:{'X-Admin-Key':key}});if(r.status===401){localStorage.removeItem('adminKey');$('sub').textContent='Wrong key - reload and try again.';throw 0}
 if(r.status===503){$('sub').textContent='ADMIN_KEY is not set on the server.';throw 0}return r.json()}
async function load(){const j=await api('/dashboard/data');rows=j.posts;pages=j.pages;growth=j.growth||[];daily=j.daily||[];
 const sel=$('fpage'),cur=sel.value;sel.replaceChildren(new Option('All pages',''),...pages.map(p=>new Option(p.page_name,p.page_name)));sel.value=cur;render()}
function renderGrowth(){
 const pg=$('fpage').value,gr=$('fgroup').value,keep=x=>(!pg||x.page===pg)&&(!gr||x.group===gr);
 const sg=n=>n==null?'-':(n>0?'+':'')+fmt(n),dash=n=>n==null?'-':fmt(n);
 const hdr=(id,names)=>{const tr=el('tr');names.forEach((h,i)=>tr.append(el('th',i?'n':'',h)));$(id).replaceChildren(tr)};
 hdr('ghead',['Page','Followers','+24h','+7d','Since start','Profile visits (7d)','New followers (7d)','Reach (7d)']);
 const gb=$('gbody');gb.replaceChildren();const g=growth.filter(keep);
 if(!g.length){const tr=el('tr'),td=el('td','empty','No follower history yet. It starts with the next refresh and builds each day.');td.colSpan=8;tr.append(td);gb.append(tr)}
 for(const x of g){const tr=el('tr');tr.append(el('td',0,x.page),el('td','n',fmt(x.followers)),el('td','n',sg(x.d1)),el('td','n',sg(x.d7)),el('td','n',sg(x.since)),el('td','n',dash(x.pv7)),el('td','n',dash(x.nf7)),el('td','n',dash(x.reach7)));gb.append(tr)}
 hdr('dhead',['Day','Page','Followers','New followers','Profile visits','Reach','Posts']);
 const db=$('dbody');db.replaceChildren();const dd=daily.filter(x=>keep({page:x.page_name,group:x.page_group})).slice(0,120);
 if(!dd.length){const tr=el('tr'),td=el('td','empty','Nothing yet.');td.colSpan=7;tr.append(td);db.append(tr)}
 for(const x of dd){const tr=el('tr');tr.append(el('td',0,x.day),el('td',0,x.page_name),el('td','n',dash(x.followers)),el('td','n',dash(x.new_followers)),el('td','n',dash(x.profile_views)),el('td','n',dash(x.reach)),el('td','n',x.posts?String(x.posts):'-'));db.append(tr)}
}
function filtered(){const pg=$('fpage').value,gr=$('fgroup').value,q=$('fq').value.toLowerCase();
 return rows.filter(r=>(!pg||r.page===pg)&&(!gr||r.group===gr)&&(!q||(r.caption||'').toLowerCase().includes(q)))}
function render(){renderGrowth();const f=filtered();const sum=k=>f.reduce((a,r)=>a+(r[k]||0),0);
 $('sub').textContent=f.length+' posts - '+pages.length+' pages';
 $('cards').replaceChildren(...[['Posts',f.length],['Views',sum('views')],['Reach',sum('reach')],['Likes',sum('likes')],['Comments',sum('comments')],['Saves',sum('saves')],['Shares',sum('shares')],['Reposts',sum('reposts')],['Follows',sum('follows')],['Visits',sum('profile_visits')]].map(([l,v])=>{const c=el('div','card');c.append(el('b',0,fmt(v)),el('span',0,l));return c}));
 $('pages').replaceChildren(...pages.map(p=>{const d=el('div','pg'+(p.analytics_error?' err':''));d.append(el('b',0,p.page_name),el('small',0,(p.page_group||'')+(p.active?'':' (paused)')+' - pulled '+(p.pulled||'never')));
  if(p.analytics_error)d.append(el('small','e',p.analytics_error));return d}));
 $('head').replaceChildren((()=>{const tr=el('tr');for(const [k,l,n] of COLS){const th=el('th',n?'n':'',l+(sortKey===k?(asc?' ▲':' ▼'):''));th.onclick=()=>{if(sortKey===k)asc=!asc;else{sortKey=k;asc=false}render()};tr.append(th)}return tr})());
 const key=sortKey==='posted'?'ts':sortKey;
 f.sort((a,b)=>{const x=a[key],y=b[key];const c=(typeof x==='string'?(x||'').localeCompare(y||''):(x||0)-(y||0));return asc?c:-c});
 const body=$('body');body.replaceChildren();
 if(!f.length){const tr=el('tr'),td=el('td','empty','No posts yet. Click Refresh now, then reload in a minute.');td.colSpan=COLS.length;tr.append(td);body.append(tr);return}
 for(const r of f){const tr=el('tr');
  tr.append(el('td',0,r.page),el('td',0,r.posted||''));
  const c=el('td','cap');if(r.permalink){const a=el('a',0,(r.caption||'(no caption)').slice(0,90));a.href=r.permalink;a.target='_blank';a.rel='noopener noreferrer';c.append(a)}else c.textContent=(r.caption||'').slice(0,90);tr.append(c);
  for(const k of ['views','reach','likes','comments','saves','shares','reposts','follows','profile_visits'])tr.append(el('td','n',fmt(r[k])));
  tr.append(el('td','n'+(r.views_delta>0?' up':''),r.views_delta>0?'+'+fmt(r.views_delta):'-'));body.append(tr)}}
for(const id of ['fpage','fgroup'])$(id).onchange=render;$('fq').oninput=render;
$('refresh').onclick=async()=>{$('refresh').disabled=true;await api('/dashboard/refresh',{method:'POST'});$('sub').textContent='Refreshing - reload in a minute...';setTimeout(()=>{$('refresh').disabled=false},4000)};
load().catch(()=>{});
</script></body></html>"""

@app.route("/dashboard", methods=["GET"])
def dashboard_page():
    return Response(DASHBOARD_HTML, mimetype="text/html")

def _growth_summary(daily):
    """Per page: followers now, change over 1 day / 7 days / since the first snapshot, and 7-day totals."""
    from datetime import date, timedelta
    by_page = {}
    for d in daily:
        by_page.setdefault(d["fan_page_id"], []).append(d)
    out = []
    for pid, rows in by_page.items():
        rows.sort(key=lambda r: r["day"])
        snap = {r["day"]: r["followers"] for r in rows if r["followers"] is not None}
        if not snap:
            continue
        latest_day = max(snap)
        latest = snap[latest_day]
        ld = date.fromisoformat(latest_day)
        prev = lambda n: snap.get((ld - timedelta(days=n)).isoformat())
        first = snap[min(snap)]
        week = [r for r in rows if date.fromisoformat(r["day"]) > ld - timedelta(days=7)]
        total = lambda k: (sum(r[k] for r in week if r[k] is not None) if any(r[k] is not None for r in week) else None)
        out.append({"page": rows[0]["page_name"], "group": rows[0]["page_group"], "followers": latest,
                    "d1": None if prev(1) is None else latest - prev(1),
                    "d7": None if prev(7) is None else latest - prev(7),
                    "since": None if len(snap) < 2 else latest - first,
                    "pv7": total("profile_views"), "nf7": total("new_followers"), "reach7": total("reach")})
    out.sort(key=lambda x: x["page"])
    return out

@app.route("/dashboard/data", methods=["GET"])
def dashboard_data():
    denied = _admin_guard()
    if denied:
        return denied
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT p.page_name AS page, p.page_group AS "group", m.permalink, m.media_type, m.caption,
                   to_char(m.posted_at AT TIME ZONE 'America/New_York', 'Mon DD, HH12:MI AM') AS posted,
                   EXTRACT(EPOCH FROM m.posted_at)::bigint AS ts,
                   m.views, m.reach, m.likes, m.comments, m.saves, m.shares, m.reposts, m.follows, m.profile_visits,
                   m.views - COALESCE(m.prev_views, m.views) AS views_delta
            FROM post_metrics m JOIN fan_pages p ON p.id = m.fan_page_id
            ORDER BY m.posted_at DESC NULLS LAST LIMIT 3000""")
        posts = cur.fetchall()
        cur.execute("""
            SELECT id, page_name, page_group, active, analytics_error,
                   to_char(analytics_pulled_at AT TIME ZONE 'America/New_York', 'Mon DD HH12:MI AM') AS pulled
            FROM fan_pages ORDER BY id""")
        pages = cur.fetchall()
        cur.execute("""
            SELECT d.fan_page_id, p.page_name, p.page_group, d.day::text AS day, d.followers, d.new_followers,
                   d.profile_views, d.reach
            FROM page_daily d JOIN fan_pages p ON p.id = d.fan_page_id
            WHERE d.day >= CURRENT_DATE - 45 ORDER BY d.day DESC, p.page_name""")
        daily = cur.fetchall()
        cur.execute("""SELECT fan_page_id, (posted_at AT TIME ZONE 'UTC')::date::text AS day, COUNT(*) AS n
                       FROM post_metrics WHERE posted_at >= NOW() - INTERVAL '45 days' GROUP BY 1, 2""")
        posts_per_day = {(r["fan_page_id"], r["day"]): r["n"] for r in cur.fetchall()}
        cur.close()
        conn.close()
        for d in daily:
            d["posts"] = posts_per_day.get((d["fan_page_id"], d["day"]), 0)
        return jsonify({"posts": posts, "pages": pages, "daily": daily, "growth": _growth_summary(daily)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/dashboard/refresh", methods=["POST"])
def dashboard_refresh():
    denied = _admin_guard()
    if denied:
        return denied
    if _pull_lock.locked():
        return jsonify({"started": False, "note": "a refresh is already running"})
    threading.Thread(target=_pull_analytics_once, daemon=True).start()
    return jsonify({"started": True})

# ── PAGE DISPLAY NAMES (page_names.json: {"<fan_pages id>": "Name"}; applied at startup, survives reconnects) ──

PAGE_NAMES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "page_names.json")

def _apply_page_names():
    try:
        with open(PAGE_NAMES_FILE, encoding="utf-8") as f:
            names = json.load(f)
    except FileNotFoundError:
        return
    except Exception as e:
        print(f"[names] could not read page_names.json: {type(e).__name__}")
        return
    try:
        conn = get_conn()
        cur = conn.cursor()
        for pid, name in names.items():
            if str(pid).isdigit() and isinstance(name, str) and name.strip():
                cur.execute("UPDATE fan_pages SET page_name=%s, name_locked=TRUE WHERE id=%s",
                            (name.strip()[:80], int(pid)))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[names] {type(e).__name__}")

# ── INSTAGRAM LOGIN (accounts connect directly; no Facebook Page involved) ───────────────────────

IG_APP_ID = os.environ.get("IG_APP_ID")
IG_APP_SECRET = os.environ.get("IG_APP_SECRET")
IG_SCOPES = os.environ.get("IG_SCOPES", "instagram_business_basic,instagram_business_content_publish,instagram_business_manage_insights")
IG_GRAPH = "https://graph.instagram.com/" + os.environ.get("FB_GRAPH_VERSION", "v23.0")

def _ig_redirect_uri():
    return os.environ.get("BASE_URL", "https://fan-page-manager.onrender.com").rstrip("/") + "/connect/instagram/callback"

IG_CONNECT_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Connect Instagram</title>
<style>
:root{--bg:#f6f5fb;--card:#fff;--fg:#1c1b22;--muted:#6b6a78;--accent:#d62976;--line:#dcdae8}
@media(prefers-color-scheme:dark){:root{--bg:#0f0e14;--card:#1a1923;--fg:#f1f0f7;--muted:#9a98aa;--line:#2e2c3b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.5 system-ui,sans-serif;display:grid;place-items:center;min-height:100vh;padding:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:28px;width:100%;max-width:480px}
h1{margin:0 0 6px;font-size:22px}p{margin:0 0 14px;color:var(--muted);font-size:14px}
label{display:block;font-size:13px;margin:14px 0 6px}select{width:100%;padding:12px;border-radius:10px;border:1px solid var(--line);background:transparent;color:var(--fg);font:inherit}
button{margin-top:20px;width:100%;padding:13px;border:0;border-radius:10px;background:var(--accent);color:#fff;font:inherit;font-weight:600;cursor:pointer}
#msg{margin-top:14px;font-size:14px;color:#dc2626;min-height:20px}
</style></head><body><div class="card"><h1>Connect an Instagram account</h1>
<p>No Facebook Page needed. The account owner logs in on Instagram and approves. The account must be a Professional (Business or Creator) account and be added to the app as an Instagram tester.</p>
<label for="g">This account will post:</label>
<select id="g"><option value="clipper">Clips (clipper pages)</option><option value="promo">Mia promo content</option></select>
<button id="b">Log in with Instagram</button><div id="msg" role="status"></div></div>
<script>
const b=document.getElementById('b'),m=document.getElementById('msg');
b.onclick=async()=>{let key=localStorage.getItem('adminKey')||'';if(!key){key=prompt('Enter admin key:')||'';localStorage.setItem('adminKey',key)}
m.textContent='';b.disabled=true;
try{const r=await fetch('/connect/instagram/start',{method:'POST',headers:{'X-Admin-Key':key,'Content-Type':'application/json'},body:JSON.stringify({group:document.getElementById('g').value})});
const j=await r.json();if(r.ok&&j.url){location.href=j.url;return}
if(r.status===401)localStorage.removeItem('adminKey');m.textContent=j.error||'Something went wrong.';}
catch(e){m.textContent='Network error.'}b.disabled=false};
</script></body></html>"""

@app.route("/connect/instagram", methods=["GET"])
def connect_instagram_page():
    return Response(IG_CONNECT_HTML, mimetype="text/html")

@app.route("/connect/instagram/start", methods=["POST"])
def connect_instagram_start():
    denied = _admin_guard()
    if denied:
        return denied
    if not (IG_APP_ID and IG_APP_SECRET):
        return jsonify({"error": "IG_APP_ID and IG_APP_SECRET are not set on the server"}), 503
    group = (request.get_json(silent=True) or {}).get("group", "clipper")
    if group not in PAGE_GROUPS:
        return jsonify({"error": "bad group"}), 400
    from urllib.parse import urlencode
    url = "https://www.instagram.com/oauth/authorize?" + urlencode({
        "client_id": IG_APP_ID, "redirect_uri": _ig_redirect_uri(), "response_type": "code",
        "scope": IG_SCOPES, "state": _sign_state(group), "force_authentication": "1"})
    return jsonify({"url": url})

@app.route("/connect/instagram/callback", methods=["GET"])
def connect_instagram_callback():
    if not (ADMIN_KEY and IG_APP_ID and IG_APP_SECRET):
        return _result_page("Not configured", ["ADMIN_KEY, IG_APP_ID and IG_APP_SECRET must be set."], ok=False), 503
    group = _check_state(request.args.get("state"))
    if not group:
        return _result_page("Link expired", ["Start again from /connect/instagram."], ok=False), 400
    if request.args.get("error") or not request.args.get("code"):
        return _result_page("Not connected", [request.args.get("error_description", "Instagram login was cancelled.")], ok=False), 400
    try:
        code = request.args["code"].split("#")[0]
        tok = requests.post("https://api.instagram.com/oauth/access_token",
                            data={"client_id": IG_APP_ID, "client_secret": IG_APP_SECRET, "grant_type": "authorization_code",
                                  "redirect_uri": _ig_redirect_uri(), "code": code}, timeout=20).json()
        if isinstance(tok.get("data"), list) and tok["data"]:
            tok = tok["data"][0]
        short = tok.get("access_token")
        if not short:
            return _result_page("Instagram login failed", [tok.get("error_message") or (tok.get("error") or {}).get("message", "no token returned")], ok=False), 400
        long_ = requests.get("https://graph.instagram.com/access_token",
                             params={"grant_type": "ig_exchange_token", "client_secret": IG_APP_SECRET,
                                     "access_token": short}, timeout=20).json()
        token = long_.get("access_token")
        if not token:
            return _result_page("Token exchange failed", [(long_.get("error") or {}).get("message", "no token returned")], ok=False), 400
        expires_in = int(long_.get("expires_in") or 5184000)
        me = requests.get(f"{IG_GRAPH}/me", params={"fields": "user_id,username,account_type", "access_token": token},
                          timeout=20).json()
        ig_id = str(me.get("user_id") or tok.get("user_id") or "")
        username = me.get("username")
        if not ig_id:
            return _result_page("Could not read the account", [(me.get("error") or {}).get("message", "no account id returned")], ok=False), 400
        if me.get("account_type") == "PERSONAL":
            return _result_page("Not a Professional account", [f"@{username} is a personal account. Switch it to a Business or Creator account in Instagram settings, then try again."], ok=False), 400
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO fan_pages (fb_page_id, ig_account_id, page_name, page_access_token, page_group,
                                   auth_type, token_expires_at)
            VALUES (%s, %s, %s, %s, %s, 'instagram', NOW() + (%s || ' seconds')::interval)
            ON CONFLICT (fb_page_id) DO UPDATE
              SET ig_account_id=EXCLUDED.ig_account_id,
                  page_name=CASE WHEN fan_pages.name_locked THEN fan_pages.page_name ELSE EXCLUDED.page_name END,
                  page_access_token=EXCLUDED.page_access_token, auth_type='instagram',
                  token_expires_at=EXCLUDED.token_expires_at
            RETURNING (xmax = 0) AS inserted""", (f"ig:{ig_id}", ig_id, username or ig_id, token, group, str(expires_in)))
        inserted = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        days = expires_in // 86400
        return _result_page(f"Connected @{username}", [
            f"{'Added to ' + group if inserted else 'Updated (group unchanged)'} - no Facebook Page needed.",
            f"Token lasts about {days} days and the bot renews it automatically."])
    except Exception as e:
        print(f"[ig-connect] {type(e).__name__}")   # not str(e): request errors can contain tokens in the URL
        return _result_page("Something went wrong", ["Check the server logs and try again."], ok=False), 500

def _ig_refresh_tokens_once():
    """Renew Instagram-login tokens that expire within 30 days (each refresh gives another 60 days)."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT id, page_access_token FROM fan_pages
                       WHERE auth_type='instagram' AND (token_expires_at IS NULL OR token_expires_at < NOW() + INTERVAL '30 days')""")
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ig-refresh] {type(e).__name__}")
        return
    for row in rows:
        try:
            r = requests.get("https://graph.instagram.com/refresh_access_token",
                             params={"grant_type": "ig_refresh_token", "access_token": row["page_access_token"]},
                             timeout=20).json()
            if r.get("access_token"):
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("""UPDATE fan_pages SET page_access_token=%s,
                               token_expires_at=NOW() + (%s || ' seconds')::interval WHERE id=%s""",
                            (r["access_token"], str(int(r.get("expires_in") or 5184000)), row["id"]))
                conn.commit()
                cur.close()
                conn.close()
                print(f"[ig-refresh] page {row['id']} renewed")
            else:
                print(f"[ig-refresh] page {row['id']}: {(r.get('error') or {}).get('message', 'no token')[:120]}")
        except Exception as e:
            print(f"[ig-refresh] page {row['id']}: {type(e).__name__}")

def _ig_token_refresh_loop():
    time.sleep(120)
    while True:
        _ig_refresh_tokens_once()
        time.sleep(12 * 3600)

# ── STARTUP ───────────────────────────────────────────────────────────────────

init_db()
_recover_interrupted_clips()
_apply_page_names()
_run_jobs_once()
threading.Thread(target=_analytics_loop, daemon=True).start()
threading.Thread(target=_clip_queue_loop, daemon=True).start()
threading.Thread(target=_ig_token_refresh_loop, daemon=True).start()
threading.Thread(target=_quote_scheduler_loop, daemon=True).start()
threading.Thread(target=_auto_feed_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
