"""One-time copy of the bot's own tables from the old database to the new one (for example Supabase -> Neon).

Run it on your own computer so the database addresses and passwords never leave it:

    PowerShell:
        $env:OLD_DATABASE_URL = "<old connection string>"
        $env:NEW_DATABASE_URL = "<new connection string>"
        python tools/migrate_db.py            # shows what it would copy (changes nothing)
        python tools/migrate_db.py --go       # copies it

It only reads from the old database. In the new one it only adds rows: nothing is deleted, truncated or overwritten
(rows that already exist are skipped). Only the bot's own tables are copied; any other tables are left alone.
The new database needs the bot's tables to exist first: start the bot once against it and it creates them.
"""
import os
import sys

import psycopg2
from psycopg2.extras import execute_values

TABLES = ["fan_pages", "app_settings", "post_log", "post_metrics", "page_daily", "clip_submissions", "quote_queue",
          "analytics_snapshots"]


def columns(cur, table):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s "
                "ORDER BY ordinal_position", (table,))
    return [r[0] for r in cur.fetchall()]


def main():
    go = "--go" in sys.argv
    old_url, new_url = os.environ.get("OLD_DATABASE_URL"), os.environ.get("NEW_DATABASE_URL")
    if not old_url or not new_url:
        print("Set OLD_DATABASE_URL and NEW_DATABASE_URL first (see the top of this file).")
        return 1
    if old_url == new_url:
        print("The old and new addresses are the same - refusing to run.")
        return 1
    old = psycopg2.connect(old_url, connect_timeout=15)
    new = psycopg2.connect(new_url, connect_timeout=15)
    oc, nc = old.cursor(), new.cursor()
    print(f"{'table':22} {'old rows':>9} {'new rows (before)':>18}")
    plan = []
    for t in TABLES:
        ocols, ncols = columns(oc, t), columns(nc, t)
        if not ocols or not ncols:
            print(f"{t:22} {'-':>9}   (missing in {'old' if not ocols else 'new'} database, skipped)")
            continue
        oc.execute(f'SELECT COUNT(*) FROM "{t}"')
        n_old = oc.fetchone()[0]
        nc.execute(f'SELECT COUNT(*) FROM "{t}"')
        n_new = nc.fetchone()[0]
        print(f"{t:22} {n_old:>9} {n_new:>18}")
        plan.append((t, [c for c in ocols if c in ncols], n_old))
    if not go:
        print("\nNothing was changed. Run again with --go to copy.")
        return 0
    print()
    for t, cols, n_old in plan:
        if not n_old:
            continue
        collist = ", ".join(f'"{c}"' for c in cols)
        oc.execute(f'SELECT {collist} FROM "{t}"')
        rows = oc.fetchall()
        execute_values(nc, f'INSERT INTO "{t}" ({collist}) VALUES %s ON CONFLICT DO NOTHING', rows, page_size=500)
        if "id" in cols:      # keep the numbering going after the highest copied id
            nc.execute("SELECT pg_get_serial_sequence(%s, 'id')", (t,))
            seq = nc.fetchone()[0]
            if seq:
                nc.execute(f'SELECT COALESCE(MAX(id), 0) FROM "{t}"')
                top = nc.fetchone()[0]
                if top:
                    nc.execute("SELECT setval(%s, %s)", (seq, top))
        new.commit()
        nc.execute(f'SELECT COUNT(*) FROM "{t}"')
        print(f"copied {t:22} now {nc.fetchone()[0]} rows in the new database")
    print("\nDone. Restart the bot on Render, then check /pages.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
