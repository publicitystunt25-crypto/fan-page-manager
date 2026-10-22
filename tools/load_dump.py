"""Load the bot's own tables from a data-only SQL dump (COPY ... FROM stdin sections) into the new database.

Run it on your own computer so the connection string and the data never leave it:

    PowerShell:
        $env:NEW_DATABASE_URL = "<the Neon connection string the bot uses>"
        python tools/load_dump.py "C:\\Users\\Nathaniel\\Downloads\\db_neon.sql"          # shows what it found, changes nothing
        python tools/load_dump.py "C:\\Users\\Nathaniel\\Downloads\\db_neon.sql" --go     # loads it

Only the bot's own tables are loaded; everything else in the file is ignored. Nothing is deleted, truncated or
overwritten: rows that already exist are skipped. The tables must already exist (start the bot once against the new
database and it creates them). `--parse-only` reads the file without connecting to any database.
"""
import io
import os
import re
import sys

TABLES = ["fan_pages", "app_settings", "post_log", "post_metrics", "page_daily", "clip_submissions", "quote_queue",
          "analytics_snapshots"]
HEADER = re.compile(r"^COPY public\.(\w+) \((.+)\) FROM stdin;\s*$")


def sections(path):
    """Yield (table, [columns], [data lines]) for each wanted COPY section of the dump."""
    table = cols = None
    lines = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if table is None:
                m = HEADER.match(line)
                if m and m.group(1) in TABLES:
                    table, cols, lines = m.group(1), [c.strip().strip('"') for c in m.group(2).split(",")], []
            elif line.rstrip("\r\n") == "\\.":
                yield table, cols, lines
                table = None
            else:
                lines.append(line)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        print(__doc__)
        return 1
    found = list(sections(args[0]))
    print(f"{'table':20} {'rows in file':>12}")
    for t, cols, lines in found:
        print(f"{t:20} {len(lines):>12}")
    missing = [t for t in TABLES if t not in {s[0] for s in found}]
    if missing:
        print("not in the file (nothing to load for them):", ", ".join(missing))
    if "--parse-only" in sys.argv:
        return 0
    url = os.environ.get("NEW_DATABASE_URL")
    if not url:
        print("\nSet NEW_DATABASE_URL first (see the top of this file).")
        return 1
    import psycopg2
    conn = psycopg2.connect(url, connect_timeout=15)
    cur = conn.cursor()
    print(f"\n{'table':20} {'rows now in the new database':>30}")
    for t, cols, lines in found:
        cur.execute(f'SELECT COUNT(*) FROM "{t}"')
        print(f"{t:20} {cur.fetchone()[0]:>30}")
    if "--go" not in sys.argv:
        print("\nNothing was changed. Run again with --go to load.")
        return 0
    print()
    for t, cols, lines in found:
        if not lines:
            continue
        collist = ", ".join(f'"{c}"' for c in cols)
        cur.execute(f'CREATE TEMP TABLE _stage (LIKE "{t}" INCLUDING DEFAULTS) ON COMMIT DROP')
        cur.copy_expert(f"COPY _stage ({collist}) FROM STDIN", io.StringIO("".join(lines)))
        cur.execute(f'INSERT INTO "{t}" ({collist}) SELECT {collist} FROM _stage ON CONFLICT DO NOTHING')
        added = cur.rowcount
        if "id" in cols:      # keep the numbering going after the highest loaded id
            cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (t,))
            seq = cur.fetchone()[0]
            if seq:
                cur.execute(f'SELECT COALESCE(MAX(id), 0) FROM "{t}"')
                top = cur.fetchone()[0]
                if top:
                    cur.execute("SELECT setval(%s, %s)", (seq, top))
        conn.commit()
        print(f"loaded {t:20} {added} of {len(lines)} rows added")
    print("\nDone. Restart the bot on Render, then check /pages.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
