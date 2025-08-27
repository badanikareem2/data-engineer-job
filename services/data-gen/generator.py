import os, time, uuid, json, random, math
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import Json

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB   = os.getenv("POSTGRES_DB", "eventsdb")
PGUSER = os.getenv("POSTGRES_USER", "app")
PGPASS = os.getenv("POSTGRES_PASSWORD", "app_pw")
RPS    = float(os.getenv("DATA_GEN_RPS", "5"))  # events/second

devices = ["ios", "android", "web-chrome", "web-safari", "tv-os"]
event_types = ["play", "pause", "finish", "click"]

def conn():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDB, user=PGUSER, password=PGPASS
    )

def ensure_content_rows(cur):
    # If no content, create ~20 items
    cur.execute("SELECT count(*) FROM content")
    (cnt,) = cur.fetchone()
    if cnt >= 5:
        return
    choices = [("podcast", 1800), ("video", 600), ("newsletter", None)]
    for i in range(20):
        ctype, length = random.choice(choices)
        slug = f"{ctype}-{i+1}-{uuid.uuid4().hex[:6]}"
        cur.execute("""
            INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (slug) DO NOTHING
        """, (str(uuid.uuid4()), slug, f"{ctype.title()} #{i+1}", ctype, length))

def pick_content(cur):
    cur.execute("SELECT id, content_type, COALESCE(length_seconds, 0) FROM content ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "type": row[1], "len": int(row[2])}

def gen_event(cur):
    c = pick_content(cur)
    if not c:
        return
    et = random.choices(event_types, weights=[50, 20, 10, 20], k=1)[0]
    # duration rules
    duration_ms = None
    if et in ("play", "pause"):
        upper = max(2000, (c["len"] or 900) * 1000)  # up to content length (or ~15m) in ms
        duration_ms = random.randint(250, min(upper, 300000))  # cap 5 minutes per event
    elif et == "finish":
        duration_ms = (c["len"] or random.randint(300, 1200)) * 1000
    # click => None

    payload = {
        "app_version": f"1.{random.randint(0,9)}.{random.randint(0,9)}",
        "ip": f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
        "ab": random.choice(["A","B"]),
    }

    cur.execute("""
        INSERT INTO engagement_events (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        c["id"],
        str(uuid.uuid4()),
        et,
        datetime.now(timezone.utc),
        duration_ms,
        random.choice(devices),
        Json(payload)
    ))
    (new_id,) = cur.fetchone()
    return new_id

def main():
    interval = 1.0 / RPS if RPS > 0 else 0.2
    print(f"[data-gen] connecting to postgres at {PGHOST}:{PGPORT}/{PGDB} as {PGUSER}, rate={RPS} eps")
    with conn() as cx:
        cx.autocommit = True
        with cx.cursor() as cur:
            ensure_content_rows(cur)
    # main loop
    while True:
        try:
            with conn() as cx:
                cx.autocommit = True
                with cx.cursor() as cur:
                    new_id = gen_event(cur)
                    if new_id:
                        print(f"[data-gen] inserted engagement_event id={new_id}")
        except Exception as e:
            print(f"[data-gen] error: {e}")
        time.sleep(interval)

if __name__ == "__main__":
    main()
