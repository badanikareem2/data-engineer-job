import os, json, time, logging
from datetime import datetime, timezone
import requests, redis
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

logging.basicConfig(level=logging.INFO, format='[fanout] %(asctime)s %(levelname)s %(message)s')

BROKER  = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC   = os.getenv("TOPIC", "dbserver1.public.engagement_events")
GROUP   = os.getenv("GROUP_ID", "fanout-g2")
OFFSET  = os.getenv("AUTO_OFFSET_RESET", "earliest")
EXT_URL = os.getenv("EXTERNAL_URL", "http://external-mock:9009/event")
RHOST   = os.getenv("REDIS_HOST", "redis")
RPORT   = int(os.getenv("REDIS_PORT", "6379"))
WINDOW  = int(os.getenv("WINDOW_SECONDS", "600"))  # 10 minutes (approx)

def parse_msg(value_bytes):
    """Support Debezium schema/payload, Debezium 'after', and flat JSON."""
    try:
        j = json.loads(value_bytes.decode("utf-8"))
    except Exception:
        return {}

    rec = None
    if isinstance(j, dict) and "payload" in j:
        p = j.get("payload") or {}
        if str(p.get("__deleted", "false")).lower() == "true":
            return {}
        rec = p.get("after") if isinstance(p.get("after"), dict) else p
    elif isinstance(j, dict) and "after" in j and isinstance(j["after"], dict):
        rec = j["after"]
    elif isinstance(j, dict):
        rec = j

    if not isinstance(rec, dict):
        return {}

    out = {
        "id": rec.get("id"),
        "content_id": rec.get("content_id"),
        "user_id": rec.get("user_id"),
        "event_type": rec.get("event_type"),
        "duration_ms": rec.get("duration_ms"),
        "device": rec.get("device"),
        "event_ts": rec.get("event_ts"),
    }
    try:
        if out["event_ts"]:
            dt = datetime.fromisoformat(str(out["event_ts"]).replace("Z", "+00:00"))
        else:
            dt = datetime.now(timezone.utc)
        out["ts_epoch"] = int(dt.timestamp())
    except Exception:
        out["ts_epoch"] = int(time.time())

    try:
        if out["duration_ms"] is not None:
            out["engagement_seconds"] = round(float(out["duration_ms"]) / 1000.0, 2)
    except Exception:
        pass

    return out

def make_consumer():
    backoff = 1
    while True:
        try:
            logging.info(f"Connecting Kafka broker={BROKER} topic={TOPIC} group={GROUP} offset={OFFSET}")
            c = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BROKER],
                group_id=GROUP,
                enable_auto_commit=True,
                auto_offset_reset=OFFSET,
                # IMPORTANT: do NOT set consumer_timeout_ms=0 (that ends iteration immediately)
                # Either omit it (block) or set to a positive value:
                consumer_timeout_ms=10000,
                request_timeout_ms=30000,
                session_timeout_ms=10000,
            )
            logging.info("Connected to Kafka.")
            return c
        except NoBrokersAvailable:
            logging.warning(f"Broker not available, retrying in {backoff}s ...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

def main():
    r = redis.Redis(host=RHOST, port=RPORT, decode_responses=True)

    while True:
        consumer = make_consumer()
        try:
            for msg in consumer:
                try:
                    rec = parse_msg(msg.value)
                    if not rec or not rec.get("content_id"):
                        continue
                    cid = rec["content_id"]
                    now = rec.get("ts_epoch", int(time.time()))

                    # 1) Rolling leaderboard
                    r.zincrby("hot:totals", 1.0, cid)
                    # 2) Last-seen timestamp
                    r.zadd("hot:last_ts", {cid: now})

                    # 3) External POST
                    payload = {
                        "id": rec.get("id"),
                        "content_id": cid,
                        "event_type": rec.get("event_type"),
                        "event_ts": rec.get("event_ts"),
                        "engagement_seconds": rec.get("engagement_seconds"),
                        "device": rec.get("device"),
                        "source": "fanout"
                    }
                    try:
                        requests.post(os.getenv("EXTERNAL_URL", "http://external-mock:9009/event"),
                                      json=payload, timeout=2)
                    except Exception as e:
                        logging.warning(f"External POST failed: {e}")

                    logging.info(f"processed p{msg.partition} o{msg.offset} content={cid} type={rec.get('event_type')}")
                except Exception as e:
                    logging.error(f"Process error at offset {msg.offset}: {e}", exc_info=False)
        except (KafkaError, OSError) as e:
            logging.warning(f"Kafka loop error: {e} — reconnecting in 2s")
            time.sleep(2)
            continue
        except Exception as e:
            logging.error(f"Fatal loop error: {e} — restarting in 3s")
            time.sleep(3)
            continue

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
