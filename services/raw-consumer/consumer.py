import os, json, sys, time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "dbserver1.public.engagement_events")
GROUP_ID = os.getenv("GROUP_ID", "debug-consumer")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

def decode(b):
    if b is None:
        return None
    try:
        return b.decode("utf-8")
    except Exception:
        return str(b)

def pretty(s):
    if s is None:
        return "null"
    try:
        import json as _json
        return _json.dumps(_json.loads(s), ensure_ascii=False)
    except Exception:
        return s

def main():
    print(f"[consumer] broker={KAFKA_BROKER} topic={TOPIC} group={GROUP_ID} offset={AUTO_OFFSET_RESET}", flush=True)

    # Retry loop to avoid container crash/restart
    backoff = 1
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                group_id=GROUP_ID,
                enable_auto_commit=True,
                auto_offset_reset=AUTO_OFFSET_RESET,
                consumer_timeout_ms=0
            )
            print("[consumer] connected to broker", flush=True)
            break
        except NoBrokersAvailable:
            print(f"[consumer] broker not available, retrying in {backoff}s ...", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    for msg in consumer:
        ts = datetime.utcnow().isoformat() + "Z"
        key = pretty(decode(msg.key))
        val = pretty(decode(msg.value))
        print(f"[{ts}] partition={msg.partition} offset={msg.offset}\n  key:   {key}\n  value: {val}\n", flush=True)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
