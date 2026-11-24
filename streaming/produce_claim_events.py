"""Publish claim events to Kafka, the way the claims system would.

Each event is keyed by claim_id. Kafka keeps the order of messages with the same key
(they always go to the same partition), so all events for one claim arrive in order.

Usage (Kafka running from docker-compose.yml):
    python streaming/produce_claim_events.py --file local/stream/incoming/claim_events_20261020.jsonl
"""

import argparse
import json
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def ensure_topic(bootstrap, topic, partitions=3):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        print(f"created topic {topic} ({partitions} partitions)")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="claims.events")
    ap.add_argument("--rate", type=float, default=0, help="events per second; 0 = as fast as possible")
    args = ap.parse_args()

    ensure_topic(args.bootstrap, args.topic)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",                      # wait until the broker has stored the message
        enable_idempotence=True,         # no duplicates from producer retries
        linger_ms=20,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
    )
    sent = 0
    with open(args.file, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                key = json.loads(line).get("claim_id") or "unknown"
            except json.JSONDecodeError:
                key = "malformed"        # still sent: the stream job must quarantine it
            producer.send(args.topic, key=key, value=line)
            sent += 1
            if args.rate:
                time.sleep(1 / args.rate)
    producer.flush()
    producer.close()
    print(f"sent {sent} events to {args.topic}")


if __name__ == "__main__":
    main()
