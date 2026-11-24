"""Kafka -> S3 sink: read new claim events from the topic and land them in S3 as JSON-lines files.

A small stand-in for a managed sink connector (MSK Connect S3 sink or Amazon Data Firehose).

Two modes:

  One-off (default): read everything new, write one local file, commit, stop.
      python streaming/kafka_to_s3_sink.py

  Continuous (--follow --bucket ...): keep running; every --flush-seconds, if new events
  arrived, write them to a file, upload it to s3://<bucket>/landing/stream/, then commit the
  Kafka offsets. In AWS, the new file triggers the claims-stream-claims Glue job automatically.
      python streaming/kafka_to_s3_sink.py --follow --bucket claims-data-<account-id>-dev

Offsets are committed only after the file is safely written (and uploaded), so a crash never
loses events. At worst an event is sent twice, and the pipeline removes duplicates by event_id.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer

ROOT = Path(__file__).resolve().parent.parent


def build_consumer(args):
    group = args.group
    if args.from_beginning:
        group += "-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=group,
        auto_offset_reset="earliest",          # a new group starts at the first message
        enable_auto_commit=False,              # commit only after the file is landed
    )


def summarize(lines):
    per_type = {}
    for line in lines:
        try:
            event_type = json.loads(line).get("event_type", "?")
        except json.JSONDecodeError:
            event_type = "MALFORMED"
        per_type[event_type] = per_type.get(event_type, 0) + 1
    return per_type


def write_file(lines, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"claim_events_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.jsonl"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def upload(path: Path, bucket: str, prefix: str) -> str:
    import boto3                                # only needed in --follow/--bucket mode

    key = f"{prefix.rstrip('/')}/{path.name}"
    boto3.client("s3").upload_file(str(path), bucket, key)
    return f"s3://{bucket}/{key}"


def poll_batch(consumer, seconds: float):
    """Collect messages for up to `seconds`. Returns a list of (partition, offset, key, value)."""
    batch, deadline = [], time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return batch
        records = consumer.poll(timeout_ms=int(min(remaining, 1.0) * 1000))
        for tp, messages in records.items():
            for m in messages:
                key = m.key.decode("utf-8") if m.key else None
                batch.append((tp.partition, m.offset, key, m.value.decode("utf-8")))


def rewind_to_committed(consumer):
    """Go back to the last committed offsets, so a failed batch is read again."""
    for tp in consumer.assignment():
        committed = consumer.committed(tp)
        if committed is None:
            consumer.seek_to_beginning(tp)
        else:
            consumer.seek(tp, getattr(committed, "offset", committed))


def land(batch, args):
    lines = [value for _, _, _, value in batch]
    path = write_file(lines, Path(args.out))
    where = upload(path, args.bucket, args.prefix) if args.bucket else str(path)
    return where, summarize(lines)


def run_once(consumer, args):
    batch = poll_batch(consumer, args.wait_seconds)
    for partition, offset, key, value in batch:
        print(f"  partition {partition} offset {offset}  key={key}  {summarize([value])}")
    if not batch:
        print("No new events on the topic.")
        return
    where, per_type = land(batch, args)
    consumer.commit()
    print(f"\nLanded {len(batch)} events {per_type} -> {where}")
    if not args.bucket:
        print("Next: upload it to S3 landing/stream/ (or run with --follow --bucket <data bucket>).")


def run_follow(consumer, args):
    print(f"Sink running: topic {args.topic} -> "
          f"{'s3://' + args.bucket + '/' + args.prefix if args.bucket else args.out}, "
          f"every {args.flush_seconds}s. Ctrl+C to stop.")
    try:
        while True:
            batch = poll_batch(consumer, args.flush_seconds)
            if not batch:
                continue
            try:
                where, per_type = land(batch, args)
            except Exception as exc:  # noqa: BLE001 - keep running; offsets not committed, so nothing is lost
                print(f"{datetime.now():%H:%M:%S}  upload failed, will retry these events: {exc}")
                rewind_to_committed(consumer)
                time.sleep(10)
                continue
            consumer.commit()
            print(f"{datetime.now():%H:%M:%S}  landed {len(batch)} events {per_type} -> {where}")
    except KeyboardInterrupt:
        print("Stopped.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="claims.events")
    ap.add_argument("--group", default="claims-s3-sink")
    ap.add_argument("--from-beginning", action="store_true", help="use a fresh consumer group and read everything")
    ap.add_argument("--out", default=str(ROOT / "local" / "stream" / "outgoing"))
    ap.add_argument("--bucket", help="S3 data bucket; when set, files are uploaded there")
    ap.add_argument("--prefix", default="landing/stream")
    ap.add_argument("--follow", action="store_true", help="keep running and land new events every --flush-seconds")
    ap.add_argument("--flush-seconds", type=int, default=60)
    ap.add_argument("--wait-seconds", type=int, default=15, help="one-off mode: stop after this long with no new events")
    args = ap.parse_args()

    consumer = build_consumer(args)
    try:
        run_follow(consumer, args) if args.follow else run_once(consumer, args)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
