"""Streaming ingest: claim events from Kafka (or replayed files) into the bronze zone.

AWS: a Glue streaming job reading the MSK topic `claims.events`, running all the time.
Local: the same code reads either a local Kafka (docker compose) or JSON-lines files dropped
into a folder; `availableNow` processes everything new and stops, like a scheduled micro-batch.

Every micro-batch is split in two:
  - parsed events  -> bronze/claim_events/event_date=YYYY-MM-DD   (append)
  - unparseable    -> quarantine/claim_events_malformed           (append, raw text kept)
The checkpoint remembers what was already read (Kafka offsets or file names), so a rerun
does not read the same data twice. If a batch is retried, duplicates are removed in silver
by event_id, which makes the whole path effectively exactly-once.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F  # noqa: E402

from common.runtime import run_main, zone  # noqa: E402
from common.schemas import CLAIM_EVENT_SCHEMA  # noqa: E402

ARGS = ["lake_root", "source", "stream_input", "kafka_bootstrap", "kafka_topic", "trigger_seconds"]
DEFAULTS = {"lake_root": "local/lake", "source": "files", "stream_input": "local/stream/incoming",
            "kafka_bootstrap": "localhost:9092", "kafka_topic": "claims.events", "trigger_seconds": "0"}


def read_stream(spark, args):
    if args["source"] == "kafka":
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", args["kafka_bootstrap"])
                .option("subscribe", args["kafka_topic"])
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
                .select(F.col("value").cast("string").alias("value"),
                        F.concat_ws(":", "topic", "partition", "offset").alias("_source_position")))
    return (spark.readStream.text(args["stream_input"])
            .select("value", F.input_file_name().alias("_source_position")))


def parse_events(raw):
    """Parse JSON text into typed columns; flag rows that could not be parsed."""
    parsed = raw.withColumn("e", F.from_json("value", CLAIM_EVENT_SCHEMA))
    return (parsed
            .withColumn("_is_malformed", F.col("e").isNull() | F.col("e.event_id").isNull()
                        | F.col("e.event_type").isNull() | F.col("e.event_ts").isNull())
            .select("value", "_source_position", "_is_malformed", "e.*")
            .withColumn("event_ts", F.to_timestamp("event_ts"))
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("_ingest_ts", F.current_timestamp()))


def write_batch(lake_root):
    def _write(batch_df, batch_id):
        batch_df = batch_df.withColumn("_batch_id", F.lit(batch_id)).persist()
        good = batch_df.filter(~F.col("_is_malformed")).drop("value", "_is_malformed")
        bad = batch_df.filter(F.col("_is_malformed")).select(
            "value", "_source_position", "_ingest_ts", "_batch_id",
            F.lit("MALFORMED_JSON").alias("reason"))
        good.write.mode("append").partitionBy("event_date").parquet(zone(lake_root, "bronze", "claim_events"))
        bad.write.mode("append").parquet(zone(lake_root, "quarantine", "claim_events_malformed"))
        batch_df.unpersist()
    return _write


def run(spark, args):
    root = args["lake_root"]
    events = parse_events(read_stream(spark, args))
    writer = (events.writeStream
              .foreachBatch(write_batch(root))
              .option("checkpointLocation", zone(root, "_checkpoints", "claim_events_" + args["source"])))
    seconds = int(args["trigger_seconds"] or 0)
    writer = writer.trigger(availableNow=True) if seconds == 0 else writer.trigger(processingTime=f"{seconds} seconds")
    query = writer.start()
    query.awaitTermination()
    progress = query.recentProgress
    rows = sum(p.get("numInputRows", 0) for p in progress)
    return {"job": "stream_claims_to_bronze", "source": args["source"], "rows_read": rows}


if __name__ == "__main__":
    run_main(run, ARGS, DEFAULTS, kafka=(("--source" in sys.argv) and ("kafka" in sys.argv)))
