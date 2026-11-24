"""Batch ingest: daily extracts from the policy admin, reference and finance systems into bronze.

This is the job that replaces the legacy SSIS package (see docs/ssis_to_glue_mapping.md).

For each source it:
  1. checks the expected file arrived (missing file = job fails = Step Functions alerts),
  2. reads it exactly as sent (every column as text, nothing lost),
  3. adds audit columns: source file, extract date, ingest time, run date,
  4. writes bronze/<entity>/run_date=YYYY-MM-DD, replacing only that day's partition,
     so rerunning a day is safe (idempotent),
  5. writes one row per file to audit/load_audit (rows read, rows written).
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F  # noqa: E402

from common.runtime import run_main, zone  # noqa: E402
from common.schemas import BATCH_SOURCES  # noqa: E402

ARGS = ["lake_root", "landing_root", "run_date"]
DEFAULTS = {"lake_root": "local/lake", "landing_root": "local/landing", "run_date": None}


class MissingSourceFile(Exception):
    pass


def files_for(spark, landing_root, folder, pattern, run_date):
    """Daily extracts are named <entity>_YYYYMMDD.csv; reference files have no date."""
    stamp = run_date.replace("-", "")
    name = pattern.replace("*", stamp) if "*" in pattern else pattern
    path = zone(landing_root, folder, name)
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(hpath):
        raise MissingSourceFile(f"Expected source file not found: {path}")
    return path


def run(spark, args):
    run_date = args["run_date"]
    if not run_date:
        raise ValueError("--run_date is required (YYYY-MM-DD)")
    root, landing = args["lake_root"], args["landing_root"]
    ingest_ts = datetime.now(timezone.utc).replace(microsecond=0)
    audit, summary = [], {}

    for entity, (folder, pattern) in BATCH_SOURCES.items():
        path = files_for(spark, landing, folder, pattern, run_date)
        df = (spark.read.option("header", True).option("inferSchema", False).csv(path)
              .withColumn("_source_file", F.input_file_name())
              .withColumn("_extract_date",
                          F.coalesce(F.to_date(F.regexp_extract("_source_file", r"_(\d{8})\.csv$", 1), "yyyyMMdd"),
                                     F.to_date(F.lit(run_date))))
              .withColumn("_ingest_ts", F.lit(ingest_ts).cast("timestamp"))
              .withColumn("run_date", F.lit(run_date)))
        rows = df.count()
        if rows == 0:
            raise MissingSourceFile(f"Source file is empty: {path}")
        df.write.mode("overwrite").partitionBy("run_date").parquet(zone(root, "bronze", entity))
        audit.append((run_date, entity, path, rows, rows, ingest_ts))
        summary[entity] = rows

    (spark.createDataFrame(audit, "run_date string, entity string, source_file string, rows_read long, "
                                  "rows_written long, loaded_at timestamp")
     .write.mode("overwrite").partitionBy("run_date").parquet(zone(root, "audit", "load_audit")))
    return {"job": "bronze_batch", "run_date": run_date, "rows": summary}


if __name__ == "__main__":
    run_main(run, ARGS, DEFAULTS)
