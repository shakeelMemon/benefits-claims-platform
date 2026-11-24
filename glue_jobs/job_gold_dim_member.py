"""Gold dim_member as a Slowly Changing Dimension Type 2.

A new version starts whenever a tracked attribute changes between daily extracts:
plan, coverage tier, status, province, postal FSA or employer group.
  valid_from  first version: the coverage effective date; later versions: the extract date
  valid_to    the day before the next version starts, or 9999-12-31
  is_current  true for the latest version

Claims join to the version that was valid on the service date, so a claim is reported under
the plan and province the member had when the service happened, not where they live today.

The dimension is rebuilt from the full snapshot history each run, so it is always consistent
and safe to rerun. At larger scale the same logic would MERGE only changed members into an
Apache Iceberg table (supported natively by Glue 5.0).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.runtime import run_main, zone  # noqa: E402

ARGS = ["lake_root", "run_date"]
DEFAULTS = {"lake_root": "local/lake", "run_date": None}
TRACKED = ["group_id", "plan_code", "coverage_tier", "member_status", "province", "postal_fsa"]
END_OF_TIME = "9999-12-31"


def build_scd2(snapshots):
    by_member = Window.partitionBy("member_id").orderBy("extract_date")
    changes = (snapshots
               .withColumn("_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("~")) for c in TRACKED]), 256))
               .withColumn("_prev_hash", F.lag("_hash").over(by_member))
               .filter(F.col("_prev_hash").isNull() | (F.col("_hash") != F.col("_prev_hash"))))
    versions = (changes
                .withColumn("version", F.row_number().over(by_member))
                .withColumn("valid_from", F.when(F.col("version") == 1,
                                                 F.least("coverage_effective_date", "extract_date"))
                                           .otherwise(F.col("extract_date"))))
    next_start = F.lead("valid_from").over(Window.partitionBy("member_id").orderBy("version"))
    return (versions
            .withColumn("valid_to", F.coalesce(F.date_sub(next_start, 1), F.lit(END_OF_TIME).cast("date")))
            .withColumn("is_current", next_start.isNull())
            .withColumn("member_sk", F.md5(F.concat_ws("|", "member_id", F.col("valid_from").cast("string"))))
            .select("member_sk", "member_id", "version", *TRACKED, "age_band", "coverage_effective_date",
                    "valid_from", "valid_to", "is_current"))


def run(spark, args):
    root = args["lake_root"]
    dim = build_scd2(spark.read.parquet(zone(root, "silver", "member_snapshots")))
    dim.write.mode("overwrite").parquet(zone(root, "gold", "dim_member"))
    dim = spark.read.parquet(zone(root, "gold", "dim_member"))
    return {"job": "gold_dim_member", "members": dim.select("member_id").distinct().count(),
            "rows": dim.count(), "members_with_history": dim.filter("version > 1").count()}


if __name__ == "__main__":
    run_main(run, ARGS, DEFAULTS)
