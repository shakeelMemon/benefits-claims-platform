"""Gold dimensions: plan (with employer group), provider and date. Type 1: always the latest values.

The member dimension keeps history and has its own job (job_gold_dim_member.py).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F  # noqa: E402

from common.runtime import run_main, zone  # noqa: E402

ARGS = ["lake_root", "run_date"]
DEFAULTS = {"lake_root": "local/lake", "run_date": None}


def build_dim_date(spark, start="2023-01-01", end="2027-12-31"):
    days = spark.sql(f"select explode(sequence(date'{start}', date'{end}', interval 1 day)) as calendar_date")
    return days.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        "calendar_date",
        F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.date_format("calendar_date", "yyyy-MM").alias("year_month"),
        F.date_format("calendar_date", "EEEE").alias("day_name"),
        F.dayofweek("calendar_date").isin(1, 7).alias("is_weekend"),
    )


def run(spark, args):
    root = args["lake_root"]
    silver = lambda name: spark.read.parquet(zone(root, "silver", name))  # noqa: E731
    silver("plans").write.mode("overwrite").parquet(zone(root, "gold", "dim_plan"))
    silver("providers").write.mode("overwrite").parquet(zone(root, "gold", "dim_provider"))
    build_dim_date(spark).write.mode("overwrite").parquet(zone(root, "gold", "dim_date"))
    return {"job": "gold_dimensions", "dim_plan": silver("plans").count(),
            "dim_provider": silver("providers").count()}


if __name__ == "__main__":
    run_main(run, ARGS, DEFAULTS)
