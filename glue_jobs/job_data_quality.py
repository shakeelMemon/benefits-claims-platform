"""Data-quality gate: runs after gold is built and before anyone is told the data is ready.

Results go to dq/results/run_date=YYYY-MM-DD (one row per rule). Any failed critical rule
raises an error, the Glue job fails, and Step Functions sends the failure alert. The same
rules are written in AWS Glue Data Quality's DQDL in glue_dq/claims_ruleset.dqdl.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F  # noqa: E402

from common.dq import Rule, evaluate, raise_on_critical  # noqa: E402
from common.runtime import run_main, zone  # noqa: E402

ARGS = ["lake_root", "run_date", "freshness_hours"]
DEFAULTS = {"lake_root": "local/lake", "run_date": None, "freshness_hours": "48"}


def build_rules(spark, root, run_date, freshness_hours):
    read = lambda *p: spark.read.parquet(zone(root, *p))  # noqa: E731
    fact, dim = read("gold", "fact_claim"), read("gold", "dim_member")
    claims, events = read("silver", "claims"), read("silver", "claim_events")
    quarantine, loss = read("quarantine", "claims"), read("gold", "mart_loss_ratio")

    paid_events = (events.filter("event_type = 'PAID'").join(claims.select("claim_id"), "claim_id")
                   .agg(F.coalesce(F.sum("amount_paid"), F.lit(0)).alias("source")))
    paid_fact = fact.agg(F.coalesce(F.sum("amount_paid"), F.lit(0)).alias("target"))
    cutoff = F.lit(run_date).cast("timestamp") + F.expr("interval 1 day") - F.expr(f"interval {int(freshness_hours)} hours")

    return [
        Rule("fact_claim_unique_claim_id", "critical", "Each claim appears once in fact_claim",
             lambda: fact.groupBy("claim_id").count().filter("count > 1")),
        Rule("fact_claim_member_resolved", "critical", "Every claim links to a member version",
             lambda: fact.filter("member_sk is null")),
        Rule("fact_claim_paid_within_claimed", "critical", "Paid amount never exceeds claimed amount",
             lambda: fact.filter("amount_paid > amount_claimed")),
        Rule("fact_claim_valid_status", "critical", "Status is one of the lifecycle states",
             lambda: fact.filter("claim_status not in ('SUBMITTED','ADJUDICATED','PAID','DENIED')")),
        Rule("dim_member_one_current_row", "critical", "Exactly one current version per member",
             lambda: dim.groupBy("member_id").agg(F.sum(F.col("is_current").cast("int")).alias("n")).filter("n != 1")),
        Rule("dim_member_valid_ranges", "critical", "valid_from is on or before valid_to",
             lambda: dim.filter("valid_from > valid_to")),
        Rule("reconcile_claim_count", "critical", "Silver claims and fact_claim have the same row count",
             lambda: spark.createDataFrame([(claims.count(), fact.count())], "s long, t long")
                     .filter("s != t")),
        Rule("reconcile_paid_amount", "critical", "Total paid in fact_claim equals total of PAID events",
             lambda: paid_events.crossJoin(paid_fact).filter("abs(source - target) > 0.01")),
        Rule("claim_events_fresh", "warn", f"Latest claim event is less than {freshness_hours} hours old",
             lambda: events.agg(F.max("event_ts").alias("latest")).filter(F.col("latest") < cutoff)),
        Rule("quarantine_is_empty", "warn", "Records held back in quarantine (review daily)",
             lambda: quarantine),
        Rule("loss_ratio_in_expected_range", "warn", "Month-to-date loss ratio between 0 and 1.5",
             lambda: loss.filter("loss_ratio < 0 or loss_ratio > 1.5")),
    ]


def run(spark, args):
    root, run_date = args["lake_root"], args["run_date"]
    run_id = f"{run_date}T{datetime.now(timezone.utc):%H%M%S}"
    results = evaluate(spark, build_rules(spark, root, run_date, args["freshness_hours"]), run_id)
    (results.withColumn("run_date", F.lit(run_date))
     .write.mode("overwrite").partitionBy("run_date").parquet(zone(root, "dq", "results")))
    for r in results.orderBy("severity", "rule").collect():
        print(f"  {r.status:4}  {r.severity:8}  {r.rule:34}  failed_rows={r.failed_rows}")
    raise_on_critical(results)
    summary = {r.status: r["count"] for r in results.groupBy("status").count().collect()}
    return {"job": "data_quality", "run_id": run_id, **summary}


if __name__ == "__main__":
    run_main(run, ARGS, DEFAULTS)
