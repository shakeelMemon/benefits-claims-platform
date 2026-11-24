"""Gold facts and reporting marts.

fact_claim                 one row per claim; member version as of the service date; amounts and turnaround
mart_claims_monthly        claims, paid amounts and denial rate by month, benefit type and province
mart_turnaround_by_channel how fast claims are closed per channel (operations view)
mart_loss_ratio            claims paid / premium billed per employer group and month
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F  # noqa: E402

from common.runtime import run_main, zone  # noqa: E402

ARGS = ["lake_root", "run_date"]
DEFAULTS = {"lake_root": "local/lake", "run_date": None}


def build_fact_claim(claims, dim_member):
    m = dim_member.select("member_sk", "member_id", "group_id", "province", "valid_from", "valid_to")
    joined = claims.join(
        m, (claims.member_id == m.member_id) & claims.service_date.between(m.valid_from, m.valid_to), "left")
    closed_ts = F.coalesce("paid_ts", "denied_ts")
    return joined.select(
        "claim_id", "member_sk", claims.member_id, "group_id", "province", "plan_code", "provider_id",
        "benefit_type", "channel", "claim_status", "service_date",
        F.date_format("service_date", "yyyyMMdd").cast("int").alias("service_date_key"),
        F.date_format("service_date", "yyyy-MM").alias("service_month"),
        "submitted_ts", "adjudicated_ts", "paid_ts", "denied_ts", "denial_reason",
        F.col("amount_claimed").cast("decimal(12,2)").alias("amount_claimed"),
        F.col("amount_eligible").cast("decimal(12,2)").alias("amount_eligible"),
        F.coalesce(F.col("amount_paid"), F.lit(0)).cast("decimal(12,2)").alias("amount_paid"),
        F.round((F.unix_timestamp(closed_ts) - F.unix_timestamp("submitted_ts")) / 3600, 2).alias("turnaround_hours"),
        "event_count", "last_event_ts")


def build_marts(fact, plans, premiums):
    monthly = fact.groupBy("service_month", "benefit_type", "province").agg(
        F.count("*").alias("claims"),
        F.sum(F.when(F.col("claim_status") == "PAID", 1).otherwise(0)).alias("claims_paid"),
        F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).alias("claims_denied"),
        F.sum("amount_claimed").alias("amount_claimed"),
        F.sum("amount_paid").alias("amount_paid"),
    ).withColumn("denial_rate", F.round(F.col("claims_denied") / F.col("claims"), 4))

    closed = fact.filter(F.col("turnaround_hours").isNotNull())
    channel = closed.groupBy("channel").agg(
        F.count("*").alias("claims_closed"),
        F.round(F.avg("turnaround_hours"), 2).alias("avg_turnaround_hours"),
        F.percentile_approx("turnaround_hours", 0.5).alias("median_turnaround_hours"),
        F.round(F.avg(F.when(F.col("turnaround_hours") <= 24, 1).otherwise(0)), 4).alias("pct_closed_within_24h"),
    )

    paid = (fact.filter("claim_status = 'PAID'")
            .withColumn("billing_month", F.trunc(F.to_date("paid_ts"), "month"))
            .groupBy("group_id", "billing_month").agg(F.sum("amount_paid").alias("claims_paid_cad")))
    groups = plans.select("group_id", "group_name", "industry").distinct()
    loss = (premiums.join(paid, ["group_id", "billing_month"], "left").join(groups, "group_id", "left")
            .withColumn("claims_paid_cad", F.coalesce("claims_paid_cad", F.lit(0)).cast("decimal(14,2)"))
            .withColumn("loss_ratio", F.round(F.col("claims_paid_cad") / F.col("premium_cad"), 4))
            .select("group_id", "group_name", "industry", "billing_month", "premium_cad", "claims_paid_cad",
                    "loss_ratio"))
    return monthly, channel, loss


def run(spark, args):
    root = args["lake_root"]
    read = lambda *p: spark.read.parquet(zone(root, *p))  # noqa: E731
    fact = build_fact_claim(read("silver", "claims"), read("gold", "dim_member"))
    fact.write.mode("overwrite").partitionBy("service_month").parquet(zone(root, "gold", "fact_claim"))
    fact = read("gold", "fact_claim")
    monthly, channel, loss = build_marts(fact, read("silver", "plans"), read("silver", "premiums"))
    monthly.write.mode("overwrite").parquet(zone(root, "gold", "mart_claims_monthly"))
    channel.write.mode("overwrite").parquet(zone(root, "gold", "mart_turnaround_by_channel"))
    loss.write.mode("overwrite").parquet(zone(root, "gold", "mart_loss_ratio"))
    return {"job": "gold_facts", "fact_claim": fact.count(),
            "paid_cad": float(fact.agg(F.sum("amount_paid")).first()[0])}


if __name__ == "__main__":
    run_main(run, ARGS, DEFAULTS)
