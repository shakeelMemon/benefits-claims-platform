"""Silver: clean, typed, de-duplicated and PII-free data, plus a quarantine for bad records.

Members:   one row per member per extract date, types fixed, PII removed (names, SIN, birth
           date and full postal code are dropped; age band and postal FSA are kept).
Reference: latest plans and providers; latest premium per group and month.
Claims:    1. drop duplicate events (same event_id, Kafka at-least-once delivery)
           2. quarantine events that break a rule (unknown type, missing claim id, negative
              amount, member not on file)
           3. roll the remaining events up to one row per claim with its current status;
              status follows the claim lifecycle, so out-of-order events can't move a paid
              claim back to "submitted"
           4. quarantine claims paid for more than was claimed

Silver is rebuilt from the full bronze history every run, so the result is the same no matter
how many times a day is rerun or in what order events arrived.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.pii import age_band, drop_pii, postal_fsa  # noqa: E402
from common.runtime import run_main, zone  # noqa: E402
from common.schemas import EVENT_TYPES  # noqa: E402

ARGS = ["lake_root", "run_date"]
DEFAULTS = {"lake_root": "local/lake", "run_date": None}


def latest(df, keys, order_col):
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_members(bronze_members):
    typed = (bronze_members
             .withColumn("date_of_birth", F.to_date("date_of_birth"))
             .withColumn("coverage_effective_date", F.to_date("coverage_effective_date"))
             .withColumn("extract_date", F.col("_extract_date"))
             .withColumn("member_id", F.upper(F.trim("member_id")))
             .withColumn("province", F.upper(F.trim("province"))))
    typed = latest(typed, ["member_id", "extract_date"], "_ingest_ts")
    enriched = (typed
                .withColumn("age_band", age_band(F.col("date_of_birth"), F.col("extract_date")))
                .withColumn("postal_fsa", postal_fsa(F.col("postal_code"))))
    return drop_pii(enriched).select(
        "member_id", "group_id", "plan_code", "coverage_tier", "member_status", "province", "postal_fsa",
        "age_band", "coverage_effective_date", "extract_date")


def dedupe_events(bronze_events):
    w = Window.partitionBy("event_id").orderBy("_ingest_ts", "_source_position")
    ranked = bronze_events.withColumn("_rn", F.row_number().over(w))
    return ranked.filter("_rn = 1").drop("_rn"), ranked.filter("_rn > 1").drop("_rn")


def split_valid_events(events, known_members):
    known = known_members.select("member_id").distinct().withColumn("_known", F.lit(True))
    checked = (events.join(known, "member_id", "left")
               .withColumn("reason",
                           F.when(F.col("claim_id").isNull(), "MISSING_CLAIM_ID")
                            .when(~F.col("event_type").isin(EVENT_TYPES), "UNKNOWN_EVENT_TYPE")
                            .when(F.col("amount_claimed") < 0, "NEGATIVE_AMOUNT")
                            .when(F.col("_known").isNull(), "UNKNOWN_MEMBER"))
               .drop("_known"))
    return checked.filter("reason is null").drop("reason"), checked.filter("reason is not null")


def build_claims(events):
    def first_of(event_type, col):
        return F.min(F.when(F.col("event_type") == event_type, F.col(col)))

    claims = events.groupBy("claim_id").agg(
        F.first("member_id", ignorenulls=True).alias("member_id"),
        F.first("plan_code", ignorenulls=True).alias("plan_code"),
        F.first("provider_id", ignorenulls=True).alias("provider_id"),
        F.first("benefit_type", ignorenulls=True).alias("benefit_type"),
        F.first("channel", ignorenulls=True).alias("channel"),
        F.to_date(F.first("service_date", ignorenulls=True)).alias("service_date"),
        first_of("SUBMITTED", "event_ts").alias("submitted_ts"),
        first_of("ADJUDICATED", "event_ts").alias("adjudicated_ts"),
        first_of("PAID", "event_ts").alias("paid_ts"),
        first_of("DENIED", "event_ts").alias("denied_ts"),
        F.max("amount_claimed").alias("amount_claimed"),
        F.max(F.when(F.col("event_type").isin("ADJUDICATED", "PAID"), F.col("amount_eligible"))).alias("amount_eligible"),
        F.max(F.when(F.col("event_type") == "PAID", F.col("amount_paid"))).alias("amount_paid"),
        F.first("denial_reason", ignorenulls=True).alias("denial_reason"),
        F.max("event_ts").alias("last_event_ts"),
        F.count("*").alias("event_count"),
    )
    status = (F.when(F.col("paid_ts").isNotNull(), "PAID")
               .when(F.col("denied_ts").isNotNull(), "DENIED")
               .when(F.col("adjudicated_ts").isNotNull(), "ADJUDICATED")
               .otherwise("SUBMITTED"))
    claims = claims.withColumn("claim_status", status)
    reason = (F.when(F.col("amount_paid") > F.col("amount_claimed") + 0.005, "PAID_EXCEEDS_CLAIMED")
               .when(F.col("submitted_ts").isNull(), "MISSING_SUBMISSION"))
    claims = claims.withColumn("reason", reason)
    return claims.filter("reason is null").drop("reason"), claims.filter("reason is not null")


def run(spark, args):
    root = args["lake_root"]
    bronze = lambda name: spark.read.parquet(zone(root, "bronze", name))  # noqa: E731

    members = build_members(bronze("members"))
    members.write.mode("overwrite").parquet(zone(root, "silver", "member_snapshots"))

    last_run = F.col("run_date") == bronze("plans").agg(F.max("run_date")).first()[0]
    plans = bronze("plans").filter(last_run).select("plan_code", "plan_name", "group_id", "group_name", "industry")
    plans.write.mode("overwrite").parquet(zone(root, "silver", "plans"))
    last_run = F.col("run_date") == bronze("providers").agg(F.max("run_date")).first()[0]
    providers = bronze("providers").filter(last_run).select(
        "provider_id", "provider_name", "provider_type", "benefit_type", "province")
    providers.write.mode("overwrite").parquet(zone(root, "silver", "providers"))
    premiums = latest(bronze("premiums").withColumn("billing_month", F.to_date("billing_month")),
                      ["group_id", "billing_month"], "_extract_date")
    (premiums.select("group_id", "billing_month", F.col("premium_cad").cast("decimal(14,2)").alias("premium_cad"))
     .write.mode("overwrite").parquet(zone(root, "silver", "premiums")))

    events, duplicates = dedupe_events(bronze("claim_events"))
    valid, rejected = split_valid_events(events, members)
    valid.drop("_batch_id").write.mode("overwrite").partitionBy("event_date").parquet(
        zone(root, "silver", "claim_events"))
    claims, bad_claims = build_claims(valid)
    claims.write.mode("overwrite").parquet(zone(root, "silver", "claims"))

    quarantine = (rejected.select("event_id", "claim_id", "member_id", "event_type", "event_ts",
                                  "amount_claimed", "amount_paid", "reason", "_source_position")
                  .unionByName(duplicates.select("event_id", "claim_id", "member_id", "event_type", "event_ts",
                                                 "amount_claimed", "amount_paid", "_source_position")
                               .withColumn("reason", F.lit("DUPLICATE_EVENT")))
                  .withColumn("record_level", F.lit("event")))
    quarantine = quarantine.unionByName(
        bad_claims.select("claim_id", "member_id", "amount_claimed", "amount_paid", "reason")
        .withColumn("event_id", F.lit(None).cast("string"))
        .withColumn("event_type", F.lit(None).cast("string"))
        .withColumn("event_ts", F.lit(None).cast("timestamp"))
        .withColumn("_source_position", F.lit(None).cast("string"))
        .withColumn("record_level", F.lit("claim")))
    quarantine.write.mode("overwrite").parquet(zone(root, "quarantine", "claims"))

    counts = {r["reason"]: r["count"] for r in
              spark.read.parquet(zone(root, "quarantine", "claims")).groupBy("reason").count().collect()}
    return {"job": "silver", "members": members.count(),
            "claims": spark.read.parquet(zone(root, "silver", "claims")).count(), "quarantined": counts}


if __name__ == "__main__":
    run_main(run, ARGS, DEFAULTS)
