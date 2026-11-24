"""Schemas shared by the jobs. The claim event schema is the contract for the Kafka topic."""

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CLAIM_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),          # SUBMITTED | ADJUDICATED | PAID | DENIED
    StructField("event_ts", StringType()),            # ISO-8601 UTC
    StructField("claim_id", StringType()),
    StructField("member_id", StringType()),
    StructField("plan_code", StringType()),
    StructField("provider_id", StringType()),
    StructField("benefit_type", StringType()),        # DRUG | DENTAL | PARAMEDICAL | VISION | HEALTH
    StructField("service_date", StringType()),
    StructField("channel", StringType()),             # MOBILE_APP | PROVIDER_EDI | PAPER
    StructField("currency", StringType()),
    StructField("amount_claimed", DoubleType()),
    StructField("amount_eligible", DoubleType()),
    StructField("amount_paid", DoubleType()),
    StructField("denial_reason", StringType()),
])

EVENT_TYPES = ["SUBMITTED", "ADJUDICATED", "PAID", "DENIED"]

# Batch extracts: entity -> (landing sub-folder, file pattern)
BATCH_SOURCES = {
    "members": ("policy_admin", "members_*.csv"),
    "plans": ("reference", "plans.csv"),
    "providers": ("reference", "providers.csv"),
    "premiums": ("finance", "premiums_*.csv"),
}
