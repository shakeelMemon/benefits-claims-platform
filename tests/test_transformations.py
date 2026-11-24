"""Unit tests for the transformation logic. Each test builds a few rows by hand,
so the expected answer is obvious from reading the test."""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

import job_gold_dim_member as dim_member
import job_silver as silver
import job_stream_claims_to_bronze as stream
from common import dq

TS = datetime(2026, 10, 20, 9, 0, 0)


def events_df(spark, rows):
    cols = ("event_id string, event_type string, event_ts timestamp, claim_id string, member_id string, "
            "plan_code string, provider_id string, benefit_type string, service_date string, channel string, "
            "amount_claimed double, amount_eligible double, amount_paid double, denial_reason string, "
            "_ingest_ts timestamp, _source_position string")
    full = [(r["id"], r["type"], r.get("ts", TS), r.get("claim", "C1"), r.get("member", "M1"), "PL1", "P1", "DRUG",
             "2026-10-18", "MOBILE_APP", r.get("claimed", 100.0), r.get("eligible"), r.get("paid"), None,
             r.get("ingest", TS), r.get("pos", "f1")) for r in rows]
    return spark.createDataFrame(full, cols)


def test_parse_flags_malformed_json(spark):
    raw = spark.createDataFrame(
        [('{"event_id":"e1","event_type":"SUBMITTED","event_ts":"2026-10-20T09:00:00Z","claim_id":"C1"}', "f"),
         ('{"event_id": "broken", "claim_id": ', "f")], "value string, _source_position string")
    out = {r["value"][:20]: r["_is_malformed"] for r in stream.parse_events(raw).collect()}
    assert list(out.values()) == [False, True]


def test_duplicate_events_are_dropped_and_kept_aside(spark):
    df = events_df(spark, [{"id": "e1", "type": "SUBMITTED"},
                           {"id": "e1", "type": "SUBMITTED", "ingest": datetime(2026, 10, 20, 10)}])
    kept, dups = silver.dedupe_events(df)
    assert kept.count() == 1 and dups.count() == 1
    assert kept.first()["_ingest_ts"] == TS          # the first delivery wins


def test_bad_events_go_to_quarantine_with_a_reason(spark):
    df = events_df(spark, [
        {"id": "e1", "type": "SUBMITTED"},
        {"id": "e2", "type": "REOPENED"},
        {"id": "e3", "type": "SUBMITTED", "claimed": -25.0, "claim": "C2"},
        {"id": "e4", "type": "SUBMITTED", "member": "M_UNKNOWN", "claim": "C3"},
    ])
    members = spark.createDataFrame([("M1",)], "member_id string")
    valid, rejected = silver.split_valid_events(df, members)
    assert [r.event_id for r in valid.collect()] == ["e1"]
    reasons = {r.event_id: r.reason for r in rejected.collect()}
    assert reasons == {"e2": "UNKNOWN_EVENT_TYPE", "e3": "NEGATIVE_AMOUNT", "e4": "UNKNOWN_MEMBER"}


def test_claim_status_follows_lifecycle_even_when_events_arrive_out_of_order(spark):
    df = events_df(spark, [
        {"id": "e3", "type": "PAID", "ts": datetime(2026, 10, 20, 12), "eligible": 100.0, "paid": 90.0},
        {"id": "e1", "type": "SUBMITTED", "ts": datetime(2026, 10, 20, 9)},
        {"id": "e2", "type": "ADJUDICATED", "ts": datetime(2026, 10, 20, 10), "eligible": 100.0},
    ])
    claims, bad = silver.build_claims(df)
    c = claims.first()
    assert bad.count() == 0
    assert (c.claim_status, c.amount_claimed, c.amount_eligible, c.amount_paid) == ("PAID", 100.0, 100.0, 90.0)
    assert c.submitted_ts == datetime(2026, 10, 20, 9)


def test_claim_paid_above_claimed_is_quarantined(spark):
    df = events_df(spark, [{"id": "e1", "type": "SUBMITTED"},
                           {"id": "e2", "type": "PAID", "eligible": 100.0, "paid": 150.0}])
    claims, bad = silver.build_claims(df)
    assert claims.count() == 0 and bad.first().reason == "PAID_EXCEEDS_CLAIMED"


def test_members_lose_pii_but_keep_age_band_and_fsa(spark):
    bronze = spark.createDataFrame(
        [("m1", "G1", "Aisha", "Khan", "123-456-789", "1990-05-01", "on", "M5V 2T6", "PL1", "FAMILY", "ACTIVE",
          "2024-01-01", date(2026, 10, 20), TS)],
        "member_id string, group_id string, first_name string, last_name string, sin string, date_of_birth string, "
        "province string, postal_code string, plan_code string, coverage_tier string, member_status string, "
        "coverage_effective_date string, _extract_date date, _ingest_ts timestamp")
    row = silver.build_members(bronze).first().asDict()
    for pii in ("first_name", "last_name", "sin", "date_of_birth", "postal_code"):
        assert pii not in row
    assert (row["member_id"], row["province"], row["postal_fsa"], row["age_band"]) == ("M1", "ON", "M5V", "30-39")


def snapshots(spark, rows):
    return spark.createDataFrame(
        [(m, "G1", plan, "FAMILY", "ACTIVE", prov, "M5V", "30-39", date(2024, 1, 1), d) for m, plan, prov, d in rows],
        "member_id string, group_id string, plan_code string, coverage_tier string, member_status string, "
        "province string, postal_fsa string, age_band string, coverage_effective_date date, extract_date date")


def test_scd2_opens_a_new_version_only_when_something_changes(spark):
    snaps = snapshots(spark, [
        ("M1", "STD", "ON", date(2026, 10, 20)),
        ("M1", "STD", "ON", date(2026, 10, 21)),     # no change: no new version
        ("M1", "ENH", "ON", date(2026, 10, 22)),     # plan upgrade: version 2
        ("M2", "STD", "QC", date(2026, 10, 20)),
    ])
    rows = {(r.member_id, r.version): r for r in dim_member.build_scd2(snaps).collect()}
    assert set(rows) == {("M1", 1), ("M1", 2), ("M2", 1)}
    v1, v2 = rows[("M1", 1)], rows[("M1", 2)]
    assert (v1.valid_from, v1.valid_to, v1.is_current) == (date(2024, 1, 1), date(2026, 10, 21), False)
    assert (v2.valid_from, v2.valid_to, v2.is_current) == (date(2026, 10, 22), date(9999, 12, 31), True)
    assert v2.plan_code == "ENH"


def test_scd2_is_the_same_when_rebuilt(spark):
    snaps = snapshots(spark, [("M1", "STD", "ON", date(2026, 10, 20)), ("M1", "STD", "BC", date(2026, 10, 21))])
    first = sorted(r.member_sk for r in dim_member.build_scd2(snaps).collect())
    second = sorted(r.member_sk for r in dim_member.build_scd2(snaps).collect())
    assert first == second and len(first) == 2


def test_critical_dq_failure_stops_the_pipeline(spark):
    bad = spark.createDataFrame([(1,)], "x int")
    good = spark.createDataFrame([], "x int")
    results = dq.evaluate(spark, [dq.Rule("ok", "critical", "", lambda: good),
                                  dq.Rule("only_warn", "warn", "", lambda: bad)], "r1")
    dq.raise_on_critical(results)                    # warnings don't stop the run
    results = dq.evaluate(spark, [dq.Rule("broken", "critical", "", lambda: bad)], "r2")
    with pytest.raises(dq.DataQualityError, match="broken"):
        dq.raise_on_critical(results)
    assert results.filter(F.col("status") == "FAIL").count() == 1
