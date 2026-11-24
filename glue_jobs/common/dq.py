"""Small data-quality framework: each rule is a query that returns the failing rows.

A rule passes when it finds zero failing rows. Severity decides what happens on failure:
  critical -> the job fails, Step Functions catches it and alerts; bad data never reaches reports
  warn     -> recorded in the results table and shown, but the run continues
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import DataFrame, SparkSession


@dataclass
class Rule:
    name: str
    severity: str                       # "critical" or "warn"
    description: str
    failing_rows: Callable[[], DataFrame]


class DataQualityError(Exception):
    pass


def evaluate(spark: SparkSession, rules: list[Rule], run_id: str) -> DataFrame:
    checked_at = datetime.now(timezone.utc).replace(microsecond=0)
    rows = []
    for rule in rules:
        failed = rule.failing_rows().count()
        rows.append((run_id, checked_at, rule.name, rule.severity, rule.description, failed,
                     "PASS" if failed == 0 else "FAIL"))
    return spark.createDataFrame(
        rows, "run_id string, checked_at timestamp, rule string, severity string, description string, "
              "failed_rows long, status string")


def raise_on_critical(results: DataFrame):
    bad = results.filter("status = 'FAIL' and severity = 'critical'").collect()
    if bad:
        detail = "; ".join(f"{r.rule} ({r.failed_rows} rows)" for r in bad)
        raise DataQualityError(f"Critical data-quality rules failed: {detail}")
