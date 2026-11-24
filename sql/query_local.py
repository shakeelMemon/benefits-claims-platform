"""Run the Athena business questions locally with DuckDB over the gold Parquet files,
then show the quarantine, data-quality results and load audit.

Usage:  python sql/query_local.py
"""

import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
LAKE = ROOT / "local" / "lake"
GOLD_TABLES = ["fact_claim", "dim_member", "dim_plan", "dim_provider", "dim_date",
               "mart_claims_monthly", "mart_turnaround_by_channel", "mart_loss_ratio"]


def main():
    con = duckdb.connect()
    for t in GOLD_TABLES:
        con.execute(f"create view {t} as select * from read_parquet('{LAKE / 'gold' / t}/**/*.parquet', "
                    f"hive_partitioning = true)")
    for name, path in {"quarantine": "quarantine/claims", "malformed": "quarantine/claim_events_malformed",
                       "dq_results": "dq/results", "load_audit": "audit/load_audit"}.items():
        con.execute(f"create view {name} as select * from read_parquet('{LAKE / path}/**/*.parquet', "
                    f"hive_partitioning = true)")

    sql = (ROOT / "sql" / "athena" / "business_questions.sql").read_text()
    for block in re.split(r"\n(?=-- \d+\.)", sql):
        title = re.search(r"-- (\d+\..*)", block)
        query = "\n".join(line for line in block.splitlines() if not line.strip().startswith("--")).strip()
        if title and query:
            print(f"\n{title.group(1)}")
            con.sql(query.rstrip(";")).show(max_width=250)

    print("\nQuarantined records by reason")
    print(con.sql("select reason, record_level, count(*) as records from quarantine group by all "
                  "union all select reason, 'raw line', count(*) from malformed group by all order by records desc"))
    print("\nLatest data-quality run")
    print(con.sql("select rule, severity, status, failed_rows from dq_results "
                  "where run_date = (select max(run_date) from dq_results) order by status, severity, rule"))
    print("\nLoad audit")
    print(con.sql("select run_date, entity, rows_read, loaded_at from load_audit order by run_date, entity"))


if __name__ == "__main__":
    main()
