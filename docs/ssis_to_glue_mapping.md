# From SSIS to AWS Glue: how the member extract load maps across

Many insurers still load daily extracts with SSIS packages scheduled by SQL Server Agent. `job_bronze_batch.py` and `job_silver.py` do the same work as a typical "Load member extract" package, on AWS Glue. This table maps each SSIS piece to where it lives in this project.

| SSIS piece | What it does in the package | Equivalent here |
|---|---|---|
| Package parameters / project parameters | Run date, folder paths, connection strings | Glue job arguments (`--run_date`, `--lake_root`, `--landing_root`), set by Step Functions and Terraform |
| Connection managers | Source folder, target database | S3 paths; credentials come from the Glue job's IAM role, not stored passwords |
| Foreach Loop + File System Task | Find today's file, fail or archive | `files_for()` checks the expected file exists and fails the job if it doesn't; S3 lifecycle moves old landing files to Glacier |
| Flat File Source | Read the CSV | `spark.read.csv(..., header=True)`, every column as text |
| Data Conversion | Fix data types | `to_date`, `cast(...)` in silver |
| Derived Column | Add audit columns, clean values | `withColumn` (`_source_file`, `_ingest_ts`, `upper(trim(...))`) |
| Lookup (no match output) | Find members that don't exist | Left join to members; unmatched rows go to quarantine with reason `UNKNOWN_MEMBER` |
| Conditional Split | Route bad rows away | `split_valid_events()` sends rule-breaking rows to `quarantine/` with a reason column |
| Sort + remove duplicates | De-duplicate | `row_number()` over `event_id` / `(member_id, extract_date)` |
| Slowly Changing Dimension wizard | Type 2 history on members | `job_gold_dim_member.py` (hash of tracked columns, `valid_from` / `valid_to` / `is_current`) |
| Row Count + Execute SQL Task (audit insert) | Load-audit table | `audit/load_audit` written by the bronze job |
| OLE DB Destination | Write to the warehouse table | Parquet in S3 (`bronze/`, `silver/`, `gold/`), registered in the Glue Data Catalog for Athena |
| Event handlers (OnError) + Send Mail Task | Alert on failure | Step Functions `Catch` → `NotifyFailure` (SNS email), plus a CloudWatch alarm |
| SQL Server Agent job + schedule | Nightly run, step order | EventBridge Scheduler (02:00 Toronto) → Step Functions state machine |
| Package checkpoints / restartability | Rerun from failure | Each job overwrites only its own partition or table, so rerunning a day is safe |

## How I would migrate a real SSIS estate

1. **Inventory** every package: sources, targets, schedule, row volumes, owners. Rank by business value and complexity.
2. **Freeze the contract**: capture current output tables and row counts as the baseline.
3. **Rebuild the logic** in Glue (PySpark), one package at a time, reusing a common framework (audit columns, quarantine, data-quality rules) instead of copying each package's quirks.
4. **Run in parallel** with SSIS for a few cycles and **reconcile** outputs: row counts, sums of amounts, and key-level differences.
5. **Cut over** when reconciliation is clean for an agreed number of runs; keep SSIS read-only for a rollback window, then retire it.

The same approach applies to Informatica or Talend mappings: source qualifier → read, expression → `withColumn`, router → filter/quarantine, lookup → join, target → write.
