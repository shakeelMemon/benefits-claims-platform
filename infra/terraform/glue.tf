# ---------------------------------------------------------------------------
# Glue: job scripts, the shared `common` package, one Glue job per pipeline step,
# a Data Catalog database for the gold tables, a crawler, and a Data Quality ruleset.
# ---------------------------------------------------------------------------

locals {
  jobs_dir = "${path.module}/../../glue_jobs"
  batch_jobs = {
    "bronze-batch"     = "job_bronze_batch.py"
    "silver"           = "job_silver.py"
    "gold-dim-member"  = "job_gold_dim_member.py"
    "gold-dimensions"  = "job_gold_dimensions.py"
    "gold-facts"       = "job_gold_facts.py"
    "data-quality"     = "job_data_quality.py"
  }
  lake_root    = "s3://${aws_s3_bucket.data.bucket}/lake"
  landing_root = "s3://${aws_s3_bucket.data.bucket}/landing"
  common_args = {
    "--lake_root"                        = local.lake_root
    "--landing_root"                     = local.landing_root
    "--extra-py-files"                   = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.common.key}"
    "--TempDir"                          = "s3://${aws_s3_bucket.artifacts.bucket}/glue-temp/"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--job-bookmark-option"              = "job-bookmark-disable" # jobs rebuild deterministically from bronze
    "--job-language"                     = "python"
  }
}

# Glue imports `common.runtime`, so the zip holds the common/ folder itself:
# zip glue_jobs/ but keep only common/** (no job scripts, no __pycache__)
data "archive_file" "common_pkg" {
  type        = "zip"
  source_dir  = local.jobs_dir
  output_path = "${path.module}/build/common_pkg.zip"
  excludes    = [for f in fileset(local.jobs_dir, "**") : f if !startswith(f, "common/") || strcontains(f, "__pycache__")]
}

resource "aws_s3_object" "common" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue/common.zip"
  source = data.archive_file.common_pkg.output_path
  etag   = data.archive_file.common_pkg.output_md5
}

resource "aws_s3_object" "scripts" {
  for_each = merge(local.batch_jobs, { "stream-claims" = "job_stream_claims_to_bronze.py" })
  bucket   = aws_s3_bucket.artifacts.id
  key      = "glue/${each.value}"
  source   = "${local.jobs_dir}/${each.value}"
  etag     = filemd5("${local.jobs_dir}/${each.value}")
}

resource "aws_glue_job" "batch" {
  for_each          = local.batch_jobs
  name              = "${var.prefix}-${each.key}"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0" # Spark 3.5, Python 3.11
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = 30 # minutes
  max_retries       = 0  # Step Functions owns retries

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.scripts[each.key].key}"
    python_version  = "3"
  }

  default_arguments = local.common_args

  execution_property { max_concurrent_runs = 1 }
}

resource "aws_glue_catalog_database" "gold" {
  name        = "${var.prefix}_gold"
  description = "Gold claims star schema and marts, queried with Athena and Power BI"
}

resource "aws_glue_crawler" "gold" {
  name          = "${var.prefix}-gold-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.gold.name
  dynamic "s3_target" {
    for_each = ["fact_claim", "dim_member", "dim_plan", "dim_provider", "dim_date",
    "mart_claims_monthly", "mart_turnaround_by_channel", "mart_loss_ratio"]
    content { path = "${local.lake_root}/gold/${s3_target.value}/" }
  }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
  configuration = jsonencode({
    Version  = 1.0
    Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
  })
}

resource "aws_glue_data_quality_ruleset" "fact_claim" {
  name        = "${var.prefix}-fact-claim-rules"
  description = "Same rules as job_data_quality.py, in Glue Data Quality's DQDL"
  ruleset     = file("${path.module}/../../glue_dq/claims_ruleset.dqdl")
}
