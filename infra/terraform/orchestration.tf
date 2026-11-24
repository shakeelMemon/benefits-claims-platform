# ---------------------------------------------------------------------------
# Step Functions state machine (same JSON the local runner executes),
# nightly schedule, alerts, and an alarm if executions fail.
# ---------------------------------------------------------------------------

# Alerts carry only run dates and error text, never member data. Left unencrypted on purpose:
# CloudWatch alarms can't publish to a topic encrypted with the AWS-managed SNS key.
resource "aws_sns_topic" "alerts" {
  name = "${var.prefix}-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${var.prefix}-pipeline"
  retention_in_days = 30
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.prefix}-nightly-pipeline"
  role_arn = aws_iam_role.sfn.arn
  definition = templatefile("${path.module}/../../orchestration/claims_pipeline.asl.json", {
    job_prefix    = var.prefix
    sns_topic_arn = aws_sns_topic.alerts.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = false # executions carry no member data, but keep logs lean
    level                  = "ERROR"
  }
}

resource "aws_scheduler_schedule" "nightly" {
  name                         = "${var.prefix}-nightly"
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "America/Toronto"
  state                        = var.nightly_schedule_enabled ? "ENABLED" : "DISABLED"
  flexible_time_window { mode = "OFF" }

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({}) # no run_date: the state machine uses the execution date
  }
}

resource "aws_cloudwatch_metric_alarm" "pipeline_failed" {
  alarm_name          = "${var.prefix}-pipeline-failed"
  alarm_description   = "The nightly claims pipeline failed, timed out or was aborted"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.pipeline.arn }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# Athena workgroup for querying gold (results encrypted, per-query scan limit to control cost)
resource "aws_athena_workgroup" "analytics" {
  name          = "${var.prefix}-analytics"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 1073741824 # 1 GB
    result_configuration {
      output_location = "s3://${aws_s3_bucket.artifacts.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.data.arn
      }
    }
  }
}
