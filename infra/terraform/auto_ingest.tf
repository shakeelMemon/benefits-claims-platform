# ---------------------------------------------------------------------------
# Automatic claim-event ingest: a new file in landing/stream/ loads itself into bronze.
#
#   sink uploads claim_events_*.jsonl  ->  S3 "Object Created" event  ->  EventBridge rule
#   ->  Step Functions "claims-stream-ingest"  ->  Glue job claims-stream-claims (file mode)
#
# Bronze is updated within a couple of minutes of a claim changing in BenefitHub.
# Silver and gold stay on the nightly pipeline (claims-nightly-pipeline).
#
# Several files arriving close together each start an execution. The Glue job allows one run
# at a time, so the others retry until it is free; each run reads every file it hasn't seen
# (checkpoint), so a later run may find nothing new, which is fine.
# ---------------------------------------------------------------------------

# 1. Send this bucket's S3 events to EventBridge
resource "aws_s3_bucket_notification" "data_to_eventbridge" {
  bucket      = aws_s3_bucket.data.id
  eventbridge = true
}

# 2. Match new objects under landing/stream/
resource "aws_cloudwatch_event_rule" "stream_file_arrived" {
  name        = "${var.prefix}-stream-file-arrived"
  description = "A claim-events file landed in s3://${aws_s3_bucket.data.bucket}/landing/stream/"
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.data.bucket] }
      object = { key = [{ prefix = "landing/stream/" }] }
    }
  })
}

# 3. A small state machine that runs the stream job and alerts on failure
resource "aws_sfn_state_machine" "stream_ingest" {
  name     = "${var.prefix}-stream-ingest"
  role_arn = aws_iam_role.stream_ingest_sfn.arn
  definition = jsonencode({
    Comment = "Load newly landed claim-event files into bronze (triggered by S3 via EventBridge)"
    StartAt = "LoadClaimEvents"
    States = {
      LoadClaimEvents = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.stream.name
        }
        ResultPath = null
        Retry = [
          {
            # another run is still going: wait and try again
            ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
            IntervalSeconds = 30
            MaxAttempts     = 10
            BackoffRate     = 1.5
          },
          {
            ErrorEquals     = ["Glue.ThrottlingException", "Glue.InternalServiceException"]
            IntervalSeconds = 30
            MaxAttempts     = 3
            BackoffRate     = 2
          }
        ]
        Catch = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
        Next  = "Done"
      }
      Done = { Type = "Succeed" }
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn      = aws_sns_topic.alerts.arn
          Subject       = "Claim event ingest FAILED"
          "Message.$" = "States.Format('Loading claim events into bronze failed for {}. Error: {}. Cause: {}', $.detail.object.key, $.error.Error, $.error.Cause)"
        }
        ResultPath = null
        Next       = "IngestFailed"
      }
      IngestFailed = { Type = "Fail", Error = "IngestFailed", Cause = "See the SNS alert and the Glue job logs." }
    }
  })
}

# 4. EventBridge starts the state machine for each matching file
resource "aws_cloudwatch_event_target" "start_stream_ingest" {
  rule     = aws_cloudwatch_event_rule.stream_file_arrived.name
  arn      = aws_sfn_state_machine.stream_ingest.arn
  role_arn = aws_iam_role.events_start_ingest.arn
}

# ----- IAM -----

resource "aws_iam_role" "stream_ingest_sfn" {
  name = "${var.prefix}-stream-ingest-sfn-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "states.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy" "stream_ingest_sfn" {
  name = "${var.prefix}-stream-ingest-permissions"
  role = aws_iam_role.stream_ingest_sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "RunStreamJob"
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = [aws_glue_job.stream.arn]
      },
      {
        Sid      = "SendAlerts"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.alerts.arn]
      }
    ]
  })
}

resource "aws_iam_role" "events_start_ingest" {
  name = "${var.prefix}-events-start-ingest-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "events.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy" "events_start_ingest" {
  name = "${var.prefix}-start-stream-ingest"
  role = aws_iam_role.events_start_ingest.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.stream_ingest.arn }]
  })
}

output "stream_ingest_state_machine_arn" {
  value = aws_sfn_state_machine.stream_ingest.arn
}
