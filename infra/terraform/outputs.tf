output "data_bucket" {
  description = "Upload daily extracts to s3://<this>/landing/..."
  value       = aws_s3_bucket.data.bucket
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "glue_jobs" {
  value = [for j in aws_glue_job.batch : j.name]
}

output "athena_workgroup" {
  value = aws_athena_workgroup.analytics.name
}

output "gold_database" {
  value = aws_glue_catalog_database.gold.name
}

output "kafka_bootstrap_brokers" {
  value = var.enable_streaming ? aws_msk_cluster.claims[0].bootstrap_brokers : "streaming disabled"
}

output "stream_job" {
  value = aws_glue_job.stream.name
}
