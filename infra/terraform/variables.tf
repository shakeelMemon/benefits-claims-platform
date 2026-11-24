variable "region" {
  description = "AWS region. ca-central-1 keeps Canadian member data in Canada."
  type        = string
  default     = "ca-central-1"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "prefix" {
  description = "Name prefix for every resource; also the Glue job name prefix used in the state machine"
  type        = string
  default     = "claims"
}

variable "alert_email" {
  description = "Email address that receives pipeline success and failure alerts (confirm the subscription email)"
  type        = string
}

variable "schedule_expression" {
  description = "When the nightly pipeline runs (EventBridge Scheduler, Toronto time)"
  type        = string
  default     = "cron(0 2 * * ? *)"
}

variable "glue_workers" {
  description = "G.1X workers per batch job (2 is the minimum and plenty for this data)"
  type        = number
  default     = 2
}

# ----- optional streaming path (Amazon MSK + Glue streaming job). Costs money while running. -----

variable "enable_streaming" {
  description = "Create an MSK cluster and the always-on Glue streaming job. Leave false unless you are demoing it."
  type        = bool
  default     = false
}

variable "subnet_ids" {
  description = "Private subnets (2+ AZs) for MSK and the Glue Kafka connection; only used when enable_streaming = true"
  type        = list(string)
  default     = []
}

variable "vpc_id" {
  description = "VPC of the subnets; only used when enable_streaming = true"
  type        = string
  default     = ""
}

variable "nightly_schedule_enabled" {
  description = "Run the nightly pipeline at 02:00. Turn off when no daily source files arrive (e.g. a demo account), so it doesn't fail and alert every night."
  type        = bool
  default     = true
}
