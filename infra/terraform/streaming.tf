# ---------------------------------------------------------------------------
# OPTIONAL streaming path: Amazon MSK (Kafka) + an always-on Glue streaming job.
# Only created when enable_streaming = true. Both bill by the hour, so destroy them
# (terraform apply -var enable_streaming=false) as soon as the demo is over.
# ---------------------------------------------------------------------------

resource "aws_security_group" "kafka" {
  count       = var.enable_streaming ? 1 : 0
  name        = "${var.prefix}-kafka"
  description = "MSK brokers and the Glue streaming job"
  vpc_id      = var.vpc_id

  ingress {
    description = "Kafka from members of this group (Glue workers)"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    self        = true
  }
  ingress {
    description = "Glue workers talk to each other on all ports"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_cluster" "claims" {
  count                  = var.enable_streaming ? 1 : 0
  cluster_name           = "${var.prefix}-events"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = length(var.subnet_ids)

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.kafka[0].id]
    storage_info {
      ebs_storage_info { volume_size = 20 }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.data.arn
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT" # plaintext inside the private VPC for the demo; use TLS + IAM auth in production
      in_cluster    = true
    }
  }

  client_authentication {
    unauthenticated = true
  }
}

resource "aws_glue_connection" "kafka" {
  count           = var.enable_streaming ? 1 : 0
  name            = "${var.prefix}-kafka"
  connection_type = "NETWORK"
  physical_connection_requirements {
    subnet_id              = var.subnet_ids[0]
    security_group_id_list = [aws_security_group.kafka[0].id]
    availability_zone      = data.aws_subnet.first[0].availability_zone
  }
}

data "aws_subnet" "first" {
  count = var.enable_streaming ? 1 : 0
  id    = var.subnet_ids[0]
}

# The claim-event ingest job always exists.
#   enable_streaming = false: a normal Glue job that reads event files from landing/stream/ and stops
#                             (run it before the nightly pipeline, or put files there from any producer)
#   enable_streaming = true:  an always-on Glue streaming job reading the MSK topic, one micro-batch a minute
resource "aws_glue_job" "stream" {
  name              = "${var.prefix}-stream-claims"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  connections       = var.enable_streaming ? [aws_glue_connection.kafka[0].name] : []

  command {
    name            = var.enable_streaming ? "gluestreaming" : "glueetl"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.scripts["stream-claims"].key}"
    python_version  = "3"
  }

  default_arguments = merge(local.common_args, var.enable_streaming ? {
    "--source"          = "kafka"
    "--kafka_bootstrap" = aws_msk_cluster.claims[0].bootstrap_brokers
    "--kafka_topic"     = "claims.events"
    "--trigger_seconds" = "60"
    } : {
    "--source"          = "files"
    "--stream_input"    = "${local.landing_root}/stream/"
    "--trigger_seconds" = "0"
  })
}
