# ---------------------------------------------------------------------------
# Encryption key and buckets
#   data bucket:      landing/ (raw files as received) and lake/ (bronze, silver, gold, quarantine, dq, audit)
#   artifacts bucket: Glue scripts, Glue temp files, Athena query results
# ---------------------------------------------------------------------------

resource "aws_kms_key" "data" {
  description             = "${var.prefix} claims data encryption key"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "data" {
  name          = "alias/${var.prefix}-data"
  target_key_id = aws_kms_key.data.key_id
}

locals {
  account     = data.aws_caller_identity.current.account_id
  data_bucket = "${var.prefix}-data-${local.account}-${var.environment}"
  art_bucket  = "${var.prefix}-artifacts-${local.account}-${var.environment}"
}

resource "aws_s3_bucket" "data" {
  bucket        = local.data_bucket
  force_destroy = var.environment != "prod" # lets `terraform destroy` empty a dev bucket; never in prod
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = local.art_bucket
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "buckets" {
  for_each = { data = aws_s3_bucket.data.id, artifacts = aws_s3_bucket.artifacts.id }
  bucket   = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "buckets" {
  for_each                = { data = aws_s3_bucket.data.id, artifacts = aws_s3_bucket.artifacts.id }
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Refuse any request that isn't over HTTPS
resource "aws_s3_bucket_policy" "tls_only" {
  for_each = { data = aws_s3_bucket.data, artifacts = aws_s3_bucket.artifacts }
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.buckets]
}

# Raw files move to cheaper storage after 30 days; old object versions are cleaned up after 90
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    id     = "landing-to-glacier-ir"
    status = "Enabled"
    filter { prefix = "landing/" }
    transition {
      days          = 30
      storage_class = "GLACIER_IR"
    }
  }
  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = 90 }
  }
}
