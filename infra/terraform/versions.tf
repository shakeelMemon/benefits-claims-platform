terraform {
  required_version = ">= 1.6"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.80" }
    archive = { source = "hashicorp/archive", version = "~> 2.6" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project     = "benefits-claims-platform"
      environment = var.environment
      managed_by  = "terraform"
      data_class  = "confidential" # claims and member data
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
