# terraform/main.tf
#
# AN ACKNOWLEDGED STUB. Its only job is to make `terraform validate` succeed in CI
# job 4, so the pipeline shape is honest about where infrastructure-as-code would
# sit without pretending it exists.
#
# What a real version of this file would manage: the MSK or Kafka cluster and its
# topic configs, the S3 buckets for the Delta lake and the state snapshots with
# their lifecycle rules, the IAM roles the Spark job assumes, and the EMR/EKS
# submission. None of that is meaningful against Docker Compose on one box, and
# writing it would be fiction rather than evidence.
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }
}

variable "lake_bucket" {
  description = "Delta lake bucket. MinIO locally; S3 in production."
  type        = string
  default     = "balance-lake"
}

variable "snapshot_bucket" {
  description = "Immutable state snapshots. 7-day retention in production."
  type        = string
  default     = "state-snapshots"
}

variable "spark_version_tag" {
  description = "Pins the checkpoint path. Bumping this is an upgrade — see runbooks/spark_upgrade.md."
  type        = string
  default     = "v3.5.1"
}

# Renders the paths the job actually uses, so a plan diff shows a checkpoint-path
# change as an explicit, reviewable event rather than a config edit nobody noticed.
resource "local_file" "resolved_paths" {
  filename = "${path.module}/resolved_paths.json"
  content = jsonencode({
    balances        = "s3a://${var.lake_bucket}/balances"
    integrity       = "s3a://${var.lake_bucket}/integrity_events"
    velocity        = "s3a://${var.lake_bucket}/velocity"
    checkpoint_root = "s3a://${var.lake_bucket}/checkpoints"
    checkpoint_path = "s3a://${var.lake_bucket}/checkpoints/${var.spark_version_tag}"
    snapshots       = "s3a://${var.snapshot_bucket}/balance_engine"
  })
}

output "checkpoint_path" {
  description = "The one string the upgrade drill cares about."
  value       = "s3a://${var.lake_bucket}/checkpoints/${var.spark_version_tag}"
}
