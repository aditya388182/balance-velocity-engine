# terraform/ — an acknowledged stub

This exists so CI job 4's `terraform validate` runs against something real, and so
the pipeline is honest about where infrastructure-as-code belongs.

It is **not** a production module and is not presented as one. A real version would
manage the Kafka cluster and topic configs, the S3 buckets with their lifecycle
rules, the IAM roles the job assumes, and the EMR/EKS submission. None of that is
meaningful against Docker Compose on a single box, and writing it would be fiction
rather than evidence.

What it does do that is worth keeping: it renders the resolved paths — including
`checkpoint_path`, which embeds `spark_version_tag`. A `terraform plan` diff
therefore shows a checkpoint-path change as an explicit, reviewable event rather
than a config edit nobody noticed, which is exactly the change that makes an upgrade
dangerous.
