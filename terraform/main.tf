terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# S3 data lake
# ---------------------------------------------------------------------------
# Partition strategy: raw/processed/curated zones, each date-partitioned
# (year/month/day) by the Spark writer. Terraform provisions the bucket
# and lifecycle policy; the internal prefixes are created implicitly by
# Spark's first writes rather than pre-created as empty "folders" (S3 has
# no real folder concept - prefixes only exist once an object uses them).
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name

  tags = {
    Project     = "retail-data-platform"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake_lifecycle" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "raw-zone-transition-to-ia"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "bad-records-expiry"
    status = "Enabled"

    filter {
      prefix = "raw/bad_records/"
    }

    expiration {
      days = 90
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake_block_public" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# IAM: least-privilege role for the Spark streaming job to read/write
# only the paths it needs, rather than a broad AdministratorAccess grant.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "spark_streaming_role" {
  name = "retail-platform-spark-streaming-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "spark_s3_access" {
  name = "retail-platform-spark-s3-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_s3_access" {
  role       = aws_iam_role.spark_streaming_role.name
  policy_arn = aws_iam_policy.spark_s3_access.arn
}

# ---------------------------------------------------------------------------
# CloudWatch log group for pipeline logs
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "pipeline_logs" {
  name              = "/retail-platform/pipeline"
  retention_in_days = 30
}
