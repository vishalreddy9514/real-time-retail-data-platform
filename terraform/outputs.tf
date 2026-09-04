output "data_lake_bucket_arn" {
  value = aws_s3_bucket.data_lake.arn
}

output "data_lake_bucket_name" {
  value = aws_s3_bucket.data_lake.bucket
}

output "spark_streaming_role_arn" {
  value = aws_iam_role.spark_streaming_role.arn
}
