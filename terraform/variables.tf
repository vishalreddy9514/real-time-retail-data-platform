variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "eu-west-2" # London - data residency for a UK retailer
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "dev"
}

variable "data_lake_bucket_name" {
  description = "Globally-unique S3 bucket name for the data lake"
  type        = string
}
