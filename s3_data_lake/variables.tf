variable "aws_region" {
    description = "AWS region: default is Seoul"
    type        = string
    default     = "ap-northeast-2"
}

variable "bucket_name" {
    description = "S3 bucket name"
    type        = string
    default     = "wonseokchoi-data-lake-project"

    validation {
        condition = length(var.bucket_name) > 2 && length(var.bucket_name) < 64 && can(regex("^[0-9A-Za-z-]+$", var.bucket_name))
        error_message = "Naming rules are unique and must follow: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html."
    }
}

variable "access_logging_bucket_name" {
    description = "S3 bucket name for access logging storage"
    type        = string
    default     = "wonseokchoi-data-lake-project-bucket-name"
}