locals {
  data_lake_bucket = "mmd-bucket"
}

variable "project" {
  description = "meta-movement-dist"
  default = "meta-movement-dist"
}

variable "region" {
  description = "asia-south1"
  default = "asia-south1"
  type = string
}

variable "storage_class" {
  description = "Storage class type"
  default = "STANDARD"
}

variable "dataproc_cluster_name" {
  description = "mmd-dataproc-cluster"
  type        = string
  default     = "mmd-dataproc-cluster"
}

variable "bq_dataset" {
  description = "mmd_dataset"
  type = string
  default = "mmd_dataset"
}