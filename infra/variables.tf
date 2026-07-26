variable "project_name" {
  description = "Short name used as a prefix for all resource names (lowercase, no spaces)"
  type        = string
  default     = "arxivetl"
}

variable "environment" {
  description = "Deployment environment — appended to resource names (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "southeastasia"
}

variable "tags" {
  description = "Common tags applied to every resource"
  type        = map(string)
  default = {
    project   = "arxiv-etl-pipeline"
    managedBy = "terraform"
  }
}
