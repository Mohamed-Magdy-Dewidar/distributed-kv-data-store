variable "project_id" {
  description = "The project everything is created in."
  type        = string
}

variable "region" {
  description = "The region: the cluster is regional, the database pool spans 3 of its zones."
  type        = string
  default     = "europe-west1"
}

variable "my_ip_cidr" {
  description = "The only address allowed to reach the cluster's control plane (master authorized networks), as a CIDR, e.g. 203.0.113.7/32."
  type        = string

  validation {
    condition     = can(cidrhost(var.my_ip_cidr, 0))
    error_message = "my_ip_cidr must be a CIDR block, e.g. 203.0.113.7/32."
  }
}

variable "cluster_name" {
  description = "The GKE cluster's name."
  type        = string
  default     = "kvstore"
}

variable "database_zones" {
  description = "The 3 zones of the database node pool. Empty: the region's first 3 zones that are up."
  type        = list(string)
  default     = []

  validation {
    condition     = length(var.database_zones) == 0 || length(var.database_zones) == 3
    error_message = "database_zones must be empty or name exactly 3 zones."
  }
}

variable "system_zone" {
  description = "The one zone of the system node pool. Empty: the first of the database zones."
  type        = string
  default     = ""
}

variable "machine_type" {
  description = "Both pools' machine type. e2-standard-2 is the smallest E2 that is not shared-core: no CPU bursting to confound latency measurements."
  type        = string
  default     = "e2-standard-2"
}

variable "database_nodes_per_zone" {
  description = "The database pool's nodes per zone at creation, and the autoscaler's minimum. 2 x 3 zones = 6, the GKE sample's replicas."
  type        = number
  default     = 2
}

variable "database_max_nodes_per_zone" {
  description = "The autoscaler's maximum per zone. 3 leaves a free node for the scale-to-7 test."
  type        = number
  default     = 3
}

variable "boot_disk_gb" {
  description = "Every node's pd-balanced boot disk. pd-balanced counts against the region's SSD_TOTAL_GB quota."
  type        = number
  default     = 20
}
