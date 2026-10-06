# The APIs, the network, and the nodes' service account.

locals {
  apis = [
    "compute.googleapis.com",
    "container.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
  ]
  database_zones = length(var.database_zones) == 3 ? var.database_zones : slice(sort(data.google_compute_zones.up.names), 0, 3)
  system_zone    = var.system_zone != "" ? var.system_zone : local.database_zones[0]
}

# Left enabled on destroy: disabling them would break a later apply, and
# deleting the project is the final cleanup.
resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  service            = each.value
  disable_on_destroy = false
}

data "google_compute_zones" "up" {
  region     = var.region
  status     = "UP"
  depends_on = [google_project_service.apis]
}

resource "google_compute_network" "kvstore" {
  name                    = "kvstore"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

# VPC-native: pods and Services get their addresses from the secondary
# ranges. The nodes are private, so they reach Google's APIs (Artifact
# Registry among them) through Private Google Access, and the internet
# (Docker Hub, for Prometheus and Grafana) through Cloud NAT.
resource "google_compute_subnetwork" "nodes" {
  name                     = "kvstore-nodes"
  network                  = google_compute_network.kvstore.id
  region                   = var.region
  ip_cidr_range            = "10.10.0.0/24"
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.20.0.0/16"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

resource "google_compute_router" "nat" {
  name    = "kvstore-nat"
  network = google_compute_network.kvstore.id
  region  = var.region
}

# One automatically allocated address for every node: the region's
# IN_USE_ADDRESSES quota (8 on a new billing account) would not cover a
# public address per node at full autoscaling.
resource "google_compute_router_nat" "nat" {
  name                               = "kvstore-nat"
  router                             = google_compute_router.nat.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = false
    filter = "ERRORS_ONLY"
  }
}

# The nodes run as this account, not the Compute Engine default one (which
# has Editor on the project): GKE's node role (logs, metrics), and read
# access to the image repository (registry.tf).
resource "google_service_account" "nodes" {
  account_id   = "kvstore-nodes"
  display_name = "kvstore GKE nodes"
  depends_on   = [google_project_service.apis]
}

resource "google_project_iam_member" "nodes" {
  project = var.project_id
  role    = "roles/container.defaultNodeServiceAccount"
  member  = google_service_account.nodes.member
}
