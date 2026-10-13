# A regional GKE Standard cluster with two node pools: kvstore, which
# matches operator/config/samples/kvstore_v1alpha1_kvcluster_gke.yaml, and
# system, for everything else; and, for benchmarks only, a third: loadgen.

resource "google_container_cluster" "kvstore" {
  name     = var.cluster_name
  location = var.region

  # terraform destroy must be able to delete it.
  deletion_protection = false

  # The default pool is replaced by the pools below. GKE creates it anyway,
  # so keep it small while it exists.
  remove_default_node_pool = true
  initial_node_count       = 1
  node_config {
    machine_type    = "e2-small"
    disk_type       = "pd-balanced"
    disk_size_gb    = var.boot_disk_gb
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  release_channel {
    channel = "REGULAR"
  }

  network         = google_compute_network.kvstore.id
  subnetwork      = google_compute_subnetwork.nodes.id
  networking_mode = "VPC_NATIVE"
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # Dataplane V2: enforces NetworkPolicy.
  datapath_provider = "ADVANCED_DATAPATH"

  # Private nodes, public control-plane endpoint, reachable from my_ip_cidr
  # only.
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
  }
  master_authorized_networks_config {
    cidr_blocks {
      cidr_block   = var.my_ip_cidr
      display_name = "workstation"
    }
  }

  # standard-rwo, the GKE sample's storage class, comes from this driver.
  addons_config {
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }

  # Prometheus and Grafana run in the cluster (deploy/observability); keep
  # Google's own collection to the system components.
  logging_config {
    enable_components = ["SYSTEM_COMPONENTS"]
  }
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus {
      enabled = false
    }
  }

  depends_on = [google_project_service.apis, google_project_iam_member.nodes]

  # node_config above describes only the default pool, which exists for a
  # few minutes while the cluster is created. Once it is removed, GKE
  # reports another pool's node configuration as the cluster's (in session
  # 2, the loadgen pool's n2-standard-4), so every later plan wanted an
  # in-place change to the cluster that nothing asked for. The node pools
  # are their own resources, below; ignore the cluster's node_config.
  lifecycle {
    ignore_changes = [node_config]
  }
}

# The database pool: the node pool name is GKE's cloud.google.com/gke-nodepool
# label, the GKE sample's node selector; the taint is the one its toleration
# matches. Counts are per zone. No Spot VMs: a preemption would confound the
# failure tests.
resource "google_container_node_pool" "kvstore" {
  name           = "kvstore"
  cluster        = google_container_cluster.kvstore.id
  location       = var.region
  node_locations = local.database_zones

  initial_node_count = var.database_nodes_per_zone
  autoscaling {
    min_node_count  = var.database_nodes_per_zone
    max_node_count  = var.database_max_nodes_per_zone
    location_policy = "BALANCED"
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  # One new node per zone before an old one is drained: with one pod per
  # node, the evicted pod needs somewhere to go. maxSurge counts per zone.
  upgrade_settings {
    strategy        = "SURGE"
    max_surge       = 1
    max_unavailable = 0
  }

  node_config {
    machine_type    = var.machine_type
    disk_type       = "pd-balanced"
    disk_size_gb    = var.boot_disk_gb
    spot            = false
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    taint {
      key    = "dedicated"
      value  = "kvstore"
      effect = "NO_SCHEDULE"
    }

    shielded_instance_config {
      enable_secure_boot = true
    }
  }
}

# The system pool: GKE's own components (kube-dns, metrics-server cannot run
# on the tainted pool), the operator, Prometheus and Grafana. One node in one
# zone; E2_CPUS (24 in the region) leaves no room for a second.
resource "google_container_node_pool" "system" {
  name           = "system"
  cluster        = google_container_cluster.kvstore.id
  location       = var.region
  node_locations = [local.system_zone]
  node_count     = 1

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  upgrade_settings {
    strategy        = "SURGE"
    max_surge       = 1
    max_unavailable = 0
  }

  node_config {
    machine_type    = var.machine_type
    disk_type       = "pd-balanced"
    disk_size_gb    = var.boot_disk_gb
    spot            = false
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    shielded_instance_config {
      enable_secure_boot = true
    }
  }
}

# The loadgen pool, for the k6 benchmarks (docs/benchmarks.md) only: off by
# default, created for a benchmark session and removed after it
# (loadgen_enabled). loadgen_nodes nodes in the system zone, for k6 alone:
# the taint keeps everything else off, and the GKE overlay of deploy/bench
# selects the pool by name and tolerates the taint. Two nodes for the
# two-pod check (K6_PODS=2): the overlay puts each k6 pod on its own node. N2, not E2: the database pool at
# its maximum already takes 20 of the region's 24 E2_CPUS, and N2 counts
# against its own quota.
resource "google_container_node_pool" "loadgen" {
  count          = var.loadgen_enabled ? 1 : 0
  name           = "loadgen"
  cluster        = google_container_cluster.kvstore.id
  location       = var.region
  node_locations = [local.system_zone]
  node_count     = var.loadgen_nodes

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  upgrade_settings {
    strategy        = "SURGE"
    max_surge       = 1
    max_unavailable = 0
  }

  node_config {
    machine_type    = var.loadgen_machine_type
    disk_type       = "pd-balanced"
    disk_size_gb    = var.boot_disk_gb
    spot            = false
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    taint {
      key    = "dedicated"
      value  = "loadgen"
      effect = "NO_SCHEDULE"
    }

    shielded_instance_config {
      enable_secure_boot = true
    }
  }
}
