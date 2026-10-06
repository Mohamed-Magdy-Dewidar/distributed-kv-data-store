# Everything the GKE experiments need, in one project; see docs/gke.md.
# Every session ends with terraform destroy.

terraform {
  required_version = "~> 1.16.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.6.0"
    }
  }

  # Local state, gitignored (.gitignore here): one user, a short-lived
  # project, and deleting the project is the final cleanup. It holds the
  # cluster's endpoint and CA certificate: never commit it.
}

# The project and region are always explicit, never the gcloud CLI's active
# configuration. Credentials are Application Default Credentials
# (gcloud auth application-default login), never a key file.
provider "google" {
  project = var.project_id
  region  = var.region
}
