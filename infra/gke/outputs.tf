locals {
  registry = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.kvstore.repository_id}"
}

output "get_credentials" {
  description = "Points kubectl at the cluster."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.kvstore.name} --region ${var.region} --project ${var.project_id}"
}

output "registry" {
  description = "The Docker repository."
  value       = local.registry
}

output "configure_docker" {
  description = "Lets docker push to the repository."
  value       = "gcloud auth configure-docker ${var.region}-docker.pkg.dev"
}

output "images" {
  description = "The images to push, without a tag."
  value = {
    node     = "${local.registry}/kvnode"
    operator = "${local.registry}/kvstore-operator"
  }
}

output "database_zones" {
  description = "The database pool's zones."
  value       = local.database_zones
}
