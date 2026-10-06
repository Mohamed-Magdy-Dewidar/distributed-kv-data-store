# The Docker repository for the node and operator images. Destroying it
# deletes the images in it.
resource "google_artifact_registry_repository" "kvstore" {
  location      = var.region
  repository_id = "kvstore"
  format        = "DOCKER"
  description   = "kvnode and kvstore-operator images"
  depends_on    = [google_project_service.apis]
}

# The nodes pull from it.
resource "google_artifact_registry_repository_iam_member" "nodes" {
  location   = google_artifact_registry_repository.kvstore.location
  repository = google_artifact_registry_repository.kvstore.name
  role       = "roles/artifactregistry.reader"
  member     = google_service_account.nodes.member
}
