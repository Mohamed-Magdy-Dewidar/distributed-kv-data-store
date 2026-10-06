# Copy to terraform.tfvars (gitignored) and fill in my_ip_cidr.
project_id = "kvstore-gke"
region     = "europe-west1"

# Your public address, /32. It changes with your connection: re-apply when it
# does, or kubectl cannot reach the control plane.
my_ip_cidr = "203.0.113.7/32"
