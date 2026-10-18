# Documentation

| Document | Purpose |
|---|---|
| [running-locally.md](running-locally.md) | Start a 3-node cluster from `examples/local`, read and write with grpcurl, work with siblings, and call the admin API. |
| [kubernetes.md](kubernetes.md) | The manual path: run a 10-node cluster on kind from `deploy/k8s/`: setup, deploy, and measured results for pod loss, rolling restarts, scaling up and down, and resource use. |
| [operator.md](operator.md) | The KVCluster operator: install, use, phases and conditions, how it carries out a membership change, its invariants, manual recovery, and measured end-to-end results at 6 replicas. |
| [gke.md](gke.md) | The GKE cluster in `infra/gke/` (Terraform): what it creates and why, quotas, cost per hour, and a session from `terraform apply` to `terraform destroy`. |
| [membership.md](membership.md) | The procedure for adding a node, removing one, and removing a crashed one, and what the admin API's answers mean. |
| [observability.md](observability.md) | The metrics each node serves on `/metrics`, and the local Prometheus and Grafana in `deploy/observability/`: how to run them, the dashboard, and measured latencies. |
| [benchmarks.md](benchmarks.md) | The k6 load tests in `deploy/bench/`: scenarios, how to run them on kind and GKE, how a result is shown to be the database's limit, and every run's versions, parameters and results. |
| [chaos.md](chaos.md) | The failure tests (VM, zone, partition, system node) under load: `kvchaos` and `deploy/chaos/chaos.sh`, how a prediction is made and checked request by request, and the kind rehearsal with its findings. |
| [testing.md](testing.md) | What the unit and end-to-end tests cover, how to run them, where they have been run, and what the crash tests do not prove. |
| [development.md](development.md) | Building, testing, and regenerating the checked-in gRPC code with pinned tool versions. |
| [known-limitations.md](known-limitations.md) | What the store does not do, or does imperfectly, and why: consistency, data model, membership, operations, durability. |

The project overview, architecture, key decisions and headline results are in the
[top-level README](../README.md).
