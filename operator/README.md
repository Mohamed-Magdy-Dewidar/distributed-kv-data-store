# kvstore-operator

The KVCluster operator: it runs a cluster of the store's nodes from one
`KVCluster` resource and changes its membership, one node at a time, when
`replicas` changes. It is its own Go module, scaffolded with kubebuilder
v4.16.0.

Everything about it is in [docs/operator.md](../docs/operator.md):
[installing](../docs/operator.md#install) and
[using](../docs/operator.md#use) it,
[placement on GKE](../docs/operator.md#placement-and-gke), its
[status](../docs/operator.md#status), how it
[carries out a membership change](../docs/operator.md#how-a-membership-change-is-carried-out),
[manual recovery](../docs/operator.md#manual-recovery) and its
[tests](../docs/operator.md#tests). `make help` lists the targets of this
directory's Makefile.

## License

Apache License 2.0; see [LICENSE](../LICENSE).
