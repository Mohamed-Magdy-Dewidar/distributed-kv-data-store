# Distributed KV Store

A from-scratch distributed key-value store, built as a learning project in
consensus, replication, and eventual consistency, working toward a
Kubernetes Operator that manages it.

Currently implemented: single-node store with vector-clock-based causal
tracking, Dynamo/Riak-style sibling conflict resolution, and tombstone-based
deletes.