# Known limitations

This project is a Dynamo-style key-value store built as a learning and portfolio artifact. It runs, survives failures, and can be operated on Kubernetes, but it is not a production database. This page lists what it deliberately does not do, or does imperfectly, so readers can see the tradeoffs.

## Consistency and write semantics

- **A failed write has an unknown outcome.** Any error from Put means the write may or may not persist. Retrying with the same causal context is safe, but can leave a duplicate sibling holding the same value.
- **Rollback is local only.** When a write misses its quorum, the coordinator rolls back its own copy. A replica that already applied the write keeps it, and anti-entropy can later reinstall it on the coordinator.
- **Rollback can't hide flushed versions.** If the previous version was already flushed to an SSTable, rollback returns `ErrRestoreIncomplete` (logged).
- **Failover on `Unavailable` is not airtight.** gRPC can report `Unavailable` after the server applied a write. Forwarding then retries on another replica, which can produce a spurious sibling.
- **Errors are untyped.** Internally, failures are wrapped strings. Client-facing status codes cover only coarse categories.

## Data model

- **Values are JSON-encoded `any`.** The client API accepts only UTF-8 values. Non-string values would change Go type across a restart or a network hop (for example, `int` becomes `float64`). No current path produces them.
- **No delete in the client API.** The storage layer supports tombstones, but they are never exposed to clients and never garbage-collected.

## Membership

- **Membership is static.** The peer list comes from config. Adding or removing a node requires a coordinated restart of all nodes.
- **Membership mismatches are not detected.** Nodes with different peer lists or a different N misroute writes silently. Nothing verifies that a peer's identity or ring matches. (This entry goes away if the membership fingerprint is implemented.)
- **Replicate does not check ownership.** A node accepts replicated writes without checking that it is a replica for the key.

## Operations and cost

- **Anti-entropy is unscoped.** Every round rebuilds full Merkle trees per peer, including keys the two nodes don't share, so those buckets stay marked divergent. Correct, but wasteful.
- **Hints never expire.** Delivery markers accumulate forever.
- **Hints are skipped during shutdown.** Hint creation is skipped once shutdown has begun. This is safe given the shutdown order, which stops listeners first.
- **In-memory mode is for tests and demos.** With `dataDir: ""` nothing survives a restart, and each process takes a new writer identity (see "Never restore a node's data dir" under Durability), so a restarted in-memory node comes back empty and is a new writer.
- **In-memory nodes have no hinted handoff.** With `dataDir: ""`, a write to an unreachable replica converges only through anti-entropy, and no log line marks that a hint was skipped rather than stored.
- **The shutdown budget bounds only the gRPC stop.** `timeouts.shutdown` limits `rpc.Listener.StopWithin`. The steps after it (stopping background loops, closing storage) are not bounded; they wait for in-flight work such as an anti-entropy round or a flush.
- **Long RPCs are not waited for at shutdown.** An RPC still running when the shutdown budget expires is left behind. In practice this is only `GetMerkleTree`, because `merkle.Build` cannot be cancelled. It keeps running, read-only, until the process exits.
- **No authentication or encryption.** Inter-node and client traffic is plaintext gRPC with insecure credentials.

## Durability

- **Never restore a node's data dir from a stale backup.** A node's writes are versioned under its node ID plus an incarnation stored in the data dir's `IDENTITY` file, which is what keeps a replacement node on an empty dir from being mistaken for its predecessor. A restored old dir brings back an old incarnation with old counters: replicas that have since seen higher counts for it drop the node's new writes as older, while acknowledging them. Replace the node with an empty dir instead.
- **Durability is verified on Linux only.** Directory fsync is a no-op on Windows; the code relies on NTFS metadata journaling there.
- **Crash tests cover process crashes, not power loss.** Kill-and-restart tests prove recovery from a process crash. They do not prove recovery from power loss or a lying disk cache.