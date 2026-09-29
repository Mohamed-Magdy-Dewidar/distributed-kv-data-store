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

- **Membership changes are epoch-versioned and must be serialized, one node added or removed per epoch.** Every change carries a higher epoch, and two different memberships at the same epoch are a conflict that nodes report and do not resolve (they stay split until a higher epoch is announced). Nothing stops an operator from changing several nodes in one step, or starting the next change before the previous handoff has finished; the design assumes neither, and doing so can leave keys with fewer than N copies until anti-entropy repairs them. See [membership.md](membership.md).
- **Reads may be stale during rebalancing.** Until the handoff has finished, a new owner of a key may not hold it yet. A read that gets its quorum from new owners can return not-found or an older version, and reads do not repair.
- **Orphaned copies on former owners are never reclaimed.** Handoff copies data and never deletes. A node that stops owning a key keeps its copy for good; it is no longer consulted for the key, but it still takes up space.
- **Writes coordinated with a stale view during a change reach new owners through anti-entropy, not handoff.** Handoff moves what a node holds when it lists its keys. A write accepted by a node that has not yet adopted the new membership goes to the old owners and may land after that listing. Anti-entropy carries it over only if at least one copy landed on a node that remains an owner: it reconciles a key only between two nodes that both own it in the current membership (`sharesReplicaSet`), so it never pulls from a former owner. That is guaranteed when membership changes are made one node at a time and W ≥ 2: a write acknowledged by W nodes reaches at least one node that stays an owner. New writes stop reaching a removed node only once the remaining members have adopted the new epoch, and the leaving node's drained check is what confirms that (`GET /admin/handoff` returns 200 only after every remaining member has been seen at the new epoch), so the node must not be stopped before it returns. The anti-entropy round triggered when a handoff completes, and the periodic ones, do the carrying.
- **Removing a crashed node relies on the surviving replicas.** The crashed node's data can only come from other replicas. With W ≥ 2, every acknowledged write is on at least one survivor after a single failure; with W = 1 a write acknowledged only by the crashed node is lost.
- **A hung peer gets no hints until it is marked dead.** A write is hinted only when the replica answers `Unavailable`. A peer that accepts connections and never answers times out instead, which creates no hint. Once heartbeats mark it dead (about `health.maxMissedHeartbeats` × `intervals.heartbeat`, 3 s by default) writes to it fail fast as `Unavailable` and are hinted; writes in flight before that are left to anti-entropy.
- **The admin API and membership propagation are unauthenticated.** Any node or client that can reach the cluster's gRPC or HTTP ports can announce a higher epoch (in a heartbeat, or with `POST /admin/membership`) and have every node adopt it.
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