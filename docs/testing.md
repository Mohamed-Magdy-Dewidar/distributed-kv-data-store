# Testing

There are two layers of tests. Both run with `go test ./...`.

## Unit and in-process tests

Every package under `internal/` has its own tests. Most run a single process
and many run several nodes in one test binary, talking real gRPC over
localhost: quorum reads and writes, forwarding, siblings, hinted handoff,
anti-entropy, heartbeats, membership changes and handoff, the admin API, and
the storage engine's crash-recovery paths (WAL replay, flush, compaction).
They are fast and deterministic, using test hooks to hold a goroutine at a
chosen point rather than relying on timing.

```sh
go test -race ./...
```

## End-to-end tests (`e2e/`)

The `e2e` package builds `cmd/node` from the working tree once, then starts
real node processes from generated configs, each with its own ports and data
directory, and talks to them the way a client and an operator would: the
`KVClient` gRPC API for reads and writes, the admin HTTP API for membership,
and `KVReplication.FetchItem` to read one node's **own** copy of a key, with
no quorum and no forwarding, so a test can say where data physically is.

A crash is `kill` (SIGKILL on Linux, `TerminateProcess` on Windows); the test
waits for the process to be gone before restarting it on the same data
directory. The configs use fast heartbeats (100 ms) and hint delivery
(500 ms) and push anti-entropy out of the way (24 h), so handoff and hints are
shown to work on their own.

| Test | What it proves |
|---|---|
| `TestWholeClusterCrashRecovery` | Kill all three nodes at once, restart them: every one of 200 acknowledged writes reads back. |
| `TestHintedHandoffAcrossProcessCrash` | Writes a replica missed while its process was dead reach its own store after it restarts, through hints alone. |
| `TestScaleUpThenDown` | Adding a fifth node (started with a config at a higher epoch, which it announces) moves every key to its new owners with exactly the versions it had, concurrent siblings included. Removing a node through `POST /admin/membership` makes it report not ready, drain (`GET /admin/handoff` returns 200 only once its data is pushed and the remaining members have the new membership), and leaves every key on its new owners and readable through every remaining node. |
| `TestGracefulShutdown` | SIGTERM stops a node with exit code 0, and it restarts on the same data with its data. Skipped on Windows, which cannot send SIGTERM. |

Run them:

```sh
go test -count=1 ./e2e          # about 20 s
go test -count=1 -v -run TestScaleUpThenDown ./e2e
go test -short ./...            # everything except the e2e tests
```

Before a test writes, it waits until every running node reports every other
running node `reachable` in `GET /admin/membership`, not just ready: see the
cold-start entry in [known-limitations.md](known-limitations.md).

`-count=1` matters: the node binary is built inside the test, so Go's test
cache does not see changes to code the test package doesn't import (such as
`cmd/node`). On failure, each node's log is printed.

### What the crash tests do not prove

They kill processes. The operating system still flushes what a killed process
had written, so they prove recovery from a **process crash**, not from power
loss, a kernel crash, or a disk that loses writes it acknowledged. See
[known-limitations.md](known-limitations.md).
