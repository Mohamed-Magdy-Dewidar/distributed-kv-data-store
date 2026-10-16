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

A few tests check that a hot path allocates nothing, such as timing a
request or a WAL fsync for `/metrics`. The race detector's instrumentation
allocates, so these tests skip themselves under `-race`: run them again
without it, alongside the `-race` suite:

```sh
go test -count=1 ./internal/telemetry/ ./internal/storage/wal/ ./internal/storage/engine/
```

## Write benchmarks

Three Go benchmarks measure the write path from a fixed number of concurrent
goroutines, for comparing changes to how writes wait for each other and for
the WAL's fsync:

| Benchmark | What it measures |
|---|---|
| `BenchmarkPutParallel` (`internal/storage/engine`) | `StorageEngine.Put` from 1, 8 and 64 writers, each write a new key, with group commit off (`mode=serial`) and on (`mode=group`) in the same run; it also reports `writes/fsync`, the average batch |
| `BenchmarkDataStorePutParallel` (`internal/store`) | The same one layer up, `DataStore.Put` with no context (a read of the key, then the write), where a node's writes enter |
| `BenchmarkDataStoreGetWhileWriting` (`internal/store`) | `DataStore.Get` of existing keys from 8 readers while 8 writers Put other keys: how long a read waits behind writes |

Each reports ns/op (the inverse of throughput), the p50 and p99 latency of
one operation (`p50-ms`, `p99-ms`), and the Put benchmarks report
`fsyncs/op`, the WAL fsyncs per write. Values are 256 bytes and the memtable
4 MiB, as in the cluster, so flushes run during the benchmark. Results, and
how to compare commits within one run: [benchmarks.md](benchmarks.md#the-go-write-benchmarks).

They measure fsync, so they run on Linux in a container's own filesystem: a
Windows bind mount or `FlushFileBuffers` gives other numbers. Copy the source
in, with the temporary directory outside the module, and run each benchmark
several times, for `benchstat`. Run a fixed number of operations
(`-benchtime 1000x`), not
a duration: a Get can take the store's lock ahead of goroutines already
waiting for it, so the short run Go times first to choose `b.N` can be far
faster than the rest, and the `b.N` it picks then runs for minutes. A fixed
count also gives every compared run the same number of writes:

```sh
docker run --rm -v "${PWD}:/src:ro" -v kv-gomod:/go/pkg/mod -v kv-gocache:/root/.cache golang:1.26 sh -c "mkdir -p /work/src /work/tmp && cp -r /src/. /work/src/ && cd /work/src && TMPDIR=/work/tmp go test -run '^$' -bench . -benchtime 1000x -count 6 ./internal/storage/engine/ ./internal/store/"
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

### Running on Linux

The full suite, e2e included, has been run three times in a row on Linux, all
passing, in the `golang:1.26` container. `TestGracefulShutdown` runs there; on
Windows, which cannot send SIGTERM, it is skipped. The source is copied
onto a named volume and `TMPDIR` points into it, so the build, the test
binaries and the nodes' data directories are on ext4 inside Docker Desktop's
Linux VM: a virtualized Linux filesystem, not bare metal, and not a bind mount
of the Windows drive. `df -T /work` prints the filesystem type at the start of
the run.

```sh
docker run --rm -v "${PWD}:/src:ro" -v kv-e2e-work:/work golang:1.26 sh -c "find /work -mindepth 1 -delete && cp -r /src/. /work/ && mkdir -p /work/tmp && cd /work && df -T /work && TMPDIR=/work/tmp go test -race -count=1 -v ./..."
```

### What the crash tests do not prove

They kill processes. The operating system still flushes what a killed process
had written, so they prove recovery from a **process crash**, not from power
loss, a kernel crash, or a disk that loses writes it acknowledged. See
[known-limitations.md](known-limitations.md).
