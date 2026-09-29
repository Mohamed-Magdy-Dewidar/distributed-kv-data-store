# Running a local cluster with cmd/node

`examples/local/node-{1,2,3}.yaml` configure a real 3-node cluster
(`n=3, w=2, r=2`) on `localhost`, each with its own gRPC port, HTTP probe
port, and data directory. Unlike `cmd/cluster` (which runs several nodes
in one process for demos), each of these is a separate `cmd/node` process
— closer to how a real deployment runs one node per pod.

Start each node in its own terminal, from the repo root:

```sh
go run ./cmd/node -config examples/local/node-1.yaml
go run ./cmd/node -config examples/local/node-2.yaml
go run ./cmd/node -config examples/local/node-3.yaml
```

Each node logs once it's ready, and its `/readyz` endpoint (on its
`listen.http` port) returns 200:

```sh
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/readyz
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8081/readyz
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8082/readyz
```

`/livez` always returns 200 once the process is up; `/readyz` reflects
whether the node has finished startup and isn't shutting down.

Stop a node with Ctrl+C: it marks itself not-ready, stops accepting new
gRPC requests (bounded by `timeouts.shutdown`), stops its background
loops, and closes its storage before exiting. A second Ctrl+C kills it
immediately instead of waiting.

Data persists under each node's `dataDir` (`data/local/kv-*`, gitignored)
— restarting a node with the same config picks its data back up.

Each config sets `KV_NODE_ID`'s value directly via `nodeId`; to override
which node a config file runs as without editing it, set the `KV_NODE_ID`
environment variable instead (it takes precedence over the file).
