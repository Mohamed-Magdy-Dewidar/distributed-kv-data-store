# Running a local cluster with cmd/node

`examples/local/node-{1,2,3}.yaml` configure a real 3-node cluster
(`n=3, w=2, r=2`) on `localhost`, each with its own gRPC port, HTTP probe
port, and data directory. Each node is a separate `cmd/node` process, as
in a real deployment, which runs one node per pod.

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

## Reading and writing data (gRPC)

Every node serves the client API, `kvstore.KVClient`, on its `listen.grpc`
port, next to the node-to-node service. Server reflection is on, so
[grpcurl](https://github.com/fullstorydev/grpcurl) needs no `.proto` file. Any
node will do: one that is not a replica for the key forwards the write to one
that is.

```sh
grpcurl -plaintext localhost:7000 list
grpcurl -plaintext localhost:7000 describe kvstore.KVClient
```

Values are UTF-8 strings.

```sh
grpcurl -plaintext -d '{"key": "greeting", "value": "héllo ✓"}' localhost:7000 kvstore.KVClient/Put
grpcurl -plaintext -d '{"key": "greeting"}' localhost:7001 kvstore.KVClient/Get
```

`Get` returns every live version of the key in `values`, `found`, and a
`context`. The context is the causal history of what you read. Send it back
with your next `Put` for the key to say "this replaces what I read".

### Concurrent writes and siblings

Two writes that do not know about each other are both kept, as siblings, and
`Get` returns both. Simulate two clients that both read a key before either
wrote it (an empty `context` means "build on nothing"), writing through
different nodes:

```sh
grpcurl -plaintext -d '{"key": "cart", "value": "apples",  "context": {}}' localhost:7000 kvstore.KVClient/Put
grpcurl -plaintext -d '{"key": "cart", "value": "oranges", "context": {}}' localhost:7001 kvstore.KVClient/Put
grpcurl -plaintext -d '{"key": "cart"}' localhost:7002 kvstore.KVClient/Get
```

The `Get` shows two values and a context naming both writers:

```json
{
  "values": ["apples", "oranges"],
  "context": {
    "entries": {
      "kv-0#3f9a5c1e7b2d4a60": 1,
      "kv-1#d20c8e64a915f7b3": 1
    }
  },
  "found": true
}
```

The keys in `entries` are clock IDs: a node's ID plus a random suffix that is
created once per data directory. **The ones above are only an example; yours
will differ, so copy the `context` from your own `Get`.** Put a merged value
with that context, exactly as returned, and the siblings collapse into one
(the JSON shape is `"context": {"entries": {"<clock id>": <count>, ...}}`):

```sh
grpcurl -plaintext -d '{"key": "cart", "value": "apples and oranges", "context": {"entries": {"kv-0#3f9a5c1e7b2d4a60": 1, "kv-1#d20c8e64a915f7b3": 1}}}' localhost:7002 kvstore.KVClient/Put
grpcurl -plaintext -d '{"key": "cart"}' localhost:7000 kvstore.KVClient/Get
```

The second `Get` returns the single value `"apples and oranges"`. Its
`context` now also has an entry for the node that coordinated the merge
(here `kv-2`): that is the history to send with the next `Put`.

A `Put` without a `context` builds on whatever versions the coordinating
replica has, which is what you want when you do not care about concurrent
writers. A key that was never written (or was deleted) has `found: false` and
no values, and still returns a context. `grpcurl` leaves fields with their
default values out of its output, so for a missing key it prints only
`"context": {}`.

Errors: an empty key is `InvalidArgument`; a call that runs out of time is
`DeadlineExceeded`; anything else, such as a quorum that cannot be reached, is
`Unavailable`. **Any error from `Put` means the outcome is unknown**: the write
may have been applied. Retrying with the same context is safe; at worst it
leaves an extra sibling with the same value.

## Membership (admin HTTP)

The admin API is on each node's `listen.http` port. It has no authentication.
See [membership.md](membership.md) for what to do with it.

```sh
# the node's epoch, members, fingerprint, handoff status and peers
curl -s http://localhost:8080/admin/membership

# apply a membership (here the one the nodes already have: "changed": false)
curl -s -X POST http://localhost:8080/admin/membership \
  -d '{"epoch": 0, "members": {"kv-0": "localhost:7000", "kv-1": "localhost:7001", "kv-2": "localhost:7002"}}'

# block until this node is done for epoch 0 (200), or 504 after the timeout
curl -s -w '\n%{http_code}\n' 'http://localhost:8080/admin/handoff?epoch=0&timeout=5s'
```
