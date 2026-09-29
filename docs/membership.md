# Changing the membership

The membership is the set of nodes (ID and address) that make up the cluster, tagged with an **epoch**. Changing it means announcing a membership with a higher epoch. Nodes learn of it from each other through heartbeats, adopt it, write it to `<dataDir>/MEMBERSHIP`, and then move data to the nodes that now own it (the *handoff*, recorded in `<dataDir>/HANDOFF`). This page is the procedure for doing that by hand. What the procedure cannot promise is listed in [known-limitations.md](known-limitations.md).

## Ground rules

- **One node per change.** Add or remove a single node, then wait for the handoff to finish everywhere before starting the next change.
- **Every change needs a higher epoch.** Read the current one first. Two different memberships at the same epoch are a *conflict*: nodes log `MEMBERSHIP CONFLICT`, adopt neither, and stay as they are until a higher epoch is announced.
- **The member list must have at least N members** (`cluster.n`), every ID and address unique, and no `#` in an ID.
- **N, W and R do not change** with the membership, and must be the same on every node. A membership fetched from a node with a different N fails its fingerprint check and is rejected.

## The admin API

It is served on the probe address (`listen.http`), next to `/livez` and `/readyz`, and it has **no authentication**.

| Request | Meaning |
|---|---|
| `GET /admin/membership` | The node's epoch, members, fingerprint, handoff status, and for each other member whether it is alive and the last epoch it reported. |
| `POST /admin/membership` with `{"epoch": E, "members": {"kv-0": "host:7000", …}}` | Apply a membership on this node. `200 {"changed": true}` when adopted, `200 {"changed": false}` when the node already holds exactly this; `409` for an older epoch, or the node's epoch with different members (the body says which and gives the node's current epoch); `400` for an invalid member set, bad JSON, or a body over 64 KiB. |
| `GET /admin/handoff?epoch=E&timeout=60s` | Blocks until this node is done for epoch E: `200` with the status when it is, `504` with the status when `timeout` (a duration, default 60 s, at most 10 min) runs out first. |

"Done" depends on whether the node is in the membership:

- **A member** is done when its handoff to epoch E has completed.
- **A node that is not in the membership** (it is being removed) is *drained* when its handoff has completed **and** every remaining member has been seen, in a heartbeat reply, at epoch E or later. It is safe to stop only then.

A node that is not in its current membership also answers `503` on `/readyz`.

## Scale up: add node X

1. Read the current epoch E from `GET /admin/membership` on any node.
2. Start X with a configuration whose `cluster.epoch` is E+1 and whose `cluster.members` lists every existing node **and X**, with the same `n`, `w`, `r`. X adopts that membership at startup and announces it in its first heartbeats; the other nodes fetch it from X's address and adopt it.

   Alternatively, `POST` the new membership to any existing node. The others learn of it through heartbeats, and X, once it is up, does too.
3. On each existing node, wait for the handoff: `GET /admin/handoff?epoch=E+1` until `200`. Existing owners push the keys X now owns to X; until they have, reads that depend on X may not find those keys.
4. Bring the configuration files of the existing nodes up to date (same epoch and member list). A node that has adopted a newer epoch keeps it and logs that the configuration's epoch is older, so this is housekeeping, not a requirement.

## Scale down: remove node X

1. `POST` the membership without X, at epoch E+1, to any node (X included or not).
2. On X, `GET /admin/handoff?epoch=E+1&timeout=10m` until `200`. X pushes everything it holds to the new owners, retrying any target that is unreachable, so this only completes once every push has landed and the remaining members have reported epoch E+1. On `504`, the body shows what is left (`handoff.pending`) and which members have not been seen at the new epoch (`peers`).
3. Stop X.
4. Update the other nodes' configuration files to the smaller list and the new epoch.

X's data is not deleted: it stays on X's disk. Do not start X again under the same ID with that data directory unless you intend to; if you do start it with an old configuration, it hears the newer epoch from its old peers, finds it is not a member, and reports not ready.

## Remove a crashed node X

1. `POST` the membership without X, at epoch E+1, to any surviving node.
2. The survivors adopt it and re-replicate: each pushes the keys that now have a new owner to that owner. On each survivor, `GET /admin/handoff?epoch=E+1` until `200`.

Data that was only on X cannot be recovered. With W ≥ 2, an acknowledged write is on at least one survivor after a single failure.

## When a request is refused

- `409 stale epoch`: the node already holds a newer membership. Read `currentEpoch`, decide what the cluster should look like now, and use a higher epoch.
- `409 membership conflict`: the node holds a different membership at this epoch. Nothing was changed. Use a higher epoch.
- `400 invalid membership`: the message names the rule (fewer than N members, a duplicate address, an empty or `#`-containing ID).
