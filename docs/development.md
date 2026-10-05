# Development

## Building and testing

The module needs the Go version in `go.mod`. Nothing else is needed to build
or test: the generated gRPC code is checked in.

```sh
go build ./...
go vet ./...
go test -race ./...
```

See [testing.md](testing.md) for what the tests cover. The operator is its
own module, under `operator/`; see [operator.md](operator.md#tests) for its
tests.

## Make targets

The top-level `Makefile` wraps the commands from the docs, for a shell with
`make` (Linux, WSL, or the `golang:1.26` container). Each target is one or a
few plain commands; on Windows without `make`, type them in PowerShell, as
below, from the repository root. Defaults: kind cluster `kvstore`, images
`kvnode:dev` and `kvstore-operator:dev`, `N=6`.

| Target | What it does | In PowerShell |
|---|---|---|
| `make test` | Vets and tests the database module as [testing.md](testing.md) does: the `-race` suite, then the allocation tests that skip themselves under `-race`. | `go vet ./...; go test -race -count=1 ./...; go test -count=1 ./internal/telemetry/ ./internal/storage/wal/` |
| `make operator-test` | Tests the operator module (`operator/Makefile`'s `test`; needs Linux for envtest). | `docker run --rm -v "${PWD}:/src" -v kv-gomod:/go/pkg/mod -w /src/operator golang:1.26 make test` |
| `make image` | Builds the node and operator images and loads them into kind. | `docker build -t kvnode:dev .; docker build -t kvstore-operator:dev operator; kind load docker-image kvnode:dev kvstore-operator:dev --name kvstore` |
| `make kind-up` | Creates the kind cluster of [kubernetes.md](kubernetes.md#create-the-cluster) (then install metrics-server as described there). | `kind create cluster --name kvstore --wait 120s --image kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0` |
| `make kind-down` | Deletes the kind cluster, with every volume in it. | `kind delete cluster --name kvstore` |
| `make operator-deploy` | Installs the operator and creates the KVCluster `kv` (6 replicas) in `kvstore`. | see below |
| `make scale N=7` | Scales `kv` to N replicas, one node at a time ([operator.md](operator.md)). | `kubectl -n kvstore scale kvc/kv --replicas=7` |
| `make load` | Starts the steady writer `load` (`deploy/loadgen/writer.yaml`) and the toolbox. Delete the pod to stop it. | `kubectl apply -f deploy/loadgen/loadgen.yaml -f deploy/loadgen/writer.yaml` |
| `make operator-<target>` | Any target of `operator/Makefile`: `lint`, `manifests`, `generate`, `test-e2e`, ... | |

`make operator-deploy` in PowerShell:

```powershell
kubectl kustomize operator/config/default | ForEach-Object { $_ -replace 'image: controller:latest', 'image: kvstore-operator:dev' } | kubectl apply --server-side -f -
kubectl -n kvstore-operator-system rollout status deploy/kvstore-operator-controller-manager --timeout=180s
kubectl create namespace kvstore --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kvstore apply -f operator/config/samples/kvstore_v1alpha1_kvcluster.yaml
```

A typical session: `make kind-up` (once), `make image`,
`make operator-deploy`, `make load`, `make scale N=7`, `make scale`.

## Regenerating the gRPC code

`internal/rpc/pb/kvstore.pb.go` and `kvstore_grpc.pb.go` are generated from
`internal/rpc/pb/kvstore.proto` and are tracked in git, so building never
requires protoc. After changing the `.proto` file, regenerate them and commit
the result together with the change.

The checked-in files were generated with these versions (their headers record
them):

| Tool | Version |
|---|---|
| protoc | 36.2 (`protoc --version` prints `libprotoc 36.2`; the generated headers show it as `v7.36.2`) |
| protoc-gen-go | v1.36.12 (matches `google.golang.org/protobuf` in `go.mod`) |
| protoc-gen-go-grpc | v1.6.2 |

protoc comes from the [protobuf releases](https://github.com/protocolbuffers/protobuf/releases);
the two plugins install with Go:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
```

With all three on `PATH`, from the repository root:

```sh
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       internal/rpc/pb/kvstore.proto
```

With these versions, running it on an unchanged `.proto` reproduces the
checked-in files exactly. Other versions produce working code but a different
header and possibly other small differences, so use these to keep diffs
limited to the actual change.
