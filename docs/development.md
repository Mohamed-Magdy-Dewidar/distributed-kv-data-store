# Development

## Building and testing

The module needs the Go version in `go.mod`. Nothing else is needed to build
or test: the generated gRPC code is checked in.

```sh
go build ./...
go vet ./...
go test -race ./...
```

See [testing.md](testing.md) for what the tests cover.

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
