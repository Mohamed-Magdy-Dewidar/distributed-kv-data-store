# syntax=docker/dockerfile:1

# Build: static cmd/node and cmd/kvchaos binaries (kvchaos: the failure
# tests' tool, docs/gke.md; it runs from this image, never as the node).
FROM golang:1.26 AS build
WORKDIR /src

# Dependencies first, so this layer is reused until go.mod or go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd/ cmd/
COPY internal/ internal/
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvnode ./cmd/node && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvchaos ./cmd/kvchaos

# Run: distroless static, as the nonroot user (uid/gid 65532). There is no
# shell; the node reads its config from KV_CONFIG and keeps its data in the
# config's dataDir, which must be a writable volume.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kvnode /kvnode
COPY --from=build /out/kvchaos /kvchaos
USER nonroot:nonroot
ENV KV_CONFIG=/etc/kv/config.yaml
# 7000: gRPC, for clients and other nodes. 8080: /livez, /readyz and the
# (unauthenticated) admin API.
EXPOSE 7000 8080
ENTRYPOINT ["/kvnode"]
