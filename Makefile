# Thin wrappers over the commands in docs/: every recipe is a plain command,
# and docs/development.md gives each one as typed in PowerShell, where make
# is not usually installed. Targets for the operator module delegate to
# operator/Makefile (operator-<target>, e.g. make operator-test); its tests
# need Linux (envtest), so run those in the golang:1.26 container or WSL.

KIND_CLUSTER ?= kvstore
# The kind node image docs/kubernetes.md pins (kind v0.33.0, Kubernetes 1.35.8).
KIND_NODE_IMAGE ?= kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0
NODE_IMG ?= kvnode:dev
OPERATOR_IMG ?= kvstore-operator:dev
# Replicas for make scale; 6 is the GKE target.
N ?= 6

.PHONY: help
help: ## Show the targets.
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z%_-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# The allocation tests skip themselves under -race, so they run again without
# it (docs/testing.md).
.PHONY: test
test: ## Test the database module as docs/testing.md does (the operator's: make operator-test).
	go vet ./...
	go test -race -count=1 ./...
	go test -count=1 ./internal/telemetry/ ./internal/storage/wal/

.PHONY: image
image: ## Build the node and operator images and load them into kind.
	docker build -t $(NODE_IMG) .
	docker build -t $(OPERATOR_IMG) operator
	kind load docker-image $(NODE_IMG) $(OPERATOR_IMG) --name $(KIND_CLUSTER)

.PHONY: kind-up
kind-up: ## Create the kind cluster of docs/kubernetes.md.
	kind create cluster --name $(KIND_CLUSTER) --wait 120s --image $(KIND_NODE_IMAGE)

.PHONY: kind-down
kind-down: ## Delete the kind cluster, and every volume in it.
	kind delete cluster --name $(KIND_CLUSTER)

.PHONY: operator-deploy
operator-deploy: ## Install the operator and create the KVCluster kv (6 replicas) in kvstore.
	kubectl kustomize operator/config/default | sed 's|image: controller:latest|image: $(OPERATOR_IMG)|' | kubectl apply --server-side -f -
	kubectl -n kvstore-operator-system rollout status deploy/kvstore-operator-controller-manager --timeout=180s
	kubectl create namespace kvstore --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n kvstore apply -f operator/config/samples/kvstore_v1alpha1_kvcluster.yaml

.PHONY: scale
scale: ## Scale the KVCluster kv to N replicas (default 6), one node at a time.
	kubectl -n kvstore scale kvc/kv --replicas=$(N)

.PHONY: load
load: ## Start the steady writer (deploy/loadgen/writer.yaml) and the toolbox.
	kubectl apply -f deploy/loadgen/loadgen.yaml -f deploy/loadgen/writer.yaml

# operator-<target> runs <target> in operator/Makefile (test, lint,
# manifests, generate, test-e2e, ...).
operator-%:
	$(MAKE) -C operator $*
