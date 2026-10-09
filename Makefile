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

# image, operator-deploy and operator-test-e2e are for the kind cluster only:
# on another cluster (GKE) they would install images that exist only on this
# machine, or delete the KVCluster and its volumes. They refuse to run unless
# kubectl's current context is kind's. The GKE path is docs/gke.md.
KIND_CONTEXT := kind-$(KIND_CLUSTER)

.PHONY: kind-context
kind-context:
	@ctx="$$(kubectl config current-context 2>/dev/null)"; \
	if [ "$$ctx" != "$(KIND_CONTEXT)" ]; then \
		echo "Refusing: this target is for the kind cluster only, and kubectl's current context is '$$ctx', not '$(KIND_CONTEXT)'." >&2; \
		echo "Switch with: kubectl config use-context $(KIND_CONTEXT). For GKE, follow docs/gke.md." >&2; \
		exit 1; \
	fi

.PHONY: image
image: kind-context ## Build the node and operator images and load them into kind (kind only).
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
operator-deploy: kind-context ## Install the operator and create the KVCluster kv (6 replicas) in kvstore (kind only).
	kubectl kustomize operator/config/default | sed 's|image: controller:latest|image: $(OPERATOR_IMG)|' | kubectl --context $(KIND_CONTEXT) apply --server-side -f -
	kubectl --context $(KIND_CONTEXT) -n kvstore-operator-system rollout status deploy/kvstore-operator-controller-manager --timeout=180s
	kubectl create namespace kvstore --dry-run=client -o yaml | kubectl --context $(KIND_CONTEXT) apply -f -
	kubectl --context $(KIND_CONTEXT) -n kvstore apply -f operator/config/samples/kvstore_v1alpha1_kvcluster.yaml

.PHONY: scale
scale: ## Scale the KVCluster kv to N replicas (default 6), one node at a time.
	kubectl -n kvstore scale kvc/kv --replicas=$(N)

.PHONY: load
load: ## Start the steady writer (deploy/loadgen/writer.yaml) and the toolbox.
	kubectl apply -f deploy/loadgen/loadgen.yaml -f deploy/loadgen/writer.yaml

.PHONY: operator-test-e2e
operator-test-e2e: kind-context ## Run the operator's kind e2e (kind only; the test pins kind's context too).
	$(MAKE) -C operator test-e2e KIND_CLUSTER=$(KIND_CLUSTER)

# operator-<target> runs <target> in operator/Makefile (test, lint,
# manifests, generate, ...).
operator-%:
	$(MAKE) -C operator $*
