# The distributed-actor cluster on Kubernetes, run locally with kind.
#
# Prerequisites: Docker, plus
#   kind:    https://kind.sigs.k8s.io/docs/user/quick-start/#installation
#   kubectl: https://kubernetes.io/docs/tasks/tools/
#
# Quick start, from this directory:
#   make cluster-create   # a local Kubernetes cluster in Docker
#   make deploy           # build the image, load it into kind, roll out deploy/k8s.yaml
#   make port-forward     # the API on http://localhost:8080
#   make cluster-delete   # tear the whole thing down
#
# `make k8s` from the repository root runs usecases.sh instead, which drives all
# of the above and asserts every scenario in the README, then tears down.

.DEFAULT_GOAL := help

CLUSTER_NAME ?= nodeakt-k8s
IMAGE ?= nodeakt-k8s:dev
REPO_ROOT := ../..

.PHONY: help cluster-create cluster-delete image deploy undeploy status logs port-forward test

help: ## list the targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

cluster-create: ## create the local kind cluster
	kind create cluster --name $(CLUSTER_NAME) --config deploy/kind-config.yaml --wait 2m

cluster-delete: ## delete the local kind cluster
	kind delete cluster --name $(CLUSTER_NAME)

image: ## build the node image and load it into kind
	docker build -f Dockerfile -t $(IMAGE) $(REPO_ROOT)
	kind load docker-image $(IMAGE) --name $(CLUSTER_NAME)

deploy: image ## build, load, and roll out the StatefulSet and services
	kubectl apply -f deploy/k8s.yaml
	kubectl rollout status statefulset/nodeakt --timeout=300s

undeploy: ## remove the StatefulSet and services, keep the kind cluster
	kubectl delete -f deploy/k8s.yaml --ignore-not-found

status: ## pods and services at a glance
	kubectl get pods -o wide
	kubectl get svc

logs: ## follow every pod's log, prefixed
	kubectl logs -l app.kubernetes.io/name=nodeakt -f --tail=100 --prefix

port-forward: ## the load-balanced HTTP API on localhost:8080
	kubectl port-forward service/nodeakt-api 8080:3000

test: ## assert every scenario against the already-deployed cluster
	./usecases.sh --deployed
