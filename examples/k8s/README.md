# Distributed actors on Kubernetes

The [dns-actors](../dns-actors/README.md) cluster, deployed on Kubernetes. Every pod of a StatefulSet runs the same program and image. Actors are spawned with cluster-wide unique names, spread across the cluster by a placement strategy, or claimed as singletons; a caller on any pod reaches an actor on any other by name. When a pod dies, hard or gracefully, the coordinator recreates its relocatable actors on the survivors, and scaling the StatefulSet grows and shrinks the actor cluster live.

Discovery is plain DNS: no Kubernetes API client, no RBAC, no operator. A headless service's name resolves to the address of every pod behind it; `DnsDiscovery` reads that record, attaches the fixed gossip port to each address, and the node joins over the result. The same program runs under Docker Compose in [dns-actors](../dns-actors/README.md), where a shared network alias plays the headless service's part; only the environment differs.

## How discovery works here

Three manifest details in [deploy/k8s.yaml](deploy/k8s.yaml) make DNS discovery work on Kubernetes:

- **`publishNotReadyAddresses: true` on the headless service.** A pod must be visible in DNS before it is ready, because readiness here means "sees the member quorum", which needs discovery first. Without it no pod could ever find a peer.
- **`podManagementPolicy: Parallel` on the StatefulSet.** All pods start together. The default ordered start would wait for pod 0 to become ready before creating pod 1, and pod 0 alone can never reach the member quorum of two.
- **Fully qualified names.** The provider queries DNS records directly, and a direct record query does not walk the `resolv.conf` search path, so the manifest passes `nodeakt.<namespace>.svc.cluster.local`, never a bare `nodeakt`.

Each pod advertises its own IP, handed to it by the downward API, and that IP is its member identity. The stable per-pod DNS name a StatefulSet offers is deliberately not used: member identity must die with the process. A replaced pod gets a fresh IP, so the survivors detect the old member's death and relocate its actors, and the replacement joins as a new member; under the stable name the replacement would answer on the dead member's address and silently impersonate it, and no death would ever be declared. The provider can also read the `SRV` records a headless service publishes per named port; this example uses the `A`-record mode with a fixed gossip port, the same configuration as the Compose examples.

## Run it

Prerequisites: [Docker](https://docs.docker.com/get-docker/), [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation), and [kubectl](https://kubernetes.io/docs/tasks/tools/).

From the repository root, one command creates a local Kubernetes cluster in Docker, builds and deploys the image, asserts every scenario below, and tears everything down again:

```sh
make k8s
```

Or step by step, from this directory:

```sh
make cluster-create   # a local Kubernetes cluster in Docker
make deploy           # build the image, load it into kind, roll out deploy/k8s.yaml
make status           # three nodeakt pods, two services
make logs             # every pod's log, prefixed
make port-forward     # the API on http://localhost:8080
make test             # assert every scenario against the deployed cluster
make cluster-delete   # tear the whole thing down
```

## The HTTP API

`nodeakt-api` is an ordinary ClusterIP service over the pods' HTTP port: Kubernetes routes each request to one ready pod, so any pod answers for the whole cluster. That is the point of the example: the API is location-transparent, and the actor you reach does not live on the pod you happened to ask.

| Method | Path                 | Effect                                                      |
|--------|----------------------|-------------------------------------------------------------|
| GET    | `/health`            | this pod's identity and the member count it sees             |
| GET    | `/ready`             | 200 once this pod sees the member quorum, 503 before         |
| PUT    | `/workers/:name`     | spawn a worker on this pod, unique across the cluster        |
| PUT    | `/spread/:name`      | place a worker on the pod the round-robin strategy chooses   |
| PUT    | `/singletons/:name`  | claim the one cluster-wide singleton worker of that name     |
| GET    | `/where/:name`       | which pod currently hosts the named worker                   |
| GET    | `/greet/:name?who=X` | a greeting from the named worker, wherever it runs           |

`/ready` doubles as the manifest's readiness probe, so a pod that is still forming, or cut off in a minority partition, drops out of the API service until it can serve the cluster again. `?region=X` on a spawn tags the worker; it is construction data, so it travels in the worker's recipe and survives a relocation.

## Try the scenarios

With `make port-forward` running, every request below goes to `localhost:8080` and lands on whichever pod Kubernetes picks, which is the demonstration itself:

```sh
# Spawn a worker somewhere, then reach it by name through any pod.
curl -X PUT localhost:8080/workers/orders?region=eu
curl localhost:8080/where/orders           # -> the IP of the pod hosting it
curl localhost:8080/greet/orders?who=ada    # -> "eu:ada", wherever the request lands

# Spread three workers across the cluster with the placement strategy.
for n in a b c; do curl -X PUT localhost:8080/spread/$n; done
for n in a b c; do curl localhost:8080/where/$n; done   # different pods

# One singleton, the same host every time you ask.
curl -X PUT localhost:8080/singletons/sequencer
curl localhost:8080/where/sequencer
```

To pin a request to one pod, port-forward that pod directly, for example `kubectl port-forward pod/nodeakt-1 3101:3000`.

Failure and elasticity drills:

```sh
# Relocation after a crash: force-delete the pod hosting orders, no graceful leave.
# The survivors detect the death and the coordinator recreates the actor from its
# recipe; the StatefulSet independently brings a fresh pod back under the same name.
kubectl delete pod nodeakt-0 --grace-period=0 --force
curl localhost:8080/where/orders           # -> a survivor's IP, after a few seconds
curl localhost:8080/greet/orders?who=ada    # -> "eu:ada", its region intact

# Graceful leave: an ordinary delete sends SIGTERM, the node hands its actors to
# the survivors before it exits, and the replacement pod rejoins.
kubectl delete pod nodeakt-2

# Elasticity: new pods appear in the headless service's DNS and join; scaling down
# leaves gracefully and relocates whatever the departing pods hosted.
kubectl scale statefulset/nodeakt --replicas=5
curl localhost:8080/health                 # -> "members":5, after the pods boot
kubectl scale statefulset/nodeakt --replicas=3
```

Watch it happen with `make logs`: every pod prints its cluster events, membership changes, and the actors it receives through relocation.

## Scope

The example demonstrates DNS discovery on Kubernetes plus placement, location-transparent messaging, singletons, and relocation. The worker is stateless beyond its region, so a fresh start after relocation is all it needs, which is the relocation contract: identity and configuration travel, in-flight messages during a move are lost. A worker that must recover accumulated state would reload it in `preStart` from its own source of truth.

Cluster and discovery types are internal, so the entry point imports them from `src` directly; a published-package consumer uses the public actor-system surface.
