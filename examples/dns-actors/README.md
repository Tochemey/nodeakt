# Distributed actors over a DNS cluster

A three-node cluster of full actor systems, discovered over DNS with Docker Compose.
Actors are spawned with cluster-wide unique names, spread across the cluster by a
placement strategy, or claimed as singletons; a caller on any node reaches an actor
on any other by name. When a node departs, gracefully or by a hard kill, the
coordinator recreates its relocatable actors on the survivors, and lookups on the
survivors reach them at their new home.

Each node runs the same image and joins the shared network alias `nodeakt`, so an
A-record lookup of that name returns all three container IPs, the headless-service
pattern. Each node advertises its own service name and drives an ActorSystem with
clustering enabled, exposing a small HTTP API to place and reach actors.

## Run it

```sh
docker compose up --build          # from this directory
# or, from the repo root:
make actors                        # boots the cluster and asserts every use case
```

Node HTTP ports are published as `3001`, `3002`, `3003` (node1/2/3).

## HTTP API

| Method | Path                 | Effect                                                        |
|--------|----------------------|--------------------------------------------------------------|
| GET    | `/health`            | readiness, with the member count this node sees              |
| PUT    | `/workers/:name`     | spawn a worker on this node, unique across the cluster        |
| PUT    | `/spread/:name`      | place a worker on the node the round-robin strategy chooses   |
| PUT    | `/singletons/:name`  | claim the one cluster-wide singleton worker of that name      |
| GET    | `/where/:name`       | which node currently hosts the named worker                   |
| GET    | `/greet/:name?who=X` | a greeting from the named worker, wherever it runs            |

`?region=X` on a spawn tags the worker; it is construction data, so it travels in the
worker's recipe and survives a relocation.

## Try the scenarios

```sh
# Spawn a worker on node1, then reach it from node2 and node3 by name.
curl -X PUT localhost:3001/workers/orders?region=eu
curl localhost:3002/where/orders          # -> the node hosting it
curl localhost:3003/greet/orders?who=ada   # -> "eu:ada", answered across nodes

# Spread three workers across the cluster with the placement strategy.
for n in a b c; do curl -X PUT localhost:3001/spread/$n; done
for n in a b c; do curl localhost:3002/where/$n; done   # different hosts

# One singleton, reached by the same name from every node.
curl -X PUT localhost:3001/singletons/sequencer
curl localhost:3002/where/sequencer
curl localhost:3003/where/sequencer        # same host from both

# Relocation after a crash: hard-kill the node hosting a worker with SIGKILL, so it
# dies with no graceful leave; the survivors detect the failure and recreate it.
HOST=$(curl -s localhost:3002/where/orders | sed 's/.*"host":"\([^"]*\)".*/\1/')
CID=$(docker compose ps -q "$HOST")
docker update --restart=no "$CID"          # so the crashed node stays down
docker kill --signal=KILL "$CID"           # hard-kill it, no graceful leave
sleep 30                                    # let the coordinator recover it
curl localhost:3002/where/orders           # -> a survivor now hosts it
curl localhost:3003/greet/orders?who=ada   # -> "eu:ada", its region intact
```

A graceful leave takes the same recovery path: `docker compose stop node1` sends
SIGTERM, and node1 leaves the cluster before it departs so the coordinator recreates
its actors on the survivors.

## Scope

The example demonstrates placement, location-transparent messaging, singletons, and
relocation. A worker that must recover accumulated state after a relocation would
reload it in `preStart` from its own source of truth (see the framework's design on
entity state); the worker here is stateless beyond its region, so a fresh start is all
it needs, which is the relocation contract: identity and configuration travel, in-flight
messages during a move are lost.

Cluster and discovery types are internal, so the entry point imports them from `src`
directly; a published-package consumer uses the public actor-system surface.
