# DNS-discovered three-node cluster

A runnable three-node cluster that finds its peers through DNS, forms over TCP, and exercises the distributed key/value store end to end. It proves the cluster runtime across separate processes on a real network: discovery, formation, distributed writes and reads that route to the owning partition, a conditional write that keeps a key unique, a cluster-wide scan, live cluster events, split-brain resolution, crash recovery, and graceful leave.

Every replica runs the same program ([`node.ts`](./node.ts)). On boot it resolves one DNS name that returns every replica's address, joins over those addresses, and then serves an HTTP API while writing a heartbeat every few seconds and scanning the whole cluster.

This is the actor-agnostic store the actor system builds its distributed registry and placement on. The example drives it directly to prove the layer underneath.

## How discovery works here

The three services share the network alias `nodeakt`. Docker's embedded DNS answers an `A` lookup of a shared alias with the IP of every container that carries it, so `nodeakt` resolves to all three replicas. `DnsDiscovery` reads that record and attaches the fixed gossip port to each address, producing the peer addresses a node joins. This mirrors a Kubernetes headless service, whose name resolves to its pod IPs.

Each node also advertises its own service name (`node1`, `node2`, `node3`), which resolves to that one container, so once nodes have exchanged membership they dial each other directly. A cold start races: a node that resolves only itself anchors a fresh cluster, and later nodes merge into it as the record fills in. No node is special.

## Run it

From the repository root:

```sh
docker compose -f examples/dns-cluster/docker-compose.yml up --build
```

Each node logs its boot, the members and coordinator it sees, and, every five seconds, the peer heartbeats it can read. Because each heartbeat routes to the partition that owns its key (usually another node), a log line like `reads peer heartbeats from [node2, node3]` on `node1` is proof that it is reading keys written by `node2` and `node3`: the store is genuinely distributed. A node never reports its own heartbeat, only its peers'.

To boot the cluster and assert every use case below automatically (formation, distributed placement, scan, unique claim, graceful leave, rejoin, and crash recovery), then tear it down, run `make cluster` from the repository root. It drives the same HTTP API this document describes.

## The HTTP API

Each node's API is published on the host: node1 on `3001`, node2 on `3002`, node3 on `3003`.

| Request                          | What it does                                                     |
|----------------------------------|------------------------------------------------------------------|
| `GET /health`                    | This node's address, whether it joined, and the coordinator      |
| `GET /members`                   | Every member this node sees                                      |
| `PUT /kv?key=K&value=V`          | Distributed write, routed to the partition that owns `K`         |
| `PUT /kv?key=K&value=V&unique=1` | Conditional write: applies only if `K` is unset (`applied` says) |
| `GET /kv?key=K`                  | Distributed read, routed to the same owner                       |
| `GET /keys`                      | Cluster-wide scan of every live key and value                    |

## Distributed key/value

A value written through one node is readable through any other, because it lives on the partition that owns its key, not on the node you happened to ask:

```sh
curl -s -X PUT "localhost:3001/kv?key=color&value=blue"   # write through node1
curl -s      "localhost:3003/kv?key=color"                # read through node3 -> blue
curl -s      "localhost:3002/keys"                         # scan the whole cluster from node2
```

## Keeping a key unique

A conditional write applies only if the key is unset, so two nodes racing to claim one key cannot both win. This is the store primitive a distributed name registry is built on:

```sh
curl -s -X PUT "localhost:3001/kv?key=leader&value=node1&unique=1"   # -> {"applied": true}
curl -s -X PUT "localhost:3002/kv?key=leader&value=node2&unique=1"   # -> {"applied": false}
curl -s      "localhost:3003/kv?key=leader"                          # -> node1, from any node
```

## Failure scenarios

- **Graceful leave.** `docker compose -f examples/dns-cluster/docker-compose.yml stop node1` sends `SIGTERM`; node1 drains its partitions to the survivors and leaves membership before it exits. The other two log `node-left`, keep serving, and `/members` on them drops to two.
- **Crash recovery.** `docker kill nodeakt-dns-cluster-node2-1` removes a node abruptly. The survivors detect the death, promote its partitions from their replicas, log the departure once the repair settles, and keep serving every key.
- **Split-brain.** The member quorum is set to two, so a node cut off from the other two cannot reach a majority and stops serving rather than accept writes that would fork the cluster. Its heartbeat log switches to `cluster not serving`, and it resumes when the partition heals. Cut a node off with `docker network disconnect nodeakt-dns-cluster_default nodeakt-dns-cluster-node3-1` and reconnect it to watch it stop and recover.

Bring it all down with `docker compose -f examples/dns-cluster/docker-compose.yml down`.

## A note on scope

This example drives the cluster runtime directly to prove it works end to end. The runtime is internal infrastructure: an application does not use it directly but through the actor system, which builds on it for distributed placement, the name registry, singletons, and cluster events. This is the proof of the layer underneath.
