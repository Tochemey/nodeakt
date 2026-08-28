# Clustering

Clustering turns a set of single-node actor systems into one **cluster**: actors spawn, are addressed, and are messaged by name across every node, without a caller knowing which machine an actor runs on. A node opens to its peers, its name registry is distributed across them, and an actor on any node reaches an actor on any other through its name alone.

It is built on three things the runtime already has, joined by one new directory:

- **[Membership](membership.md)** keeps every node's view of who is in the cluster, using a gossip protocol with failure detection.
- A **distributed registry** shards the `name → owning node` directory across the cluster, so a lookup reads a name's record from whichever node owns that partition.
- **[Remoting](../remoting/index.md)** delivers `tell`, `ask`, `request`, `watch`, `forward`, and `pipeTo` to the owning node once its address is known.

One machine runs one logical actor system. A clustered node has a **cluster identity** (its data address) and an **actor identity** (its remoting address); it advertises the second in its membership metadata, so every node can map a cluster member to the endpoint its actors are reached at.

Example: [`examples/dns-actors`](https://github.com/Tochemey/nodeakt/blob/main/examples/dns-actors/README.md), a three-node cluster discovered over DNS under Docker Compose, driven by an HTTP API, with a crash-and-recover drill. Run it with `make actors`.

## Architecture

<svg role="img" aria-label="Each node runs the actor tree over the cluster placement, joined to peers by gossip, the sharded directory, and remoting" viewBox="0 0 720 340" style="width:100%;max-width:720px;height:auto;display:block;margin:24px auto;font-family:inherit">
  <defs>
    <marker id="arch-a" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 Z" fill="var(--vp-c-text-3, #929295)"/>
    </marker>
  </defs>
  <rect x="16" y="24" width="310" height="296" rx="10" fill="none" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="32" y="48" font-size="13" fill="var(--vp-c-text-2, #67676c)">Each node</text>
  <rect x="36" y="64" width="130" height="44" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="101" y="91" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Actor tree</text>
  <rect x="36" y="150" width="130" height="58" rx="8" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="101" y="173" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Cluster</text>
  <text x="101" y="193" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">placement</text>
  <path d="M101 108 V146" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#arch-a)"/>
  <rect x="196" y="64" width="110" height="44" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="251" y="91" text-anchor="middle" font-size="13.5" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Membership</text>
  <rect x="196" y="150" width="110" height="58" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="251" y="173" text-anchor="middle" font-size="13.5" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Registry</text>
  <text x="251" y="193" text-anchor="middle" font-size="13.5" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">shard</text>
  <rect x="196" y="236" width="110" height="44" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="251" y="263" text-anchor="middle" font-size="13.5" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Remoting</text>
  <path d="M166 168 L192 90" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#arch-a)"/>
  <path d="M166 179 H192" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#arch-a)"/>
  <path d="M166 190 L192 256" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#arch-a)"/>
  <rect x="548" y="142" width="160" height="84" rx="10" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <rect x="536" y="130" width="160" height="84" rx="10" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="616" y="177" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Other nodes</text>
  <path d="M306 80 L532 150" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#arch-a)"/>
  <text x="396" y="88" font-size="11.5" fill="var(--vp-c-text-2, #67676c)">gossip</text>
  <path d="M306 179 L532 172" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#arch-a)"/>
  <text x="418" y="164" text-anchor="middle" font-size="11.5" fill="var(--vp-c-text-2, #67676c)">sharded directory</text>
  <path d="M306 258 L532 192" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#arch-a)"/>
  <text x="408" y="240" text-anchor="middle" font-size="11.5" fill="var(--vp-c-text-2, #67676c)">actor messages</text>
  <rect x="380" y="30" width="150" height="40" rx="20" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="455" y="55" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Discovery</text>
  <path d="M455 70 V98 H310" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#arch-a)"/>
  <text x="463" y="88" font-size="11.5" fill="var(--vp-c-text-2, #67676c)">seeds at boot</text>
</svg>

Every node runs the same stack. The **cluster placement** is the layer this documentation is about: it wraps the actor tree and joins membership, the registry, and remoting so that spawning, addressing, and messaging work cluster-wide. Discovery seeds the node at boot; after that, the three fabrics (gossip membership, the sharded directory, and remoting) connect it to every peer.

## Enable it

Clustering is a construction-time option, `ClusterOptions`. A clustered node must be reachable for actor messages, so [`remote`](../remoting/index.md) is required alongside it: constructing a system with `cluster` set and no `remote` is a typed error (`ErrClusterRequiresRemote`), and a wildcard remoting host that peers cannot dial back is refused (`ErrClusterRequiresRoutableHost`).

```ts
import { ActorSystem, DnsDiscovery, DnsRecordType } from "@tochemey/nodeakt";

const system = new ActorSystem("orders", {
  remote: { host: "0.0.0.0", advertisedHost: "node-a.svc", port: 4000 },
  cluster: {
    discovery: new DnsDiscovery({ hostname: "orders.svc", recordType: DnsRecordType.address, port: 7946 }),
    gossipPort: 7946,
  },
});

await system.start();
```

- The node's **remoting endpoint** is its actor identity. Every local actor's path becomes `nodeakt://orders@node-a.svc:4000/name`, which is what other nodes use to reach it. Bind the wildcard host and set `advertisedHost` to the name peers resolve you by.
- [`discovery`](discovery.md) is the one required cluster field: how the node finds seed peers at boot. Everything else defaults.
- `stop()` leaves the cluster gracefully, then shuts the system down.

Every cluster call below rejects on a system created without a `cluster` configuration; `spawn`, `actorOf`, and the rest keep their single-node behavior.

## The model

| Concept | What it means |
| --- | --- |
| **Cluster-unique names** | A top-level actor name is unique across the whole cluster. `spawn("orders")` on one node and `spawn("orders")` on another race for the name; the loser is refused with `ErrActorAlreadyExists`. |
| **[Placement](placement.md)** | `spawn` keeps an actor local. `spawnOn` places it on the node a strategy chooses. Either way the `name → node` record goes in the distributed registry. |
| **[Singletons](singletons.md)** | `spawnSingleton` gives a name exactly one live instance cluster-wide, pinned to the coordinator, idempotent to create. |
| **[Location-transparent messaging](messaging.md)** | `actorOf` and `actorOfAsync` return a `PID` that routes to the owning node, wherever it runs. `tell`, `ask`, `watch`, and the rest keep their call sites. |
| **[Relocation](relocation.md)** | When a node departs, gracefully or by crash, the coordinator recreates its relocatable actors on the survivors from their stored recipes. |
| **[Events](events.md)** | `subscribe` delivers membership and relocation events (`NodeJoined`, `NodeLeft`, `RelocationCompleted`, and friends). |

## What clustering is not

- **Not a persistence layer.** The framework stores no actor state on disk, ever. A relocated actor is rebuilt from its recipe and recovers its own state in `preStart`; see [relocation](relocation.md#state-and-prestart).
- **Not authorized.** The trust model is a private network whose nodes trust each other, exactly as [remoting](../remoting/index.md) describes. Do not expose a gossip, data, or remoting port to an untrusted network.
- **Not a message-delivery guarantee across a move.** Messages in flight when an actor relocates are lost; the actor's identity and configuration come back, its in-flight mailbox does not.
