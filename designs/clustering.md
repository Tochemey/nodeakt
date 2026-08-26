# Clustering integration and node discovery: design and delivery plan

This is the plan for the last piece of the distributed store: the integration layer that turns the finished, transport-blind `src/kv/` package plus the SWIM membership engine and the TCP carrier into a running cluster of real nodes. It is slice 12 of the store's delivery plan, and it is the only slice that touches the network transport decision. It also introduces **node discovery**, the pluggable mechanism by which a fresh node finds the cluster to join, so an operator can run nodeakt on a static host list, in a container orchestrator, or behind any service registry, without the core taking on a single third-party dependency.

## 1. What this layer does

`src/kv/` is deliberately blind to transport, membership, and discovery. It reaches the outside world through two interfaces it defines itself and supplies a third:

| Interface          | Meaning                                                      | Supplied by                         |
|--------------------|--------------------------------------------------------------|-------------------------------------|
| `ClusterView`      | who is in the cluster, oldest first, with `ready`/`draining` | the membership adapter (this layer) |
| `KvTransport`      | addressed request/response with no delivery guarantee        | the TCP adapter (this layer)        |
| `ReplicationGroup` | the per-partition durability authority                       | the store itself (already built)    |

Everything the store does, routing, replication, crash recovery, drain, refill, anti-entropy, and the split-brain verdict, is already implemented and tested against an in-memory harness. This layer supplies the two missing interfaces over real sockets and real gossip, drives the bodies the store exposes on real timers, applies the resolver verdict, and exposes the registry API the actor runtime calls. No new distributed algorithm is introduced here; it is wiring, adaptation, and the one policy the store deliberately left to the layer that owns the membership view.

The package is a flat `src/` module (`clustering.ts` and companions), the one place the boundary rule permits to import `src/kv/`. It also imports the membership engine, the TCP carrier, and the discovery module.

## 2. Node discovery

Discovery has exactly one job, and it happens once. At boot a node asks a `DiscoveryProvider` for the seed contact points to join, hands them to the membership engine, and is done; it is never consulted again. Every topology change after that, a node joining, leaving, failing, or changing its metadata, flows through membership gossip and reaches this layer as a membership event, not through discovery. Discovery finds the door; membership runs the house. Solving topology is the whole reason the membership engine exists, so discovery must not duplicate it.

```ts
/** Resolves the seed contact points a node attempts to join at boot, as host:port strings. */
export interface DiscoveryProvider {
  /**
   * The seeds this node should try to join at startup. May return an empty list
   * while the environment is still coming up; bootstrap retries until it is not
   * empty. Consulted only during boot, never for topology once the node has joined.
   */
  resolve(): Promise<readonly string[]>;
}
```

Built-ins use only platform modules, no dependency: `StaticDiscovery` over a fixed `host:port` list (the fallback, and the provider tests and single-host deployments use), and `DnsDiscovery` over an `SRV` or `A` lookup through `node:dns` (how a headless service in a container orchestrator exposes its pods). A Kubernetes API lookup, a NATS or Consul registry, or a cloud instance query is a user-implemented `DiscoveryProvider`; those may pull whatever client they need, the nodeakt core never does, and the repository ships one or two as documented examples under `examples/`.

**Bootstrap.** The boot sequence is short and leans entirely on the membership engine's push-pull join to converge:

1. `resolve()` the seeds; if the list is empty, retry on a short interval until it is not, since a still-starting environment may have nothing registered yet.
2. `start()` the membership engine, then `join(seeds)`. A join is a table exchange that **merges** two member tables, so it both attaches to an existing cluster and fuses two nodes that each believed they were alone. On a failure, a seed whose listener is not up yet, it retries against the full seed list until it reaches a peer or a boot deadline elapses; a node that reaches no peer by the deadline anchors a fresh cluster that later joiners merge into. Consistent seeds across nodes plus retry-until-reached is what makes a simultaneous cold start converge to one cluster.
3. Once joined and its initial fragment intake is complete, the node marks itself `ready` and hands topology over to membership for good.

The bootstrap is a freestanding piece driven by a `Clock`, so both retry loops are unit-testable without real DNS or sockets.

## 3. The transport adapter: `KvTransport` over `src/net/`

The store frames its own protocol bytes, so the adapter needs only an addressed request that returns a correlated reply, plus an inbound handler. The `src/net/` package supplies exactly that, actor-blind, with correlation, connection pooling, reconnect, flow control, and TLS already solved.

- **Outbound.** One `Peer(host, port, localHello)` per target member, cached in a `Map<name, Peer>` keyed by the member's `host:port`. `request(to, body, deadlineMs)` wraps `body` in a `DataEnvelope` (a fixed control path in `to`, the store bytes in `payload`) and calls `peer.ask(envelope, deadlineMs)`, returning the reply's `payload`. The peer lazily dials and reuses one pooled connection per lane, and rejects on deadline, which is exactly the "no delivery guarantee, surfaced as a timeout" contract the store is written against.
- **Inbound.** One `NetServer.listen({ host, port, local }, handlers)`. In `handlers.onData(session, envelope, correlation)` the adapter hands `envelope.payload` and the peer's `host:port` (from `session.remote`) to the store dispatch, then answers with `session.reply(correlation, replyEnvelope)` carrying the response bytes, or `session.replyError` on a decode failure.
- **TLS** is the carrier's `TlsConfig` passed straight through; no store code changes.
- **`close()`** closes every cached `Peer` and shuts the `NetServer` down.

One sizing note: the carrier negotiates a maximum message size in its `Hello` handshake, and a fragment chunk is up to `FRAGMENT_CHUNK_BYTES` (256 KiB), so the adapter sets the negotiated maximum above that. Large fragment transfers ride the carrier's large-transfer lane, which is what it is for.

## 4. The membership adapter: `ClusterView` over `Swim`

The store's `ClusterView` wants live members **oldest first**, each carrying a stable `startedAt` and the `ready`/`draining` flags. `Swim` returns detached `MemberRecord` snapshots in map insertion order, including dead and left records, and carries only an opaque metadata blob. The adapter bridges the gap.

- **Identity and order.** A member's `name` is its `host:port` (`record.member`). `startedAt` is **not** the record's `stateChangeTime`, which moves when a node is refuted and would reorder the coordinator under churn; it is a process-start timestamp the node writes into its **metadata** once at construction and never changes. The adapter decodes it and sorts alive members by `(startedAt, name)`, so `members()[0]` is the stable oldest member the coordinator and resolver rely on.
- **Liveness.** `members()` keeps the `ALIVE` and `SUSPECT` records mapped to `ClusterMember` and drops only `DEAD` and `LEFT`, so a node drops out on confirmed death and crash recovery and coordinator pruning fire exactly as their tests assume. Suspicion is deliberately treated as still present: it is silent in the event stream and false suspicions are refuted routinely, so excluding a suspect node would make its presence depend on an unrelated event firing and would flap the coordinator whenever the oldest member was briefly suspected. Reacting on death, not suspicion, matches how recovery and the resolver already fire.
- **Metadata codec.** A tiny fixed record: a `u64` `startedAt` and a flags byte for `ready` and `draining`. The adapter encodes it for this node and decodes it for every peer. This is the only thing the opaque membership metadata carries for the store.
- **Change notification.** `onChange` bridges the single synchronous `SwimOptions.onEvent` callback; `joined`, `left`, `dead`, and `updated` all re-emit the current `members()` snapshot to the store's listener.

### The one membership addition this requires

`Swim` fixes metadata at construction and exposes no way to change it afterward, but `ready` flips true once a joining node finishes intake and `draining` flips true on a graceful leave, and both must **gossip** so the coordinator sees them. The primitives already exist privately (a local-truth apply plus an incarnation bump with the self-defense that refutes stale echoes); this layer requires a small, reviewed public method on the membership engine:

```ts
// added to Swim: re-announce self at a higher incarnation with new metadata bytes.
updateMetadata(metadata: Uint8Array): void;
```

It is the single change to the already-reviewed membership package the whole slice needs, and it is a focused addition over machinery that is already there. It is called exactly twice by this layer: to set `ready` after bootstrap intake, and to set `draining` at the start of a graceful leave.

## 5. The clustering engine

`clustering.ts` owns one `Cluster` object per node that constructs and wires the store's pieces and drives them. It holds the `Engine`, the `Coordinator`, the `Replicator`, the `Recovery` orchestrator, an `AntiEntropy`, the `KeepMajorityResolver`, the `KvTransport` adapter, and the `ClusterView` adapter over `Swim`.

**Membership is the only topology signal.** After boot nothing polls for who is in the cluster. The engine subscribes to the membership engine's events, and every reaction hangs off them: a `joined`, `left`, or `dead` event republishes the view to the store's `ClusterView` listener and re-derives the coordinator, which is simply the oldest live member. There is no election. When the coordinator leaves, the next-oldest node's `Coordinator.isCoordinator()` becomes true on the very same event and it takes over, recomputing and pushing the table; every node reaches the identical conclusion from the identical view. The engine turns these membership events into the cluster's own lifecycle events (section 6) and drives the timers and the resolver below off the same signal, never off discovery.

**Canonical inbound dispatch.** The transport's single `listen` handler is the one authoritative router from a message kind to its server, replacing the per-test hand-rolled routers the store's unit tests use today. It decodes the envelope, reads the message kind, and dispatches: table pushes to `Coordinator.receive`; writes, reads, peeks, replicate, and fragment pull/push to `Replicator.receive`; the anti-entropy digest, key-versions, and entries requests to `AntiEntropy.receive`. This is the single source of truth the store was designed to be driven by, and every unit test's ad-hoc handler is retired in favor of it.

**Timers.** Every body the store exposes for the integration to clock is driven here, off an injected `Clock` so the whole engine is deterministic under the simulation harness:

- `Coordinator.rebalance()` on every membership change and on `TABLE_PUSH_INTERVAL_MS`.
- `Store.sweep()` on `JANITOR_INTERVAL_MS`.
- One `AntiEntropy.sync(partition, peer)` per tick on `REPAIR_INTERVAL_MS`, cycling a primary's partitions against their replicas.
- On each table this node accepts: `Replicator.install(table, ring)` first, then `Recovery.onTable(table, ring)` for crash reconcile and refill, then `Recovery.drain(table, ring)` whenever this node has been demoted from a partition it still holds. The ring installed into the `Replicator` and the ring the coordinator computes are both built from the **non-draining** members, so every node's placement agrees.

**The resolver, applied.** On any membership change that marks members unreachable, the engine computes this half's reachable set and its last stable size and asks the `KeepMajorityResolver`. On a stop verdict it gates the store, so every operation rejects with `ClusterUnavailableError`, relinquishes coordinator authority, and steps the node's probing down until it rejoins a healthy cluster. The **last stable size** is tracked here: it advances only when the view has been quiet for a settle window, so a partition in progress cannot move the denominator under the decision. The verdict is the store's pure function; this is where it drives real state.

**The `NodeLeft` ordering gate.** A `NodeLeft` cluster event is published only after the departed node's partitions have been repaired, so a downstream rebalance does not scan the registry before a dead node's records have been promoted from their backups. The gate is a repair-epoch wait with a hard timeout backstop, since after an abrupt kill the repair can wedge on fragment moves addressed to the dead member and must not suppress the event forever.

**The stale-rejoin trigger.** On a rejoin, the engine measures how long this node was away against `Recovery.shouldReseed`; past the tombstone window it drops each fragment and re-seeds from the current owner rather than merging stale keys back to life.

## 6. The registry API

On top of the distributed store the layer exposes the registry the actor runtime consumes. Each call is a thin mapping onto a store operation:

| Registry call                                           | Store operation                                                |
|---------------------------------------------------------|----------------------------------------------------------------|
| `claimActorName(name, address)`                         | conditional put, `nx`; a rejection is the duplicate-name error |
| `putActor` / `getActor` / `removeActor` / `actorExists` | put / read / delete / read                                     |
| `nextRoundRobinValue(key)`                              | atomic increment                                               |
| `claimOnce(key, ttl)`                                   | conditional put with a TTL, for singletons and schedule claims |
| `actorsByHost(host)` / `countActorsByHost()`            | cursor-paged scan, filtered on the owner during the walk       |
| `peers()` / `members()` / `isCoordinator()`             | membership view accessors                                      |

Cluster lifecycle reaches the runtime's event stream: `NodeJoined`, `NodeLeft` (behind the ordering gate above), `CoordinatorChanged`, `RebalanceStarted`, `RebalanceCompleted`. This is the one part of the layer that touches the actor runtime; the cluster runtime beneath it (sections 2 to 5) is actor-blind and depends only on the store, membership, net, and discovery.

## 7. Delivery: sub-slices

Slice 12 is the largest integration surface in the store, so it lands as sub-slices in dependency order, each with tests, 100% coverage on touched files, and clean `tsc` and `biome`, in the cadence the earlier slices used.

- **12a. Discovery.** `DiscoveryProvider`, `StaticDiscovery`, `DnsDiscovery`, and the boot-only bootstrap: resolve-retry until seeds appear, then join-retry until a peer is reached or a boot deadline anchors a fresh cluster. No polling once joined. Freestanding, zero dependency, driven by an injected `Clock`. Tested against a fake provider and a scripted clock; no real network.
- **12b. Membership metadata and the view adapter.** The `Swim.updateMetadata` addition with its own membership tests, the `startedAt`/`ready`/`draining` metadata codec, and the `ClusterView` adapter with its oldest-first ordering and liveness filter. Tested against a real `Swim` on the loopback and against the membership harness.
- **12c. Transport adapter.** `KvTransport` over `Peer` and `NetServer`, the peer cache, envelope wrapping, TLS passthrough, and close. Tested over loopback sockets, including an oversized fragment move and a peer dropped mid-request.
- **12d. The clustering engine.** The `Cluster` object: canonical dispatch, the timers, the resolver application with the last-stable-size tracker, the `NodeLeft` ordering gate, the reseed trigger, and drain-on-demotion. Tested end to end against the store's simulation harness for the departure and split-brain scenarios, now driven through the real dispatch rather than per-test routers.
- **12e. Registry API and events.** The section 6 surface and the cluster events onto the runtime event stream. This is the actor-facing layer.
- **12f. Real-socket and multi-process proof.** A runnable `examples/` cluster that forms over TCP with `StaticDiscovery` and demonstrates a write, a cross-node read, a hard kill with recovery, a graceful drain, and a split-brain stop. The cross-runtime smoke harness gains a three-node populate, kill, and read-back.

## 8. Testing strategy

The simulation harness that proved slices 8 to 11 carries most of 12d: seeded scenarios reproduce a kill, a drain, a rejoin, and a partition from a printed seed, now exercised through the real dispatch. 12a, 12b, and 12c each add their own focused tests, membership on the loopback and the harness, transport over real sockets. 12f adds the two things a simulation cannot prove: real TCP under an oversized transfer and a mid-transfer kill, and three separate processes forming, losing a member to a hard signal, and reading every record it owned back from the survivors. Every touched file holds 100% coverage, and the throughput benchmark runs with clustering active to confirm the timers and the coordinator's pushes cost nothing measurable on the message path.

## 9. Open decisions to confirm before building

- **OD1 (membership addition).** Slice 12 needs `Swim.updateMetadata` so `ready` and `draining` gossip. This is one focused, reviewed method added to the already-reviewed membership package. The alternative, immutable metadata plus a separate side channel for the two flags, duplicates gossip and is worse. Recommendation: add the method.
- **OD2 (built-in discovery scope).** Ship `StaticDiscovery` and `DnsDiscovery` in the zero-dependency core, and Kubernetes and NATS as `examples/` implementations of the interface rather than core modules. Recommendation: as stated.
- **OD3 (registry scope).** The registry API (12e) is the one actor-coupled part. It can land in this slice, or the slice can stop at a working distributed key/value cluster (12a to 12d, 12f) and the registry facade can be its own follow-on once the cluster runtime is proven. Recommendation: build the cluster runtime first, then the registry, so the actor coupling does not gate the distributed-store proof.
- **OD4 (decomposition).** The 12a to 12f order above. Confirm or adjust.
