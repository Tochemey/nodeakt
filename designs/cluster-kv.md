# Distributed sharded key/value store: design

This is the design of the in-memory, partitioned, replicated key/value store that the cluster registry is built on, written for maintainers. It assumes the membership protocol in `designs/cluster-membership.md` is in place, and it turns that "who is in the cluster" signal into "where does each key live, and how does it survive a node leaving or dying".

This document is how the store works and why: the settled decisions, the package shape, the interfaces, the failure and repair behavior, how it scales, and how it is verified. The delivery plan, its slice ordering, and the build-risk assessment are tracked separately.

## 1. What it is, in one paragraph


Keys are hashed into a fixed set of **partitions**. Partitions are assigned to nodes by consistent hashing with bounded loads, and each partition is held by a **primary** plus a small number of **backups**. Writes go to the primary, which applies first and replicates to the backups. One node, the oldest live member, acts as **coordinator**: it computes the partition-to-node table and pushes it to everyone. When a node leaves gracefully its data is handed off before it goes; when a node crashes its data is already on the backups, so recovery promotes a survivor and reconciles the survivors rather than copying anything off the dead node. Divergence from lost messages, interrupted transfers, and healed splits is repaired continuously by background anti-entropy.

The store is built to scale with the cluster, not to a fixed size. Because the key space is partitioned and each node holds only its share, per-node memory and per-node repair cost stay bounded by a node's partition share rather than by the total record count, so the registry grows as nodes are added. The one component whose cost grows with cluster size is the coordinator, and section 10 states its bound and how the design keeps it cheap. It is internal infrastructure, not a user-facing database.

## 2. Settled decisions


Each of these was an open question; each is now decided, with the reasoning distilled to a sentence. The full argument is in the design notes.

| Decision                   | Choice                                                               | Why                                                                                                                       |
|----------------------------|----------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| Transport                  | Ride the existing `src/net/` RPC carrier                             | It already has correlation, chunking, flow control, TLS, and pooling; growing a third TCP stack would duplicate all of it |
| Durability                 | Primary/backup with reconcile, behind a `ReplicationGroup` interface | The smallest correct design for this workload; the interface keeps a consensus implementation reachable without a rewrite |
| Uniqueness under partition | Split-brain resolver: keep-majority with an oldest-member tiebreak   | Downing the losing half is how one authoritative coordinator stays single; consistent, local, and no new protocol over the membership view |
| Ordering timestamp         | Hybrid logical clock                                                 | A few bytes and a small counter remove the clock-skew hazard that raw wall-clock last-write-wins carries                  |
| Consistency defaults       | replicas 3, write quorum 2, read quorum 1                            | Durable against one failure by construction; reads stay on the fast primary-local path                                    |
| Partition count            | Operator-configured capacity knob, immutable after cluster formation | It must exceed the maximum node count by a healthy factor for balance, so a library serving clusters of any size cannot hard-code it |
| Fragment storage           | Plain in-memory `Map`                                                | Per-node memory tracks a node's partition share, not the whole registry, so it stays bounded as the cluster grows; a compact encoding is added only if a benchmark demands it |
| Resolver default           | Off for development, on for multi-node production, required for sharding | Stopping the only reachable half is worse than an impossible fork on one node; a fork corrupts at-most-one-instance once sharding rides on top |

**Explicitly out of v1 scope:** per-partition consensus, disk persistence, cross-datacenter replication, and grains. The `ReplicationGroup` interface is the one concession that keeps the first of those reachable later.

## 3. Consistency, stated plainly


The store is available under a network partition and resolves conflicts by last write wins on a hybrid logical clock. In a stable cluster it behaves consistently, because each key belongs to one partition whose single primary serializes every operation on it.

Two guarantees and one honest limitation:

- **No acknowledged write is lost when any single node fails.** Write quorum 2 puts every acknowledged write on a second node; crash recovery unions all survivors into the new primary, so the write is recovered whichever node died.
- **A local read on the primary is linearizable** in a stable cluster, because the primary is the single writer and applies before acknowledging. Read quorum 1 is therefore correct and cheap; higher read quorum is defense in depth, not a requirement.
- **A partition would fork the ownership authority if left alone.** During a split both halves elect a coordinator and accept writes, and the merge on heal discards one half. Split-brain resolution (section 9) closes this by making the losing half stop. This is the CAP choice, and for a name registry the correct one: a brief minority outage beats two nodes each believing they own the same name. It cannot be avoided, only chosen; the resolver is how the choice is made safely.

## 4. Where it lives


A new freestanding package, `src/kv/`, under the same boundary the codebase already enforces for its other internal packages:

- A module in `src/kv/` imports only platform modules or another module in `src/kv/`.
- No flat `src` module imports `src/kv/` except the `clustering.*` integration modules.
- The existing import-guard test gains a third package block, so the rule is enforced from the first commit.

The package is actor-blind, membership-blind, and transport-blind. It reaches the outside world only through interfaces it defines itself, so the transport and durability decisions never leak into it.

## 5. The interfaces


Four interfaces are the entire contract between `src/kv/` and the rest of the system. Everything else in the package is an implementation detail.

```ts
/** What the store needs to know about cluster membership. */
export interface ClusterView {
  readonly self: string;
  members(): readonly ClusterMember[];
  onChange(listener: (members: readonly ClusterMember[]) => void): () => void;
}

export interface ClusterMember {
  readonly name: string;
  readonly startedAt: number;   // decides the coordinator, oldest first
  readonly ready: boolean;      // has completed initial fragment intake
  readonly draining: boolean;   // is gracefully leaving
}

/** Addressed request/response with no ordering or delivery guarantee. */
export interface KvTransport {
  request(to: string, body: Uint8Array, deadlineMs: number): Promise<Uint8Array>;
  listen(handler: (from: string, body: Uint8Array) => Promise<Uint8Array>): void;
  close(): Promise<void>;
}

/**
 * The replication authority for one partition. The v1 implementation is
 * primary/backup with quorum acknowledgment and reconcile; a consensus
 * implementation could replace it without touching anything above it.
 */
export interface ReplicationGroup {
  propose(op: WriteOp): Promise<WriteResult>;   // conditional writes included
  read(key: string): Promise<Entry | undefined>;
  reconcile(peers: readonly string[]): Promise<void>;  // union survivors
  memberChange(owners: readonly string[]): void;
}
```

The `clustering.ts` module supplies the first three by adapting the membership engine and the `src/net/` carrier. The store supplies the fourth. `startedAt`, `ready`, and `draining` travel in the membership metadata bytes, which is what that opaque field exists for; the integration module owns that encoding and membership stays unaware of it.

## 6. Package layout


```
src/kv/
  ports.ts          the four interfaces above, plus shared value types
  errors.ts         the typed error family
  constants.ts      every tunable, internal, exported for tests only
  hlc.ts            hybrid logical clock
  hash.ts           partition hashing
  ring.ts           consistent hashing with bounded loads
  wire.ts           binary codec for every message and record
  entry.ts          the stored value, tombstones, last-write-wins comparison
  partition.ts      one fragment: its map, its rolling digest, its TTL
  store.ts          all fragments on one node, the janitor, chunked iteration
  table.ts          the versioned routing table
  engine.ts         single-node operations through the per-partition pipeline
  coordinator.ts    election, table computation, push, ownership reports
  replication.ts    the ReplicationGroup implementation, quorum acknowledgment
  antientropy.ts    digest comparison, bucketed escalation, read repair
  recovery.ts       departure, promotion, reconcile, refill, rebalance
  resolver.ts       the pure split-brain strategy: a view plus the last stable size yields a survive-or-stop verdict

test/kv/
  sim.ts            deterministic clock, transport, and membership harness
  *.test.ts         one suite per module, plus the scenario suites

src/
  clustering.ts     the integration: wires membership + net + kv into the API
```

## 7. How keys survive a node leaving or dying


This is the heart of the store, so it gets its own section. The single idea: **keys are owned by partitions, not by nodes, and replication has already placed each partition on more than one node.** Redistribution is therefore a routing decision plus, at most, a reconcile among the nodes that already hold copies. It is never a copy off the departed node, which for a crash is impossible anyway.

Three cases, three mechanisms:

**Graceful leave** is the only case that actively migrates, because the leaver is still running. It drains first and announces its membership departure last:

1. It sets a `draining` flag in its membership metadata. It stays a full member.
2. The coordinator publishes a new table demoting the leaver to a previous owner of each partition it held and naming a replacement. Writes move immediately; reads still fall back to the leaver. No data has moved yet.
3. The leaver streams each fragment to its replacement in bounded chunks; the receiver merges rather than replaces.
4. Once the transfers drain, the coordinator prunes the leaver, and only then does the leaver leave membership and exit. A drain that overruns its timeout falls back to the crash path.

**Crash** moves nothing off the dead node. On the death signal, for each partition the dead node owned:

1. A surviving replica is named the new primary. It need not be the most complete one.
2. The new primary reconciles with every other surviving replica, pulling in any write it is missing under last write wins. Because every acknowledged write was on a second node, unioning the survivors recovers all of them. This reconcile is proportional to how far the replicas diverged, which for a healthy partition is the handful of writes in flight at the moment of death, not the whole partition.
3. Until the reconcile completes, reads gather from all survivors, so a read cannot miss an acknowledged write.
4. The partition is now short a replica; the new primary refills a fresh backup in the background without blocking reads or writes.

Dead owners are pruned on the death signal, never by waiting for the departed node to report itself empty, which it never will.

**Rejoin after a partition** merges the two diverged sides by last write wins. Tombstones make deletes survive the merge, and a node that was away longer than the tombstone lifetime re-seeds from the current owner rather than merging, so it cannot resurrect keys whose delete records have already been reaped.

**The limit, stated honestly:** losing more owners of one partition at once than the replica count survives loses the writes that lived only on the nodes that died together. The replica count is the number of simultaneous failures the store is built to survive; nothing recovers what no living node holds.

## 8. Repair, so divergence never accumulates


Migration moves data on purpose. Repair fixes what migration and replication missed: writes dropped in async mode, a backup briefly unreachable, an interrupted transfer, a healed split. Three layers, cheapest first:

- **Read repair.** A read that consulted more than one holder pushes the winner to the stale ones, off the caller's path. Free, but touches only keys someone reads.
- **Background anti-entropy.** Each primary periodically compares a partition's digest with a replica's. The digest is a commutative rolling checksum kept up to date on every write, so two agreeing replicas cost one integer comparison. When they differ, one level of bucketing narrows the exchange to the keys that actually diverged. This is the real convergence guarantee.
- **Merge on ownership change.** Every fragment that arrives merges by last write wins and never replaces, which makes every transfer idempotent and restartable.

Every transfer obeys the same four rules, which is why an interrupted move, a doubled move, and a move to a node that dies mid-transfer are all non-events: the sender drops its copy only after acknowledgment, the receiver merges rather than replaces, transfers are therefore idempotent, and under-replication never blocks the foreground.

## 9. Split-brain resolution


A network partition is the one failure the recovery of section 7 does not cover, because it does not lose a node, it clones the cluster. Each half sees the other go unreachable and converges to a view of only itself, so each half elects a coordinator and accepts writes; on heal the halves merge by last write wins and one half's writes vanish. For the registry that is a duplicate name, and for sharding on top it is two live instances of one entity.

This cannot be engineered away. Under a partition a system either keeps both halves serving and accepts the fork, or keeps one half and stops the other; that is the availability-versus-consistency choice, not a bug. Resolution is the deliberate choice of consistency: the losing half shuts down until the partition heals, at a genuine availability cost on that half. The design's job is to make that choice safe and consistent, not to deny its cost.

The resolver runs at the clustering layer, over the membership view, and adds no new protocol. On any membership change that marks members unreachable, every node evaluates the same strategy on its own view and, if it loses, stops answering operations, relinquishes coordinator authority, and stops probing until it rejoins a healthy cluster. Three properties make it correct:

- **Local and consistent.** The halves cannot communicate, so each decides alone and the decisions must agree that at most one half survives. The default strategy, keep-majority, keeps a half running only if it holds a strict majority of the last stable cluster size. Both halves use the same denominator, so only one can pass.
- **Decided before the loser is forgotten.** The denominator is the member count from before the partition, so the resolver evaluates while the departed side is still present as suspect or newly dead, not after retention reaps it. The last stable size advances only when the view is quiet, so a partition in progress cannot move it.
- **Even splits break deterministically.** A 50/50 partition has no majority, so the tiebreak keeps the half containing the oldest member of the last stable view, computed by the same oldest-first rule the coordinator uses. Every node reaches the same verdict from the same facts.

Resolution removes the sustained fork but not the brief window before it fires, during which the losing half may have accepted writes that are discarded when it stops. Synchronous majority-quorum writes, the section 3 default, close that window from the other side by refusing to acknowledge a write the surviving majority never saw. The two are complementary.

It is configurable and, by default, off: `minimumMemberQuorum` of 1 disables the resolver, which is correct for single-node and development clusters where stopping the only reachable half is worse than a fork that a single node cannot produce. Any multi-node production deployment should enable keep-majority, and the cluster sharding layer requires it, because at-most-one-instance cannot survive a forked ownership authority. Keep-majority with the oldest-member tiebreak is the one bundled strategy; a static reference-node or external-arbiter strategy is left reachable but unbuilt.

## 10. Scaling


The store is built to scale with the cluster rather than to a fixed size. Because the key space is partitioned and each node owns only its share, the per-node costs, memory, replication, and anti-entropy, stay bounded by the partition count divided by the node count, not by the total record count. Adding nodes adds capacity, and the gossip-based membership layer beneath already scales to large clusters.

The coordinator is the one component whose cost grows with cluster size, because it recomputes and pushes the routing table on every membership change. Three things keep it cheap even for large clusters: the table is small and fixed in size, one row per partition and independent of the node count; pushes are diffed, so a single join ships only the partitions that moved rather than the whole table; and the periodic re-push is the safety net, so a missed diff heals with no extra protocol. This holds the coordinator to control-plane rates far below the message path. If a cluster ever outgrew a single coordinator, the additive escape hatch is gossip-assisted table dissemination, which does not disturb the ownership model. Sizing the partition count for the target maximum cluster is the one capacity decision that must be made up front, since it cannot change after the cluster forms.

## 11. Test strategy


The membership package's approach transfers directly, and it is the reason to be confident: a deterministic harness plus seeded scenario suites, so distributed failure modes are reproducible rather than flaky.

- **Unit.** Codec round-trips and malformed-input rejection on every decode path. Ring distribution and movement bounds. Last-write-wins including tombstones. HLC monotonicity. Pipeline serialization under concurrent conditional writes.
- **Scenario, each seeded, each printing its seed on failure.** A key is readable cluster-wide one table version after it is written. A join moves a bounded fraction of partitions and loses no key. A single-node kill loses no acknowledged write at the default replica settings. The post-crash reconcile recovers a write that lived on only one non-promoted survivor, which is the case a naive promotion would drop. A deleted key stays deleted across a replica that missed the delete and a full anti-entropy pass. A conditional write never succeeds twice for one key across a full join, kill, and rejoin campaign. A scan started before a rebalance returns every key that existed at its start. A healed split converges both sides to one value per key.
- **Split-brain, with the resolver enabled.** A partition into a majority and a minority stops the minority and keeps the majority serving, and both halves reach that same verdict independently. An even split keeps exactly the half holding the oldest member and stops the other, with no double-survivor and no double-stop. A stopped half rejoins cleanly when the partition heals and never keeps a duplicate claim. With the resolver disabled, the same partition is allowed to fork, proving the gate is what prevents it rather than luck.
- **Real sockets.** The chosen transport, including oversized fragment moves and a peer killed mid-transfer.
- **Multi-process.** Three processes over real TCP: populate the registry, kill one with a hard signal, assert every record it owned is still readable and that survivors converge on one owner per partition; restart it and assert it rejoins without duplicating a key.
- **Cross-runtime.** The existing smoke harness gains a three-node registry populate, kill, and read-back.
- **Coverage and cost.** Touched files hold 100% coverage. The throughput benchmark runs with clustering active to prove the store's timers and the coordinator's pushes cost nothing measurable on the message path.

## 12. Constants


Most are internal, exported for tests only, never public options. Public configuration is limited to the partition count, replica count, replication mode, read and write quorums, and minimum member quorum, because those are capacity and consistency choices an operator owns; everything else is an internal operational control.

The partition count is the one capacity decision that must be made before the cluster forms and cannot change afterward, since changing it re-hashes every key. The rule is to set it to roughly ten times the maximum node count the cluster will ever reach, so that bounded-load distribution stays even: too few partitions and large clusters leave nodes idle, too many and small clusters pay for per-partition machinery they do not need. It need not be prime, because the avalanche finisher on the hash already diffuses the low bits that the modulo reads. The default suits a moderate cluster; a deployment expecting hundreds or thousands of nodes raises it deliberately at formation time.

| Name                      | Value   | Note                                                                |
|---------------------------|---------|--------------------------------------------------------------------|
| `DEFAULT_PARTITION_COUNT` | 512     | Operator-configured; ~10x max node count, immutable after formation |
| `LOAD_FACTOR`             | 1.25    | Bounded-load ceiling multiplier                                    |
| `RING_POINTS_PER_MEMBER`  | 20      | Ring positions per member                                          |
| `DEFAULT_REPLICA_COUNT`   | 3       | Primary plus two backups                                           |
| `DEFAULT_WRITE_QUORUM`    | 2       | Acknowledge only after a second copy holds it                      |
| `DEFAULT_READ_QUORUM`     | 1       | Primary is authoritative                                           |
| `DEFAULT_MEMBER_QUORUM`   | 1       | 1 disables the resolver; keep-majority derives its threshold from the last stable size, not this number |
| `TABLE_PUSH_INTERVAL_MS`  | 60000   | Periodic heal re-push                                              |
| `REQUEST_TIMEOUT_MS`      | 5000    | Per RPC                                                            |
| `BOOTSTRAP_TIMEOUT_MS`    | 10000   | Initial table plus fragment intake                                |
| `FRAGMENT_CHUNK_BYTES`    | 262144  | Per move chunk                                                     |
| `SCAN_PAGE_SIZE`          | 256     | Entries per page                                                   |
| `SCAN_YIELD_EVERY`        | 1024    | Entries between event-loop yields                                  |
| `JANITOR_INTERVAL_MS`     | 30000   | TTL sweep                                                          |
| `REPAIR_INTERVAL_MS`      | 10000   | Per-partition anti-entropy tick                                    |
| `REPAIR_BUCKETS`          | 64      | Sub-digests per partition                                         |
| `TOMBSTONE_TTL_MS`        | 600000  | Also the stale-rejoin cutoff                                       |
| `LEAVE_DRAIN_TIMEOUT_MS`  | 30000   | Graceful-leave handoff backstop                                   |
| `MAX_KEY_BYTES`           | 1024    |                                                                    |
| `MAX_VALUE_BYTES`         | 1048576 |                                                                    |
