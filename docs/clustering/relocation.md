# Relocation and recovery

When a node departs the cluster, its actors go with it. **Relocation** is how a departed node's actors come back: the [coordinator](membership.md#the-coordinator) recreates them on the surviving nodes from the recipes they were spawned with. It is what keeps a distributed actor reachable after the machine it ran on is gone.

Three things move when a cluster changes, and it helps to keep them apart:

- **Rebalancing** redistributes the registry's own data on every membership change. It is transparent to actors: a lookup still resolves a name to its current owner.
- A **join** never moves an actor; it only widens where future placements can land.
- **Relocation** recreates an actor instance, and only when the node it ran on **departs**.

## Graceful leave and crash are the same path

There is no separate handoff protocol. Whether a node calls `stop()` (a graceful leave) or its process is killed (a crash), it leaves its placement records in the registry, and the coordinator recreates its relocatable actors from those records. The only difference is how the survivors learn of the departure: a graceful leave is announced over gossip, a crash is found by the [failure detector](membership.md). Recovery does not care which.

## The recovery flow

Every survivor sees the departure, but **only the coordinator acts**. It makes one scan of the registry that does three jobs at once, then places the orphaned actors and recreates them.

<svg role="img" aria-label="Recovery flow: a departed node's actors are classified and recreated on survivors" viewBox="0 0 720 320" style="width:100%;max-width:720px;height:auto;display:block;margin:24px auto;font-family:inherit">
  <defs>
    <marker id="rel-a" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 Z" fill="var(--vp-c-text-3, #929295)"/>
    </marker>
  </defs>
  <rect x="16" y="30" width="136" height="44" rx="22" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="84" y="57" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Node departs</text>
  <rect x="192" y="24" width="200" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="292" y="47" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Coordinator scans</text>
  <text x="292" y="67" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">the registry once</text>
  <rect x="442" y="24" width="170" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="527" y="47" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Classify</text>
  <text x="527" y="67" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">each actor</text>
  <path d="M152 52 H188" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#rel-a)"/>
  <path d="M392 52 H438" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#rel-a)"/>
  <path d="M527 80 V102" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5"/>
  <path d="M110 102 H610" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5"/>
  <path d="M110 102 V124" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#rel-a)"/>
  <path d="M360 102 V124" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#rel-a)"/>
  <path d="M610 102 V124" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#rel-a)"/>
  <text x="118" y="118" font-size="12" fill="var(--vp-c-text-2, #67676c)">non-relocatable</text>
  <text x="368" y="118" font-size="12" fill="var(--vp-c-text-2, #67676c)">singleton</text>
  <text x="618" y="118" font-size="12" fill="var(--vp-c-text-2, #67676c)">relocatable</text>
  <rect x="20" y="128" width="180" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="110" y="161" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Free the name</text>
  <rect x="270" y="128" width="180" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="360" y="161" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Assign to coordinator</text>
  <rect x="520" y="128" width="180" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="610" y="151" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Balanced fill</text>
  <text x="610" y="171" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">across survivors</text>
  <path d="M360 184 V244" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#rel-a)"/>
  <path d="M610 184 V216 H396 V244" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#rel-a)"/>
  <rect x="270" y="248" width="180" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="360" y="271" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Recreate</text>
  <text x="360" y="291" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">from recipes</text>
  <rect x="500" y="254" width="204" height="44" rx="22" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="602" y="281" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Reachable at new homes</text>
  <path d="M450 276 H496" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#rel-a)"/>
</svg>

The one scan does three jobs at once: it collects the departed node's actors, reads their stored recipes, and counts how many actors each survivor owns. Every other survivor just refreshes its routing caches.

**Balanced fill** spreads the orphans evenly: the coordinator raises the emptiest survivors to a common level and drops any remainder on the emptiest, breaking ties by member id. Actor counts end up equal across the cluster to within one, computed from the single scan's tally rather than a scan per actor. Because the rule is deterministic, every coordinator computes the identical plan, which is what makes a successor's resume converge instead of reshuffle. The coordinator recreates its own share locally and ships each other node its share to build; a **non-relocatable** actor's name is freed only while its record still names the dead node.

## Idempotent and resumable

Every recreate is a **compare-and-set** on the placement record, gated on it still naming the dead node. That makes the whole pass safe to repeat:

- A **successor coordinator** (one that took over because the first coordinator also died mid-relocation) recomputes the same plan and its writes collapse the already-done work to no-ops.
- A **build that fails** after its compare-and-set has moved the record, a `preStart` that throws while recovering state, hands the record back to the dead node rather than deleting it, so the actor is retried, not lost.
- A **periodic orphan sweep** on the coordinator re-runs the same conditional fill for any record still naming a departed node, until every orphan lands.

So `RelocationFailed` is not terminal: the names it reports survive in the registry and the next sweep retries them. The whole pass is observable on the [event stream](events.md): `RelocationStarted`, then `RelocationCompleted` with the names re-placed, or `RelocationFailed` with the names a pass could not place yet.

## What is lost

A relocated actor is a **fresh start** on a survivor, not a live transfer. Between the old instance stopping and the new one opening, messages to the name are lost: nothing buffers or forwards them, an `ask` is refused, a `tell` dead-letters. The actor's **identity and configuration** come back; its **in-flight mailbox** does not. This is the same contract a crash always had, and it is deliberate: trading the message-loss window for a buffering-and-forwarding handoff protocol was rejected as complexity the contract does not need. An actor that must not lose work should make its operations idempotent and retriable by the sender, and keep durable state as below.

## State and `preStart`

The framework has no persistence layer and will not grow one: no disk, no write-ahead log, no external store, ever. Actor state fits inside that decision because the lifecycle already carries the recovery hook.

A relocated actor is rebuilt from its **recipe** (its registered class and constructor arguments), and then its own **`preStart(ctx)`** runs before it takes a message, exactly as on a fresh spawn or a supervisor restart. `preStart` is where an actor acquires its dependencies and recovers its state, from whatever source of truth it owns, reached through the `Context` that hands it the actor system.

- An actor whose state must outlive its process keeps that state in its own store and **reloads it in `preStart`**.
- An actor whose state is a cache over something authoritative elsewhere just **rebuilds the cache**.

What travels in the recipe is configuration, the constructor arguments, not accumulated state. The cluster carries the actor's identity; the developer owns its state. There is deliberately no framework `snapshot()`/`restore()`, which would be a second recovery path competing with the `preStart` an actor already implements.

## Opting in and out

Relocation is on by default. Control it at two levels:

```ts
// System-wide default: make actors node-bound unless a spawn opts back in.
new ActorSystem("orders", { remote, cluster: { discovery, relocation: false } });

// Per actor, wins over the system default:
await system.spawnOn("gpu-worker", Props.create(GpuWorker), { relocatable: false });
await system.spawn("cache", Props.create(Cache), { relocatable: true });
```

| Level | Option | Default |
| --- | --- | --- |
| System | `cluster.relocation` | `true` (departing nodes' actors are recreated) |
| Per actor | spawn option `relocatable` | falls back to the system default |

A **non-relocatable** actor is stopped with its node and its name is freed (only while the record still names the dead node, so a name a live node has reclaimed is never clobbered); nothing recreates it. Use it for an actor bound to a resource that lives only on its node, or whose identity is meaningless elsewhere. A [singleton](singletons.md) is always relocatable regardless of these flags, since its recovery *is* a relocation.

::: tip
Relocation recreates the actor; it does not replay its messages. Design a relocatable actor to rebuild its own state in `preStart` and to tolerate losing whatever was in flight when its node departed.
:::
