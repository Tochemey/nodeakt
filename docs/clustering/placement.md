# Placement

In a cluster a top-level actor name is **unique across every node**. Where the actor runs is placement, and there are two ways to decide it:

- **`spawn(name, props)`** keeps the actor on the calling node, as always. The name is now claimed cluster-wide.
- **`spawnOn(name, props, { strategy })`** places the actor on the node a strategy chooses, and returns a routed handle to it.

Either way, a `name → owning node` record goes into the distributed registry, and any node can then reach the actor by name.

## Cluster-wide unique names

`spawn("orders")` on two nodes at once is a race for the one name. The winner builds the actor; the loser is refused with `ErrActorAlreadyExists`, exactly as a duplicate `spawn` is refused on a single node. Handing the loser a handle to an actor built from someone else's `Props` would turn a race into silent misconfiguration, so the loser is told, not quietly aliased. A caller that wants the existing actor looks it up with [`actorOf`](messaging.md); a caller that wants idempotent creation uses a [singleton](singletons.md).

```ts
const ref = await system.spawn("orders", Props.create(Orders, "eu"));
// On another node, at the same instant:
await system.spawn("orders", Props.create(Orders, "us")); // rejects: ErrActorAlreadyExists
```

## Placing on a chosen node

`spawnOn` places an actor on a node the caller does not run on, choosing the node by a **strategy** carried in the options, not a raw address, so call sites express intent, not topology.

```ts
const ref = await system.spawnOn("worker-42", Props.create(Worker, config), { strategy: "leastLoad" });
```

The options are the `SpawnOnOptions` type, whose `strategy` is a `PlacementStrategy`. On a system created without clustering, `spawnOn` and `spawnSingleton` reject with `ErrClusteringDisabled`.

The owner is the one that claims the name and builds the actor, so a `spawnOn` that loses the name race is refused with `ErrActorAlreadyExists` just like a `spawn`. The returned `PID` is a [routed handle](messaging.md): `tell`, `ask`, `request`, `watch`, `forward`, and `pipeTo` on it deliver to whichever node owns the actor.

<svg role="img" aria-label="spawnOn: the caller ships the recipe, the owner claims the name and builds, or refuses a held name" viewBox="0 0 720 372" style="width:100%;max-width:720px;height:auto;display:block;margin:24px auto;font-family:inherit">
  <defs>
    <marker id="pl-a" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 Z" fill="var(--vp-c-text-3, #929295)"/>
    </marker>
  </defs>
  <path d="M130 50 V360" fill="none" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5" stroke-dasharray="4 4"/>
  <path d="M360 50 V360" fill="none" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5" stroke-dasharray="4 4"/>
  <path d="M590 50 V360" fill="none" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5" stroke-dasharray="4 4"/>
  <rect x="60" y="10" width="140" height="40" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="130" y="35" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Caller</text>
  <rect x="290" y="10" width="140" height="40" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="360" y="35" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Owner</text>
  <rect x="520" y="10" width="140" height="40" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="590" y="35" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Registry</text>
  <path d="M130 80 H356" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#pl-a)"/>
  <text x="243" y="70" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">ship the recipe</text>
  <path d="M360 110 H586" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#pl-a)"/>
  <text x="473" y="100" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">claim the name</text>
  <rect x="40" y="136" width="650" height="214" rx="6" fill="none" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <rect x="40" y="136" width="104" height="24" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1"/>
  <text x="92" y="152" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">name free</text>
  <path d="M590 176 H364" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#pl-a)"/>
  <text x="477" y="166" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">claimed</text>
  <path d="M360 200 H428 V224 H364" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#pl-a)"/>
  <text x="436" y="216" font-size="13" fill="var(--vp-c-text-2, #67676c)">build the actor</text>
  <path d="M360 248 H134" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#pl-a)"/>
  <text x="247" y="238" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">routed handle</text>
  <path d="M40 268 H690" fill="none" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5" stroke-dasharray="5 4"/>
  <rect x="40" y="268" width="104" height="24" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1"/>
  <text x="92" y="284" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">name held</text>
  <path d="M590 312 H364" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#pl-a)"/>
  <text x="477" y="302" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">refused</text>
  <path d="M360 340 H134" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#pl-a)"/>
  <text x="247" y="330" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">ErrActorAlreadyExists</text>
</svg>

The owner does the single claim; the caller never pre-claims. That is what keeps two callers racing the same name from both writing a record. When the strategy picks the calling node itself, the same claim-and-build happens locally with no network hop.

## Strategies

The strategies are a closed set of four. A raw address is deliberately not a strategy.

| Strategy | Chooses | Notes |
| --- | --- | --- |
| `roundRobin` | The next member in rotation | The default. Rides an atomic cluster counter, so every node advances the same rotation and load spreads with no coordinator on the path. |
| `random` | A uniformly random live member | |
| `local` | The calling node | Opt a call site out of distribution without changing its shape. |
| `leastLoad` | The member owning the fewest actors | Tallies every host's load in one registry scan, so choosing costs one scan whatever the cluster's size. |

Every strategy considers only live, ready, non-draining members, so a node still joining or already leaving is never chosen.

## What crosses to the owner

`spawnOn` builds the actor on the owning node, so the construction crosses the wire as a **recipe**, exactly as a [remote spawn](../remoting/index.md#remote-spawn) does:

- The actor class must be registered under the same name on both nodes (`registerActor(Worker)` at module scope). An unregistered class rejects with `ActorNotRegisteredError`.
- Constructor arguments cross the wire codec, validated structured-cloneable. The `reentrancy`, `passivationStrategy`, and `relocatable` options travel; live-object options (`mailbox`, `supervisor`) are node-local and refused, as in every `Props` spawn.

Because the recipe is what a survivor also needs to rebuild the actor after a crash, a placed actor stores it for [relocation](relocation.md) automatically.

## Opting an actor out of relocation

By default a placed actor is recreated on a survivor when its node departs. Set `relocatable: false` to bind it to its node instead, so it dies with the node rather than moving:

```ts
await system.spawnOn("gpu-worker", Props.create(GpuWorker), { strategy: "leastLoad", relocatable: false });
```

The system-wide default is the `cluster.relocation` option; the per-actor `relocatable` wins over it. See [relocation](relocation.md#opting-in-and-out).
