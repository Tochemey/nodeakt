# Singletons

A cluster singleton is an actor with **exactly one live instance across the whole cluster**, reached by the same name from every node. Use it for work that must not run twice at once: a sequence generator, a scheduler, a leader that owns an external resource.

```ts
const ref = await system.spawnSingleton("sequencer", Props.create(Sequencer));
```

A singleton is not a new kind of actor. It is an ordinary named, always-relocatable actor with three guarantees. It is also spawned long-lived, so unlike an ordinary actor it never [passivates](../actor/passivation.md) on an idle window: a quiet period cannot stop the one instance out from under the cluster.

## Idempotent to create

Call `spawnSingleton(name, props)` from every node that wants the singleton. The first call wins the name; every other call gets a **routed handle to the existing instance** instead of `ErrActorAlreadyExists`. That is the difference from `spawn` and `spawnOn`, which refuse a losing caller: `spawnSingleton` is the one creation call that is safe to run everywhere.

<svg role="img" aria-label="Singleton race: two nodes claim one name; the winner builds, the loser resolves the existing instance" viewBox="0 0 720 316" style="width:100%;max-width:720px;height:auto;display:block;margin:24px auto;font-family:inherit">
  <defs>
    <marker id="sg-a" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 Z" fill="var(--vp-c-text-3, #929295)"/>
    </marker>
  </defs>
  <path d="M130 50 V305" fill="none" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5" stroke-dasharray="4 4"/>
  <path d="M360 50 V305" fill="none" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5" stroke-dasharray="4 4"/>
  <path d="M590 50 V305" fill="none" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5" stroke-dasharray="4 4"/>
  <rect x="60" y="10" width="140" height="40" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="130" y="35" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Node A</text>
  <rect x="290" y="10" width="140" height="40" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="360" y="35" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Node B</text>
  <rect x="520" y="10" width="140" height="40" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="590" y="35" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Registry</text>
  <path d="M130 80 H586" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#sg-a)"/>
  <text x="245" y="70" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">claim the name</text>
  <path d="M360 108 H586" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#sg-a)"/>
  <text x="473" y="98" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">claim the name</text>
  <path d="M590 136 H134" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#sg-a)"/>
  <text x="245" y="126" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">won</text>
  <path d="M590 164 H364" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#sg-a)"/>
  <text x="477" y="154" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">lost</text>
  <path d="M130 188 H198 V212 H134" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#sg-a)"/>
  <text x="206" y="204" font-size="13" fill="var(--vp-c-text-2, #67676c)">build the instance</text>
  <path d="M360 248 H586" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#sg-a)"/>
  <text x="473" y="238" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">resolve the owner</text>
  <path d="M590 276 H364" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#sg-a)"/>
  <text x="477" y="266" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">handle to that instance</text>
</svg>

Both callers end up holding a handle to the same one instance. When two nodes race with different `Props`, the winner's `Props` win. That is the price of idempotence, and the reason a singleton's constructor arguments should be the same everywhere it is created.

## Hosted on the coordinator

A singleton runs on the [coordinator](membership.md#the-coordinator), the oldest live member every node agrees on, so its location is predictable. It moves only when its host departs, and then it is re-established on the new coordinator.

## Always relocatable

When a singleton's host departs, the ordinary [relocation](relocation.md) pass recreates it, pinned to the current coordinator rather than chosen by a placement strategy. There is no separate lease and no renewal clock: at-most-one rests on the same cluster-wide name claim that makes every name unique, so there is never a second recovery path that could mint a duplicate.

## Shared namespace

Singletons share the one actor namespace with every other actor. After `spawnSingleton("sequencer")`, a `spawn("sequencer")` anywhere in the cluster is refused as a duplicate, and the reverse holds too. A name is a singleton or an ordinary actor, never both.

::: warning
At-most-one depends on the [split-brain resolver](membership.md#split-brain-and-quorum). A partitioned cluster whose halves each keep serving could otherwise elect two coordinators and mint two instances of one singleton. Run a clustered deployment with `minimumMemberQuorum` set so a minority half stops instead.
:::
