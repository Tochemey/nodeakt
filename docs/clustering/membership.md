# Membership

After [discovery](discovery.md) has joined a node to the cluster, membership takes over. Every node keeps a live **view** of the cluster: who is in it, when each member joined, and whether each is ready or draining. Nodes gossip that view to each other continuously, and a lightweight failure detector notices when a node stops responding. Every topology change after boot, a join, a leave, a failure, or a metadata change, is reported by membership, not by discovery.

<svg role="img" aria-label="Member states: joining, alive, suspect, dead, and leaving" viewBox="0 0 720 210" style="width:100%;max-width:720px;height:auto;display:block;margin:24px auto;font-family:inherit">
  <defs>
    <marker id="mem-a" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 Z" fill="var(--vp-c-text-3, #929295)"/>
    </marker>
  </defs>
  <circle cx="26" cy="60" r="5" fill="var(--vp-c-text-3, #929295)"/>
  <path d="M33 60 H62" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#mem-a)"/>
  <rect x="66" y="38" width="120" height="44" rx="22" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="126" y="65" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Joining</text>
  <rect x="266" y="38" width="110" height="44" rx="22" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="321" y="65" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Alive</text>
  <rect x="456" y="38" width="120" height="44" rx="22" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="516" y="65" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Suspect</text>
  <rect x="636" y="38" width="70" height="44" rx="22" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="671" y="65" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Dead</text>
  <path d="M186 60 H262" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#mem-a)"/>
  <text x="224" y="28" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">view converges</text>
  <path d="M376 52 H452" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#mem-a)"/>
  <text x="414" y="28" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">probes missed</text>
  <path d="M456 70 H380" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#mem-a)"/>
  <text x="418" y="98" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">answers again</text>
  <path d="M576 60 H632" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#mem-a)"/>
  <text x="604" y="28" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">confirmed</text>
  <path d="M321 82 V148" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#mem-a)"/>
  <text x="329" y="120" font-size="12" fill="var(--vp-c-text-2, #67676c)">stop()</text>
  <rect x="266" y="152" width="110" height="44" rx="22" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="321" y="179" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Leaving</text>
  <path d="M376 174 H671 V86" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#mem-a)"/>
  <text x="500" y="166" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">departs</text>
</svg>

The runtime uses a gossip-based membership protocol with a probe-and-suspect failure detector tuned for a local network: a member that misses its probes is **suspected**, other members corroborate, and if it stays silent it is **confirmed dead**; a member that answers again before confirmation is revived. A node that stops gracefully broadcasts its departure, so survivors learn of it at once rather than waiting for detection.

## The coordinator

Exactly one member is the **coordinator**: the oldest live member, the one deterministic choice every node's view agrees on. It is not elected and holds no lease; it is simply whoever has been in the cluster longest, and it changes only when that member departs, at which point the next-oldest takes over.

The coordinator is the single driver of cluster-wide work that must happen once:

- It recomputes registry partition ownership on every membership change (rebalancing).
- It drives [relocation](relocation.md): when a node departs, only the coordinator recreates that node's actors on the survivors. Every other survivor just refreshes its routing caches.

Because the choice is deterministic, a coordinator that dies is replaced without an election, and a successor recomputes the same work and resumes it idempotently.

## Split-brain and quorum

A network partition can split a cluster into halves that can no longer gossip. If both halves keep serving, each could believe it holds a name and mint a second instance, breaking [cluster-wide uniqueness](placement.md) and [singletons](singletons.md). The **split-brain resolver** prevents that: set `minimumMemberQuorum` to the smallest number of members a functioning half must see, and a half that falls below it stops rather than serving on.

```ts
new ActorSystem("orders", {
  remote: { host: "0.0.0.0", advertisedHost: host, port: 4000 },
  cluster: { discovery, minimumMemberQuorum: 2 },
});
```

::: warning
`minimumMemberQuorum` defaults to `1`, which **disables** the resolver. Name uniqueness and at-most-one singletons depend on the resolver being on, so a real clustered deployment sets a quorum (typically a majority of the expected size). Leaving it at `1` is fine for a single node or a test, not for production.
:::

## Ready and draining

A member carries a **ready** flag (it has finished joining and can take placements) and a **draining** flag (it is leaving and should take no new ones). [Placement strategies](placement.md) and [relocation](relocation.md) only ever choose live, ready, non-draining members as targets, so a node that is still joining or already leaving never receives a fresh actor.

## Observing membership

Every membership change reaches your code as an event on the system stream. Subscribe to watch the cluster form and change:

```ts
system.subscribe((event: unknown): void => {
  if (event instanceof NodeJoined) console.log("joined", event.address);
  if (event instanceof NodeLeft) console.log("left", event.address);
  if (event instanceof CoordinatorChanged) console.log("coordinator", event.coordinator);
});
```

See [cluster events](events.md) for the full family, including the rebalance and [relocation](relocation.md) events.
