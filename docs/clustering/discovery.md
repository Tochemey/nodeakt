# Discovery

Discovery answers one question, once: **which peers does a fresh node try to reach at boot?** A node resolves a seed list, dials those seeds to join their cluster, and never calls discovery again. Every topology change after that, a node joining, leaving, failing, or changing metadata, is reported by [membership](membership.md), not by discovery.

<svg role="img" aria-label="Boot flow: resolve seeds, join through one, retry until the deadline, then anchor a new cluster" viewBox="0 0 720 260" style="width:100%;max-width:720px;height:auto;display:block;margin:24px auto;font-family:inherit">
  <defs>
    <marker id="disc-a" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 Z" fill="var(--vp-c-text-3, #929295)"/>
    </marker>
  </defs>
  <rect x="16" y="30" width="120" height="44" rx="22" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="76" y="57" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Node boots</text>
  <rect x="176" y="30" width="170" height="44" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="261" y="57" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Resolve seed list</text>
  <rect x="396" y="24" width="170" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="481" y="47" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Any seed</text>
  <text x="481" y="67" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">reachable?</text>
  <rect x="596" y="24" width="110" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="651" y="47" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Join via</text>
  <text x="651" y="67" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">a seed</text>
  <path d="M136 52 H172" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#disc-a)"/>
  <path d="M346 52 H392" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#disc-a)"/>
  <path d="M566 52 H592" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#disc-a)"/>
  <text x="579" y="44" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">yes</text>
  <path d="M481 80 V112" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#disc-a)"/>
  <text x="489" y="100" font-size="12" fill="var(--vp-c-text-2, #67676c)">no / empty</text>
  <rect x="396" y="116" width="170" height="44" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="481" y="143" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Deadline left?</text>
  <path d="M392 138 H261 V78" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#disc-a)"/>
  <text x="310" y="130" font-size="12" fill="var(--vp-c-text-2, #67676c)">yes, retry</text>
  <path d="M481 160 V196" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#disc-a)"/>
  <text x="489" y="182" font-size="12" fill="var(--vp-c-text-2, #67676c)">no</text>
  <rect x="340" y="200" width="180" height="44" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="430" y="227" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">Anchor a new cluster</text>
  <rect x="560" y="200" width="150" height="44" rx="22" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="635" y="227" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">In the cluster</text>
  <path d="M520 222 H556" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#disc-a)"/>
  <path d="M651 80 V196" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#disc-a)"/>
</svg>

An empty seed list is not an error: while an environment is still coming up (no pod of a headless service has registered yet), the provider returns empty and the boot sequence retries until a seed appears or the bootstrap deadline passes, at which point the node anchors a new cluster as its first member.

The discovery provider is the one part of clustering you plug in yourself. Two implementations ship in the zero-dependency core, and you can write your own.

## DNS discovery

`DnsDiscovery` resolves a hostname to its addresses, ideal for a headless service in a container orchestrator, where one DNS name resolves to every pod's IP.

```ts
import { DnsDiscovery, DnsRecordType } from "@tochemey/nodeakt";

new DnsDiscovery({
  hostname: "orders.default.svc.cluster.local",
  recordType: DnsRecordType.address, // A records; each IP + the given gossip port
  port: 7946,
});
```

| Option       | Meaning                                                                                                                                                                                  |
|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hostname`   | The name to resolve.                                                                                                                                                                     |
| `recordType` | `DnsRecordType.srv` (the default) reads `SRV` records and takes host and port from each; `DnsRecordType.address` (`A`) and `DnsRecordType.address6` (`AAAA`) read IPs and attach `port`. |
| `port`       | The gossip port to attach to each address in `address`/`address6` mode; ignored for `srv`, which carries its own.                                                                        |
| `resolver`   | An optional resolver to query; defaults to the platform's `node:dns`.                                                                                                                    |

With `SRV` records the service registers each node's gossip host and port, so no `port` is needed. With `A` records every node shares one gossip port, which is what the [example](https://github.com/Tochemey/nodeakt/blob/main/examples/dns-actors/README.md) uses: a Compose network alias resolves to all three containers. The [Kubernetes example](https://github.com/Tochemey/nodeakt/blob/main/examples/k8s/README.md) runs the same configuration against a headless service, whose name resolves to every pod behind it.

These options are the `DnsDiscoveryOptions` type, and `recordType` is a `DnsRecordTypeValue`. To point discovery at a non-default DNS source, pass a custom `resolver` implementing `DnsResolver`, whose `SRV` answers are `DnsSrvRecord`s.

## Static discovery

`StaticDiscovery` resolves a fixed list, for a known topology or a test.

```ts
import { StaticDiscovery } from "@tochemey/nodeakt";

new StaticDiscovery(["10.0.0.1:7946", "10.0.0.2:7946", "10.0.0.3:7946"]);
```

The list may be empty to start a node that anchors its own cluster and waits for others to find it. Each entry is a `host:port` string; a blank entry is a `TypeError` at construction.

## A custom provider

Discovery is deliberately pluggable, so the core takes on no cloud or orchestration dependency. Implement `DiscoveryProvider` to pull seeds from a Kubernetes API query, a Consul or NATS registry, or a cloud instance lookup:

```ts
import type { DiscoveryProvider } from "@tochemey/nodeakt";

class ConsulDiscovery implements DiscoveryProvider {
  async resolve(): Promise<readonly string[]> {
    // Query your registry and return each peer's gossip endpoint as "host:port".
    // Return [] (not a rejection) while the source is simply not populated yet.
    return await lookupGossipEndpoints();
  }
}
```

- `resolve()` is called **once**, at boot, and never again. Do not poll it for topology.
- Return an empty array while the source is still coming up; the boot sequence treats empty as "not ready" and retries. Reject only on a genuine failure to query the source at all.
