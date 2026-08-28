# Location-transparent messaging

In a cluster you message an actor by name, and the runtime routes to whichever node owns it. The `PID` you get back is a **routed handle**: `tell`, `ask`, `request`, `watch`, `unWatch`, `forward`, and `pipeTo` on it keep their call sites and their semantics, and the address decides whether the message stays local or crosses the wire. Replying to `ctx.sender` routes back to the sender's node, wherever that is.

An actor's path already carries the address of the node it lives on (`nodeakt://orders@10.0.0.5:4000/name`), and the distributed registry maps each name to its owning node. Looking a name up joins the two: read the owner from the registry, get a handle that routes there.

## `actorOf` and `actorOfAsync`

There are two lookups, and the difference is whether they wait for the network.

```ts
const local = system.actorOf("orders");            // PID | undefined, never blocks
const anywhere = await system.actorOfAsync("orders"); // Promise<PID | undefined>, resolves cross-node
```

|                                                  | `actorOf(name)`                                       | `actorOfAsync(name)`                                          |
|--------------------------------------------------|-------------------------------------------------------|---------------------------------------------------------------|
| Returns                                          | `PID \| undefined`                                    | `Promise<PID \| undefined>`                                   |
| Blocks on the network                            | Never                                                 | Only on a cold miss (one registry read)                       |
| Local actor                                      | Returns it                                            | Returns it                                                    |
| Cross-node actor already in this node's view     | Returns a routed handle                               | Returns a routed handle                                       |
| Cross-node actor **not yet** in this node's view | Returns `undefined`                                   | Reads the registry, returns a routed handle                   |
| Use it for                                       | The hot path, local lookups, code that must not await | Reaching a name placed anywhere, the usual application lookup |

`actorOf` answers instantly from a **warm view**, the routing cache this node builds from the placements it makes, from resolution reads, and from relocation events. That makes it perfect for the hot path, but it means the *first* lookup of a name owned by another node, one this node has not learned yet, reads as `undefined` even though a live owner exists. `actorOfAsync` closes that gap by awaiting a single registry read when the view is cold, so an application that reaches an actor placed on some other node gets it on the first call.

<svg role="img" aria-label="Lookup flow: local actor, warm view, then actorOf answers undefined while actorOfAsync reads the registry" viewBox="0 0 720 292" style="width:100%;max-width:720px;height:auto;display:block;margin:24px auto;font-family:inherit">
  <defs>
    <marker id="msg-a" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 Z" fill="var(--vp-c-text-3, #929295)"/>
    </marker>
  </defs>
  <rect x="16" y="30" width="150" height="44" rx="22" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="91" y="57" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">look up name</text>
  <rect x="206" y="30" width="150" height="44" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="281" y="57" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">local actor?</text>
  <rect x="560" y="30" width="150" height="44" rx="22" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="635" y="57" text-anchor="middle" font-size="15" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">routed handle</text>
  <path d="M166 52 H202" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#msg-a)"/>
  <path d="M356 52 H556" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#msg-a)"/>
  <text x="456" y="44" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">yes</text>
  <path d="M281 74 V112" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#msg-a)"/>
  <text x="289" y="96" font-size="12" fill="var(--vp-c-text-2, #67676c)">no</text>
  <rect x="206" y="116" width="150" height="48" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="281" y="145" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">in the warm view?</text>
  <path d="M356 140 H655 V78" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#msg-a)"/>
  <text x="368" y="132" font-size="12" fill="var(--vp-c-text-2, #67676c)">yes</text>
  <path d="M281 164 V204" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#msg-a)"/>
  <text x="289" y="188" font-size="12" fill="var(--vp-c-text-2, #67676c)">no</text>
  <rect x="206" y="208" width="150" height="44" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="281" y="235" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">which lookup?</text>
  <rect x="16" y="208" width="140" height="44" rx="22" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="86" y="235" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">undefined</text>
  <path d="M202 230 H164" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#msg-a)"/>
  <text x="183" y="220" text-anchor="middle" font-size="11.5" fill="var(--vp-c-text-2, #67676c)">actorOf</text>
  <rect x="436" y="208" width="170" height="44" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="521" y="235" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">read the registry</text>
  <path d="M356 230 H432" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#msg-a)"/>
  <text x="394" y="220" text-anchor="middle" font-size="11.5" fill="var(--vp-c-text-2, #67676c)">actorOfAsync</text>
  <path d="M606 230 H676 V78" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#msg-a)"/>
  <text x="668" y="150" text-anchor="end" font-size="12" fill="var(--vp-c-text-2, #67676c)">found</text>
  <path d="M521 252 V274 H86 V256" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#msg-a)"/>
  <text x="300" y="268" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">absent</text>
</svg>

Rule of thumb: reach for **`actorOfAsync`** in application code that looks a name up to message it. Keep `actorOf` for lookups that must stay synchronous and are local or already warm.

## What a routed handle does

A routed handle carries the [remoting contract](../remoting/index.md#what-a-remote-pid-does):

- **`tell`** returns `null` when the transport accepted the message, not when the far mailbox did. An undeliverable message, an unknown name, a full mailbox, a stopped target, becomes a [dead letter](../actor-system/events.md) on the node that discovered it.
- **`ask` / `request`** carry the reply back and settle on it; a timeout rejects with `ErrRequestTimeout`, sentinel errors keep their identity across the wire.
- **Ordering** holds per actor: messages to one actor ride a single lane keyed by its path, so per-actor order survives the hop.
- **`watch`** delivers one `Terminated` when the actor stops or its node dies; node death and connection loss are deliberately indistinguishable.
- **`isRunning()`** is `false` for a routed handle; liveness across nodes is not synchronously knowable. Watch it instead.

## Following a moved actor

A routed handle pins nothing. When an actor [relocates](relocation.md) to another node, the routing cache heals by re-resolving: the next time the resolver reads a cached owner that membership no longer lists, it drops the entry and reads the registry again, so lookups reach the actor's new home. A send that raced the move can be lost, an `ask` to the old owner is refused and re-resolves, a `tell` dead-letters, which is the [message-loss window](relocation.md#what-is-lost) relocation accepts.

**Watch is per-incarnation.** A relocation stops the old instance and starts a fresh one, so a watcher receives `Terminated` even though the name lives on elsewhere. To keep following a relocatable name, re-look it up with `actorOfAsync` and re-`watch`; a [`RelocationCompleted`](events.md) subscriber can automate exactly that.
