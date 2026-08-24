# The remoting design

The layer that makes an actor on another machine an ordinary `PID`: lookup, spawn, respawn, stop, tell, ask, request, watch, forward, and pipeTo across nodes, riding the TCP transport described in [tcp-transport.md](tcp-transport.md). This document describes the design as shipped.

## Goals

- Location transparency at the call site: the PID a remote lookup or spawn returns is used exactly like a local one, and the send paths do not branch on "remote" anywhere new.
- The runtime's failure taxonomy carries across the wire unchanged: sentinel errors settle asks by identity, undeliverable messages become dead letters on the side that discovered them, and connection loss is surfaced through death watch.
- One seam: the transport stays actor-blind, and the runtime reaches it through a single module boundary.
- A system that never enables remoting pays nothing: no listener, no transport code on any hot path, no change to local send numbers.

## Non-goals

- Clustering: no discovery, no membership, no sharding. Nodes are addressed explicitly by host and port.
- Transport security: traffic is plaintext TCP for a private, trusted network. TLS is a future server configuration, not a protocol change.
- Remote reach into worker isolates: inbound deliveries resolve on the receiving node's main isolate. Combining remoting with worker placement works for outbound traffic; making a worker-placed actor reachable from other nodes is future work on the placement seam.

## Where it lives

`src/remoting.ts` and `src/remoting.codec.ts` are the seam: the only modules outside `src/net/` allowed to import from it, enforced by the boundary test. The runtime reaches the wire only through the seam, so the dependency flows one way and the transport never learns what an actor is.

The seam plugs into the runtime at the point the isolate transport already defined: `PID.tell/ask/request/watch/unWatch` branch on a PID's route (`IsolateRoute`), and a remote actor is a `routedPid` whose route sends over a `Peer`/`Session` instead of a `MessagePort`.

```
PID.tell / ask / request / watch / forward / pipeTo
  -> _route (IsolateRoute)
       -> MessagePort  (same machine, another isolate)   src/port.transport.ts
       -> Peer/Session (another machine, over TCP)       src/remoting.ts
```

The route carries a sentinel worker id (`-1`), so a remote handle can never be mistaken for a placed one.

## Public API

Remoting is enabled by the construction-time `RemoteOptions` (`host`, `port`); without it every remote method rejects with `ErrRemotingDisabled` and nothing binds.

- `ActorSystem`: `remoteLookup`, `remoteSpawn`, `remoteReSpawn`, `remoteStop`, plus `host()` and `port()`.
- `ReceiveContext`: `remoteLookup`, `remoteReSpawn`, `remoteStop`, delegating to the system (spawn stays system-only).
- Everything else needs no new method: it works on the routed PID a lookup or spawn returns.

`start()` binds the endpoint before guardians spawn and rejects on a bind failure, so a node that cannot open its endpoint fails to start rather than starting deaf. `stop()` closes the seam before guardians stop: peers close first, dead-lettering what they hold, then the server tears down every connection, so in-flight remote asks fail cleanly and teardown hooks cannot dial fresh connections (the seam's closed flag refuses them with immediate dead letters).

## The endpoint

One `NetServer` bound to the configured host and port, advertising a HELLO built from the transport's own ceilings: the transport's current capability revision (imported, so the seam can never lag or outrun the wire), 16 MiB frame and message caps, a 16 MiB receive window, and four concurrent large transfers. Every advertisement negotiates down to the pairwise minimum.

An ephemeral port (`0`) is resolved to the bound port in both directions: the server patches the HELLO it hands to accepted sessions, and the seam rebuilds its local HELLO from the bound port for every peer it dials, so an advertised endpoint is always one a peer can actually reach. Actor paths advertise the same endpoint, which is what makes inbound envelopes resolvable and reply dials possible.

The outbound side is a map of `Peer`s keyed `host:port`, created on first use; dialing stays lazy inside the peer (single-flight, exponential backoff, fail-fast inside the window, per the transport design).

## The codec bridge

`remoting.codec.ts` keeps the exact contract of the in-process codec over the transport's binary value encoding:

- Encode runs the registry check first (an unregistered class instance is refused on the sending side) and then `encodeValue` into a retained scratch writer; the payload handed out is a copy the caller owns.
- Decode runs `decodeValue` and then restores the registered prototype for a nonempty type ref, without running the constructor and dropping `__proto__` and `constructor` keys, so `instanceof` narrowing survives the hop.
- Failures settle asks through the wire's error body with a sentinel bias: zero means no sentinel, otherwise one plus the runtime's sentinel index, so identity-compared errors decode to the identical instance on the other side.

The value codec's own domain rule applies at the top level: a bare `Uint8Array`, `Date`, or `Map` is not a message by itself (it would be indistinguishable from a registered class's data); such values cross as fields of a registered class or under a plain object.

## Outbound sends

- `tell` encodes, then hands the envelope to the peer; the return reports transport accept. An encode failure returns its error and dead-letters immediately; everything the peer later discovers undeliverable comes back through the dead-letter callback.
- `ask` rejects non-positive timeouts before touching the wire, then rides the peer's pending table; the transport's expiry sentinel is lifted to `ErrRequestTimeout`, and a peer's application failure decodes back to the error the far side encoded. The ask timer arms once the lane is acquired, so a first ask on a cold lane can additionally wait out the dial before its budget starts.
- `request` runs admission against the sender's reentrancy configuration on this side, sends as an ask, and delivers the continuation on the sender's own turn. A request carrying no timeout, or one cancelled mid-flight, keeps its pending entry on the connection until the connection ends: the transport offers no withdrawal, so the entry is bounded by the connection's life.

## Inbound delivery

Arrived envelopes dispatch in `onData`:

- An empty target path is a control request (below).
- Watch and unwatch kinds go to the watch bookkeeping.
- Everything else decodes (an undecodable payload becomes a dead letter carrying a copy of the bytes, or a request-scoped failure for an ask), resolves its target, and delivers through the ordinary send paths.

Target resolution parses the path, resolves it on the local tree, and enforces incarnation pinning: an envelope pinned to a uid that no longer matches the living actor is undeliverable, so sends through a stale handle dead-letter here rather than reaching a different actor of the same name.

Sender resolution gives every delivery a usable `ctx.sender`: an absent or malformed sender falls back to the system's NoSender; a sender on this very node resolves to its live PID; a foreign sender resolves cache-first to a stable routed handle carrying its path and incarnation, so replying to `ctx.sender` dials back to the node it lives on and the same sender always resolves to the same handle instance.

Asks deliver through `deliverAsk` with reply callbacks; a throwing handler is contained as a request-scoped failure. Every reply the session refuses (an oversize reply, a full admission budget) falls back to a request-scoped error frame, which is admission-exempt, so the asker settles with the real reason instead of waiting out its timeout.

## The control endpoint

Lookup, spawn, respawn, and stop have no dedicated wire frame; each rides as an ask to the node itself, addressed by an empty target path, under type refs `nodeakt.remote.{lookup,spawn,respawn,stop}`. A control tell is meaningless and dropped.

- Payloads are plain values through the value codec, shape-validated on arrival; a payload that does not decode or does not carry the expected fields answers a bad-request failure, as does an unknown control name, so a peer settles instead of timing out and the connection is never poisoned by a malformed request.
- Lookup answers the target's path and incarnation, or null.
- Spawn constructs by registered class name (`Props.restore` from the carried recipe: name, class, arguments, and the `reentrancy` option, the one spawn option that is data); an unregistered class answers `ActorNotRegisteredError`, and a spawn failure travels back settling the ask, sentinel identity preserved.
- Respawn restarts the named actor in place; stop shuts it down gracefully, idempotently succeeding for a name nobody holds. Both refuse, with an explanatory error, an actor the node placed on one of its worker isolates: reaching it through the pool is not wired yet.

## The watch protocol

Watch registrations live on both sides, and every rule below exists to keep one invariant: a watcher receives exactly one `Terminated` per settled watch, never a spurious or duplicate one.

- Outbound, the seam records each watch keyed by watcher and target path, tagged with the node holding it, and sends a watch envelope (empty body, the watcher riding as sender) on the control lane. Unwatch deletes the record and tells the far side.
- Inbound, a watch registers the resolved sender handle as a watcher on the local actor, so the actor's eventual stop tells the handle, which routes the `Terminated` back over the wire. Watching an actor that is already gone answers with an immediate `Terminated`: once a watch crossed the boundary, the watcher is always eventually notified of a death that is or becomes true. A watch without a resolvable sender is a forged frame and dropped.
- Remote death and connection loss are indistinguishable by design. When a control-lane connection to a node dies, every outbound watch over that node settles now with one `Terminated` per watcher; a watch envelope the peer could never deliver settles the same way at once.
- Inbound registrations are tracked per delivering session and swept when that session closes, because the watching node treats the same connection loss as the death of everything it watched here. Sessions this node dialed report no per-session close, so any lane closure also lazily sweeps registrations whose delivering session has since closed. The sweep must not assume the control lane: control asks ride ordinary lanes (the transport routes only watch kinds to the control lane), so it runs at the top of every lane-close callback.
- An inbound `Terminated` is delivered only when it settles a watch this node holds (the registration is deleted as it delivers). The notification travels on the far node's own dialed connection, so without this gate a lost unwatch would still notify, and a connection sweep followed by the actor's real stop would notify the same watcher twice. The gate also filters user-forged `Terminated` messages.

The gate has a second job: it is what makes future sender-cache eviction survivable. Evicting a cached sender handle breaks unwatch-by-identity on this side, leaving a dead registration until the target stops or the session sweeps, and the stray notification that produces is dropped by the watching node's gate.

## Reliability rules

1. An ask settled by a peer failure rejects with the decoded error, sentinel identity preserved; an unanswered ask rejects with `ErrRequestTimeout`.
2. A connection lost with asks in flight rejects every pending ask; tells are at-most-once with one kernel-confirmed redelivery, and every undeliverable outcome surfaces as a dead letter with the original sender and receiver attributed.
3. Remote death and connection loss are indistinguishable; death watch is the mechanism that reports both.
4. A late, duplicate, or forged death notification is dropped by the watch-settlement gate; a degenerate ask carrying one is answered with a bad-request failure instead of stranding the asker.
5. Incarnation pinning holds across the wire: a stale handle's sends dead-letter on the receiving node.
6. A malformed inbound envelope is request-scoped, never connection-scoped: undecodable payloads, unknown targets, and malformed control requests settle that one message (dead letter or error reply) and the connection lives. Byte-level violations below the envelope layer remain the transport's connection-scoped territory.
7. A throwing receive handler on an inbound ask settles that ask with the failure; the endpoint keeps serving.

## Trust and resource model

The trust model is a private, trusted network. Traffic is plaintext, and an envelope's sender path is self-declared: a hostile peer can grow the sender cache and steer reply dials with forged paths. Two caches grow without bound before the hardening work lands, both documented at their declarations: the peer map (one entry per distinct endpoint ever contacted; bounded by deployment topology in practice) and the sender cache (one handle per distinct foreign sender incarnation ever heard from). Bounding and eviction are designed against the watcher-identity constraint above; authentication and TLS gate exposure beyond trusted networks; transport-level ask withdrawal would unpin cancelled requests before connection end. None of it changes the wire layout.

## Verification

- `test/remoting/`: endpoint identity and bound-port advertisement, the codec bridge, lookup/tell/ask/request, watch and `Terminated` on graceful stop, node death, and unreachable nodes, the control endpoint including malformed and forged input, forward and pipeTo across nodes, and the failure taxonomy end to end.
- A seeded malformed-envelope soak (`test/remoting/soak.test.ts`): protocol-conforming but hostile envelopes, forged senders, fabricated deaths, garbage payloads; the property is survival and continued clean service on the same connection.
- The cross-runtime smoke (`test/smoke/net.sh`) runs a remoting round trip (lookup, tells, ask, death watch, remote stop) under Node, Bun, and Deno alongside the transport smoke. Remote spawn stays out of it: placement needs built fixture modules the from-source smoke does not carry, and the vitest suite covers it.
- The loopback bench (`benchmark/remoting.bench.ts`): about 0.5M remote tells/sec through the full seam path, about 0.25M pipelined ask round trips/sec, sequential asks latency-bound, on the reference M1 machine. The local send paths are untouched and keep their measured numbers.

## Files

- `src/remoting.ts`: the seam described above.
- `src/remoting.codec.ts`: the message and failure bridge.
- `src/remote.options.ts`: the public `RemoteOptions`.
- `src/actor.system.ts`, `src/receive.context.ts`: the public methods.
- `src/pipe.ts`: routed targets bypass the local liveness gate and deliver through the route.
- `src/routed.pid.ts`, `src/actor.ref.ts`: the route seam remoting implements.
