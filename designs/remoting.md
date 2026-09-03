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

The outbound side is a map of `Peer`s keyed `host:port`, created on first use; dialing stays lazy inside the peer (single-flight, exponential backoff, fail-fast inside the window, per the transport design). Alongside it, the seam keeps the accepted sessions it is holding, grouped by the node identity their HELLO declared, so a node that dialed us but whose own endpoint is not dialable back can still be reached over the connection it opened (see carrier election).

The seam also answers for the transport in the metrics snapshot (`designs/observability.md`): the number of nodes it holds a peer or an accepted session with, the frame and byte totals over every connection it has held, and the bytes its live connections hold for sending. Live peers and sessions are summed at collection; a peer being reclaimed and an accepted session closing each fold their final totals into the seam's running sum first, so the node's numbers never run backward.

## The codec bridge

`remoting.codec.ts` keeps the exact contract of the in-process codec over the transport's binary value encoding:

- Encode runs the registry check first (an unregistered class instance is refused on the sending side) and then `encodeValue` into a retained scratch writer; the payload handed out is a copy the caller owns.
- Decode runs `decodeValue` and then restores the registered prototype for a nonempty type ref, without running the constructor and dropping `__proto__` and `constructor` keys, so `instanceof` narrowing survives the hop.
- Failures settle asks through the wire's error body with a sentinel bias: zero means no sentinel, otherwise one plus the runtime's sentinel index, so identity-compared errors decode to the identical instance on the other side.

The value codec's own domain rule applies at the top level: a bare `Uint8Array`, `Date`, or `Map` is not a message by itself (it would be indistinguishable from a registered class's data); such values cross as fields of a registered class or under a plain object.

## Outbound sends

Every routed send rides the node's *carrier* (see the next section), a peer or an accepted session; the shapes below hold for either.

- `tell` encodes, then admits the message to the tell coalescer (below); the return reports transport accept. An encode failure returns its error and dead-letters immediately; everything a peer later discovers undeliverable comes back through its dead-letter callback, and a refusal from a session carrier is routed to the same dead-letter path so the two carriers fail alike.
- `ask` bounds every call by a deadline: its own positive timeout, or the system's `askTimeout` when that is omitted or non-positive, so no reply-bearing call is ever unbounded. It rides the carrier's pending table; the transport's expiry sentinel is lifted to `ErrRequestTimeout`, and a peer's application failure decodes back to the error the far side encoded. The ask timer arms once the lane is acquired, so a first ask on a cold lane can additionally wait out the dial before its budget starts.
- `request` runs admission against the sender's reentrancy configuration on this side, sends as an ask, and delivers the continuation on the sender's own turn. It is bounded the same way `ask` is, so its pending transport entry always clears at the deadline; a cancel settles the handle early, and the entry then clears when the deadline or the reply arrives.

## The tell coalescer

Tells fired within one turn for the same sender-receiver pair travel as **one batch envelope** instead of one envelope each, because per-envelope transport cost (framing, queueing, credit accounting, per-frame parse) dominates the remote send path once the codec is fast. The mechanism is invisible above the seam: no public API, no configuration, no wire-protocol change.

- The flush policy has no timer and no threshold to wait for: a batch flushes at the microtask boundary, the same instant the connection layer was going to write anyway, so a lone tell leaves immediately and coalescing emerges only when a caller was already sending faster than the wire turn. Caps (256 entries, 64 KB, internal constants) flush early, never delay; both sit far below the large-lane threshold so a batch rides the same ordinary lane as the singles it replaces.
- The batch is an ordinary DATA envelope under the internal type ref `nodeakt.remote.batch` (the control-namespace convention: a class name can never contain a dot, so no registration collides). Its payload is a hand-rolled stream: a type table (each type ref paid once per batch), an entry count, then one `(type index, length, payload bytes)` triple per entry. Encoding appends each message straight into the batch's retained buffer; emission hands the buffer over by view; the receiver iterates by view. Nothing is copied per message on either side.
- **Ordering is absolute.** A batch is one sender's ordered stream to one receiver; entries deliver to the mailbox one at a time in send order, so the actor still receives exactly one message per turn. An ask or request to a node first flushes what the coalescer holds for that node, so a reply-bearing call can never overtake the tells before it. A message at the byte cap flushes its pair's pending batch and then travels alone. A lone buffered entry is emitted as the plain envelope it would have been without coalescing.
- Failures keep tell semantics exactly: a batch a carrier cannot deliver fans out into one dead letter per entry, message and sender attribution restored; a stopping seam drains its buffered tells to dead letters the same way. Inbound, an entry that does not decode dead-letters alone (the rest of the batch delivers), a structural violation dead-letters the unparseable remainder whole, a batch claiming to be an ask is refused request-scoped, and a death notification inside a batch meets the same settlement gate a single delivery does.
- The receive side resolves target and sender once per batch and reuses the interned-path handle cache, which is what makes the unpack loop nearly free.

## Carrier election and duplex reuse

The transport is full duplex: an acceptor can send on a connection a peer dialed. The seam uses that so a node whose advertised endpoint is not dialable back (NAT, one-way container networking) is still answered over the connection it opened.

- A node pair elects exactly one *carrier* for the routed traffic to a node: a dialed peer is preferred, an accepted session is the fallback when no peer reaches the node, and a fresh dial is the last resort. The choice is recorded and reused until that carrier closes.
- Sticky election is the ordering rule: mixing a dialed peer and an accepted session for one node could reorder a single actor's messages, so once a carrier is elected it stays elected until that carrier closes. Control asks (lookup and friends) address the node itself and keep dialing directly; a peer they create is preferred only at the next election, so a node pair can briefly hold both a dialed peer and an elected session. Watch settlement therefore belongs to the elected carrier alone: a session carrier's close settles the node's watches, and a peer's control-lane loss settles only when no session carries the node, so the stray peer's failures can never fabricate a death for actors still reachable over the carrier.
- The reply and `Terminated` a foreign sender's handle routes are the first beneficiaries: minted handles send over the elected carrier, so answering `ctx.sender` works even when the sender's own address is unreachable from here. An ask's reply still travels back on the exact session that delivered it, which is the carrier whenever that node has no dialed peer.
- Failure semantics are unchanged. A session carrier's close settles that node's outbound watches exactly as a control-lane peer close does, and clears the election so the next send re-elects; the accepted-session registry is pruned on the same close.

## Inbound delivery

Arrived envelopes dispatch in `onData`:

- An empty target path is a control request (below).
- Watch and unwatch kinds go to the watch bookkeeping.
- Everything else decodes (an undecodable payload becomes a dead letter carrying a copy of the bytes, or a request-scoped failure for an ask), resolves its target, and delivers through the ordinary send paths.

Target resolution parses the path, resolves it on the local tree, and enforces incarnation pinning: an envelope pinned to a uid that no longer matches the living actor is undeliverable, so sends through a stale handle dead-letter here rather than reaching a different actor of the same name.

A path the tree does not hold falls back to the placement registry: its head segment (the top-level name, which is what the registry is keyed by) resolves to the owning worker's route through the same table `actorOf` consults, and delivery composes the two transports, the wire in and the `MessagePort` across. The owning worker holds the whole subtree, so a child of a placed actor resolves there exactly as its parent does. The delivery handle is minted around the envelope's own path, exactly one per delivery, so the worker resolves the exact actor and enforces whatever incarnation pin the envelope carries. A tell rides the isolate route as any routed tell does; an ask sends through the route and bridges the route's settlement back to the wire correlation, so the remote asker cannot tell the placement. Placed handles are never cached on the connection's interned paths, because the owning isolate can change when the name is re-placed; only the registry lookup tracks that.

The reverse direction composes through the isolate transport's own target resolution: an envelope naming a foreign node's path (a placed actor answering a remote sender, or its Terminated to a remote watcher) resolves on the main isolate to the wire-backed handle the seam's sender cache serves, so identity stays stable across both doors. On a worker facade the same seam routes the envelope to the main isolate instead, whose own fallback carries it over the wire, so a reply that crossed a second worker (a placed actor forwarding a remote sender's message to another placed actor) still finds its way home; the hop count is placement's concern, never the sender's. The worker facade adopts the node's advertised address at boot, which is what makes one canonical path space span the wire, the main isolate, and every worker.

Sender resolution gives every delivery a usable `ctx.sender`: an absent or malformed sender falls back to the system's NoSender; a sender on this very node resolves to its live PID; a foreign sender resolves cache-first to a stable routed handle carrying its path and incarnation, so replying to `ctx.sender` dials back to the node it lives on and the same sender resolves to the same handle instance while cached. The cache is a capped LRU (4096 entries, an internal constant) in map insertion order: a hit at the cap reinserts its entry, an insert past the cap evicts the least recently heard-from entry, so actor churn on peer nodes and forged sender paths meet a bound. A sender registered as an inbound watcher is pinned per watched target and never evicted while a pin holds (see the watch section); a pass that finds only pinned entries lets the cache exceed the cap and logs once.

Asks deliver through `deliverAsk` with reply callbacks; a throwing handler is contained as a request-scoped failure. Every reply the session refuses (an oversize reply, a full admission budget) falls back to a request-scoped error frame, which is admission-exempt, so the asker settles with the real reason instead of waiting out its timeout.

## The control endpoint

Lookup, spawn, respawn, and stop have no dedicated wire frame; each rides as an ask to the node itself, addressed by an empty target path, under type refs `nodeakt.remote.{lookup,spawn,respawn,stop}`. A control tell is meaningless and dropped.

- Payloads are plain values through the value codec, shape-validated on arrival; a payload that does not decode or does not carry the expected fields answers a bad-request failure, as does an unknown control name, so a peer settles instead of timing out and the connection is never poisoned by a malformed request.
- Lookup answers the target's path and incarnation, or null.
- Spawn constructs by registered class name (`Props.restore` from the carried recipe: name, class, arguments, and the `reentrancy` option, the one spawn option that is data); an unregistered class answers `ActorNotRegisteredError`, and a spawn failure travels back settling the ask, sentinel identity preserved.
- Respawn restarts the named actor in place; stop shuts it down gracefully, idempotently succeeding for a name nobody holds. An actor the node placed on a worker isolate goes through the pool's control plane instead: a `restart` or `stop-actor` order to the owning worker, answered by a `controlled` reply that settles the control ask (failures crossing in wire form, sentinel identity preserved), with the owning worker's death rejecting whatever it still owed. The order acts only on the isolate that owns the actor, so a facade or a name nobody owns answers not-found (respawn) or succeeds idempotently (stop).

## The watch protocol

Watch registrations live on both sides, and every rule below exists to keep one invariant: a watcher receives exactly one `Terminated` per settled watch, never a spurious or duplicate one.

- Outbound, the seam records each watch keyed by watcher and target path, tagged with the node holding it, and sends a watch envelope (empty body, the watcher riding as sender) on the control lane. Unwatch deletes the record and tells the far side.
- Inbound, a watch registers the resolved sender handle as a watcher on the local actor, so the actor's eventual stop tells the handle, which routes the `Terminated` back over the wire. Watching an actor that is already gone answers with an immediate `Terminated`: once a watch crossed the boundary, the watcher is always eventually notified of a death that is or becomes true. A watch without a resolvable sender is a forged frame and dropped.
- An inbound watch on a placed actor composes the same way through the isolate route: the owning worker registers the remote watcher handle (keyed by the watcher's path over there, so nothing pins the sender cache here), the worker's `Terminated` travels main-isolate-first and then over the wire, and the whole worker's death settles it through the mesh transport's close. Unwatch and the session sweep cancel through the route, idempotently; a placed target the worker already lost answers the immediate `Terminated` from over there.
- Remote death and connection loss are indistinguishable by design. When a control-lane connection to a node dies, every outbound watch over that node settles now with one `Terminated` per watcher; a watch envelope the peer could never deliver settles the same way at once.
- Inbound registrations are tracked per delivering session and swept when that session closes, because the watching node treats the same connection loss as the death of everything it watched here. Sessions this node dialed report no per-session close, so any lane closure also lazily sweeps registrations whose delivering session has since closed. The sweep must not assume the control lane: control asks ride ordinary lanes (the transport routes only watch kinds to the control lane), so it runs at the top of every lane-close callback.
- An inbound `Terminated` is delivered only when it settles a watch this node holds (the registration is deleted as it delivers). The notification travels on the far node's own dialed connection, so without this gate a lost unwatch would still notify, and a connection sweep followed by the actor's real stop would notify the same watcher twice. The gate also filters user-forged `Terminated` messages.

The gate has a second job: it is what makes a forced sender-cache eviction survivable. Evicting a cached sender handle breaks unwatch-by-identity on this side, leaving a dead registration until the target stops or the session sweeps, and the stray notification that produces is dropped by the watching node's gate.

The eviction policy avoids that failure rather than leaning on it: a watch registration pins its sender-cache entry, keyed per watched target so nothing can release a pin twice, and the release paths are exact because watcher registration and removal report whether they changed anything. A pin releases on the unwatch that removes the watcher, on the session sweep that removes it, or on the outbound `Terminated` a watched actor's own stop routes through the handle, which is the seam's only sight of that stop.

## Reliability rules

1. An ask settled by a peer failure rejects with the decoded error, sentinel identity preserved; an unanswered ask rejects with `ErrRequestTimeout`.
2. A connection lost with asks in flight rejects every pending ask; tells are at-most-once with one kernel-confirmed redelivery, and every undeliverable outcome surfaces as a dead letter with the original sender and receiver attributed.
3. Remote death and connection loss are indistinguishable; death watch is the mechanism that reports both.
4. A late, duplicate, or forged death notification is dropped by the watch-settlement gate; a degenerate ask carrying one is answered with a bad-request failure instead of stranding the asker.
5. Incarnation pinning holds across the wire: a stale handle's sends dead-letter on the receiving node.
6. A malformed inbound envelope is request-scoped, never connection-scoped: undecodable payloads, unknown targets, and malformed control requests settle that one message (dead letter or error reply) and the connection lives. Byte-level violations below the envelope layer remain the transport's connection-scoped territory.
7. A throwing receive handler on an inbound ask settles that ask with the failure; the endpoint keeps serving.

## Trust and resource model

The trust model is a private network whose nodes trust each other. An envelope's sender path is self-declared, so a hostile peer can steer reply dials with forged paths. The seam's own state is bounded: the sender cache is the capped, pinned LRU above, and the peer map is reclaimed lazily on the lane-close sweep (a peer with no connection on any lane, nothing parked for redelivery, no armed backoff, and no outbound watch referencing its node is dropped; the next send recreates it whole). TLS is per-system configuration (the `tls` block of `RemoteOptions`, resolved once at start; all or nothing per system, mutual TLS via `requestCert`): the carrier encrypts and verifies certificates, but a verified certificate is an identity, not an authorization, so per-peer authorization still gates exposure beyond trusted networks. Transport-level ask withdrawal would unpin cancelled requests before connection end. None of it changes the wire layout.

## Verification

- `test/remoting/`: endpoint identity and bound-port advertisement, the codec bridge, lookup/tell/ask/request, watch and `Terminated` on graceful stop, node death, and unreachable nodes, the control endpoint including malformed and forged input, forward and pipeTo across nodes, and the failure taxonomy end to end.
- The placed suite (`test/remoting/placed.test.ts`, real worker threads, the pool pinned to one worker so placement never lands on main): ordered tell bursts and asks into a placed actor, incarnation-pinned traffic through a remotely spawned placed handle, ask timeouts bridged back, watch and `Terminated` across both hops for self-stop and whole-worker death, watch cancellation, and remote respawn and stop through the pool's control plane. The lifecycle orders themselves are unit-proven in `test/worker.runtime.test.ts` and `test/worker.pool.wire.test.ts` (in-process, since worker-thread code is invisible to coverage).
- The coalescer suite (`test/remoting/batch.test.ts`): burst order past every cap, the ask fence, oversize interleaving, mixed types, stop drain and dead-letter fan-out, and hostile batches (garbage payloads, structural violations, undecodable entries, forged and genuine death notifications, batches aimed at nothing or claiming to be asks).
- A seeded malformed-envelope soak (`test/remoting/soak.test.ts`): protocol-conforming but hostile envelopes, forged senders, fabricated deaths, garbage payloads; the property is survival and continued clean service on the same connection.
- The cross-runtime smoke (`test/smoke/net.sh`) runs a remoting round trip (lookup, tells, ask, death watch, remote stop) under Node, Bun, and Deno alongside the transport smoke. Remote spawn stays out of it: placement needs built fixture modules the from-source smoke does not carry, and the vitest suite covers it.
- The loopback bench (`benchmark/remoting.bench.ts`): about 1.7M remote tells/sec through the full seam path (1.8M over TLS; 0.47M before the throughput work), about 0.27M pipelined ask round trips/sec, sequential asks latency-bound, on the reference M1 machine. Single-process numbers serialize both nodes on one core; the cross-process bench (`benchmark/remoting.crosscore.ts`, a plain tsx script forking the receiver) measures the parallel figure a deployment sees, about 2.1M tells/sec and 0.45M pipelined asks/sec on the same machine (0.84M before). The throughput work that got here, in order of measured effect: the tell coalescer with its zero-copy batch stream, the transport's full-batch eager flush (sender and receiver overlap instead of running in phases), and the receive-side target cache. The local send paths are untouched and keep their measured numbers.
- The transport counters behind the metrics snapshot (`test/remoting/metrics.test.ts`, with the connection and peer level in `test/net/`): an idle endpoint reports zeros, a connected pair agrees on what crossed the wire once it is quiet, and a closed connection's totals survive on both the dialing and the accepting side. Their cost on the same loopback bench, measured back to back against the tree before them: remote tell 1.82M/s before against 1.80M/s after, 1.87M/s against 1.86M/s over TLS, per-message allocation unchanged, all inside the run's own spread.
- The receive-side target cache: `targetFor` attaches the resolved local PID to the delivering connection's interned inbound path (the transport's string tables already intern hot paths and carry a handle slot per entry), so a stream of sends to one actor pays `parsePath` plus the tree walk once instead of per message (measured at 472 ns per message before, a map get after). A cached handle is revalidated per message (running, incarnation pin) and re-resolved when stale; a connection below the interning revision never caches.

## Files

- `src/remoting.ts`: the seam described above.
- `src/remoting.codec.ts`: the message and failure bridge.
- `src/remote.options.ts`: the public `RemoteOptions`.
- `src/actor.system.ts`, `src/receive.context.ts`: the public methods.
- `src/pipe.ts`: routed targets bypass the local liveness gate and deliver through the route.
- `src/routed.pid.ts`, `src/actor.ref.ts`: the route seam remoting implements.
