# The cluster membership design

This is the first layer of the cluster engine: a SWIM-style gossip membership protocol. Every node maintains a full view of the cluster's members and learns of joins, graceful departures, and failures within a bounded number of protocol periods. Upper layers (the distributed actor registry, placement, and the coordinator convention) consume the view and its events; none of them exist yet, and nothing in this layer anticipates their internals beyond exposing an accurate, ordered stream of membership changes.

The protocol is SWIM as a decade of production deployment has hardened it, not the paper's minimal form: the randomized point-to-point failure detector with indirect probes, infection-style dissemination piggybacked on detector traffic plus a dedicated gossip cadence, the suspicion mechanism with incarnation-guarded refutation, periodic full-state push-pull as anti-entropy, and the local-health refinements published as Lifeguard. Where mature implementations agree on a behavior, this design follows them deliberately; internals (encoding, module shape, injection seams) are designed Node-native.

## Goals

- **Correct under churn.** Every membership update carries an incarnation number and applies through a fixed precedence table, so stale news can never overwrite fresh news regardless of arrival order. A suspected or falsely-declared-dead member defends itself by raising its own incarnation; only the member itself may do so, including at rejoin when it discovers the cluster still holds its obituary.
- **Constant per-node load.** One direct probe per protocol period, a bounded handful of indirect probes on a miss, a fixed-fanout gossip tick, and byte-budgeted payloads. Load per node does not grow with cluster size; only convergence time does, logarithmically.
- **Deterministic and simulatable.** Protocol logic never touches the wall clock, timers, or ambient randomness directly; time, transport, and randomness are injected. The full state machine runs under a simulated network with scripted loss, delay, partitions, and a seeded random source, so every failure a test finds reproduces from its seed.
- **Dependency-free, actor-blind, cross-runtime.** Platform modules only (`node:net`, `node:tls`, `node:crypto`, `DataView`). The package knows nothing about actors, registries, or mailboxes; it deals in addresses, opaque metadata bytes, and callbacks. Node 22+, Bun 1.3+, Deno 2+, verified by the smoke harness.

## Non-goals

- The distributed registry, placement, rebalancing, and coordinator election. The view exposes what those need (a consistent member table and an event stream); they are the next design.
- A UDP packet transport. The transport contract already has the packet and stream duality a UDP implementation would slot into; the first implementation carries both roles over TCP.
- Clusters beyond roughly a hundred members, WAN topologies, or cross-version gossip compatibility promises before the format is declared stable. A version byte exists so incompatibility fails fast.
- Discovery. Seed addresses come from the caller; provider plumbing (static, kubernetes, dns) is seam-phase work.

## Where it lives

The package is `src/cluster/`, the second folder in an otherwise flat `src`. It follows the same isolation rule as `net/`: modules in `cluster/` import platform modules and each other, and nothing else. It does not import `net/` either; the framing it needs is a few dozen lines, and independence keeps both packages free to evolve their internals without a shared-primitive negotiation. The runtime will reach `cluster/` only through a seam module (`clustering.ts`, a later slice) in flat `src`. The existing import-guard test extends to enforce both directions for the new folder.

| Module                 | Holds                                                                                                     |
|------------------------|-----------------------------------------------------------------------------------------------------------|
| `cluster/wire.ts`      | Message codecs and validation: header, update records, ping family, sync bodies                            |
| `cluster/view.ts`      | The member table state machine: states, incarnation precedence, revival, suspicion bookkeeping, events     |
| `cluster/broadcast.ts` | The transmit-limited queue: retransmit counting, supersession, byte-budgeted packing                        |
| `cluster/probe.ts`     | The failure detector: probe schedule, indirect probes and nacks, awareness, the gossip tick                 |
| `cluster/suspicion.ts` | Suspicion timers: confirmation-driven timeout decay                                                          |
| `cluster/sync.ts`      | Join and anti-entropy: full state exchange, merge, rejoin refutation                                        |
| `cluster/transport.ts` | The packet-and-stream transport contract and its TCP implementation                                          |
| `cluster/clock.ts`     | Injected time: now, schedule, cancel                                                                        |
| `cluster/random.ts`    | Injected randomness: seeded generator, shuffle, pick                                                        |
| `cluster/swim.ts`      | The engine composing the above; the package's public surface                                                 |

Tests mirror the modules in `test/cluster/`, one file per module, plus the simulation harness (`test/cluster/sim.ts`) and scenario suites built on it.

## Identity and the member record

A member is identified by its **name**, and a node's name is the gossip address it advertises, `host:port`, distinct from the remoting port. There is no separate instance identity: a restarted process comes back as the same member, and the incarnation machinery below makes that unambiguous. Because the name is the address, two members can never claim one address, and the address-conflict handling some implementations need never arises.

A member record carries: name, incarnation (starts at 0, raised only by the member itself), state, state change time, and metadata. Metadata is an opaque byte block, capped small, interpreted only by upper layers; the seam will pack the node's remoting address and whatever the coordinator convention needs. Metadata travels inside `alive` records, so a metadata change is announced by re-broadcasting `alive` at a raised incarnation.

## States and precedence

Four states: `alive`, `suspect`, `dead`, `left`. None is terminal. `left` is the graceful departure a member announces about itself, kept distinct from `dead` so consumers can skip the alarm paths failure triggers (rebalance urgency, warning logs). A dead or left member is retained in the table (and gossiped about) long enough for the verdict to disseminate, then reaped after a retention window.

Updates apply by precedence, where `i` is the incoming incarnation and `j` the stored one:

- `alive(i)` supersedes `alive(j)` iff `i > j`, supersedes `suspect(j)` iff `i > j`, and supersedes `dead(j)` or `left(j)` iff `i > j` (revival: this is how a restarted member re-enters past its own obituary).
- `suspect(i)` supersedes `alive(j)` iff `i >= j`, and supersedes `suspect(j)` iff `i > j`. A `suspect` about a dead or left member is ignored.
- `dead(i)` and `left(i)` supersede `alive(j)` and `suspect(j)` iff `i >= j`.

An update that does not supersede is dropped without side effects. The table lives in `view.ts` as pure code with an exhaustive test over every (stored state, incoming state, incarnation ordering) combination.

Only the member itself raises its incarnation, and it does so exactly when defending itself: on seeing itself suspected, or on discovering (usually during a rejoin's state exchange) that the cluster holds it as `dead` or `left` while it is running. The defense is an `alive(self)` broadcast at the raised incarnation.

## The failure detector

Time is divided into protocol periods. Each period, the detector probes exactly one member, chosen by randomized round-robin: the member list is shuffled once, walked to exhaustion, then reshuffled; a joining member is inserted at a random position in the current walk. Every member is probed within one full walk, so completeness is time-bounded rather than probabilistic.

- **Direct probe.** Send `PING` carrying a sequence number, await the matching `ACK` within the probe timeout.
- **Indirect probe.** On a miss, pick `k` other members at random and send each `PING-REQ(target)`; each relays a probe and forwards the `ACK` back, or answers `NACK` if the target did not respond to it either. A `NACK` is not evidence against the target; it is evidence that the indirect path worked, which feeds awareness below.
- **Suspicion.** If the period ends with no `ACK` by any path, the target becomes `suspect` locally and a `suspect` update enters the broadcast queue.
- **The buddy system.** When the scheduled probe target is currently suspected, the `PING` itself carries the `suspect` record about the target. A suspect that is merely slow thus hears its own indictment on the next probe and refutes immediately, instead of waiting for gossip to find it.

A missed `ACK` is the only failure signal. A healthy TCP connection to a wedged process still carries no `ACK`, so transport-level errors (failed dial, write error) merely fast-path the same miss; their absence proves nothing.

**Awareness.** Each node keeps a local health score, a small bounded counter. It worsens when the node's own behavior is suspicious to others (it failed to answer in time, it got suspected, an expected `NACK` did not come back) and improves with each clean probe round. The score multiplies the node's own probe timeout and period: a node struggling under an event-loop pause or CPU starvation slows its judgments of others rather than spraying false suspicions. This plus the nack channel and the decaying suspicion timer are the Lifeguard refinements, adopted whole.

## Suspicion timers

A fresh suspicion does not condemn on a fixed fuse. The timeout starts at a maximum and decays toward a minimum as independent confirmations (suspect records about the same member and incarnation from distinct suspectors) arrive:

- minimum = suspicion multiplier x ceil(log10(N + 1)) x protocol period
- maximum = max-multiplier x minimum
- on the c-th confirmation of an expected k, the remaining timeout rescales toward the minimum proportionally to log(c + 1) / log(k + 1)

One isolated accuser leaves the member the full maximum to refute; corroboration from around the cluster shortens the wait. Expiry marks the member `dead` and gossips the verdict. The logarithmic minimum exists because refutation itself needs O(log N) gossip rounds to spread; a constant timeout would silently break as the cluster grows.

## Dissemination

Updates spread two ways, both bounded:

- **Piggyback.** Every outgoing `PING`, `PING-REQ`, `ACK`, and `NACK` packs queued updates up to a byte budget.
- **The gossip tick.** On a short cadence, independent of the probe schedule, the node sends a pure gossip message (queued updates, nothing else) to a small fixed number of random members. Dissemination speed therefore never depends on probe traffic volume.

The broadcast queue holds at most one record per member (a superseding update replaces the queued one), each with a remaining-transmit counter initialized to a multiplier times ceil(log10(N + 1)). Packing prefers the least-transmitted records, with self-defense (`alive` raised by self) always first in line. A record leaves the queue when its counter reaches zero; anti-entropy heals whatever the tail misses.

Gossip targets include members that recently became `dead` or `left`, for a fixed window after the transition. A false death from a network blip then keeps hearing cluster traffic, notices its own obituary, and refutes; silence toward the freshly dead would make every false positive permanent until the victim's next outbound contact.

## Join, anti-entropy, and leave

- **Join.** The engine is started with seed addresses and performs a full state exchange (push-pull) over the stream channel with each reachable seed: both sides send their complete member tables and merge under the precedence table. If the merged state holds the joiner as `dead` or `left`, the joiner refutes with a raised incarnation. Exhausting all seeds settles the join as failed and the caller decides policy (retry, crash, wait).
- **Anti-entropy.** On a long fixed interval, the engine push-pulls with one random live member. This bounds the staleness a lost gossip record can cause and re-merges views after a healed partition.
- **Leave.** A graceful stop broadcasts `left(self)`, lingers a short drain window so the record rides out on further messages, then closes the transport. A kill without leave is simply detected as a failure.

## The transport

The contract in `transport.ts` has the packet-and-stream duality that SWIM implementations have converged on:

- **packet(to, bytes)**: fire-and-forget delivery of one bounded-size message (the ping family and gossip ticks, with piggybacked updates). No delivery report beyond local write failure; the protocol's own timeouts are the truth.
- **stream(to)**: an addressed byte stream for push-pull sync, whose bodies may exceed the packet cap.

Plus a listener the engine starts with callbacks for inbound packets and inbound streams. Protocol logic sees only this contract; the simulation harness implements it in memory, and a UDP packet implementation can arrive later without touching a line above the contract.

The first implementation carries both roles over TCP: a small pool of persistent connections for packets, capped and evicted least-recently-used, and short-lived connections for streams. Frames are length-prefixed with a version byte and type; every decode validates before it allocates, and garbage or an oversized frame closes the connection. TLS is optional and reuses the same `TlsOptions` shape and all-or-nothing rule remoting already defines. The gossip listener binds its own port, separate from remoting, so either subsystem can be enabled, drained, or torn down without the other noticing.

## Public surface of the package

The engine (`swim.ts`) exposes, to the seam only:

- `start()` / `join(seeds)` / `leave()` / `stop()`: lifecycle. Async, rejecting with typed errors.
- `members()`: a snapshot of current members with state and metadata.
- `self()`: this node's record.
- an event callback carrying `joined`, `left`, `dead`, and `updated` (metadata change) transitions, in the order the local view applied them.

Every tunable is an internal constant fixed to the values production SWIM deployments have converged on for single-network clusters, exported `@internal` for tests, never an option: protocol period 1s, probe timeout 500ms, 3 indirect probers, gossip tick every 200ms to 3 members, push-pull every 30s, retransmit multiplier 4, suspicion multiplier 4, suspicion max-timeout multiplier 6, gossip-to-the-dead and dead-retention windows 30s, awareness ceiling 8, metadata cap 512 bytes, packet budget 1400 bytes. Public configuration at the seam, when it arrives, is limited to bind host and port, seeds or a discovery provider, and TLS.

## Determinism and the simulation harness

`clock.ts` and `random.ts` are the only modules that touch `Date.now`, timers, or entropy, and the engine takes both as constructor inputs alongside the transport. The harness in `test/cluster/sim.ts` provides a scripted clock (time advances only when the test says so), an in-memory transport with programmable loss, latency, duplication, and partitions per link, and a seeded random source. Scenario tests assert protocol-level bounds: a join is known cluster-wide within a stated number of periods, a killed member is declared dead within the suspicion budget, a partition heal re-converges both sides, a paused-then-resumed member refutes and survives, a restarted member rejoins past its own obituary, a lone accuser cannot condemn faster than the maximum suspicion timeout, and no member is ever declared dead when loss and pauses stay inside the stated tolerance. Every scenario failure prints its seed.

## Verification

- **Unit.** Codec round-trips plus malformed-input rejection on every decode path; the precedence and revival table exhaustively; broadcast packing, supersession, and budget edge cases; suspicion decay arithmetic; awareness transitions; clock and random contracts.
- **Simulation.** The scenario suites above, run across a spread of seeds in CI.
- **Real sockets.** Transport tests on port 0: connect, pool eviction, half-open detection, garbage bytes, slow reader, TLS pair, mixed TLS rejection, stream alongside packets.
- **Multi-process.** A smoke-style script boots several processes, kills one with SIGKILL, and asserts the survivors converge on `dead` within the bound; further runs exercise graceful leave and kill-then-restart rejoin. Extends the existing cross-runtime smoke harness.
- **Coverage and benches.** Touched files hold 100% coverage. Membership runs entirely off the message send path; when the seam lands, the tell-throughput bench runs with membership active to prove the probe and gossip timers cost nothing measurable.

## Delivery plan

Each slice lands green on its own: code, mirrored tests, coverage held, no dead ends.

1. **Spec.** `.spec/cluster-membership.md`: exact frame layouts, update record encoding, the precedence and revival table, awareness rules, and every timing formula with its constants.
2. **Wire.** `wire.ts`: codecs and validation.
3. **View.** `view.ts`: member table, precedence, revival, retention, events. Pure state machine, exhaustively tested.
4. **Broadcast.** `broadcast.ts`: transmit-limited queue and packing.
5. **Clock, random, sim.** `clock.ts`, `random.ts`, and the simulation harness; the harness is a dependency of every slice after it.
6. **Suspicion.** `suspicion.ts`: confirmation-driven decay timers, under simulation.
7. **Probe.** `probe.ts`: the detector with indirect probes, nacks, awareness, the buddy system, and the gossip tick, under simulation.
8. **Sync.** `sync.ts`: push-pull, join, rejoin refutation, anti-entropy, leave drain, under simulation.
9. **Engine.** `swim.ts`: composition, lifecycle, public surface; full scenario suites.
10. **TCP transport.** `transport.ts`: real sockets, pool, streams, TLS, fault injection; multi-process smoke.
11. **Hardening.** Seed-spread simulation campaign in CI, cross-runtime smoke, constants documented in the spec.

The seam (`clustering.ts`, system options, eventstream integration, discovery providers) is deliberately outside this plan; it starts the next design alongside the registry, so the membership package proves itself in isolation first, exactly as `net/` did before remoting.
