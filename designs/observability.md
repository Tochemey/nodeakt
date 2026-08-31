# Observability: metrics design

Metrics let an operator watch a running system in production. This document is the design of record: what the runtime exposes, the public interface, how the numbers are produced and merged, and why the shape is what it is. It is written for the architects and maintainers who will build and evolve this layer.

## Purpose

The runtime already knows a great deal about itself: how many actors are alive, how deep their mailboxes and stashes run, how many messages it has processed, how often actors restart or land in dead letters, and, when asked, how long a message takes to handle. Observability is the work of surfacing what the runtime already knows, cheaply, in a form an operator's dashboard can read.

## Non-goals

No metrics SDK, no exporter, no collector protocol, no bundled OpenTelemetry. Each of those is a dependency, and dependencies live in a user package, never in the core. The core's responsibility ends at a typed, aggregated snapshot of its own state.

Distributed tracing is out of scope. A span allocated per message is steady-state garbage on the hot path, and threading trace context through every send and every isolate boundary is a large undertaking whose cost lands exactly where this runtime works hardest to stay cheap. If request-level tracing is wanted later, the direction is an out-of-band, packet-level mechanism (for instance eBPF observing the transport) that watches the wire without touching the message path, not an in-process span on every receive.

## The governing constraint

Two rules shape every decision below:

1. The core takes no dependency. The vendor integration lives outside it.
2. The core costs nothing on the message hot path until an operator turns a signal on. A system that never enables metrics runs exactly as it does today, byte for byte on the drain loop.

Everything else follows from holding these two at once.

## Three tiers of signal

Every metric the runtime can offer falls into one of three tiers, ordered by what it costs to collect. Each metric is assigned to exactly one tier, so no number is counted twice and none is paid for twice. This tiering is the central design idea.

**Tier 0, already emitted.** Lifecycle transitions (actor started, stopped, restarted, passivated, and dead letters) are already published on the event stream. A counter derived from them costs one subscription and an increment per transition, and transitions are rare relative to messages. These become monotonic counters, and they touch the message path not at all.

**Tier 1, pull from live state.** Gauges (active actors, suspended actors, mailbox depth, stash depth) are already readable from live objects. Nothing is maintained for them between collections; they are read on demand when a snapshot is taken. Cost on the message path: zero.

**Tier 2, opt-in on the hot path.** Message-processing latency is the only signal that can neither be derived from an existing event nor pulled from live state, because a distribution needs every sample. It requires reading the clock around every `receive`, which is precisely the cost the drain loop is built to avoid. It is therefore gated behind an explicit opt-in and, when off, adds nothing to the loop. Message throughput sits just inside this tier: it needs a per-message increment but no clock read, so it rides along whenever metrics are enabled at a cost of one predicted branch and one in-place increment.

## Architecture

### A framework-owned registry, one per isolate

When metrics are enabled, each isolate holds one internal registry. It is core state, not a user object. It maintains the Tier 0 counters from its own event stream, accumulates the Tier 2 latency histogram when timing is on, and reads live actors for Tier 1 gauges only when a snapshot is requested. Nothing here calls user code, and nothing crosses an isolate boundary per message. When metrics are not enabled, the registry does not exist: no subscription, no counters, no histogram.

### Collection is a pull, and the vendor lives outside the core

The system exposes one method that returns a snapshot of its own state. An adapter, written by the user over that one method, maps the snapshot onto whatever backend it targets. The core owns no timer and no exporter; collection happens only when the adapter asks, on whatever schedule it chooses. Pull is the collection model both major backends already speak (OpenTelemetry observable instruments, a Prometheus scrape), so a pull core stays vendor-blind while integrating naturally with either.

### The histogram is the one continuously-maintained exception

A latency distribution cannot be reconstructed by sampling live state, so it is the single piece of numeric state the core accumulates continuously, into a fixed set of buckets, and only when timing is enabled. Everything else in a snapshot is either an event-fed counter or a live-state read.

### Fleet metrics are unlabeled; per-actor is a separate call

Per-actor labels are the classic way to melt a time-series database. The fleet snapshot therefore carries no actor identity: it is aggregate by construction. When an operator wants one actor's numbers (a health endpoint, a debug command), a separate on-demand call answers from fields the actor already keeps. Introspection and fleet metrics are deliberately different surfaces so the cheap, unbounded one cannot leak into the expensive, bounded one.

## The interface

The design produces a small public surface: one option to turn metrics on, one method to read them, and one call for single-actor introspection. Everything an adapter consumes is plain readonly data, no methods and no vendor types, so any backend can be mapped over it from outside the core.

### Enabling

```ts
interface MetricsOptions {
  /** Turns on the per-isolate registry and the Tier 0 and Tier 1 signals. */
  readonly enabled: true;

  /**
   * Also times every message through `receive` and accumulates a
   * processing-duration histogram. Off by default: it reads a monotonic
   * clock around every message, which measurably lowers peak throughput.
   * Turn it on for latency, not just liveness.
   */
  readonly processingDuration?: boolean;
}

interface ActorSystemOptions {
  // ...existing fields...
  metrics?: MetricsOptions;
}
```

`enabled: true` rather than a bare boolean so the option reads as a decision and can grow without a breaking change. `processingDuration` is a feature enable with a stated cost, not a tuning knob, so it is a public flag; the histogram bucket boundaries behind it stay internal.

### Reading

```ts
// on ActorSystem
collectMetrics(): Promise<MetricsSnapshot>

// on PID
metrics(): ActorMetrics
```

`collectMetrics` returns a machine-wide snapshot, merged across isolates. It rejects with `ErrActorSystemNotStarted` when the system is not running, and answers a zeroed but valid snapshot (with `messages.processingDurationMs` absent) when metrics were never enabled, so an adapter can be wired unconditionally. `metrics` on a PID is synchronous, on-demand, always available regardless of the option, allocates one `ActorMetrics` per call, and reads fields the actor already keeps.

### The snapshot

```ts
interface MetricsSnapshot {
  readonly system: string;       // the actor system name
  readonly collectedAt: number;  // epoch ms, main isolate clock
  readonly isolates: number;     // how many isolates contributed
  readonly actors: ActorFleetMetrics;
  readonly messages: MessageMetrics;
  readonly mailbox: MailboxMetrics;
  readonly deadlettersTotal: number; // counter, from Deadletter events
}

interface ActorFleetMetrics {
  readonly active: number;          // gauge: live actors now (Tier 1 pull)
  readonly suspended: number;       // gauge: currently suspended (Tier 1 pull)
  readonly startedTotal: number;    // counter: ActorStarted
  readonly stoppedTotal: number;    // counter: ActorStopped
  readonly restartedTotal: number;  // counter: ActorRestarted
  readonly passivatedTotal: number; // counter: ActorPassivated
}

interface MessageMetrics {
  readonly processedTotal: number;               // monotonic fleet counter
  readonly processingDurationMs?: HistogramData; // present only when timing is on
}

interface HistogramData {
  readonly count: number;                       // total samples
  readonly sum: number;                         // total ms
  readonly buckets: readonly HistogramBucket[]; // cumulative, by upper bound
}

interface HistogramBucket {
  readonly leMs: number;   // inclusive upper bound in ms; Infinity for the last
  readonly count: number;  // cumulative count at or below leMs
}

interface MailboxMetrics {
  readonly totalDepth: number; // sum of mailbox depth over live actors (gauge)
  readonly maxDepth: number;   // deepest single mailbox (gauge)
}

interface ActorMetrics {
  readonly path: string;
  readonly processedCount: number; // per-actor, resets on restart
  readonly restartCount: number;
  readonly mailboxSize: number;
  readonly stashSize: number;
  readonly childrenCount: number;
  readonly lastActivity: number;   // epoch ms
  readonly suspended: boolean;
}
```

Fleet metrics carry no actor identity, by the cardinality decision below; single-actor numbers come only from `PID.metrics`. `MessageMetrics.processedTotal` (a monotonic fleet counter that survives restarts) and `ActorMetrics.processedCount` (the per-actor field that resets on restart and also drives passivation) are computed from different state and must not be conflated.

## Internal mechanics

### The registry

`MetricsRegistry` is constructed only when `metrics.enabled`, one per isolate. It owns the Tier 0 counters incremented from a subscription to the local event stream, the monotonic `processedTotal` accumulator, the processing-duration histogram when `processingDuration` is set, and an `isolateSnapshot()` method that reads Tier 1 gauges from live actors and returns this isolate's contribution to the merge. It holds no reference to user code and is long-lived, so it produces no per-message garbage.

### The hot-path guard and throughput counter

The enable check and the counter access are the same field. Each `PID` holds `_metrics: MetricsRegistry | null`, null when metrics are off. The drain loop, after a successful `receive`, does:

```ts
const m = this._metrics;
if (m !== null) { m.processed++; }
```

One monomorphic pointer load, one compare, one not-taken branch when off. `m.processed` is a numeric field mutated in place. It lives on the registry rather than the PID so a restart resetting `PID._processedCount` cannot make the fleet counter run backward. `_processedCount` and `_latestActivity` stay exactly as they are and keep serving passivation and `PID.metrics()`. There are two increment sites to touch, matching the two existing `_processedCount++` sites (the synchronous drain and the async-behavior path).

### The processing-duration histogram

Enabled only when `processingDuration` is set, gated by the same `_metrics` reference plus a boolean `m.timing`:

- Clock: `performance.now()`, monotonic, so a wall-clock adjustment cannot yield a negative sample.
- Synchronous drain: read the clock once per message boundary and take successive deltas, so the fast path pays one clock read per message, not two.
- Async-behavior path: read before the await and after resolution (two reads); that path already yields, so the relative cost is smaller.
- Storage: bucket counts in a `Uint32Array` sized to the boundaries, plus a `number` running `sum`. No allocation per sample, and no small-integer-to-double transition on the counts.
- Bucket boundaries, an internal constant in milliseconds, reported cumulatively:

  ```
  0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, Infinity
  ```

The histogram's `count` equals the number of timed messages; `processedTotal` remains the always-on counter when timing is off.

### Event-derived counters

The registry subscribes to the local event stream and maps each lifecycle event to one counter:

| Event | Counter |
|---|---|
| `ActorStarted` | `actors.startedTotal` |
| `ActorStopped` | `actors.stoppedTotal` |
| `ActorRestarted` | `actors.restartedTotal` |
| `ActorPassivated` | `actors.passivatedTotal` |
| `Deadletter` | `deadlettersTotal` |

`ActorSuspended` and `ActorReinstated` are not consumed: the `suspended` gauge is a Tier 1 pull (`PID.isSuspended()` over live actors), which reflects current state directly. Classification is a switch on the event, not a chain of `instanceof`, and runs at lifecycle cadence, never per message.

### The Tier 1 gauge read

`isolateSnapshot()` walks the isolate's live actors once and accumulates `active` (count), `suspended` (count of `isSuspended()`), `mailbox.totalDepth` (sum of `Mailbox.len()`), and `mailbox.maxDepth` (max of `Mailbox.len()`). The walk is O(live actors) and happens only at collection cadence.

## Collection and multi-core merge

Actors run across worker isolates, so a machine-wide view must aggregate across them. Aggregating on pull, at dashboard cadence, means the only cross-isolate traffic is one request and one reply per collection, rather than a forwarded event on every actor start and stop.

### The control message

Collection on the main isolate broadcasts a new control message over the existing `ControlPlane`, the channel already used for placement and name control, with its reply-by-sequence correlation:

- Request kind: `CONTROL_METRICS`.
- Reply payload: `IsolateMetrics`, this isolate's counters, gauge accumulations, and histogram bucket array. Internal, not exported.

### Merge rules

The main isolate merges its own `isolateSnapshot()` with every reply:

| Field | Merge |
|---|---|
| every `*Total` counter, `deadlettersTotal` | sum |
| `actors.active`, `actors.suspended` | sum |
| `mailbox.totalDepth` | sum |
| `mailbox.maxDepth` | max |
| `messages.processedTotal` | sum |
| histogram `buckets[i]`, `count`, `sum` | element-wise sum |
| `isolates` | count of contributors, including main |
| `collectedAt` | main isolate clock at merge |
| `system` | the system name |

### Missing isolates

A worker that dies mid-collection is absent from the merge; its counters are gone with it, which is the honest semantics for a crashed isolate. Collection does not fail because an isolate dropped: it resolves with the contributors that answered, and `isolates` reflects how many that was.

## Cluster and remoting sections

A single machine's snapshot describes the actors on it. A node in a cluster also has a transport and a membership view, and both are worth reporting. The snapshot carries them as two optional sections, present only when remoting or clustering is active, so a single-machine snapshot is unchanged.

Metrics stay per node. Each node reports its own transport and its own view of the membership; the operator's backend rolls those up across nodes, the same way it already scrapes one endpoint per pod. The core never pulls a peer's metrics over the wire: a fleet-wide fan-out would put metric traffic on the transport that transport is built to keep free, and it would duplicate the aggregation every metrics backend already does.

### The interface

```ts
/** This node's transport. */
interface RemotingMetrics {
  readonly peers: number;             // connected peers
  readonly messagesSent: number;      // cumulative frames sent
  readonly messagesReceived: number;  // cumulative frames received
  readonly bytesSent: number;         // cumulative over this node's life
  readonly bytesReceived: number;
  readonly sendQueueBytes: number;    // currently queued for send
}

/** This node's view of the cluster membership. */
interface ClusterMetrics {
  readonly members: number;           // total known
  readonly alive: number;
  readonly suspect: number;
  readonly dead: number;
  readonly left: number;              // gracefully departed, distinct from dead
  readonly isCoordinator: boolean;
  readonly coordinatorChanges: number;  // times this node's coordinator changed
  readonly relocationsTotal: number;    // actor recreations this node has driven
}

interface MetricsSnapshot {
  // ...existing fields...
  readonly remoting?: RemotingMetrics;  // present when remoting is enabled
  readonly cluster?: ClusterMetrics;    // present when clustering is enabled
}
```

### The cost posture

These sections hold to the same bar as Tier 0 through Tier 2: the only per-message work is counter increments, which are within noise, and everything else is pulled at collection or derived from membership transitions. Nothing reads a clock and nothing makes a cross-isolate or cross-node call on any path. The clock-bearing latency histogram remains the single opt-in signal, because a clock read is the one genuinely expensive per-message operation; a counter increment is not.

- **Bytes come from the socket.** The runtime already maintains a cumulative read and written byte count on every socket. The transport sums those across its live connections at collection and folds a connection's final counts into a per-node accumulator when it closes, so `bytesSent` and `bytesReceived` are monotonic over the node's life without touching the message path at all. A connection closing is a rare event, not the hot path.
- **Message counts are a frame-level increment.** `messagesSent` and `messagesReceived` count frames where the transport already accounts each frame for flow control: it writes queued bytes per frame on send and loops per frame on receive, so the added increment sits in that same tier. It is a non-boxed integer add, the same posture as the throughput counter that is already always-on when metrics are enabled and benches within noise. Counts and bytes are both standard transport signals and answer different questions: bytes are bandwidth, counts are the operation rate, and their ratio is the average message size.
- **Queue depth and peer count read state that already exists.** `sendQueueBytes` sums the per-connection queued-and-in-flight bytes the flow controller already tracks; `peers` is the size of the peer table. Both are read at collection.
- **The cluster section is pulled and event-derived.** `members`, `alive`, `suspect`, `dead`, `left`, and `isCoordinator` are counted from the membership state at collection, and membership gossip is off the message path to begin with. `coordinatorChanges` and `relocationsTotal` are counters bumped on membership transitions, which are node-scale events, never per-message operations.

The proof is the standing rule: the tell-throughput benchmark ships off against on, and the transport counters must land within noise. A counter that measurably moves throughput is a signal to fix the hot path, not to drop the metric.

### Collecting them

Remoting and the cluster engine live on the main isolate, so both sections are read from the main isolate alone and attached to the snapshot after the isolate merge, the way dead letters already are. The multi-core merge is unchanged: worker isolates carry neither a transport nor a membership view, so they contribute nothing here.

## Performance and GC posture

The design is built so the cost is proportional to what is switched on.

**Off is genuinely free.** With metrics disabled the drain loop does at most one predicted, not-taken branch, and allocates nothing. The commitment is that the tell-throughput benchmark with metrics off matches the current baseline within noise.

**Counting is allocation-free in steady state.** The throughput counter is a numeric field on one long-lived registry object, mutated in place. It crosses the small-integer boundary once early in the process and thereafter is stored as a double field that V8 updates without boxing a new number per increment, so there is no per-message garbage. The latency histogram is backed by a typed integer array, so its bucket counts never allocate either.

**Latency is the one real, benched cost.** It reads a monotonic clock around every `receive`, and the clock read is a known dominant per-message cost in this runtime. That is the whole reason it is a separate opt-in that is off by default: an operator who needs latency accepts a measured throughput reduction, quantified in the benchmark; one who does not pays nothing.

**Collection allocates, but off the hot path.** Building a snapshot and merging across isolates allocates short-lived objects at dashboard cadence, collected cheaply in the young generation. The registry itself is long-lived and produces no churn.

Every hot-path change ships with the tell-throughput benchmark showing off against on, per the standing rule that hot-path changes are benched before they land.

## Design decisions

**Pull, not push, for everything but the histogram.** A pull model lets the core expose one data method, stay vendor-blind, merge multi-core once per collection rather than per signal, and keep zero user calls on the message path. The histogram is the sole exception, because a distribution cannot be pulled from live state.

**No user-implemented recorder for metrics.** Forcing the user to implement a recorder would put a virtual call on every signal and, for worker-isolate actors, a cross-isolate call per message. A snapshot the adapter reads means the core never calls user code on the hot path.

**Fleet metrics unlabeled; per-actor is on-demand introspection.** Keeps the unbounded-cardinality surface out of the time series, and gives single-actor inspection its own cheap, always-available call.

**Latency is opt-in and off by default.** It is the only signal that touches the clock on the hot path the drain loop is built to protect.

**Enabled state is fixed at construction.** The hot-path guards are stable branches the JIT predicts, not per-message decisions.

**The core maintains registry state only when enabled.** Off is genuinely off: no subscription, no histogram, no counters on a system that did not ask for metrics.

**Merge on pull, not forward on every event.** Multi-core aggregation happens once per collection over the control plane, so an actor start or stop on a worker never generates cross-isolate metric traffic.

## Testing

- Unit: counters against synthesized event streams; histogram bucketing against known sample sets; the merge against hand-built `IsolateMetrics` (counters and gauges sum, `maxDepth` takes the max, buckets add).
- Integration: a running system with metrics enabled asserting `collectMetrics()` reflects spawns, stops, restarts, dead letters, and mailbox depth; a multi-core system asserting the merge includes worker-isolate actors.
- Hot path: tell-throughput benchmark reported for metrics off (must match baseline), metrics on without timing, and timing on, the latter two with their measured cost.
- Coverage: touched files stay at full coverage, per the standing rule.
