# Metrics

The runtime can report what it knows about itself: how many actors are alive, how many messages it has processed, how often actors restart or land in dead letters, and how deep their mailboxes run. Metrics are off by default, cost nothing on the message path until you turn them on, and pull in no dependency: the runtime hands you a plain snapshot of numbers, and you map it onto whatever backend you use (OpenTelemetry, Prometheus, a log line) from your own code.

## Turn it on

Pass `metrics` when you create the system:

```ts
import { ActorSystem } from "@tochemey/nodeakt";

const system = new ActorSystem("orders", {
  metrics: { enabled: true },
});
```

`MetricsOptions` has one required field and one optional one:

```ts
interface MetricsOptions {
  enabled: true;
  processingDuration?: boolean;
}
```

`enabled` switches the metrics on. `processingDuration` additionally times every message through `receive` to build a latency distribution; it is off by default because timing reads a clock around every message, which lowers peak throughput. Leave it off unless you need latency, not just liveness. A system created without `metrics` maintains no metrics state at all, and its message loop is untouched.

## Read a snapshot

`collectMetrics` returns a point-in-time `MetricsSnapshot`. It is plain readonly data you read on whatever schedule your backend scrapes:

```ts
const snapshot = await system.collectMetrics();

console.log(snapshot.actors.active);
console.log(snapshot.messages.processedTotal);
console.log(snapshot.deadlettersTotal);
```

The snapshot groups the numbers by concern:

```ts
interface MetricsSnapshot {
  system: string;
  collectedAt: number;
  isolates: number;
  actors: ActorFleetMetrics;
  messages: MessageMetrics;
  mailbox: MailboxMetrics;
  deadlettersTotal: number;
}
```

`ActorFleetMetrics` carries the actor counts: the `active` and `suspended` gauges, and the `startedTotal`, `stoppedTotal`, `restartedTotal`, and `passivatedTotal` counters. `MessageMetrics` carries `processedTotal`, the count of messages the fleet has handled, and, when `processingDuration` is on, a `processingDurationMs` distribution. `MailboxMetrics` carries mailbox depth across the fleet, both the summed `totalDepth` and the deepest single mailbox.

The distribution, when present, is a `HistogramData`: a total `count`, a `sum` of milliseconds, and cumulative `HistogramBucket` entries by upper bound.

The fleet snapshot carries no actor identity, so it never turns into a high-cardinality time series. Calling `collectMetrics` on a system that never enabled metrics resolves to a valid, zeroed snapshot, so an adapter can be wired unconditionally; calling it on a system that is not running rejects with `ErrActorSystemNotStarted`.

## Inspect one actor

When you want a single actor's numbers, a health check or a debug command rather than a dashboard, `PID.metrics` answers on demand from the fields the actor already keeps. It needs no `metrics` option and never becomes a fleet metric:

```ts
const stats: ActorMetrics = pid.metrics();

console.log(stats.mailboxSize, stats.restartCount, stats.stashSize);
```

```ts
interface ActorMetrics {
  path: string;
  processedCount: number;
  restartCount: number;
  mailboxSize: number;
  stashSize: number;
  childrenCount: number;
  lastActivity: number;
  suspended: boolean;
}
```

## Wiring a backend

Because `collectMetrics` returns plain data, an adapter is a few lines you own, outside the runtime. Read the snapshot on a timer, or from your metrics backend's own collection callback, and record each field as a counter or gauge there. The runtime takes on no metrics dependency, so you are free to choose the backend and keep it out of the core.
