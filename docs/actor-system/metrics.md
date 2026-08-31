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

## Report on a timer

Because `collectMetrics` returns plain data, the simplest adapter is a few lines you own: read the snapshot on an interval and do something with it. This is a whole working reporter, no dependency involved:

```ts
const reporter = setInterval(() => {
  void system.collectMetrics().then((snapshot) => {
    console.log(
      `active=${snapshot.actors.active}`,
      `processed=${snapshot.messages.processedTotal}`,
      `mailbox=${snapshot.mailbox.totalDepth}`,
      `deadletters=${snapshot.deadlettersTotal}`,
    );
  });
}, 1_000);

// on shutdown
clearInterval(reporter);
```

When `processingDuration` is on, the snapshot also carries the latency distribution, and you read a percentile straight off the cumulative buckets: walk them in order and take the upper bound of the first bucket whose running `count` reaches your target rank.

```ts
function quantileMs(histogram: HistogramData, q: number): number {
  const target = q * histogram.count;
  for (const bucket of histogram.buckets) {
    if (bucket.count >= target) {
      return bucket.leMs;
    }
  }

  return Number.POSITIVE_INFINITY;
}
```

The [metrics example](https://github.com/Tochemey/nodeakt/blob/main/examples/metrics/main.ts) (`make metrics`) puts both together: it drives one busy worker with a burst of messages and prints active count, throughput, mailbox depth, and the latency average and percentiles as the backlog fills and drains.

## An OpenTelemetry adapter, in your own code

The runtime takes on no metrics dependency, so a vendor adapter lives outside the core, in a file you own that adds the vendor SDK itself. OpenTelemetry pulls on its own schedule through a collection callback, which lines up exactly with a pull-based snapshot: scrape once per callback and fan the numbers out across the instruments.

```ts
// your-app/otel.ts: nodeakt does not depend on this; you add
// @opentelemetry/api in your own project and keep the wiring here.
import { metrics } from "@opentelemetry/api";
import type { ActorSystem } from "@tochemey/nodeakt";

export function wireOpenTelemetry(system: ActorSystem): void {
  const meter = metrics.getMeter("nodeakt");

  const active = meter.createObservableGauge("nodeakt.actors.active");
  const suspended = meter.createObservableGauge("nodeakt.actors.suspended");
  const processed = meter.createObservableCounter("nodeakt.messages.processed");
  const mailbox = meter.createObservableGauge("nodeakt.mailbox.depth");
  const deadletters = meter.createObservableCounter("nodeakt.deadletters");

  // One batch callback scrapes once and observes every instrument.
  // OpenTelemetry decides when it runs; the runtime just answers.
  meter.addBatchObservableCallback(
    async (batch) => {
      const snapshot = await system.collectMetrics();
      batch.observe(active, snapshot.actors.active);
      batch.observe(suspended, snapshot.actors.suspended);
      batch.observe(processed, snapshot.messages.processedTotal);
      batch.observe(mailbox, snapshot.mailbox.totalDepth);
      batch.observe(deadletters, snapshot.deadlettersTotal);
    },
    [active, suspended, processed, mailbox, deadletters],
  );
}
```

The processing-duration histogram needs a different mapping: its buckets arrive already aggregated, so you do not re-record them into an OpenTelemetry histogram (that instrument expects raw observations). Expose each cumulative bucket as its own observable gauge keyed by the `leMs` upper bound, which is the shape a Prometheus histogram exports anyway, or publish the average from `sum / count` alongside the percentiles the reporter above computes. Either way the mapping stays in your adapter, and the core keeps handing out nothing but numbers.
