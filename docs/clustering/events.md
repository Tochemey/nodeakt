# Cluster events

A clustered system publishes membership and relocation events on the same [event stream](../actor-system/events.md) as its lifecycle and dead-letter events. Subscribe to watch the cluster form, change, and heal, then narrow with `instanceof`, the same type switch used for `ctx.message`.

```ts
import {
  NodeJoined, NodeLeft, CoordinatorChanged,
  RelocationStarted, RelocationCompleted, RelocationFailed,
} from "@tochemey/nodeakt";

system.subscribe((event: unknown): void => {
  if (event instanceof NodeJoined) log("joined", event.address);
  if (event instanceof NodeLeft) log("left", event.address);
  if (event instanceof CoordinatorChanged) log("coordinator is now", event.coordinator);
  if (event instanceof RelocationCompleted) log("relocated", event.relocated, "off", event.departed);
  if (event instanceof RelocationFailed) log("could not yet relocate", event.names);
});
```

::: tip
Subscribe **after** `await system.start()`. Subscribing needs the system running.
:::

## The events

Every event carries a `timestamp` (epoch milliseconds). Addresses are cluster addresses (`host:port`).

| Event                 | Fields                  | Fired when                                                                                   |
|-----------------------|-------------------------|----------------------------------------------------------------------------------------------|
| `NodeJoined`          | `address`               | A node joins the cluster.                                                                    |
| `NodeLeft`            | `address`               | A node departs, gracefully or by failure.                                                    |
| `CoordinatorChanged`  | `coordinator`           | The [coordinator](membership.md#the-coordinator) changes, because the previous one departed. |
| `RebalanceStarted`    |                         | The coordinator begins redistributing registry partitions after a membership change.         |
| `RebalanceCompleted`  |                         | That redistribution finishes.                                                                |
| `RelocationStarted`   | `departed`              | A [relocation](relocation.md) pass begins for a departed node.                               |
| `RelocationCompleted` | `departed`, `relocated` | A pass finishes; `relocated` is the names it re-placed this pass.                            |
| `RelocationFailed`    | `departed`, `names`     | A pass could not place `names` yet; a later sweep retries them (not terminal).               |

## Membership versus relocation

The two families answer different questions:

- **Membership** (`NodeJoined`, `NodeLeft`, `CoordinatorChanged`, `RebalanceStarted`, `RebalanceCompleted`) tells you how the cluster's shape is changing.
- **Relocation** (`RelocationStarted`, `RelocationCompleted`, `RelocationFailed`) tells you how actors are being recreated after a node departs.

`RelocationCompleted` reports only the names a given pass moved, so a successor coordinator resuming an interrupted pass reports what *it* placed, not names an earlier coordinator already moved. To follow a specific relocatable actor across a move, re-resolve it with [`actorOfAsync`](messaging.md) when you see its node leave, rather than parsing `relocated` lists.

## A follow-the-actor pattern

A watcher that must keep messaging a relocatable actor across moves can re-resolve on relocation:

```ts
let target = await system.actorOfAsync("sequencer");

system.subscribe(async (event: unknown): Promise<void> => {
  if (event instanceof RelocationCompleted) {
    // Its owner may have changed; resolve the current home.
    target = await system.actorOfAsync("sequencer");
  }
});
```

Because a routed handle re-resolves on a stale send anyway (see [messaging](messaging.md#following-a-moved-actor)), this is an optimization for latency-sensitive callers, not a correctness requirement.
