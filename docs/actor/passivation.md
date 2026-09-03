# Passivation

Passivation is a graceful stop of an idle actor: queued messages drain, `postStop` runs, and the actor is removed. An actor spawned without a strategy passivates after `DefaultPassivationTimeout` of inactivity; pass a `LongLivedStrategy` to opt out and run until it is explicitly stopped.

Passivation strategies are live objects. They cannot ride a [`Props`](../multi-core/index.md) spawn.

```ts
import { DefaultPassivationTimeout, MessagesCountBasedStrategy, TimeBasedStrategy } from "@tochemey/nodeakt";

await system.spawn("cache", new Cache(), {
  passivationStrategy: new TimeBasedStrategy(DefaultPassivationTimeout), // 120_000 ms
});
```

## Strategies

| Class | Passivates when |
| --- | --- |
| `TimeBasedStrategy(timeout)` (default) | The actor has processed no message for `timeout` milliseconds. `timeout` must be a positive finite number; otherwise the constructor throws `RangeError`. |
| `MessagesCountBasedStrategy(maxMessages)` | The actor has processed `maxMessages` messages. `maxMessages` must be a positive integer; otherwise `RangeError`. |
| `LongLivedStrategy` | Never; the actor runs until it is explicitly stopped. |

An actor spawned without an explicit strategy gets a `TimeBasedStrategy(DefaultPassivationTimeout)`. `DefaultPassivationTimeout` is `120_000` (two minutes).

`PassivationStrategy` is the union of these three classes. It is not an open interface: the runtime only schedules the strategies above.

## Idle

The scheduler passivates a time-based actor only when it is idle: no message is being processed, the mailbox is empty, the stash is empty, and no [request](reentrancy.md) is in flight. A stashed message counts as pending work because stopping would drop it.

Time-based scheduling uses one shared timer for the system. Message processing only writes a timestamp; when the timer fires, each due actor is re-checked against its latest activity and rescheduled if it ran in the meantime.

Message-count strategies involve no timer: the actor passivates itself after it has processed the configured number of messages.
