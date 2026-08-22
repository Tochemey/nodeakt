# Events and dead letters

Subscribe to the system's runtime event stream after `start`. The first event kind is `Deadletter`; later runtime events use the same subscription. Narrow with `instanceof`, the same type switch as `ctx.message`.

```ts
import { ActorSystem, Deadletter } from "@tochemey/nodeakt";

const system = new ActorSystem("orders");
await system.start();

system.subscribe((event) => {
  if (event instanceof Deadletter) {
    console.warn(`dead letter for ${event.receiver}: ${event.reason}`);
  }
});
```

`subscribe` and `unsubscribe` throw `ErrActorSystemNotStarted` when the system is not running. Subscribing the same function twice is a no-op. `unsubscribe` ignores unknown subscribers. Keep a reference to the function you passed in if you intend to unsubscribe it.

A subscriber that throws is logged (`event subscriber failed`) and does not stop the publisher or the other subscribers.

`stop()` closes the stream.

## When a message becomes a dead letter

A **user** message that the runtime could not hand to its receiver is published as `Deadletter`. Typical reasons:

- the target is not running (`ErrDead`)
- a bounded mailbox is at capacity (`ErrMailboxFull`)
- the mailbox has been disposed (`ErrMailboxDisposed`)
- the receiver called `ctx.unhandled()` (`ErrUnhandled`)
- encoding or cloning failed on a cross-isolate send

`PostStart`, `Terminated`, and internal runtime commands never become dead letters.

On a **local** send, `PID.tell` returns the mailbox error immediately and the message is also published as a dead letter. On a **cross-isolate** `tell`, transport accept is not mailbox accept: `tell` returns `null` when the envelope was posted, and a full or missing mailbox surfaces later as a dead letter on the receiving isolate. See [Multi-core](../multi-core/index.md).

## `Deadletter` fields

| Field | Meaning |
| --- | --- |
| `sender` | Canonical path string of the sending actor, or `undefined` when the delivery carried no sender. |
| `receiver` | Canonical path string of the actor that could not receive. |
| `message` | The message that was not handled. |
| `sendTime` | When the failed send happened, milliseconds since the epoch. |
| `reason` | Why it was not handled: the failing error's `message`. |

Use `ctx.unhandled()` when unknown messages are expected. Throwing engages [supervision](../actor/supervision.md). `unhandled` routes the message to dead letters and processing continues.

## `EventStream`

`EventStream` and `StreamSubscriber` are exported. Application code does not construct a stream. Subscribe through `ActorSystem.subscribe`. The stream is a synchronous topic channel. Callbacks run on the publisher's stack.
