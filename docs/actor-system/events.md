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

## Observability

The event stream is the runtime's built-in observation channel: an in-process, synchronous topic the system publishes its own events to, separate from the messages your actors exchange. Subscribe to watch what the runtime is doing and feed it into logging, metrics, or a dashboard, without coupling that plumbing to any actor.

The stream carries two families of events: **dead letters** (`Deadletter`, below), the signal for lost work, and **lifecycle events**, one per transition an actor makes. A rising dead-letter rate means senders are outrunning a mailbox, addressing stopped actors, or sending messages nobody handles. Lifecycle events let you count live actors, trace restarts, or watch passivation without touching actor code. Narrow each with `instanceof` and turn it into a metric or a log line from the subscriber.

Delivery is synchronous on the publisher's stack, so subscribers must stay fast. Hand slow work such as a network or disk write to something outside the callback rather than blocking the publisher.

Publishing is free when nothing is subscribed: the runtime builds a lifecycle event only once the stream has a subscriber, so an unobserved system pays nothing for them.

## Lifecycle events

Each lifecycle event names the actor it is about through `actorPath`, the actor's canonical path string, and carries a `timestamp` in milliseconds since the epoch. They are published for **user actors only**; the runtime's own guardians and the dead-letter sink stay silent.

| Event | Published when | Extra fields |
| --- | --- | --- |
| `ActorStarted` | The actor has run `preStart`, is registered, and is ready to receive. | |
| `ActorChildCreated` | An actor spawns a child. The child's own `ActorStarted` is published alongside it. | `parent`: the parent's canonical path string. |
| `ActorStopped` | The actor has fully stopped: mailbox drained, `postStop` run, left the tree. Every stop publishes it, whatever the cause. | |
| `ActorPassivated` | The actor is stopped because it stayed idle past its [passivation](../actor/passivation.md) strategy. Published ahead of the same actor's `ActorStopped`, marking that stop as idle-triggered. | |
| `ActorRestarted` | A faulted actor was [restarted](../actor/supervision.md) in place: same path, `preStart` re-run, processing resumed. No `Terminated` is sent, because the actor did not stop. | |
| `ActorSuspended` | A fault left the actor [suspended](../actor/supervision.md): it holds its state but processes nothing until restarted or reinstated. | `reason`: the failing error's message, or `"restart budget exhausted"`. |
| `ActorReinstated` | A suspended actor was revived without resetting its state. | |

```ts
import { ActorStarted, ActorStopped, ActorRestarted } from "@tochemey/nodeakt";

const live = new Set<string>();

system.subscribe((event) => {
  if (event instanceof ActorStarted) {
    live.add(event.actorPath);
  } else if (event instanceof ActorStopped) {
    live.delete(event.actorPath);
  } else if (event instanceof ActorRestarted) {
    console.warn(`restarted ${event.actorPath}`);
  }
});
```

Two pairings follow from how the runtime actually moves an actor:

- An idle actor publishes `ActorPassivated` and then `ActorStopped`. A subscriber counting live actors keys on `ActorStarted` and `ActorStopped` and treats `ActorPassivated` as extra context.
- A supervised restart publishes `ActorSuspended` and then `ActorRestarted`. A fault always suspends the actor first; the supervisor then decides, and a restart holds it suspended through any backoff before reviving it in place. An actor left suspended for good (no matching directive, or the restart budget spent) publishes `ActorSuspended` with no `ActorRestarted` to follow.

## Dead-letter causes

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
