# Errors

The public API fails in two shapes: sentinel errors and class errors. Whether a failure is returned, thrown, or rejected depends on the call site: `PID.tell` returns the error, `ctx.tell` throws it, and async methods reject with it. Each reference page documents the form at the point of use; this page is the index.

## Sentinels

Sentinel errors are singleton `Error` values. The same object represents that failure everywhere. Compare them by identity:

```ts
import { ErrDead, ErrMailboxFull } from "@tochemey/nodeakt";

const err = outside.tell(target, message);
if (err === ErrDead || err === ErrMailboxFull) {
  // ...
}
```

| Sentinel | Arises when |
| --- | --- |
| `ErrActorAlreadyExists` | [`system.spawn`](actor-system/index.md) with a top-level name that is still held, including by a suspended or currently stopping actor. |
| `ErrActorSystemNotStarted` | `spawn`, `noSender`, `subscribe`, `unsubscribe`, or any [scheduling call](actor/scheduling.md) on a [system](actor-system/index.md) that is not running. |
| `ErrDead` | The actor is not running: [`tell` / `ask`](actor/messaging.md) to a stopped target, registering a [schedule](actor/scheduling.md) whose target has stopped, or `spawnChild`, `ctx.child`, `ctx.stop`, `restart` through a stopped actor. Also the dead-letter reason for a send through a [router](actor/routers.md) with no live routee. |
| `ErrFanOutAsk` | An `ask` or `request` through a fan-out [router](actor/routers.md); a broadcast has no single answer. |
| `ErrInvalidActorName` | An [actor name](actor-system/index.md) is empty, longer than 255 characters, or syntactically invalid. |
| `ErrInvalidActorSystemName` | A [system name](actor-system/index.md) violates the system-name syntax (stricter than actor names). |
| `ErrInvalidInterval` | [`schedule` / `scheduleOnce`](actor/scheduling.md) with a delay or interval that is not a positive number. |
| `ErrInvalidPoolSize` | [`spawnRouter`](actor/routers.md) with a pool size that is not a positive integer, or an `AdjustRouterPoolSize` whose size is not a non-negative integer. |
| `ErrInvalidReentrancyMode` | An unknown [reentrancy](actor/reentrancy.md) mode at spawn or on request options. |
| `ErrInvalidRouteeDirective` | [`spawnRouter`](actor/routers.md) with an unknown routee directive. |
| `ErrInvalidRoutingStrategy` | [`spawnRouter`](actor/routers.md) with an unknown routing strategy. |
| `ErrInvalidTimeout` | [`ask`](actor/messaging.md) with a timeout that is not positive. |
| `ErrMailboxDisposed` | Enqueue on a [mailbox](actor/mailboxes.md) after the actor stopped, or [`ctx.stash`](actor/behaviors.md) while the actor is stopping. |
| `ErrMailboxFull` | A bounded [mailbox](actor/mailboxes.md) is at capacity. |
| `ErrNameRequired` | The [system name](actor-system/index.md) is empty. |
| `ErrPipeTimeout` | A [pipe](actor/pipeto.md)'s timeout expired before its task settled. The reason on the resulting dead letter; nothing is delivered. |
| `ErrReentrancyDisabled` | [`ctx.request`](actor/reentrancy.md) without a `reentrancy` config, or with mode `off`. |
| `ErrReentrancyInFlightLimit` | A [request](actor/reentrancy.md) past the actor's `maxInFlight` cap. |
| `ErrRemotingDisabled` | A remote operation such as `remoteLookup` on a system created without a `remote` configuration. See [Remoting](remoting/index.md). |
| `ErrRequestCanceled` | A [request](actor/reentrancy.md) completed by `cancel()`. |
| `ErrRequestTimeout` | An [`ask`](actor/messaging.md) or [`request`](actor/reentrancy.md) unanswered within one to two timeout periods. |
| `ErrReservedName` | A name starting with the reserved prefix `NodeAkt`. |
| `ErrRoutingKeyRequired` | [`spawnRouter`](actor/routers.md) with the consistent-hash strategy and no routing key extractor. |
| `ErrScheduleAlreadyExists` | [Registering a schedule](actor/scheduling.md) under a reference that is already held. |
| `ErrScheduleNotFound` | [`cancelSchedule`, `pauseSchedule`, or `resumeSchedule`](actor/scheduling.md) with a reference no schedule holds. |
| `ErrStashBufferEmpty` | [`ctx.unstash`](actor/behaviors.md) with nothing stashed. |
| `ErrUndefinedActor` | [`ctx.stop`](actor/hierarchy.md) on the PID that represents an absent sender. |
| `ErrUndefinedTask` | [`pipeTo`](actor/pipeto.md) or `pipeToName` given a null or undefined task. The reason on the resulting dead letter; nothing is delivered. |
| `ErrUnhandled` | The `Deadletter` reason after [`ctx.unhandled`](actor-system/events.md). |

## Classes

Class errors are constructed per failure and carry context. Inspect them with `instanceof`. Their `name` field matches the class name.

| Class | Arises when |
| --- | --- |
| `ActorInitializationError` | `preStart` failed during [spawn](actor-system/index.md) or system start. The underlying failure is on `error.cause`; the actor is not registered. |
| `ActorNotFoundError` | [`ctx.child`](actor/hierarchy.md) with no running child of that name, `ctx.stop` on a PID that is not a live child of this actor, or a [`pipeToName`](actor/pipeto.md) settling when no running top-level actor holds the name. |
| `ActorNotRegisteredError` | A [`Props`](multi-core/index.md) spawn whose class was never `registerActor`'d. |

## Standard errors

The runtime also uses built-ins where the mistake is in the calling code:

- `TypeError`: [`registerActor` / `registerMessage`](multi-core/index.md) misuse, a `Props` spawn with live options or non-cloneable constructor arguments, and [`shutdown()`](actor/hierarchy.md) on a handle whose actor is owned by another isolate.
- `RangeError`: a [mailbox](actor/mailboxes.md) or [passivation](actor/passivation.md) constructor given a capacity, timeout, or count that is not a positive number.
