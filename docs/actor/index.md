# Actors

An actor owns private state and a mailbox. It communicates only through messages. The runtime processes those messages one at a time, so the state needs no lock. Do not share that state or mutate it outside `receive`.

```ts
import type { Actor, Context, ReceiveContext } from "@tochemey/nodeakt";

class Greeter implements Actor {
  private count = 0;

  preStart(ctx: Context): void | Promise<void> {
    // acquire resources, recover state
  }

  receive(ctx: ReceiveContext): void | Promise<void> {
    // handle ctx.message
  }

  postStop(ctx: Context): void | Promise<void> {
    // release resources
  }
}
```

Initialize state in `preStart`, not in the constructor. An instance can be constructed without ever starting. A supervisor that restarts a failed actor reuses the same instance and re-runs `preStart`.

See [`examples/helloworld`](../../examples/helloworld/main.ts).

## Lifecycle

```
preStart → PostStart (first mailbox message) → receive… → postStop
```

| Hook | When | Failure |
| --- | --- | --- |
| `preStart` | Before any message. | Aborts the start. Surfaces as `ActorInitializationError` (cause on `error.cause`) wherever the actor was spawned. The actor never receives a message and is not registered. On a restart, a failed `preStart` leaves the actor [suspended](supervision.md). |
| `receive` | One mailbox delivery at a time. | Engages the actor's [supervisor](supervision.md). |
| `postStop` | After queued messages drain, before the actor is fully gone. | Logged. Does not prevent the stop. Keep the hook short. |

Both hooks may be async. When `receive` returns a promise, the runtime awaits it before dequeuing the next message. Long-blocking synchronous work in `receive` stalls every actor sharing that event loop. Put CPU-bound work on another isolate. See [Multi-core](../multi-core/index.md).

`PostStart` is delivered as the first mailbox message after start, from `system.noSender()`. Use it for work that must run inside the message loop (for example spawning children through `ctx.self`). Runtime actors created while the system itself is still starting do not receive it; every user actor does.

## `Context` vs `ReceiveContext`

`Context` is handed to `preStart` and `postStop`. It describes the actor, not a message:

| Method | Returns |
| --- | --- |
| `actorSystem()` | The hosting `ActorSystem`. |
| `actorName()` | The name the actor is registered under. |
| `logger()` | The system logger. |

It is immutable and only valid for the lifecycle phase it was handed to. You do not construct one.

`ReceiveContext` accompanies one message. See [Messaging](messaging.md).

## `PID`

A `PID` is the handle you send to. You receive one from `spawn`, `actorOf`, `ctx.self`, `ctx.sender`, or `ctx.child`. You do not construct one.

| Method | Meaning |
| --- | --- |
| `id()` | Canonical path string. |
| `name()` | Last path segment. |
| `path()` | The actor's [`Path`](#path). |
| `kind()` | Constructor name of the implementation (`"Object"` for object-literal actors). |
| `actorSystem()` | The hosting system. |
| `equals(other)` | Same name and location. Does not compare incarnation (`uid`). |
| `isRunning()` | Started, not stopping, not suspended. A handle for an actor on another isolate always reports `false`. Liveness across isolates is not synchronously knowable. [Watch](hierarchy.md) it instead. |
| `isSuspended()` | Faulted: holds state but processes nothing until restarted or reinstated. |
| `restartCount()` | Times this instance has been restarted. |
| `tell` / `ask` / `request` | See [Messaging](messaging.md). |
| `spawnChild` / `child` / `children` / `parent` / `stop` / `watch` / `unWatch` | See [Hierarchy](hierarchy.md). |
| `shutdown()` | Graceful stop. Queued messages drain. Children shut down with it. Repeated calls return the same promise. Shutting down an actor that never started is a no-op. An actor owned by another isolate cannot be stopped through its handle: `shutdown()` rejects with `TypeError`. |
| `restart()` | Tear down and re-initialize this actor. Children are stopped; `postStop` runs (unless already suspended); behaviors and stash reset; `preStart` runs again; queued mailbox messages are then processed. Throws `ErrDead` if the actor is stopping, already restarting, or never started. A failed `preStart` leaves it suspended. |
| `reinstate(target)` | Resume a suspended actor without resetting state. Pass a `PID`, or a child name relative to this actor. |

`PID.actor()` returns the implementation object. On a handle for an actor owned by another isolate that object is a stub, not the live instance. Send messages. Do not call methods on the stub.

From code that is not `receive`, send with a PID you hold. `system.noSender()` is the PID for an absent sender:

```ts
system.noSender().tell(pid, message);
await system.noSender().ask(pid, message, 1_000);
```

From inside `receive`, use `ctx.tell` / `ctx.ask` / `ctx.request` so this actor is recorded as the sender.

## `Path`

A path is the logical address of an actor. Sends go to a path, so a handle names an actor on this isolate or another.

Canonical form:

```text
nodeakt://<system>@<host>:<port>/<name>
nodeakt://<system>@<host>:<port>/<ancestor>/.../<parent>/<name>
```

The string encodes the full ancestor chain, so two actors with the same local name under different parents have distinct paths. Guardians are not segments of that chain: a top-level actor's path starts at its own name.

Paths are immutable for the actor's life, including across supervisor restarts. You receive a path from `pid.path()`; you do not construct one.

| Method | Meaning |
| --- | --- |
| `name()` | Last segment. Unique among siblings. |
| `parent()` | Parent path, or `undefined` at the top level. |
| `system()` / `host()` / `port()` / `hostPort()` | The node's identity. |
| `uid()` | Process-local incarnation id of the live instance. Empty on a path restored from its string form. |
| `toString()` | Canonical string. |
| `equals(other)` | Same name and location; `uid` is not compared. |
| `sameUid(other)` | `equals` and the same non-empty `uid`. Same living instance. |

## Spawn options recap

Passed to `system.spawn` or `ctx.spawn` / `pid.spawnChild`:

- [Mailbox](mailboxes.md): default unbounded FIFO
- [Passivation](passivation.md): default long-lived (never passivated)
- [Supervisor](supervision.md): default, any failure stops the actor
- [Reentrancy](reentrancy.md): default, `request` disabled

Live options (`mailbox`, `supervisor`, `passivationStrategy`) apply only to instance spawns. [Props spawns](../multi-core/index.md) accept only data options (`reentrancy`).

## Pages in this section

- [Messaging](messaging.md): `tell`, `ask`, `request`, system messages, `unhandled`, `forward`
- [Behaviors and stash](behaviors.md)
- [Hierarchy, watch, and stop](hierarchy.md)
- [Supervision](supervision.md)
- [Mailboxes](mailboxes.md)
- [Passivation](passivation.md)
- [Reentrancy](reentrancy.md)
