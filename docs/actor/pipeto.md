# PipeTo

Actors constantly need the result of asynchronous work: a database read, an HTTP call, a file load. Making `receive` async parks the actor for the whole operation, serializing everything behind the slowest call. Firing the promise and calling `tell` from its `.then` works, but every author rewrites the same pattern, and nothing checks that the actor still exists when the result lands.

`pipeTo` is that pattern, built in. It runs a task off the actor's message loop and delivers its result back as an **ordinary message**, so the actor keeps handling other messages while the work is in flight, and the result still arrives on the actor's own turn. The async result stays inside the actor model: no shared state, no locks, no callback reaching into the actor from another context.

```ts
class Loader implements Actor {
  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Load) {
      ctx.pipeTo(ctx.self as PID, fetchOrder(ctx.message.id));
    }

    if (ctx.message instanceof Order) {
      // the fetched result, delivered like any other message
    }
  }
}
```

Example: [`examples/pipeto`](https://github.com/Tochemey/nodeakt/blob/main/examples/pipeto/main.ts).

> [!NOTE]
> `pipeTo` is for **asynchronous results**, not CPU offloading. The task shares the actor's event loop; it just does not park the actor's mailbox. To run CPU-bound work on another core, place the actor with `Props`. See [Multi-core](../multi-core/index.md).

## API

`pipeTo` and `pipeToName` live on both [`ReceiveContext`](messaging.md) and [`PID`](index.md#pid), so an actor pipes from inside `receive`, and code holding a handle pipes on the actor's behalf from outside. Either way the piping actor is recorded as `ctx.sender`, and delivery happens on the target's own turn, the same guarantee [`request`/`onReply`](reentrancy.md) gives.

| Call | Where | Effect |
| --- | --- | --- |
| `ctx.pipeTo(to, task, opts?)` | `ReceiveContext` | Run `task`; deliver its result to the `to` PID. |
| `ctx.pipeToName(name, task, opts?)` | `ReceiveContext` | Same, but the target is resolved by name when the task settles. |
| `pid.pipeTo(to, task, opts?)` | `PID` | Pipe on behalf of `pid` from outside `receive`. |
| `pid.pipeToName(name, task, opts?)` | `PID` | Same, target by name. |

## The task

The task is a `PipeTask`: the promise itself, or a thunk returning one. Prefer the thunk when the pipe has a timeout; it receives an `AbortSignal` that fires when the deadline expires, so the task can stop the underlying work. A thunk runs on the piping actor's own turn, so it should return its promise promptly rather than do heavy synchronous work first.

The task's resolution value **is** the delivered message, so resolve it to a message class you narrow with `instanceof`, exactly like any other message. A pipe that resolves to a bare string or number delivers that value; wrap it in a class if the receiver distinguishes messages by type.

## Options

The options argument is a `PipeOptions` with an optional `timeout` in milliseconds (no deadline when omitted or non-positive):

```ts
ctx.pipeTo(worker, (signal) => fetchOrder(id, { signal }), { timeout: 5_000 });
```

When the deadline expires before the task settles, the task's `AbortSignal` fires and the pipe dead-letters with `ErrPipeTimeout`; a result arriving later is dropped.

## State safety

This is the rule that makes `pipeTo` safe, and the easiest one to break.

> [!WARNING]
> A pipe's task runs **off the actor's turn**: its asynchronous work, and anything chained on it with `.then`, are not serialized with the actor's message processing. Do not read or mutate the actor's fields (`this`) from there. A write bypasses the single-writer discipline that lets actor state stay lock-free, and can interleave with an in-flight `receive`; a read sees whatever value happens to exist when the task settles, not when it was piped. Capture what the task needs into a local variable first, return the result as a message, and touch actor state only when that message arrives.

```ts
// Wrong: the task mutates actor state after awaiting, off the actor's turn.
receive(ctx: ReceiveContext): void {
  if (ctx.message instanceof Load) {
    const id = ctx.message.id;
    ctx.pipeTo(ctx.self as PID, async () => {
      const order = await fetchOrder(id);
      this.lastId = order.id; // runs off the turn: unserialized with receive
      return order;
    });
  }
}

// Right: capture inputs on the turn, return a message, mutate on receipt.
receive(ctx: ReceiveContext): void {
  if (ctx.message instanceof Load) {
    const id = ctx.message.id; // capture the message value
    const endpoint = this.endpoint; // and the state it needs, on the turn
    ctx.pipeTo(ctx.self as PID, () => fetchOrder(id, endpoint));
  }

  if (ctx.message instanceof Order) {
    this.lastId = ctx.message.id; // on this actor's own turn: safe
  }
}
```

The reward for following it: the task can run as long as it likes without ever parking the actor or racing its state. This is the same discipline `request`/`onReply` enforces by running the continuation on the actor's turn; `pipeTo` cannot run your task there, so the capture is yours to make.

## By name

`pipeToName(actorName, task, options)` addresses the target by name instead of by handle: how an actor targets a peer it never resolved to a `PID`. The name is resolved among the running [top-level actors](../actor-system/index.md) when the task settles, and the result is delivered to whoever holds it then. This composes with placement: once addresses reach across machines, a name resolves wherever the actor lives.

## Failures

A pipe never throws, and a failing pipe delivers nothing; failures go to [dead letters](../actor-system/events.md), the runtime's uniform channel for undeliverable and failed sends:

| Failure                                   | Dead letter reason                                                                        |
|-------------------------------------------|-------------------------------------------------------------------------------------------|
| Task rejected                             | The rejection, also logged. Nothing is delivered.                                         |
| Target not running when the task settles  | `ErrDead`, also logged; the result is the dead letter's message.                          |
| No running top-level actor holds the name | `ActorNotFoundError`, also logged; the result is the dead letter's message.               |
| Timeout expired first                     | `ErrPipeTimeout`; the task's `AbortSignal` fires, and a result arriving later is dropped. |
| Task is null or undefined                 | `ErrUndefinedTask`; nothing runs and nothing is delivered.                                |

A handler that wants the failure as a message maps the rejection before piping, which keeps the failure channel uniform:

```ts
ctx.pipeTo(ctx.self as PID, fetchOrder(id).catch((err) => new LoadFailed(err)));
```

The rejection handler is attached before `pipeTo` returns, so a piped task never produces an unhandled rejection warning.

> [!NOTE]
> Stopping the piping actor does not cancel the task: the promise is already running, and its result is still delivered when it settles. A task that must stop with the pipe observes the timeout's abort signal.
