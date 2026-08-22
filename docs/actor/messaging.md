# Messaging

Actors never share memory. They send messages. Define messages as classes and narrow `ctx.message` with `instanceof`. Each branch is fully typed.

```ts
const msg = ctx.message;

if (msg instanceof Greet) {
  console.log(`Hello ${msg.name}`);
} else if (msg instanceof HowMany) {
  ctx.response(this.count);
} else {
  ctx.unhandled();
}
```

A `ReceiveContext` is only valid for the current `receive` call. The runtime can recycle it after the behavior returns.

Example: [`examples/helloworld`](https://github.com/Tochemey/nodeakt/blob/main/examples/helloworld/main.ts).

## `ReceiveContext`

| Member          | Meaning                                                                                  |
|-----------------|------------------------------------------------------------------------------------------|
| `message`       | The payload. Typed as `unknown`.                                                         |
| `self`          | This actor's `PID`.                                                                      |
| `sender`        | Who sent it. When the send used `system.noSender()`, `ctx.sender === system.noSender()`. |
| `actorSystem()` | The hosting system.                                                                      |

Actor-facing methods throw if called on a detached context (one not attached to a receiving actor). You do not construct contexts.

## Tell

Fire and forget. The sender is recorded; the receiver sees it as `ctx.sender`.

From a PID, `tell` **returns** the outcome instead of throwing:

```ts
const err = sender.tell(target, message);
// null = accepted
// ErrDead = target not running
// ErrMailboxFull / ErrMailboxDisposed = mailbox rejected it
```

From `receive`, `ctx.tell(to, message)` throws that same error.

A rejected user message is also published as a [dead letter](../actor-system/events.md).

Cross-isolate `tell` reports transport accept, not mailbox accept. Posting the envelope returns `null`. A full or missing mailbox on the far side becomes a dead letter there. Encode or clone failures return their error immediately. See [Multi-core](../multi-core/index.md).

## Ask

Send a message and wait for `ctx.response(value)` on the receiving side.

```ts
const total = await outside.ask(greeter, new HowMany(), 1_000);
```

The first `response` wins; later calls are ignored. `response` is a no-op when the message was not delivered by ask.

> [!WARNING]
> Do not ask an actor that is processing this call. It cannot reply until the current message finishes. Asking `self` from `receive` never completes. For call cycles, use [`request`](reentrancy.md).

`timeout` is a positive duration in milliseconds. The wait is a lower bound with coarse expiry: an unanswered ask is rejected between one and two timeout periods, so the send path never reads the clock.

| Failure             | When                                     |
|---------------------|------------------------------------------|
| `ErrDead`           | Target not running.                      |
| `ErrInvalidTimeout` | `timeout` is not positive.               |
| `ErrRequestTimeout` | No reply in time.                        |
| mailbox error       | Delivery rejected (`ErrMailboxFull`, …). |

`ctx.ask` forwards to `PID.ask` and rejects with the same errors.

## Request

Non-parking ask. Requires the actor to be spawned with [reentrancy](reentrancy.md). Returns a `RequestCall` immediately; register `onReply`. The continuation runs on this actor's own turn.

```ts
ctx.request(peer, new Get(), { timeout: 1_000 }).onReply((reply, error) => {
  // serialized with this actor's messages
});
```

## PipeTo

Actors constantly need results of asynchronous work: a database read, an HTTP call, a file load. Making `receive` async parks the actor for the whole operation. `pipeTo(to, task, options)` runs the task off the actor's message processing and delivers its resolution value to `to` as an ordinary message, through the normal send path. The call returns immediately; the actor keeps processing messages while the task runs, and delivery happens on the target's own turn.

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

The piped message arrives with the piping actor as `ctx.sender`. Both `ReceiveContext` and `PID` carry `pipeTo`, so code outside `receive` can pipe on behalf of an actor it holds a handle to.

Example: [`examples/pipeto`](https://github.com/Tochemey/nodeakt/blob/main/examples/pipeto/main.ts).

The task is a `PipeTask`: the promise itself, or a thunk returning one. Prefer the thunk when the pipe has a timeout; it receives an `AbortSignal` that fires when the deadline expires, so the task can stop the underlying work. A thunk runs on the piping actor's own turn, so it should return its promise promptly rather than do heavy synchronous work first.

The options argument is a `PipeOptions` with an optional `timeout` in milliseconds (no deadline when omitted or non-positive):

```ts
ctx.pipeTo(worker, (signal) => fetchOrder(id, { signal }), { timeout: 5_000 });
```

### By name

`pipeToName(actorName, task, options)` addresses the target by name instead of by handle: how an actor targets a peer it never resolved to a `PID`. The name is resolved among the running [top-level actors](../actor-system/index.md) when the task settles, and the result is delivered to whoever holds it then.

### Failures

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

## Forward, unhandled, shutdown

`ctx.forward(to)` sends the current message to `to` and keeps the original sender. The next behavior sees `ctx.sender` as whoever sent the message here, not this actor.

`ctx.unhandled()` routes the current message to dead letters with reason `ErrUnhandled` and continues normally. Use this when unknown messages are expected. Throwing engages supervision.

`ctx.shutdown()` begins a graceful stop of the receiving actor.

> [!WARNING]
> Do not await `ctx.shutdown()` from `receive`. Shutdown waits for the receive loop to go idle, so awaiting it from inside that loop never completes.

```ts
ctx.shutdown();
```

From outside, `await pid.shutdown()`.

## System messages

Handle these with `instanceof` alongside your own classes.

| Message       | Delivered to behavior? | Meaning                                                                                                                                                                                         |
|---------------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `PostStart`   | Yes                    | First message after start.                                                                                                                                                                      |
| `Terminated`  | Yes                    | An actor this one [watched](hierarchy.md) has stopped. Carries `actorPath: string`.                                                                                                             |
| `PanicSignal` | Yes                    | [Escalated](supervision.md) failure. `reason` is the error. The sender is the failing actor.                                                                                                    |
| `PoisonPill`  | **No**                 | Instructs a graceful stop. Travels through the mailbox so everything ahead of it runs first. The runtime consumes it; it is never passed to `receive`. Send with `tell(pid, new PoisonPill())`. |
| `Deadletter`  | No (event)             | Published on the [event stream](../actor-system/events.md), not delivered to the original receiver.                                                                                             |

Messages already accepted **behind** a `PoisonPill` still drain before the actor stops. Sends arriving after the pill is consumed are rejected.

## Compare send APIs

|                                 | `tell`         | `ask`          | `request`                                 | `pipeTo`                           |
|---------------------------------|----------------|----------------|-------------------------------------------|------------------------------------|
| Waits for a reply               | No             | Yes (parks)    | No (continuation)                         | No (task result becomes a message) |
| Receiver replies with           |                | `ctx.response` | `ctx.response`                            |                                    |
| Safe in call cycles             | Yes            | No             | Yes, with `allowAll`                      | Yes                                |
| From code that is not `receive` | a PID's `tell` | a PID's `ask`  | No. Needs a reentrant actor as the issuer | a PID's `pipeTo`                   |
