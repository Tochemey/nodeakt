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

Example: [`examples/helloworld`](../../examples/helloworld/main.ts).

## `ReceiveContext`

| Member | Meaning |
| --- | --- |
| `message` | The payload. Typed as `unknown`. |
| `self` | This actor's `PID`. |
| `sender` | Who sent it. Outside sends carry the system's NoSender (`ctx.sender === system.noSender()`). |
| `actorSystem()` | The hosting system. |

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

Do not ask an actor that is processing this call. It cannot reply until the current message finishes. Asking `self` from `receive` never completes. For call cycles, use [`request`](reentrancy.md).

`timeout` is a positive duration in milliseconds. The wait is a lower bound with coarse expiry: an unanswered ask is rejected between one and two timeout periods, so the send path never reads the clock.

| Failure | When |
| --- | --- |
| `ErrDead` | Target not running. |
| `ErrInvalidTimeout` | `timeout` is not positive. |
| `ErrRequestTimeout` | No reply in time. |
| mailbox error | Delivery rejected (`ErrMailboxFull`, …). |

`ctx.ask` forwards to `PID.ask` and rejects with the same errors.

## Request

Non-parking ask. Requires the actor to be spawned with [reentrancy](reentrancy.md). Returns a `RequestCall` immediately; register `onReply`. The continuation runs on this actor's own turn.

```ts
ctx.request(peer, new Get(), { timeout: 1_000 }).onReply((reply, error) => {
  // serialized with this actor's messages
});
```

## Forward, unhandled, shutdown

`ctx.forward(to)` sends the current message to `to` and keeps the original sender. The next behavior sees `ctx.sender` as whoever sent the message here, not this actor.

`ctx.unhandled()` routes the current message to dead letters with reason `ErrUnhandled` and continues normally. Use this when unknown messages are expected. Throwing engages supervision.

`ctx.shutdown()` begins a graceful stop of the receiving actor. Do not await it from `receive`. Shutdown waits for the receive loop to go idle, so awaiting it from inside that loop never completes.

```ts
ctx.shutdown();
```

From outside, `await pid.shutdown()`.

## System messages

Handle these with `instanceof` alongside your own classes.

| Message | Delivered to behavior? | Meaning |
| --- | --- | --- |
| `PostStart` | Yes | First message after start. |
| `Terminated` | Yes | An actor this one [watched](hierarchy.md) has stopped. Carries `actorPath: string`. |
| `PanicSignal` | Yes | [Escalated](supervision.md) failure. `reason` is the error. The sender is the failing actor. |
| `PoisonPill` | **No** | Instructs a graceful stop. Travels through the mailbox so everything ahead of it runs first. The runtime consumes it; it is never passed to `receive`. Send with `tell(pid, new PoisonPill())`. |
| `Deadletter` | No (event) | Published on the [event stream](../actor-system/events.md), not delivered to the original receiver. |

Messages already accepted **behind** a `PoisonPill` still drain before the actor stops. Sends arriving after the pill is consumed are rejected.

## Compare send APIs

| | `tell` | `ask` | `request` |
| --- | --- | --- | --- |
| Waits for a reply | No | Yes (parks) | No (continuation) |
| Receiver replies with | | `ctx.response` | `ctx.response` |
| Safe in call cycles | Yes | No | Yes, with `allowAll` |
| From outside an actor | `noSender().tell` | `noSender().ask` | No. Needs a reentrant actor as the issuer |
