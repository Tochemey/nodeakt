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

Example: [`examples/helloworld`](https://github.com/Tochemey/nodeakt/blob/main/examples/helloworld/main.ts).

## `ReceiveContext`

`ReceiveContext` is the single argument `receive` is called with. It wraps **one delivery** and is the actor's entire interface to the outside world for that message: it carries the message, tells the actor who sent it and which actor is receiving, and exposes every action the actor can take in response, all bound to the current turn.

That is its purpose. Instead of reaching for global functions or shared handles, an actor does everything through the context it was handed: read `ctx.message`, reply to `ctx.sender`, and send, ask, [pipe](pipeto.md), [spawn](hierarchy.md) children, [watch](death-watch.md) peers, [switch behavior](behaviors.md), [stash](behaviors.md), forward, or mark the message unhandled. Because those methods run through the context, each is automatically attributed to this actor and serialized with its message processing.

It describes one delivery:

| Member          | Meaning                                                                                  |
|-----------------|------------------------------------------------------------------------------------------|
| `message`       | The payload. Typed as `unknown`; narrow it with `instanceof`.                            |
| `self`          | This actor's `PID`.                                                                      |
| `sender`        | Who sent it. When the send used `system.noSender()`, `ctx.sender === system.noSender()`. |
| `actorSystem()` | The hosting system.                                                                      |

A context is valid **only for the current `receive` call**. The runtime recycles it once the behavior returns, so never store one on the actor or capture it in a callback that runs later; copy the values you need instead. Its action methods throw if called on a detached context (one not attached to a receiving actor), and you never construct one yourself: the runtime hands you the context with each message.

Contrast it with [`Context`](index.md#context-vs-receivecontext), the lighter object passed to `preStart` and `postStop`: that one describes the actor outside of any message and carries no `message`, `sender`, or send methods.

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

Cross-isolate `tell` reports transport accept, not mailbox accept. Posting the envelope returns `null`. A full or missing mailbox on the far side becomes a dead letter there. Encode or clone failures return their error immediately. See [Multi-core](../multi-core/index.md). The same contract holds across machines: a `tell` to a remote PID reports what the network transport accepted, and an undeliverable envelope becomes a dead letter on the node that discovered it. See [Remoting](../remoting/index.md).

## Ask

Send a message and wait for `ctx.response(value)` on the receiving side.

```ts
const total = await outside.ask(greeter, new HowMany(), 1_000);
```

The first `response` wins; later calls are ignored. `response` is a no-op when the message was not delivered by ask.

> [!WARNING]
> Do not ask an actor that is processing this call. It cannot reply until the current message finishes. Asking `self` from `receive` never completes. For call cycles, use [`request`](reentrancy.md).

`timeout` is a duration in milliseconds. A non-positive or omitted value falls back to the system's [`askTimeout`](../actor-system/index.md), so an ask is never unbounded. The wait is a lower bound with coarse expiry: an unanswered ask is rejected between one and two timeout periods, so the send path never reads the clock.

| Failure             | When                                     |
|---------------------|------------------------------------------|
| `ErrDead`           | Target not running.                      |
| `ErrRequestTimeout` | No reply in time.                        |
| mailbox error       | Delivery rejected (`ErrMailboxFull`, …). |

`ctx.ask` forwards to `PID.ask` and rejects with the same errors.

An ask to a remote PID crosses the network and settles with the same failures, sentinel identity preserved; see [Remoting](../remoting/index.md#failures).

## Request

Non-parking ask. Requires the actor to be spawned with [reentrancy](reentrancy.md). Returns a `RequestCall` immediately; register `onReply`. The continuation runs on this actor's own turn.

```ts
ctx.request(peer, new Get(), { timeout: 1_000 }).onReply((reply, error) => {
  // serialized with this actor's messages
});
```

## PipeTo

`pipeTo` runs asynchronous work off the actor's message loop and delivers its result back as an ordinary message, so the actor never parks while a database read or HTTP call is in flight. It has its own page: **[PipeTo](pipeto.md)**.

## Forward

`ctx.forward(to)` sends the current message to `to` and keeps the original sender. The next behavior sees `ctx.sender` as whoever sent the message here, not this actor.

The preserved sender survives any boundary: forwarding to an actor on another isolate or [another node](../remoting/index.md) carries the origin along, so the receiver can reply straight to it, wherever it lives.

## Unhandled

`ctx.unhandled()` routes the current message to dead letters with reason `ErrUnhandled` and continues normally. Use this when unknown messages are expected. Throwing engages supervision.

## Shutdown

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
| `Terminated`  | Yes                    | An actor this one [watched](death-watch.md) has stopped. Carries `actorPath: string`.                                                                                                             |
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
