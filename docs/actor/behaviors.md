# Behaviors and stash

An actor's default behavior is its `receive` method. It can replace that handler for subsequent messages, or push a temporary handler and pop it later. Stash parks the current message to replay it after the behavior has changed.

Examples: [`examples/behaviors`](https://github.com/Tochemey/nodeakt/blob/main/examples/behaviors/main.ts), [`examples/stash`](https://github.com/Tochemey/nodeakt/blob/main/examples/stash/main.ts).

## Behavior

```ts
type Behavior = (ctx: ReceiveContext) => void | Promise<void>;
```

The runtime awaits a returned promise before the next message, same as `receive`.

## Switch

| Method                        | Effect                                                                                   |
|-------------------------------|------------------------------------------------------------------------------------------|
| `ctx.become(behavior)`        | Replace the current handler. Stays in effect until changed again or `unBecome`.          |
| `ctx.becomeStacked(behavior)` | Push on top of the current handler. The previous one is restored with `unBecomeStacked`. |
| `ctx.unBecome()`              | Discard every installed behavior and return to `receive`.                                |
| `ctx.unBecomeStacked()`       | Pop the top stacked behavior.                                                            |

The switch applies to the **next** message. The message being handled when you call `become` finishes under the behavior that was already running; the new one takes over on the following delivery.

Reach for `become` when the actor moves to a new state it stays in, so the previous handler is no longer wanted, an unauthenticated actor becoming authenticated, for example. Reach for `becomeStacked` when a handler is a temporary overlay you will pop back out of, a confirmation step or a one-off protocol phase, so the handler underneath resumes untouched.

```ts
class Account implements Actor {
  private balance = 0;

  preStart(): void {}

  // Default state: only a login is accepted.
  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Login) {
      ctx.become((c) => this.authenticated(c));
    } else {
      ctx.unhandled();
    }
  }

  private authenticated(ctx: ReceiveContext): void {
    if (ctx.message instanceof Deposit) {
      this.balance += ctx.message.amount;
    } else if (ctx.message instanceof Logout) {
      ctx.unBecome(); // back to the default handler
    } else {
      ctx.unhandled();
    }
  }

  postStop(): void {}
}
```

A [restart](supervision.md) resets the behavior stack back to `receive`.

## Stash

The stash is a private unbounded buffer on the actor, independent of the mailbox.

| Method             | Effect                                                                                                                                                                        |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ctx.stash()`      | Buffer the current message. Throws `ErrMailboxDisposed` if the stash has been disposed (the actor is stopping).                                                               |
| `ctx.unstash()`    | Re-deliver the oldest stashed message to the **mailbox**. Throws `ErrStashBufferEmpty` when nothing is stashed, or the mailbox rejection (`ErrDead`, …) if re-delivery fails. |
| `ctx.unstashAll()` | Re-deliver all stashed messages in original arrival order, oldest first.                                                                                                      |

Re-delivered messages join the **mailbox tail**: messages already queued run before them; messages arriving afterwards run behind them.

The classic pattern pairs stash with a behavior switch: buffer the messages the current state cannot serve, `become` the state that can, then `unstashAll` to replay them in arrival order.

```ts
class Gate implements Actor {
  private open = false;

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Open) {
      this.open = true;
      ctx.unstashAll(); // replay everything buffered while closed
    } else if (ctx.message instanceof Request) {
      if (this.open) {
        // serve it
      } else {
        ctx.stash(); // not ready: buffer for later
      }
    }
  }

  postStop(): void {}
}
```

`pid.stashSize()` reports how many messages are currently buffered, which is useful for metrics or applying backpressure when the buffer grows without bound. The [`stashNonReentrant`](reentrancy.md) reentrancy mode uses the same buffer automatically, stashing user messages while a request is in flight and replaying them when it completes.

A restart disposes the stash. A suspended actor keeps its stash until it is restarted, reinstated, or stopped.
