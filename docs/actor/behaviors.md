# Behaviors and stash

An actor's default behavior is its `receive` method. It can replace that handler for subsequent messages, or push a temporary handler and pop it later. Stash parks the current message to replay it after the behavior has changed.

Examples: [`examples/behaviors`](../../examples/behaviors/main.ts), [`examples/stash`](../../examples/stash/main.ts).

## Behavior

```ts
type Behavior = (ctx: ReceiveContext) => void | Promise<void>;
```

The runtime awaits a returned promise before the next message, same as `receive`.

## Switch

| Method | Effect |
| --- | --- |
| `ctx.become(behavior)` | Replace the current handler. Stays in effect until changed again or `unBecome`. |
| `ctx.becomeStacked(behavior)` | Push on top of the current handler. The previous one is restored with `unBecomeStacked`. |
| `ctx.unBecome()` | Discard every installed behavior and return to `receive`. |
| `ctx.unBecomeStacked()` | Pop the top stacked behavior. |

A [restart](supervision.md) resets the behavior stack back to `receive`.

## Stash

The stash is a private unbounded buffer on the actor, independent of the mailbox.

| Method | Effect |
| --- | --- |
| `ctx.stash()` | Buffer the current message. Throws `ErrMailboxDisposed` if the stash has been disposed (the actor is stopping). |
| `ctx.unstash()` | Re-deliver the oldest stashed message to the **mailbox**. Throws `ErrStashBufferEmpty` when nothing is stashed, or the mailbox rejection (`ErrDead`, …) if re-delivery fails. |
| `ctx.unstashAll()` | Re-deliver all stashed messages in original arrival order, oldest first. |

Re-delivered messages join the **mailbox tail**: messages already queued run before them; messages arriving afterwards run behind them.

For example, stash messages the current behavior does not handle, `become` the behavior that can, then `unstashAll`.

A restart disposes the stash. A suspended actor keeps its stash until it is restarted, reinstated, or stopped.
