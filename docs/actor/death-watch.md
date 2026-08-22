# Death watch

Death watch lets an actor observe when another actor stops. The watcher registers interest in a target, and when that target terminates, the watcher receives a `Terminated` message on its own turn, like any other message. Use it to release references to the gone actor, fail over to a replacement, or drive custom recovery.

Watching is explicit and peer-to-peer: any actor can watch any other, whether or not they are related in the [tree](hierarchy.md). It is separate from [supervision](supervision.md), which is a parent reacting to a child's *failure*; death watch reacts to a *stop*, from any cause.

Examples: [`examples/watch`](https://github.com/Tochemey/nodeakt/blob/main/examples/watch/main.ts), [`examples/chat`](https://github.com/Tochemey/nodeakt/blob/main/examples/chat/main.ts).

## API

`watch` and `unWatch` live on both [`ReceiveContext`](messaging.md) and [`PID`](index.md#pid).

| Call                  | Where            | Effect                                                           |
|-----------------------|------------------|------------------------------------------------------------------|
| `ctx.watch(target)`   | `ReceiveContext` | Register this actor to receive `Terminated` when `target` stops. |
| `ctx.unWatch(target)` | `ReceiveContext` | Cancel this actor's watch on `target`.                           |
| `pid.watch(target)`   | `PID`            | Register `pid` as a watcher of `target`, from outside `receive`. |
| `pid.unWatch(target)` | `PID`            | Cancel `pid`'s watch on `target`.                                |

## The `Terminated` message

When a watched actor stops, each of its watchers receives a `Terminated`, delivered to `receive` and narrowed with `instanceof` like any other message:

```ts
receive(ctx: ReceiveContext): void {
  if (ctx.message instanceof Terminated) {
    this.peers.delete(ctx.message.actorPath); // react to the stop
  }
}
```

`Terminated.actorPath` is the **canonical path string** of the stopped actor, for example `nodeakt://sys@127.0.0.1:0/room/alice`. It identifies which watched actor stopped when one actor watches several.

## What counts as a stop

`Terminated` fires whenever the watched actor actually terminates, whatever the cause: a graceful `shutdown()`, a `PoisonPill`, [passivation](passivation.md) after idle, its parent stopping, or a terminal failure that [supervision](supervision.md) resolves to a stop.

A [restart](supervision.md) is **not** a stop: the actor keeps its `PID` and its place in the tree and re-initializes in place, so watchers get no `Terminated`. Watch reports the end of an actor, not a hiccup in its lifecycle.

## Watch live actors

Watching an actor that is not running is a **no-op**. Death watch observes a *future* stop, so register the watch while the target is alive; watching an already-stopped actor registers nothing and delivers no `Terminated`. `unWatch` cancels a registration before the target terminates; cancelling an unknown registration is ignored.

## Automatic unwatch

You do not need to `unWatch` after receiving `Terminated`. When the watched actor stops, the runtime notifies its watchers and clears the registration on both sides, so nothing lingers. The cleanup is symmetric: when a *watcher* itself stops, it is automatically removed from every actor it was watching, so a gone watcher never receives a stale `Terminated`. Call `unWatch` only to stop watching a still-running actor.

## Across isolates

Death watch uses the same API across isolates. A watcher on one isolate receives `Terminated` when its target on another isolate stops, and it still fires when the far isolate itself dies, so a watch is the reliable way to learn that a remote actor is gone. A handle for an actor owned by another isolate always reports `isRunning()` as `false`, because liveness across isolates is not synchronously knowable; watch it rather than polling that handle. See [Multi-core](../multi-core/index.md).

## When to use

- **Dependencies**: watch a peer or child your work depends on; recreate or fail over when it stops.
- **Resource cleanup**: drop references, sockets, or map entries keyed by the stopped actor's path.
- **Coordination**: a supervisor or coordinator that reacts to members leaving, as the chat example does when a room member disconnects.
