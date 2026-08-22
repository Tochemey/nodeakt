# Hierarchy, watch, and stop

Every user actor spawned with `system.spawn` is a child of the user guardian. From `receive`, spawn further children with `ctx.spawn`. The parent executes the child's [supervision](supervision.md) directive when that child fails.

The child's path extends the parent's:

```text
nodeakt://sys@127.0.0.1:0/room
nodeakt://sys@127.0.0.1:0/room/alice
```

Example: [`examples/watch`](../../examples/watch/main.ts), [`examples/chat`](../../examples/chat/main.ts).

## Spawn a child

```ts
const child = await ctx.spawn(name, actor, options?);
// same method on the PID: await pid.spawnChild(name, actor, options?)
```

`actor` is a **live instance**. `ReceiveContext.spawn` does not accept `Props`; children always run on the **same isolate** as the parent. To place work on another core, spawn that actor at the top level with `Props`. See [Multi-core](../multi-core/index.md).

Name rules match top-level spawn (`ErrReservedName`, `ErrInvalidActorName`). `preStart` failure throws `ActorInitializationError` and the child is not registered.

If this actor is not running, `spawnChild` throws `ErrDead`.

If a child with that name already exists under this parent, `spawnChild` returns the existing PID instead of throwing. Top-level `system.spawn` does the opposite: a taken name throws `ErrActorAlreadyExists`.

## Inspect

| Method | Meaning |
| --- | --- |
| `ctx.children()` | Running children. |
| `ctx.child(name)` | The running child of that name. Throws `ErrDead` if this actor is not running; throws `ActorNotFoundError` if no running child holds the name. |
| `pid.parent()` | The PID that spawned this actor, or `undefined` at the top of the user tree. |

## Stop a child

```ts
await ctx.stop(child);
```

The child finishes its current message, drains, and runs `postStop`.

| Failure | When |
| --- | --- |
| `ErrDead` | This actor is not running. |
| `ErrUndefinedActor` | `child` is the PID returned by `system.noSender()`. |
| `ActorNotFoundError` | `child` is not a live child of this actor (not running and not suspended, or not this parent's child). |

Stopping the receiving actor itself is `ctx.shutdown()` (fire-and-forget from `receive`) or `await pid.shutdown()` from outside. Children shut down with their parent.

Sending `new PoisonPill()` via `tell` also begins a graceful stop, after messages already ahead of the pill in the mailbox.

## Watch

```ts
ctx.watch(other);
// later
if (ctx.message instanceof Terminated) {
  console.log(`stopped: ${ctx.message.actorPath}`);
}
```

`Terminated.actorPath` is the canonical path string of the stopped actor.

Watching an actor that is not running is a no-op. Death watch observes a future stop, so watch actors while they are alive. `unWatch` cancels a registration. Unknown registrations are ignored.

Cross-isolate watch uses the same API. If the far isolate dies, watchers still receive `Terminated`. A handle for a remote actor reports `isRunning()` as `false` even while that actor is alive; watch it rather than polling.

## Stopping from another isolate

`pid.shutdown()` on a handle whose actor lives on another isolate rejects with:

```text
TypeError: an actor owned by another isolate cannot be stopped through its handle
```

Stop a remote actor by sending it a message it handles by calling `ctx.shutdown()`, or by sending `PoisonPill`.
