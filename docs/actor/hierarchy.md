# Hierarchy and stop

Every user actor spawned with `system.spawn` is a child of the user guardian. From `receive`, spawn further children with `ctx.spawn`. The parent executes the child's [supervision](supervision.md) directive when that child fails.

Example: [`examples/watch`](https://github.com/Tochemey/nodeakt/blob/main/examples/watch/main.ts), [`examples/chat`](https://github.com/Tochemey/nodeakt/blob/main/examples/chat/main.ts).

## The tree

Actors form a supervision tree. Above your own actors sit three runtime guardians: the **root guardian** at the top, the **system guardian** that parents the runtime's internal actors (dead letters and other machinery), and the **user guardian** that parents every actor you spawn with `system.spawn`. Each of those actors can spawn children of its own with `ctx.spawn`.

<svg role="img" aria-label="The actor tree: root guardian over the system and user guardians; top-level actors under the user guardian parent their children" viewBox="0 0 720 316" style="width:100%;max-width:720px;height:auto;display:block;margin:24px auto;font-family:inherit">
  <defs>
    <marker id="act-hi" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 Z" fill="var(--vp-c-text-3, #929295)"/>
    </marker>
  </defs>
  <rect x="290" y="20" width="140" height="40" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="360" y="45" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">root guardian</text>
  <rect x="60" y="100" width="260" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="190" y="124" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">system guardian</text>
  <text x="190" y="144" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">dead letters, runtime actors</text>
  <rect x="400" y="100" width="260" height="56" rx="8" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="530" y="124" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">user guardian</text>
  <text x="530" y="144" text-anchor="middle" font-size="12" fill="var(--vp-c-text-2, #67676c)">parents your top-level actors</text>
  <path d="M340 60 L190 96" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#act-hi)"/>
  <path d="M380 60 L530 96" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#act-hi)"/>
  <rect x="460" y="180" width="140" height="48" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="530" y="199" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">room</text>
  <text x="530" y="217" text-anchor="middle" font-size="11.5" font-family="var(--vp-font-family-mono, ui-monospace, monospace)" fill="var(--vp-c-text-2, #67676c)">system.spawn</text>
  <path d="M530 156 V176" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#act-hi)"/>
  <rect x="350" y="256" width="160" height="48" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="430" y="275" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">alice</text>
  <text x="430" y="293" text-anchor="middle" font-size="11.5" font-family="var(--vp-font-family-mono, ui-monospace, monospace)" fill="var(--vp-c-text-2, #67676c)">ctx.spawn</text>
  <rect x="550" y="256" width="160" height="48" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="630" y="275" text-anchor="middle" font-size="14" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">bob</text>
  <text x="630" y="293" text-anchor="middle" font-size="11.5" font-family="var(--vp-font-family-mono, ui-monospace, monospace)" fill="var(--vp-c-text-2, #67676c)">ctx.spawn</text>
  <path d="M510 228 L430 252" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#act-hi)"/>
  <path d="M550 228 L630 252" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#act-hi)"/>
</svg>

The guardians are supervision structure, not part of any address: an actor's path starts at its own top-level name, never at `/user`. So `room`'s path is `nodeakt://sys@127.0.0.1:0/room`, and every child extends its parent's path:

```text
nodeakt://sys@127.0.0.1:0/room
nodeakt://sys@127.0.0.1:0/room/alice
```

## Spawn a child

```ts
const child = await ctx.spawn(name, actor, options?);
// same method on the PID: await pid.spawnChild(name, actor, options?)
```

`actor` is a **live instance**. `ReceiveContext.spawn` does not accept `Props`; children always run on the **same isolate** as the parent. To place work on another core, spawn that actor at the top level with `Props`. See [Multi-core](../multi-core/index.md).

Name rules match top-level spawn (`ErrReservedName`, `ErrInvalidActorName`). `preStart` failure throws `ActorInitializationError` and the child is not registered.

If this actor is not running, `spawnChild` throws `ErrDead`.

If a child with that name already exists under this parent, `spawnChild` returns the existing PID instead of throwing. Top-level `system.spawn` does the opposite: a taken name throws `ErrActorAlreadyExists`.

The two spawn entry points, their options, and every failure are covered together in [Spawning](spawning.md).

## Inspect

| Method            | Meaning                                                                                                                                        |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `ctx.children()`  | Running children.                                                                                                                              |
| `ctx.child(name)` | The running child of that name. Throws `ErrDead` if this actor is not running; throws `ActorNotFoundError` if no running child holds the name. |
| `pid.parent()`    | The PID that spawned this actor, or `undefined` at the top of the user tree.                                                                   |

## Stop a child

```ts
await ctx.stop(child);
```

The child finishes its current message, drains, and runs `postStop`.

| Failure              | When                                                                                                   |
|----------------------|--------------------------------------------------------------------------------------------------------|
| `ErrDead`            | This actor is not running.                                                                             |
| `ErrUndefinedActor`  | `child` is the PID returned by `system.noSender()`.                                                    |
| `ActorNotFoundError` | `child` is not a live child of this actor (not running and not suspended, or not this parent's child). |

Stopping the receiving actor itself is `ctx.shutdown()` (fire-and-forget from `receive`) or `await pid.shutdown()` from outside. Children shut down with their parent.

Sending `new PoisonPill()` via `tell` also begins a graceful stop, after messages already ahead of the pill in the mailbox.

## Watch

An actor can watch another and receive a `Terminated` message when it stops. Death watch has its own page: **[Death watch](death-watch.md)**.

## Stopping from another isolate

`pid.shutdown()` on a handle whose actor lives on another isolate rejects with:

```text
TypeError: an actor owned by another isolate cannot be stopped through its handle
```

Stop a remote actor by sending it a message it handles by calling `ctx.shutdown()`, or by sending `PoisonPill`.
