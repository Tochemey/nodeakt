# Spawning actors

An actor comes into being when you spawn it. Spawning constructs the actor, runs its [`preStart`](lifecycle.md) hook, registers it in the [tree](hierarchy.md) under a unique name, and delivers `PostStart` as its first message. The call resolves with a [`PID`](index.md#pid), the handle you send to.

There are two entry points, and they differ only in where the new actor lands in the tree.

## Two ways to spawn

| Call                                    | Spawns from                          | The new actor is a child of               |
|-----------------------------------------|--------------------------------------|-------------------------------------------|
| `system.spawn(name, actor, options?)`   | Outside any actor.                   | The user guardian: a **top-level** actor. |
| `ctx.spawn(name, actor, options?)`      | Inside `receive`.                    | The receiving actor.                      |
| `pid.spawnChild(name, actor, options?)` | Any code holding the parent's `PID`. | That `PID`'s actor.                       |

`ctx.spawn` is `pid.spawnChild` on `ctx.self`: the same call, spelled for the actor currently running.

```ts
// Top-level, from outside any actor:
const greeter = await system.spawn("greeter", new Greeter());

// A child, from inside receive:
const worker = await ctx.spawn("worker", new Worker());
```

<svg role="img" aria-label="system.spawn creates a top-level actor; ctx.spawn creates a child of the receiving actor" viewBox="0 0 720 184" style="width:100%;max-width:720px;height:auto;display:block;margin:24px auto;font-family:inherit">
  <defs>
    <marker id="act-sp" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">
      <path d="M0 0 L10 5 L0 10 Z" fill="var(--vp-c-text-3, #929295)"/>
    </marker>
  </defs>
  <rect x="16" y="24" width="260" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="146" y="48" text-anchor="middle" font-size="13.5" font-family="var(--vp-font-family-mono, ui-monospace, monospace)" fill="var(--vp-c-text-1, #3c3c43)">system.spawn(name, actor)</text>
  <text x="146" y="68" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">from outside an actor</text>
  <rect x="436" y="24" width="260" height="56" rx="8" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="566" y="48" text-anchor="middle" font-size="14.5" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">top-level actor</text>
  <text x="566" y="68" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">under the user guardian</text>
  <path d="M276 52 H432" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#act-sp)"/>
  <rect x="16" y="104" width="260" height="56" rx="8" fill="var(--vp-c-bg-soft, #f6f6f7)" stroke="var(--vp-c-divider, #c9c9cc)" stroke-width="1.5"/>
  <text x="146" y="128" text-anchor="middle" font-size="13.5" font-family="var(--vp-font-family-mono, ui-monospace, monospace)" fill="var(--vp-c-text-1, #3c3c43)">ctx.spawn(name, actor)</text>
  <text x="146" y="148" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">from inside receive</text>
  <rect x="436" y="104" width="260" height="56" rx="8" fill="var(--vp-c-brand-soft, #e8ebf8)" stroke="var(--vp-c-brand-1, #3451b2)" stroke-width="1.5"/>
  <text x="566" y="128" text-anchor="middle" font-size="14.5" font-weight="500" fill="var(--vp-c-text-1, #3c3c43)">child actor</text>
  <text x="566" y="148" text-anchor="middle" font-size="13" fill="var(--vp-c-text-2, #67676c)">under the receiving actor</text>
  <path d="M276 132 H432" fill="none" stroke="var(--vp-c-text-3, #929295)" stroke-width="1.5" marker-end="url(#act-sp)"/>
</svg>

Every spawn returns a `Promise<PID>`: it resolves only once `preStart` has finished, so a resolved `PID` is always ready to receive.

## An instance, or `Props`

`actor` is either a **live `Actor` instance** or a **`Props`**. The difference is not local versus remote; it is whether the actor is **pinned** to this isolate or free to be **placed**.

- A **live instance** runs on the isolate that constructed it, this one, always.
- **`Props`** is construction as data, the actor's class and its constructor arguments, so the runtime can build it on whichever isolate it chooses. That may be this one: on a one-core machine it always is, and even with a worker pool the placement can land here. Only on a machine running the pool might it be another core.

The rule: spawn `Props` for a CPU-heavy actor that should run on another core; spawn an instance for everything else, and when in doubt spawn an instance. See [Which to use](../multi-core/index.md#which-to-use-instance-or-props).

```ts
const pinned = await system.spawn("greeter", new Greeter("fr")); // always this isolate
const placed = await system.spawn("worker", Props.create(Worker, "fr")); // the runtime chooses, possibly this isolate
```

Only `system.spawn` accepts `Props`. `ctx.spawn` and `pid.spawnChild` always take a live instance, and a child always runs on the **same isolate** as its parent. Spawning at the top level with `Props` is what lets the runtime spread actors across cores. See [Multi-core](../multi-core/index.md).

## Naming and uniqueness

A `name` must:

- start with an alphanumeric character
- contain only alphanumerics, `-`, `_`, or `.`
- be at most 255 characters
- not start with the reserved prefix `NodeAkt`

Top-level names are unique across the system; once a worker pool is active, they are unique across every isolate. A child's name is unique among its siblings, so the full [path](index.md#path) stays unambiguous even when two actors share a local name under different parents.

A **taken name** is handled differently by the two entry points:

- `system.spawn` **throws** `ErrActorAlreadyExists` if the name is still held, including by a suspended or currently stopping actor.
- `pid.spawnChild` (and `ctx.spawn`) **returns the existing child** under that name instead, so spawning the same child twice is idempotent.

## What a spawn does, in order

1. Construct the actor (`Props` builds it on the chosen isolate; a live instance is used as given).
2. Run [`preStart`](lifecycle.md). Until it resolves the actor is not registered and not reachable by name.
3. Register the actor in the tree; its [path](index.md#path) is now fixed for its whole life.
4. Enqueue `PostStart` as the very first message, ahead of any other sender.
5. Begin processing messages in [`receive`](messaging.md).

An [`ActorStarted`](../actor-system/events.md) event is published as the actor becomes ready, and an `ActorChildCreated` alongside it when the spawn was a child. Initialize state in `preStart`, never in the constructor: a [restart](lifecycle.md#restart) reuses the same instance and re-runs `preStart` but never the constructor.

## Options

`SpawnOptions` are all optional and independent:

| Option                | Default                                | Notes                                                                                                                  |
|-----------------------|----------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `mailbox`             | `UnboundedMailbox`                     | The queue backing the actor. Live object, refused on `Props` spawns. See [Mailboxes](mailboxes.md).                    |
| `passivationStrategy` | `TimeBasedStrategy` (2 min idle)        | Passivates the actor after `DefaultPassivationTimeout` of idleness; pass `LongLivedStrategy` to opt out. Live object, refused on `Props` spawns. See [Passivation](passivation.md). |
| `supervisor`          | any failure **stops** the actor        | How a failure in `receive` is handled. Live object, refused on `Props` spawns. See [Supervision](supervision.md).      |
| `reentrancy`          | requests disabled                      | Whether the actor may issue non-parking [`request`](reentrancy.md)s. Data, allowed on `Props` spawns.                  |

The three live options are refused on a `Props` spawn because they cannot cross an isolate boundary; `reentrancy` is plain data and travels with the `Props`.

```ts
const pid = await system.spawn("orders", new Orders(), {
  mailbox: new BoundedMailbox(1_000),
  supervisor: new Supervisor({ anyErrorDirective: RestartDirective }),
});
```

## When a spawn fails

| Failure                    | When                                                                                                                                           |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `ErrActorSystemNotStarted` | The system is not running (`system.spawn`).                                                                                                    |
| `ErrDead`                  | The parent actor is not running (`ctx.spawn` / `pid.spawnChild`).                                                                              |
| `ErrReservedName`          | The name starts with `NodeAkt`.                                                                                                                |
| `ErrInvalidActorName`      | The name is empty, too long, or violates the syntax.                                                                                           |
| `ErrActorAlreadyExists`    | A top-level name is still held (`system.spawn` only).                                                                                          |
| `ActorInitializationError` | `preStart` threw. The cause is on `error.cause`; the actor is not registered.                                                                  |
| `ActorNotRegisteredError`  | `actor` is a `Props` whose class was never `registerActor`'d.                                                                                  |
| `TypeError`                | A `Props` spawn carrying a live `mailbox`, `supervisor`, or `passivationStrategy`, or a constructor argument that cannot be structured-cloned. |

A failed `preStart` on a top-level spawn is surfaced to the caller; on a child, it also surfaces to whoever called `spawn` and the child is not registered.

## Finding an actor you already spawned

Hold onto the `PID` a spawn returns. To look one up again later:

- `system.actorOf(name)` returns the running top-level actor of that name, or `undefined`. After workers boot, it also finds actors placed on other isolates.
- `ctx.child(name)` returns a running child of the receiving actor by name.

Actors deeper in the tree are reached through their parent, not by bare name, and the runtime's own actors (guardians, dead letters, `system.noSender()`) are not resolvable.

## Where to next

- [Lifecycle](lifecycle.md): what happens after the spawn, from `PostStart` to stop.
- [Hierarchy and stop](hierarchy.md): the tree, inspecting children, and stopping them.
- [Multi-core](../multi-core/index.md): `Props` and spawning across cores.
