# Multi-core

A live `Actor` instance can only run on the isolate that constructed it. `Props` is construction as data: the class and its constructor arguments. Passing `Props` to `system.spawn` is what allows the runtime to build the actor on whichever isolate it chooses.

There is no worker, pool, or isolate type in the public API. The spawn call is the same on one core and on many.

Examples: [`examples/props`](../../examples/props/main.ts) (`parallelism: 1`, local), [`examples/multicore`](../../examples/multicore/main.ts) (one CPU-bound actor per core).

## Place an actor

1. Export the actor class from its module.
2. Call `registerActor(TheClass)` at **module scope** in that file.
3. Spawn with `Props.create(TheClass, ...constructorArgs)`.

```ts
// greeter.actor.ts
export class Greeter implements Actor {
  constructor(readonly lang: string) {}
  preStart(): void {}
  receive(ctx: ReceiveContext): void { /* ... */ }
  postStop(): void {}
}
registerActor(Greeter);

// main.ts
const system = new ActorSystem("app"); // uses os.availableParallelism()
await system.start();
const greeter = await system.spawn("greeter", Props.create(Greeter, "fr"));
```

`Props.create` is typed by the class constructor: omitting, adding, or mistyping an argument is a compile error.

The first `Props` spawn boots the worker pool. A program that only ever spawns instances never loads that machinery.

`parallelism` on the actor system caps the pool at `os.availableParallelism()`. At `1`, the pool size is zero: every actor runs on this isolate, and the spawn call is unchanged.

```ts
new ActorSystem("app", { parallelism: 1 });
```

## Instance spawn vs Props spawn

| | `spawn(name, new Greeter())` | `spawn(name, Props.create(Greeter, "fr"))` |
| --- | --- | --- |
| Where it runs | Always this isolate | A worker the runtime chooses (or this isolate when `parallelism` is 1) |
| Registration | Not required | `registerActor` in the class's module, or `ActorNotRegisteredError` |
| Constructor args | Any | Must be structured-cloneable |
| `mailbox` / `supervisor` / `passivationStrategy` | Allowed | Refused (`TypeError`). Those are live objects. |
| `reentrancy` | Allowed | Allowed |
| Children (`ctx.spawn`) | Same isolate as the parent | Same isolate as the parent (children are instance spawns) |

Placed actors use the defaults: unbounded FIFO mailbox, stop-on-any-failure supervisor, long-lived (no passivation). Use those defaults for CPU-bound work, or keep the actor local if it needs a custom mailbox or supervisor.

## `registerActor`

```ts
registerActor(Greeter);
registerActor(Greeter, import.meta.url); // when the call goes through a wrapper
```

The class is recorded against the module that made the call, inferred from the call site. That module must be importable on every isolate. Registration is self-propagating: an isolate that imports the module to build the actor runs the same line.

Throws `TypeError` when:

- the class is anonymous
- the class is already registered from a **different** module
- the module cannot be inferred and `import.meta.url` was not passed

Registering the same class from the same module again is fine.

The class must be **exported**. Workers import it by export name.

## Messages that cross isolates

Primitives, plain objects, and arrays travel as structured clones. **Class instances** need a matching prototype on the other side so `instanceof` still works.

Call `registerMessage` at module scope in the file that defines the class:

```ts
export class CountPrimes {
  constructor(readonly upTo: number) {}
}
registerMessage(CountPrimes);
```

Every isolate that loads the module registers the same type. Sending an unregistered class instance across an isolate boundary fails on the sending side with an error whose message is `message type "<Name>" is not registered`. `tell` returns that error; `ask` rejects with it; the message is dead-lettered.

An optional second argument is the wire id. The class name is the default. Set it when two message classes share a name.

Throws `TypeError` when the id is empty or already bound to a different class. Registering the same class under the same id again is a no-op.

## What a remote handle does

`system.spawn` and `system.actorOf` return a `PID` either way. `tell`, `ask`, `request`, `watch`, and `unWatch` use the same methods.

Differences that are part of the contract:

| Operation | Remote handle |
| --- | --- |
| `tell` | Returns `null` when the **transport** accepted the envelope, not when the far mailbox did. A full or missing mailbox becomes a [dead letter](../actor-system/events.md) on the receiving isolate. Encode/clone failures return their error. |
| `ask` / `request` | The reply crosses back. Timeouts still expire in one to two periods. |
| `isRunning()` | Always `false`. Liveness across isolates is not synchronously knowable. `watch` for `Terminated`. |
| `shutdown()` | Rejects with `TypeError` (`an actor owned by another isolate cannot be stopped through its handle`). Send a message the actor handles by shutting down, or send `PoisonPill`. |
| `actor()` | A stub, not the live instance. |
| `spawnChild` | Throws `ErrDead` (`isRunning()` is false). Spawn children from **inside** the placed actor with `ctx.spawn`; they stay on that isolate. |

Top-level names remain unique across isolates once the pool is active. `actorOf` finds a placed actor by name.

## Payloads

Cross-isolate messages are structured-cloned, except that `ArrayBuffer`s reachable from the payload are **transferred**. Transfer detaches the sender's buffer: do not send a buffer the sender still needs. Posting the same buffer twice fails as a clone error.

`Props` constructor arguments are checked with `structuredClone` at spawn. A non-cloneable argument throws `TypeError`.

## Stopping the system

`system.stop()` stops the worker pool first, while the system still serves, so workers can drain and their dead letters still reach this process's event stream. Then guardians shut down.
