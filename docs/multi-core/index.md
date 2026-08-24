# Multi-core

A live `Actor` instance can only run on the isolate that constructed it. `Props` is construction as data: the class and its constructor arguments. Passing `Props` to `system.spawn` is what allows the runtime to build the actor on whichever isolate it chooses.

There is no worker, pool, or isolate type in the public API. The spawn call is the same on one core and on many.

Examples: [`examples/props`](https://github.com/Tochemey/nodeakt/blob/main/examples/props/main.ts) (`NODEAKT_PARALLELISM=1`, local), [`examples/multicore`](https://github.com/Tochemey/nodeakt/blob/main/examples/multicore/main.ts) (one CPU-bound actor per core).

## Which to use: instance or `Props`

One question decides it: **does the actor do CPU-heavy work?**

- **Yes: spawn `Props`.** Work heavy enough to block the event loop (parsing, hashing, compression, number crunching) belongs on another core, and only a `Props` spawn can run there.
- **No: spawn an instance.** Coordination, IO, live resources (sockets, streams, timers), a custom `mailbox`, `supervisor`, or `passivationStrategy`, or constructor arguments that cannot be structured-cloned: each of these pins the actor to this isolate, so construct it yourself with `new`.

When in doubt, spawn an instance. An instance is never wrong, only local; `Props` buys parallelism and pays for it with the restrictions below.

|                                                  | `spawn(name, new Greeter())` | `spawn(name, Props.create(Greeter, "fr"))`                                  |
|--------------------------------------------------|------------------------------|-----------------------------------------------------------------------------|
| Where it runs                                    | Always this isolate          | A worker the runtime chooses (this isolate when only one core is available) |
| Registration                                     | Not required                 | `registerActor` in the class's module, or `ActorNotRegisteredError`         |
| Constructor args                                 | Any                          | Must be structured-cloneable                                                |
| `mailbox` / `supervisor` / `passivationStrategy` | Allowed                      | Refused (`TypeError`). Those are live objects.                              |
| `reentrancy`                                     | Allowed                      | Allowed                                                                     |
| Children (`ctx.spawn`)                           | Same isolate as the parent   | Same isolate as the parent (children are instance spawns)                   |

Placed actors use the defaults: unbounded FIFO mailbox, stop-on-any-failure supervisor, long-lived (no passivation).

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
const system = new ActorSystem("app"); // sizes itself to the machine at start
await system.start();
const greeter = await system.spawn("greeter", Props.create(Greeter, "fr"));
```

`Props.create` is typed by the class constructor: omitting, adding, or mistyping an argument is a compile error.

There is nothing to configure. `system.start()` detects the machine (`os.availableParallelism()`) and provisions one isolate per core: this (main) isolate plus one worker per remaining core, so the isolates together match the hardware. Each new top-level `Props` actor lands on the least-occupied worker, so load stays spread as actors come and go. On a one-core machine every actor runs on this isolate; the spawn call is unchanged.

### `NODEAKT_PARALLELISM`

The detected machine is the right answer for a normal deployment, which is why there is no parallelism option in the API. The `NODEAKT_PARALLELISM` environment variable exists for the cases where the detection is wrong or deliberately unwanted:

- **Container quotas the runtime cannot see.** Detection respects CPU affinity and, on current runtimes, cgroup v2 quotas, but not every runtime and cgroup combination. A pod limited to 2 CPUs on a 32-core node may detect 32; set `NODEAKT_PARALLELISM=2` to match the quota.
- **Shared hosts.** When other processes on the machine need cores of their own, cap the system below the machine.
- **Forcing a local run.** At `1` workers never boot and every actor runs on this isolate: useful in tests, in constrained CI, and when stepping through a problem single-threaded.

```sh
NODEAKT_PARALLELISM=1 node main.js
```

The value is read once at `system.start()` and clamped between 1 and the machine's core count; a value that is not an integer is ignored. It is an operational override, not configuration: set it in the environment of a deployment, not in code.

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

`system.spawn` and `system.actorOf` return a `PID` either way. `tell`, `ask`, `request`, `watch`, and `unWatch` use the same methods. The differences below are part of the contract.

- **`tell`** returns `null` when the **transport** accepted the envelope, not when the far mailbox did. A full or missing mailbox becomes a [dead letter](../actor-system/events.md) on the receiving isolate. Encode and clone failures return their error.
- **`ask` / `request`**: the reply crosses back. Timeouts still expire in one to two periods.
- **`isRunning()`** is always `false`. Liveness across isolates is not synchronously knowable. `watch` for `Terminated` instead.
- **`shutdown()`** rejects with `TypeError` (`an actor owned by another isolate cannot be stopped through its handle`). Send a message the actor handles by shutting down, or send `PoisonPill`.
- **`actor()`** returns a stub, not the live instance.
- **`spawnChild`** throws `ErrDead` (`isRunning()` is false). Spawn children from **inside** the placed actor with `ctx.spawn`; they stay on that isolate.

Top-level names remain unique across isolates once the pool is active. `actorOf` finds a placed actor by name.

The same handle contract carries over to actors on other machines, with the network's own edges; see [Remoting](../remoting/index.md#what-a-remote-pid-does).

## Payloads

Cross-isolate messages are structured-cloned, except that `ArrayBuffer`s reachable from the payload are **transferred**.

> [!WARNING]
> Transfer detaches the sender's buffer: do not send a buffer the sender still needs. Posting the same buffer twice fails as a clone error.

`Props` constructor arguments are checked with `structuredClone` at spawn. A non-cloneable argument throws `TypeError`.

## Stopping the system

`system.stop()` stops the worker pool first, while the system still serves, so workers can drain and their dead letters still reach this process's event stream. Then guardians shut down.
