# Actor system

`ActorSystem` is the runtime that hosts actors: one logical system per process. Create it, `start` it, `spawn` actors, and `stop` it when you are done.

```ts
import { ActorSystem } from "@tochemey/nodeakt";

const system = new ActorSystem("orders");
await system.start();
// spawn, send, ...
await system.stop();
```

## Create

```ts
new ActorSystem(name, options?)
```

`name` must start with an alphanumeric character and may contain only alphanumerics, `-`, or `_`. Dots are not allowed (stricter than actor names).

| Failure | When |
| --- | --- |
| `ErrNameRequired` | `name` is empty. |
| `ErrInvalidActorSystemName` | The name violates the syntax above. |

`ActorSystemOptions`:

| Option | Default | Meaning |
| --- | --- | --- |
| `logger` | `defaultLogger` | Structured logger the runtime reports through. See [Logging](logging.md). |
| `remote` | none | Enables remoting. A `RemoteOptions` with the `host` and `port` the node binds. |
| `askTimeout` | `5000` | Fallback deadline in milliseconds for an `ask` or `request` whose own timeout is omitted or non-positive, so no reply-bearing call is ever unbounded. A positive integer. |

Without `remote`, the system is single-node: its node address is `127.0.0.1:0`, paths look like `nodeakt://orders@127.0.0.1:0/greeter`, and the network transport never loads. Pass `remote` to bind a listener and advertise a reachable endpoint:

```ts
const system = new ActorSystem("orders", { remote: { host: "127.0.0.1", port: 0 } });
await system.start();
system.host(); // "127.0.0.1"
system.port(); // the bound port; a configured 0 resolves to the port the OS chose
```

`RemoteOptions` takes a `host` (a concrete address such as `127.0.0.1` or `0.0.0.0`, not a name to resolve) and a `port` (`0` lets the operating system pick a free one). `host()` and `port()` report the node's endpoint; every actor's path advertises it, so `nodeakt://orders@127.0.0.1:5100/greeter` names the same actor from any node.

## Start and stop

`start()` creates the guardian hierarchy and begins accepting spawns. Starting a running system is a no-op.

`start()` also detects the machine's parallelism (`os.availableParallelism()`) and provisions the system to use every core for [placed actors](../multi-core/index.md). There is no option for it: the `NODEAKT_PARALLELISM` environment variable is the one operational override, for machines the detection misreads (container CPU quotas, shared hosts) or for forcing a local run with `1`. See [Multi-core](../multi-core/index.md#nodeakt_parallelism).

If a guardian fails to initialize, `start` throws `ActorInitializationError` and the system did not start.

`stop()` shuts every actor down through the guardians: mailboxes drain, `postStop` hooks run, the passivation scheduler is released, and any worker pool this system booted is torn down first so workers can still publish dead letters. Stopping a stopped system is a no-op. A stopped system can be started again.

`isRunning()` is `true` only after a successful `start` and before `stop` begins.

## What starts with the system

The runtime builds a fixed supervision tree. You never spawn these actors yourself; names beginning with `NodeAkt` are reserved.

```
root guardian
├── system guardian
│   ├── system.noSender()
│   └── dead-letter actor
└── user guardian
    └── every actor created with system.spawn(...)
```

Guardians are supervision structure only. They never appear in an actor's path. A top-level spawn named `greeter` is addressed as `nodeakt://<system>@127.0.0.1:0/greeter`, not under a guardian segment.

`system.noSender()` returns the PID that represents an absent sender. That is the only way to get it; it is not a standalone export. `ctx.sender === system.noSender()` when a delivery was sent with that PID.

`noSender()` throws `ErrActorSystemNotStarted` when the system is not started.

## Spawn a top-level actor

```ts
const pid = await system.spawn(name, actor, options?);
```

`actor` is either a live `Actor` instance or [`Props`](../multi-core/index.md): a live instance is pinned to this isolate, while `Props` lets the runtime build the actor on whichever isolate it chooses, possibly this one. The actor becomes a child of the user guardian, receives a `PostStart` message before anything else, and is returned as a `PID` once its `preStart` resolves.

Spawning has its own page covering both spawn entry points, instances versus `Props`, the name rules, every `SpawnOptions` field, and the failures a spawn can raise: **[Spawning](../actor/spawning.md)**.

## Look up a top-level actor

```ts
const pid = system.actorOf("greeter");
```

Returns the running top-level actor's `PID`, or `undefined` when no running top-level actor holds the name. After workers boot, the lookup includes actors placed on other isolates.

Actors deeper in the hierarchy are reached through their parent (`ctx.child(name)`), not by bare name. Runtime actors (guardians, `system.noSender()`, dead letters) are not resolvable.

## Other accessors

| Method | Returns |
| --- | --- |
| `name()` | The system name passed to the constructor. |
| `logger()` | The logger configured on the system. |

## Next

- [Logging](logging.md)
- [Events and dead letters](events.md)
- [Actors](../actor/index.md)
- [Multi-core](../multi-core/index.md)
