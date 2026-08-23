# Routers

A router is an actor that owns a pool of identical routees and forwards every message it receives to them according to a routing strategy. Use one to spread work over a pool or to broadcast a message to a group without hand-rolling the bookkeeping: the router spawns the routees as its children, supervises them, keeps the rotation up to date as routees die, and stops the whole pool when it stops.

```ts
import { Props } from "@tochemey/nodeakt";

const router = await system.spawnRouter("workers", 8, Props.create(Worker), {
  strategy: "roundRobin",
});

system.noSender().tell(router, new Job(42)); // routed to one routee
```

`spawnRouter(name, poolSize, routees, options?)` creates and starts the router. The router is a real actor: it lives under the given unique name like any `spawn`, it appears in the tree, and `system.actorOf(name)` resolves it. `poolSize` is a positive integer; `routees` is `Props`, so each routee is constructed fresh from the same recipe. Everything `spawn` rejects with applies, plus `ErrInvalidPoolSize` for a bad pool size, `ErrInvalidRoutingStrategy` for an unknown strategy, `ErrRoutingKeyRequired` for consistent hashing without a routing key extractor, and `ErrInvalidRouteeDirective` for an unknown routee directive.

## Strategies

`RouterOptions.strategy` is a `RoutingStrategy`; the default is round robin.

| Value | Constant | Effect |
| --- | --- | --- |
| `"roundRobin"` | `RoundRobinRouting` | Each message goes to the next routee in rotation, so a steady stream spreads evenly. |
| `"random"` | `RandomRouting` | Each message goes to a routee picked at random. |
| `"fanOut"` | `FanOutRouting` | Each message goes to every live routee: a broadcast. |
| `"consistentHash"` | `ConsistentHashRouting` | Each message goes to the routee owning its routing key, so equal keys always land on the same routee. |

Consistent hashing requires a routing key extractor in `RouterOptions.routingKey`, a `RoutingKeyFunc` mapping a message to a `string` or `number` key:

```ts
const router = await system.spawnRouter("carts", 8, Props.create(Cart), {
  strategy: "consistentHash",
  routingKey: (message) => (message as AddItem).cartId,
});
```

Keys are pinned with a consistent-hash ring, so resizing the pool moves as few keys as possible. An extractor that throws does not fail the router: the message is routed to dead letters with the thrown error as the reason.

## Forwarding semantics

The router forwards; it never processes a user message itself. The delivery is handed to the routee live, so:

- The routee sees the original sender as `ctx.sender`, not the router. Replies bypass the router.
- An `ask` sent to the router is answered by whichever routee receives it; `ctx.response` in the routee settles the asker's promise directly, and the ask timeout applies end to end.
- An `ask` (or an in-actor `request`) through a fan-out router is rejected with the `ErrFanOutAsk` sentinel: a broadcast has no single answer.

## Management messages

The router consumes two management messages itself instead of forwarding them. They are exported message classes, not methods on a handle: the router is addressed like any other actor, which keeps the surface uniform and works unchanged once messages cross isolates.

`GetRoutees` asks for the pool; the router answers with a `Routees` message whose `paths` field lists the canonical path strings of the live routees:

```ts
import { GetRoutees, Routees } from "@tochemey/nodeakt";

const routees = (await system.noSender().ask(router, new GetRoutees(), 1_000)) as Routees;
console.log(routees.paths); // ["nodeakt://sys@127.0.0.1:0/workers/routee-0", ...]
```

`AdjustRouterPoolSize` grows or shrinks the pool in place to the given number of live routees. A grow spawns fresh routees; a shrink stops the newest ones gracefully. Sent with `tell` it is fire and forget; sent with `ask` the router answers with the resulting `Routees` once the adjustment is done. A size that is not a non-negative integer is refused with the `ErrInvalidPoolSize` sentinel.

```ts
import { AdjustRouterPoolSize } from "@tochemey/nodeakt";

await system.noSender().ask(router, new AdjustRouterPoolSize(16), 1_000);
```

## Failing routees

A routee that throws while processing is handled by the routee directive chosen at spawn time, `RouterOptions.directive`, a `RouteeDirective`:

| Value | Effect |
| --- | --- |
| `"stop"` (default) | The failing routee stops and the pool shrinks. |
| `"restart"` | The failing routee is restarted in place and stays in the rotation. |
| `"resume"` | The failing routee moves past the failure with its state kept. |

A dead routee leaves the rotation as soon as its death is observed. A pool that reaches zero routees matches the send-path rules: every subsequent send becomes a dead letter carrying `ErrDead` as the reason (and an ask rejects with the same sentinel), until `AdjustRouterPoolSize` restores capacity explicitly.

## Lifecycle

The routees are children of the router, named `routee-0`, `routee-1`, and so on; names are never reused within one router. Stopping the router, with `shutdown`, a `PoisonPill`, or the system stopping, stops the pool with it. The router and its routees run on the isolate that spawned the router.
