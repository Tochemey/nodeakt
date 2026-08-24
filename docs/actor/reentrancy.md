# Reentrancy

`ask` parks the caller until the reply arrives. That deadlocks if A asks B while B is asking A, and it deadlocks if an actor asks itself from `receive`.

`ctx.request` sends the message and returns immediately with a `RequestCall`. The continuation runs later on **this actor's own turn**, serialized with its other messages, so actor state is safe to touch.

Requests are **disabled** unless the actor is spawned with a `reentrancy` option.

Example: [`examples/reentrancy`](https://github.com/Tochemey/nodeakt/blob/main/examples/reentrancy/main.ts).

```ts
await system.spawn("client", new Client(), {
  reentrancy: { mode: "allowAll", maxInFlight: 32 },
});
```

`reentrancy` is data (not a live object) and **may** ride a [`Props`](../multi-core/index.md) spawn. Its type is `Reentrancy`: a `mode` (a `ReentrancyMode`) plus an optional `maxInFlight`.

## Modes

| Mode | While a request is in flight |
| --- | --- |
| `off` | Requests disabled (same as omitting `reentrancy`). |
| `allowAll` | Every message keeps being processed. State can change between the request and its continuation. Use this to avoid deadlocks in call cycles. |
| `stashNonReentrant` | User messages are stashed; replies and runtime messages keep flowing. Preserves strict ordering at the cost of latency. Pair with timeouts and a finite `maxInFlight`. |

An unknown mode throws `ErrInvalidReentrancyMode` at spawn.

`maxInFlight`: cap on outstanding requests. A request past the cap completes with `ErrReentrancyInFlightLimit`. Zero, negative, or omitted means unlimited. Use a finite cap in production.

## Issuing a request

The options argument is a `RequestOptions`: an optional `timeout` and an optional per-call `mode`.

```ts
const call = ctx.request(peer, new GetQuote(symbol), {
  timeout: 1_000,          // optional; falls back to the system askTimeout if omitted or non-positive
  mode: "allowAll",        // optional; overrides the actor's mode for this call only
});

call.onReply((reply, error) => {
  if (error !== null) {
    // ErrRequestTimeout, ErrDead, ErrRequestCanceled, ...
    return;
  }
  this.last = reply;
});
```

`RequestCall` is not thenable. `await` would park the behavior and resume on the microtask queue, outside the actor's turn.

`onReply` runs exactly once. If the request has already completed when you register it, the callback runs immediately on the caller's stack. Register it inside the actor.

`call.cancel()` completes the continuation with `ErrRequestCanceled` on the actor's turn. A reply arriving later is ignored. Cancel after completion is a no-op.

Timeouts use the same coarse expiry as `ask`: between **one and two** timeout periods.

## Failures

A request that cannot be admitted or delivered still returns a handle. The continuation runs with the error:

| Error | When |
| --- | --- |
| `ErrReentrancyDisabled` | No `reentrancy` config, or mode `off`. |
| `ErrInvalidReentrancyMode` | Unknown mode on the request options. |
| `ErrReentrancyInFlightLimit` | Already at `maxInFlight`. |
| `ErrDead` | Target not running. |
| `ErrRequestTimeout` | No reply in time. |
| `ErrRequestCanceled` | `cancel()` was called. |
| mailbox error | Delivery rejected. |

The target replies with `ctx.response`, the same as for `ask`.
