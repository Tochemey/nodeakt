# Supervision

When `receive` throws (or returns a rejected promise), the actor's supervisor decides what happens. The supervisor is configured on the failing actor at spawn time, not on the parent. The parent executes the directive.

Example: [`examples/supervision`](https://github.com/Tochemey/nodeakt/blob/main/examples/supervision/main.ts).

## Default

Spawned without a supervisor, any failure stops that actor. That is `new Supervisor()`: a catch-all `StopDirective`.

```ts
await system.spawn("worker", new Worker(), {
  supervisor: new Supervisor({
    anyErrorDirective: RestartDirective,
    maxRetries: 3,
    timeout: 5_000,
  }),
});
```

`Supervisor` instances are immutable and can be shared. The constructor takes `SupervisorOptions`: a `strategy` (a `Strategy`), `directives` (pairs of `ErrorClass` and `Directive`), an `anyErrorDirective` catch-all, and the restart budget fields below. `ErrorClass` is any error constructor.

## Strategy

| Constant | Effect |
| --- | --- |
| `OneForOneStrategy` (default) | The directive applies to the failing actor only. Siblings keep running. |
| `OneForAllStrategy` | The directive applies to the failing actor and every sibling under the same parent. Use when the children are coupled and one failure compromises the group. |

## Directives

| Constant | Effect |
| --- | --- |
| `StopDirective` | Stop the failing actor (and siblings, if one-for-all). |
| `ResumeDirective` | Resume without resetting state. The failed message is dropped. Use for transient errors the actor can move past. |
| `RestartDirective` | Stop children, run `postStop` (unless already suspended), reset behaviors and stash, re-run `preStart`, then continue with messages still in the mailbox. The same instance is reused. |
| `EscalateDirective` | The failing actor stays suspended. The parent receives a `PanicSignal` in `receive`. `reason` is the error and `ctx.sender` is the failing actor. The parent then `reinstate`, `restart`, or `stop`s it. |

A suspended actor holds its state, queued messages, and stash, but accepts and processes nothing until it is restarted, reinstated, or stopped.

## Matching errors

> [!IMPORTANT]
> Rules match the thrown error's exact constructor. Subclasses do not match.

```ts
new Supervisor({
  directives: [
    [TypeError, ResumeDirective],
    [RangeError, RestartDirective],
  ],
});
```

| Configuration | Unmatched error |
| --- | --- |
| No options, or `anyErrorDirective` set | The catch-all applies (`StopDirective` when you passed nothing). |
| `anyErrorDirective` | Always that directive; `directives` is ignored. |
| Only `directives` | Unmatched errors suspend the actor. There is no implicit stop. |

`directive(err)` returns `undefined` when neither a rule nor a catch-all applies; the runtime then suspends.

## `RestartDirective`

These fields tune how `RestartDirective` retries: its budget and its backoff.

| Option | Default | Meaning |
| --- | --- | --- |
| `maxRetries` | `0` | Maximum consecutive restarts inside the reset window. One more fault suspends instead. Also bounds attempts when a restart itself keeps failing. `0` is unbounded. |
| `timeout` | non-positive | Fault-free window in milliseconds after which the consecutive counter resets. Also the constant delay between attempts when a restart itself keeps failing. Non-positive disables the budget (unbounded). |
| `initialDelay` | `0` (off) | Enables exponential backoff: the nth consecutive restart waits `min(initialDelay * 2^(n-1), maxDelay)` ms. Non-positive disables backoff. |
| `maxDelay` | `initialDelay` when backoff is on | Upper bound of the backoff delay; values below `initialDelay` are raised to it. |
| `backoffResetAfter` | `maxDelay` when backoff is on | Fault-free period that resets the consecutive counter when backoff is on. Takes precedence over `timeout` as the reset window. |

A bounded retry of a restart that itself fails requires both `maxRetries > 0` and `timeout > 0`.

## `PanicSignal`

Delivered only on escalate. Handle it in the parent's `receive`:

```ts
if (msg instanceof PanicSignal) {
  const failed = ctx.sender;
  ctx.self.reinstate(failed!);
}
```

`pid.reinstate(target)` accepts a `PID` or the child's spawn name. Reinstating an actor that is not suspended is a no-op.

## Initialization failures

`preStart` failure is not a supervision event. It throws `ActorInitializationError` to the spawner. On a restart, a failed `preStart` leaves the actor suspended so another restart can be attempted.
