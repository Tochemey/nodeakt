# Logging

The runtime reports through a `Logger`. Configure it when constructing the actor system; every actor can reach the same logger from a lifecycle `Context` via `ctx.logger()`.

```ts
import { ActorSystem, JsonLogger, discardLogger } from "nodeakt";

const system = new ActorSystem("orders", {
  logger: new JsonLogger({ level: "debug" }),
});
```

Pass `discardLogger` to drop every entry. The worker pool stays quiet when the system logger is `discardLogger`.

## `Logger`

| Method | Meaning |
| --- | --- |
| `debug` / `info` / `warn` / `error` | Emit a message and optional structured fields. |
| `level()` | Minimum level this logger emits (`debug`, `info`, `warn`, `error`, or `off`). |
| `enabled(level)` | Whether an entry at that level would be written. |
| `with(fields)` | Returns a logger whose every entry also carries those fields. |

Fields are `Record<string, unknown>`. Pass a function when building them is expensive: the function runs only if that level is enabled.

A call below the configured level returns without allocating. Implementations must keep disabled calls cheap.

## `JsonLogger`

The default. Each entry is one JSON line on standard error at `info` and above.

```ts
new JsonLogger({
  level: "info",          // default
  stream: process.stderr, // default
  fields: { service: "orders" },
});
```

Line shape: `time` (ISO 8601), `level`, and `msg` first, then bound fields and per-entry fields. `Error` values serialize as `{ name, message, stack }`. A circular payload still produces a line carrying the message and `unserializable: true` instead of throwing into the caller.

`defaultLogger` is `new JsonLogger()`: info level, stderr, no bound fields.

## From an actor

`Context` (handed to `preStart` and `postStop`) exposes the system logger:

```ts
preStart(ctx: Context): void {
  ctx.logger().info("starting", { actor: ctx.actorName() });
}
```
