# Logging

The runtime reports through a `Logger`. Configure it when constructing the actor system; every actor can reach the same logger from a lifecycle `Context` via `ctx.logger()`.

```ts
import { ActorSystem, JsonLogger, discardLogger } from "@tochemey/nodeakt";

// The default is a human-readable text logger; no configuration needed.
const system = new ActorSystem("orders");

// Structured, one line of JSON per entry, for log pipelines.
const piped = new ActorSystem("orders", { logger: new JsonLogger() });
```

Pass `discardLogger` to drop every entry. The worker pool stays quiet when the system logger is `discardLogger`.

## `Logger`

| Method                              | Meaning                                                                                                                                              |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `debug` / `info` / `warn` / `error` | Emit a message and optional structured fields.                                                                                                       |
| `level()`                           | Minimum level this logger emits: a `Level` (`debug`, `info`, `warn`, `error`, or `off`). `EntryLevel` excludes `off`: the levels an entry can carry. |
| `enabled(level)`                    | Whether an entry at that level would be written.                                                                                                     |
| `with(fields)`                      | Returns a logger whose every entry also carries those fields.                                                                                        |

`Fields` is `Record<string, unknown>`. Per-entry fields are `LazyFields`: ready-made `Fields`, or a function that builds them; pass a function when building them is expensive, and it runs only if that level is enabled.

A call below the configured level returns without allocating. Implementations must keep disabled calls cheap.

## `TextLogger`

The default. Each entry is one readable line on standard error at `info` and above, laid out as:

```
[%date] [%level] [%logger] [%marker] [%thread] [%caller] - %msg {%fields}
```

A part that has no value is left out rather than printed empty, so a line carries only what it actually has: a missing `%logger`, `%marker`, or `%caller` drops its column, and an entry with no fields drops the whole ` {...}` segment.

```
[2026-08-29T14:03:12.481Z] [INF] [orders] [main] [orders/main.ts:12] - actor started {actor=user/greeter, address=0.0.0.0:5001}
[2026-08-29T14:03:12.482Z] [INF] [main] [orders/main.ts:15] - shutting down
```

- `%date`: UTC timestamp from `Date#toISOString()`.
- `%level`: the entry level as a three-letter code (`DBG`, `INF`, `WRN`, `ERR`).
- `%logger`: the reserved field key `logger`, lifted out of the fields into its own column; its column is omitted when absent. Bind it once through the `fields` option or `with({ logger: ... })`.
- `%marker`: the reserved field key `marker`, lifted the same way; its column is omitted when absent.
- `%thread`: `main` on the main thread, `worker-<id>` inside a worker thread.
- `%caller`: the source location of the log call as `dir/file:line`, mapped back through source maps so a transpiled or bundled entry still reports the line you wrote; its column is omitted when it cannot be determined, on a fully bundled build or a runtime that does not expose call sites.
- `%msg`: the message string.
- `%fields`: every remaining bound and per-entry field as comma-separated `key=value` pairs in braces; the segment is omitted when there are none. Objects serialize as JSON, with `[unserializable]` for a payload that cannot be serialized; an `Error` renders as `name: message` with its stack appended on the following lines.

The layout is fixed: no pattern option and no tuning knobs. The constructor takes `TextLoggerOptions`:

```ts
new TextLogger({
  level: "info",          // default
  stream: process.stderr, // default
  fields: { logger: "orders" },
});
```

`defaultLogger` is `new TextLogger()`: info level, stderr, no bound fields.

## `JsonLogger`

The structured logger for log pipelines: each entry is one JSON line on standard error at `info` and above. The constructor takes `JsonLoggerOptions`:

```ts
new JsonLogger({
  level: "info",          // default
  stream: process.stderr, // default
  fields: { service: "orders" },
});
```

Line shape: `time` (ISO 8601), `level`, and `msg` first, then bound fields and per-entry fields. `Error` values serialize as `{ name, message, stack }`. A circular payload still produces a line carrying the message and `unserializable: true` instead of throwing into the caller.

## From an actor

`Context` (handed to `preStart` and `postStop`) exposes the system logger:

```ts
preStart(ctx: Context): void {
  ctx.logger().info("starting", { actor: ctx.actorName() });
}
```
