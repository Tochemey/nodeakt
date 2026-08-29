# Extensions

An actor system usually runs a handful of services that many actors share: an event store, a metrics recorder, a tracing client, a feature-flag provider. An extension is one of those services, installed on the system when you create it and reachable by name from any actor's context, so no actor has to be handed it through its constructor.

```ts
import { ActorSystem, type Extension } from "@tochemey/nodeakt";

class EventStore implements Extension {
  id(): string {
    return "eventStore";
  }

  async append(streamId: string, event: unknown): Promise<void> {
    // ...
  }
}

const system = new ActorSystem("orders", {
  extensions: [new EventStore(), new MetricsRecorder()],
});
```

## The `Extension` interface

One method, no base class, no lifecycle hook:

```ts
interface Extension {
  id(): string;
}
```

The system stores the instance you hand it and returns that same instance to every lookup. Anything an extension needs to do on the way up or down, opening a connection or flushing a buffer, it does through its own API, on the schedule its owner chooses. That keeps the contract to one question: what name is this service known by?

An identifier must be 2 to 255 characters, start with an alphanumeric character, and contain only alphanumerics, `-` or `_` (`^[a-zA-Z0-9][a-zA-Z0-9-_]*$`). It is read once, while the system is being created, so report a constant rather than a value derived from mutable state.

## Look them up

From anything holding the system:

```ts
const store = system.extension<EventStore>("eventStore");
const all = system.extensions();
```

`extension(id)` returns `undefined` when nothing is installed under that identifier, and `extensions()` returns every installed extension, in the order they were given, which is how you report what a running system depends on.

The type parameter is a convenience for the caller: the system stores plain `Extension` values and asserts the concrete type on the way out, so ask for the type the identifier was installed with.

## From an actor

Both contexts expose the same two lookups. The lifecycle `Context` handed to `preStart` and `postStop` is where an actor acquires the service once and keeps it:

```ts
preStart(ctx: Context): void {
  this.store = ctx.extension<EventStore>("eventStore");
}
```

The `ReceiveContext` of a message resolves the same instance, for an actor that would rather not carry the field:

```ts
receive(ctx: ReceiveContext): void {
  ctx.extension<EventStore>("eventStore")?.append("orders", ctx.message);
}
```

Both answer `undefined` when nothing is installed under the identifier, which is why the calls above are guarded. An actor that cannot work without its service should read it in `preStart` and throw when it is missing: the start aborts with an `ActorInitializationError`, and the actor never processes a message.

## Failures

An extension is misconfigured at construction or not at all: a bad identifier fails `new ActorSystem(...)` rather than leaving a lookup to answer `undefined` somewhere deep in a running system.

| Failure | When |
| --- | --- |
| `ErrInvalidExtensionId` | An extension reports an identifier that is not 2 to 255 characters, does not start with an alphanumeric character, or carries anything other than alphanumerics, `-` or `_`. |
| `ErrExtensionAlreadyExists` | Two extensions report the same identifier; an identifier names exactly one installed service. |

## Scope

Extensions are per-process object instances. They are never serialized, never sent over the wire, and never relocated with an actor. An actor placed on a [worker isolate](../multi-core/index.md) sees the extensions of the system running in that isolate, which is not the one you installed on the main isolate, so a lookup there answers `undefined`. Use extensions for services the local process owns, and keep an actor's own dependencies, the ones that must survive its restart or its relocation, on its [`Props`](../multi-core/index.md) arguments.

This is a flat name-to-instance table, not a dependency-injection container. There are no scopes, no resolution graph, and no construction on your behalf: you build the service, the system holds it, actors ask for it by name.
