# nodeakt documentation

nodeakt is an actor framework for Node.js. An actor owns private state and a mailbox. The runtime delivers one message at a time to that actor, so the state needs no lock. Actors talk only by sending messages.

These pages cover the public API from `nodeakt`. Read them in this order:

1. [Actor system](actor-system/index.md): create the runtime, start it, spawn top-level actors, log, and observe dead letters.
2. [Actors](actor/index.md): implement an actor, send messages, switch behavior, supervise children, and choose a mailbox.
3. [Multi-core](multi-core/index.md): place actors on other isolates with `Props` so CPU-bound work uses every core.

Matching programs are in [`examples/`](../examples/README.md).

## Requirements

- Node.js 22 or newer
- ESM (`"type": "module"`)

```bash
pnpm add @tochemey/nodeakt
```

```ts
import { ActorSystem } from "@tochemey/nodeakt";
```

## How to read this

Each page lists method names, defaults, thrown sentinels, and the cases that differ across isolates. Internal types (`@internal` in the source) are not part of the user API.

Sentinel errors are singleton `Error` values. Compare them by identity:

```ts
import { ErrDead, ErrMailboxFull } from "@tochemey/nodeakt";

const err = outside.tell(target, message);
if (err === ErrDead || err === ErrMailboxFull) {
  // ...
}
```

Class errors (`ActorInitializationError`, `ActorNotFoundError`, `ActorNotRegisteredError`) are constructed per failure. Inspect them with `instanceof`. Their `name` field matches the class name.

## Quick start

```ts
import type { Actor, ReceiveContext } from "@tochemey/nodeakt";
import { ActorSystem, PostStart } from "@tochemey/nodeakt";

class Greet {
  constructor(readonly name: string) {}
}

class Greeter implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const msg = ctx.message;
    if (msg instanceof PostStart) {
      return;
    }
    
    if (msg instanceof Greet) {
      console.log(`Hello, ${msg.name}!`);
    }
  }

  postStop(): void {}
}

const system = new ActorSystem("hello");
await system.start();

const greeter = await system.spawn("greeter", new Greeter());
system.noSender().tell(greeter, new Greet("Ada"));

await system.stop();
```

The same program, with expected output, is [`examples/helloworld`](../examples/helloworld/main.ts).

## Concepts

| Concept | Role |
| --- | --- |
| `ActorSystem` | One logical runtime per process. Owns the actor tree and starts or stops every actor. |
| `Actor` | Your code: `preStart`, `receive`, `postStop`. |
| `PID` | The handle you send to. Local actors and actors on other isolates share this type. |
| `Path` | Stable address: `nodeakt://system@host:port/name`. Guardians are not part of it. |
| `ReceiveContext` | One delivery: the message, the sender, and the tools to reply, spawn, watch, stash, or switch behavior. |
| `Props` | Construction as data. The spawn form the runtime can place on another isolate. |
