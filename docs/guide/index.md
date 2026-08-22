# Introduction

NodeAkt is an actor framework for Node.js. An actor owns private state and a mailbox. The runtime delivers one message at a time to that actor, so the state needs no lock. Actors talk only by sending messages.

> [!NOTE]
> nodeakt is pre-1.0. The API is settling and minor versions may still move it. Every green push to `main` publishes a nightly build; see [Releases](https://github.com/Tochemey/nodeakt#releases).

## Requirements

- Node.js 22 or newer
- ESM (`"type": "module"`)

## Install

::: code-group

```sh [📦 npm]
npm install @tochemey/nodeakt
```

```sh [⚡ pnpm]
pnpm add @tochemey/nodeakt
```

```sh [🧶 yarn]
yarn add @tochemey/nodeakt
```

```sh [🍞 bun]
bun add @tochemey/nodeakt
```

:::

Then import from the package:

```ts
import { ActorSystem } from "@tochemey/nodeakt";
```

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

The same program, with expected output, is [`examples/helloworld`](https://github.com/Tochemey/nodeakt/blob/main/examples/helloworld/main.ts). The [Tour](tour.md) walks through it and every other example.

## Concepts

| Concept          | Role                                                                                                    |
|------------------|---------------------------------------------------------------------------------------------------------|
| `ActorSystem`    | One logical runtime per process. Owns the actor tree and starts or stops every actor.                   |
| `Actor`          | Your code: `preStart`, `receive`, `postStop`.                                                           |
| `PID`            | The handle you send to. Local actors and actors on other isolates share this type.                      |
| `Path`           | Stable address: `nodeakt://system@host:port/name`. Guardians are not part of it.                        |
| `ReceiveContext` | One delivery: the message, the sender, and the tools to reply, spawn, watch, stash, or switch behavior. |
| `Props`          | Construction as data. The spawn form the runtime can place on another isolate.                          |

## Read next

The reference covers NodeAkt's public API. Read it in this order:

1. [Actor system](../actor-system/index.md): create the runtime, start it, spawn top-level actors, log, and observe dead letters.
2. [Actors](../actor/index.md): implement an actor, send messages, switch behavior, supervise children, and choose a mailbox.
3. [Multi-core](../multi-core/index.md): place actors on other isolates with `Props` so CPU-bound work uses every core.

Each page lists method names, defaults, thrown errors, and the cases that differ across isolates. Internal types (`@internal` in the source) are not part of the user API. Error conventions and the full error index are in [Errors](../errors.md).
