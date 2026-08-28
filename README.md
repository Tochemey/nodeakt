<h2 align="center">
  <img src="assets/nodeakt.svg" alt="nodeakt" width="480"/><br/>
  Distributed Actor framework for TypeScript
</h2>

<p align="center">
  <a href="https://github.com/Tochemey/nodeakt/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Tochemey/nodeakt/ci.yml?branch=main" alt="build"/></a>
  <a href="https://www.npmjs.com/package/@tochemey/nodeakt"><img src="https://img.shields.io/npm/v/%40tochemey%2Fnodeakt?logo=npm&logoColor=white&color=brightgreen" alt="npm version"/></a>
  <a href="https://codecov.io/gh/Tochemey/nodeakt"><img src="https://codecov.io/gh/Tochemey/nodeakt/graph/badge.svg" alt="codecov"/></a>
  <a href="https://biomejs.dev"><img src="https://img.shields.io/badge/code%20style-biome-60a5fa?logo=biome&logoColor=white" alt="code style: biome"/></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="license"/></a>
  <a href="https://join.slack.com/t/oss-r2l2029/shared_invite/zt-42zcqua8y-unSUH0tFlOQzwT_smzYfOQ"><img src="https://img.shields.io/badge/Slack-Join%20our%20community-4A154B?logo=slack&logoColor=white" alt="Join our Slack" /></a>
</p>

NodeAkt is a [TypeScript](https://www.typescriptlang.org/)-first distributed actor framework for [Node.js](https://nodejs.org/en), [Bun](https://bun.com/), and [Deno](https://deno.com/). It lets you build responsive, resilient, and elastic systems with typed actor messages, running as a single process or a cluster of nodes behind the same API.

- **Simpler concurrency.** Actors process one message at a time; you write plain TypeScript with no locks, channel plumbing, or shared-state bugs.
- **Location transparency.** Send a message to a local, remote, or clustered actor with the same API; the framework handles the wire.
- **Resilience by design.** Supervision trees and the "let it crash" model, inspired by Erlang/OTP, keep failures contained and recoverable.
- **Production batteries included.** Remoting, Clustering, scheduling, passivation, without re-rolling them yourself.
- **Multi-core.** Spawn actors across every machine core without touching workers or threads yourself. An actor's address works the same on any core, on Node.js, Bun, and Deno alike.

An [**actor**](https://en.wikipedia.org/wiki/Actor_model) is a small unit of computation that owns private state and a mailbox. It never shares memory; it communicates only by sending messages, and the runtime delivers those messages to it **one at a time**. That single rule is what makes actors easy to reason about: inside a handler there is no concurrency, so there are no locks, no races, and no shared-state bugs. You model a system as many actors that each do one thing and talk by message, and the framework runs them safely across cores and across machines. For more depth, watch **Carl Hewitt**, the father of the actor model, explain it in his own words:

[![Carl Hewitt on the actor model](https://img.youtube.com/vi/7erJ1DV_Tlo/hqdefault.jpg)](https://www.youtube.com/watch?v=7erJ1DV_Tlo) 


The whole runtime, the multi-core layer and the network protocol included, has **zero dependencies**: installing it brings exactly one package. Every supported runtime is exercised in CI, multi-core placement included.

## Install

One package, zero dependencies, ESM only (`import`, not `require`). Pick your runtime:

**[Node.js 22+](https://nodejs.org/en/download)**

```sh
npm install @tochemey/nodeakt
pnpm add @tochemey/nodeakt
yarn add @tochemey/nodeakt
```

**[Bun 1.3+](https://bun.com/docs/installation)**

```sh
bun add @tochemey/nodeakt
```

**[Deno 2+](https://docs.deno.com/runtime/getting_started/installation/)**

```sh
deno add npm:@tochemey/nodeakt
```

## Quick start

```ts
import type { Actor, Context, ReceiveContext } from "@tochemey/nodeakt";
import { ActorSystem } from "@tochemey/nodeakt";

class Greet {
  constructor(readonly name: string) {}
}

// An actor implements three lifecycle hooks: preStart, receive, and postStop.
class Greeter implements Actor {
  preStart(_ctx: Context): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Greet) {
      console.log(`Hello, ${ctx.message.name}!`);
    }
  }

  postStop(_ctx: Context): void {}
}

const system = new ActorSystem("hello");
await system.start();

const greeter = await system.spawn("greeter", new Greeter());
system.noSender().tell(greeter, new Greet("Ada")); // Hello, Ada!

await system.stop();
```

Head to [Getting started](https://tochemey.github.io/nodeakt/guide/) to build on this, including `ask`/`request` for typed replies, supervision, and more.

## Documentation

- [Documentation](https://tochemey.github.io/nodeakt/): getting started, the tour, and the full reference
- [Examples](examples/README.md): small programs that match those pages
- [Benchmarks](benchmark/README.md): tell and ask throughput, memory density, multi-core scaling

## Community

Ask questions and follow the work in [Issues](https://github.com/Tochemey/nodeakt/issues), or on [Slack](https://join.slack.com/t/oss-r2l2029/shared_invite/zt-42zcqua8y-unSUH0tFlOQzwT_smzYfOQ). Feedback on the [tracking issue](https://github.com/Tochemey/goakt/issues/948) helps shape what we work on next.


## Contribution

See the [contribution guide](CONTRIBUTING.md) for setup, conventions, and what a change needs to merge
