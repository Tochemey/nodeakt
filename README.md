<h2 align="center">
  <img src="assets/nodeakt.svg" alt="nodeakt" width="480"/><br/>
  Actor framework for Node, Bun, and Deno
</h2>

<p align="center">
  <a href="https://github.com/Tochemey/nodeakt/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Tochemey/nodeakt/ci.yml?branch=main" alt="build"/></a>
  <a href="https://www.npmjs.com/package/@tochemey/nodeakt"><img src="https://img.shields.io/badge/npm-registry-cb3837?logo=npm&logoColor=white" alt="npm registry"/></a>
  <a href="https://codecov.io/gh/Tochemey/nodeakt"><img src="https://codecov.io/gh/Tochemey/nodeakt/graph/badge.svg" alt="codecov"/></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="license"/></a>
  <a href="https://join.slack.com/t/oss-r2l2029/shared_invite/zt-42zcqua8y-unSUH0tFlOQzwT_smzYfOQ"><img src="https://img.shields.io/badge/Slack-Join%20our%20community-4A154B?logo=slack&logoColor=white" alt="Join our Slack" /></a>
</p>

## Overview

NodeAkt is an actor framework for Node.js, Bun, and Deno.

An **actor** is a small unit of computation that owns private state and a mailbox. It never shares memory; it communicates only by sending messages, and the runtime delivers those messages to it **one at a time**. That single rule is what makes actors easy to reason about: inside a handler there is no concurrency, so there are no locks, no races, and no shared-state bugs. You model a system as many actors that each do one thing and talk by message, and the framework runs them safely across cores and across machines.

The whole runtime, the multi-core layer and the network protocol included, has **zero dependencies**: installing it brings exactly one package. It requires Node.js 22+, Bun 1.3+, or Deno 2+, and is ESM only (`import`, not `require`). Every runtime is exercised in CI, multi-core placement included.

## Install

```sh
npm install @tochemey/nodeakt
```

The same package works on pnpm (`pnpm add @tochemey/nodeakt`), Yarn (`yarn add @tochemey/nodeakt`), Bun (`bun add @tochemey/nodeakt`), and Deno (`deno add npm:@tochemey/nodeakt`).

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

## Features

Start with a handful of actors on one machine and grow to a cluster of them without changing how you write an actor.

### The actor model

- **Typed messages.** Messages are plain classes; a handler narrows them with ordinary `instanceof` checks. `tell` sends and forgets; `ask` awaits a typed reply with a timeout; `forward` preserves the original sender.
- **Hierarchy and death watch.** Actors spawn children and `watch` any other actor, receiving a `Terminated` message when it stops. Stopping a parent drains its whole subtree cleanly.
- **Behaviors and stash.** An actor can swap its message handler at runtime, set aside messages it cannot serve yet, and replay them in order once it switches back.
- **Scheduling and pipeTo.** Send a message after a delay or on a repeating interval, cancel or pause it by reference; and deliver the result of any promise to a mailbox as an ordinary message once it settles.
- **Reentrancy.** An actor can keep processing its mailbox while one of its own requests is still in flight, with control over which messages may overtake the pending reply.

### Resilience

- **Supervision.** When an actor fails, its parent decides: stop, resume, restart, or escalate, for one child or all of them, with restart budgets and exponential backoff.
- **Mailboxes.** Unbounded and bounded FIFO, segmented, fair per-sender, and priority variants, or bring your own.
- **Passivation.** Keep an actor forever, or retire it automatically after an idle timeout or a processed-message count.
- **Event stream and dead letters.** Subscribe to what the runtime observes: undeliverable messages become dead letters, and lifecycle events (started, stopped, restarted, passivated) flow on the same stream.
- **Logging.** Structured JSON logging out of the box, or silence the runtime entirely.

### Distribution and scale

- **Multi-core.** Spawn actors across every machine core without touching workers or threads yourself. An actor's address works the same on any core, on Node.js, Bun, and Deno alike.
- **Routers.** Spread work over a pool of identical actors or broadcast to all: round-robin, random, fan-out, and consistent-hash. The router supervises its routees, resizes in place, and replies go straight to the original sender.
- **[Remoting](https://tochemey.github.io/nodeakt/remoting/).** Look up, spawn, watch, and message actors on another machine over TCP with the same `PID` API. Optional TLS.
- **[Clustering](https://tochemey.github.io/nodeakt/clustering/).** Turn a set of nodes into one cluster: discover peers, spawn and address actors by name across the cluster, place them by strategy, run cluster singletons, and have a departed node's actors recreated on the survivors automatically.

The full API is in the [Documentation](#documentation).

## Documentation

- [Documentation](https://tochemey.github.io/nodeakt/): getting started, the tour, and the full reference (source pages in [docs/](docs/))
- [Examples](examples/README.md): small programs that match those pages
- [Benchmarks](benchmark/README.md): tell and ask throughput, memory density, multi-core scaling
- [Contributing](CONTRIBUTING.md): setup, conventions, and what a change needs to merge

## Releases

- **Stable.** A stable release is cut by pushing a version tag. The maintainer bumps the version in `package.json` and writes the `CHANGELOG.md` section; the release pipeline then publishes to npm as `latest` with provenance and creates the GitHub release from the changelog.
