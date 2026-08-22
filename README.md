<h2 align="center">
  <img src="assets/nodeakt.svg" alt="nodeakt" width="480"/><br/>
  Actor framework for Node.js
</h2>

<p align="center">
  <a href="https://github.com/Tochemey/nodeakt/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Tochemey/nodeakt/ci.yml?branch=main" alt="build"/></a>
  <a href="https://codecov.io/gh/Tochemey/nodeakt"><img src="https://codecov.io/gh/Tochemey/nodeakt/graph/badge.svg" alt="codecov"/></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="license"/></a>
  <a href="https://join.slack.com/t/oss-r2l2029/shared_invite/zt-42zcqua8y-unSUH0tFlOQzwT_smzYfOQ"><img src="https://img.shields.io/badge/Slack-Join%20our%20community-4A154B?logo=slack&logoColor=white" alt="Join our Slack" /></a>
</p>

## Overview

nodeakt is an actor runtime for Node.js. An actor owns private state and a mailbox. The runtime delivers one message at a time, so that state needs no lock. Actors talk only by sending messages.

Requires Node.js 22 or newer. ESM only (`import`, not `require`).

## Features

- **Actor system.** One logical runtime per process: `start` / `stop`, top-level `spawn` and `actorOf`.
- **Typed messages.** Classes narrowed with `instanceof` in `receive`. `tell` is fire-and-forget; `ask` waits for `ctx.response`.
- **Hierarchy.** Parent/child spawn, `watch` / `Terminated`, graceful stop, `PoisonPill`.
- **Behaviors and stash.** `become` / `becomeStacked` and a per-actor stash to replay messages after a switch.
- **Supervision.** One-for-one or one-for-all; stop, resume, restart, or escalate. Restart budget and exponential backoff.
- **Mailboxes.** Unbounded and bounded FIFO, segmented, fair (per-sender), and priority (stable or not). Custom `Mailbox` implementations.
- **Passivation.** Long-lived by default; optional idle timeout or processed-message count.
- **Reentrancy.** `ctx.request` so an actor can keep working while a reply is in flight (`allowAll` or `stashNonReentrant`).
- **Event stream.** `system.subscribe` / `unsubscribe` for runtime events. `Deadletter` is the first event kind; later kinds use the same subscription. Narrow with `instanceof`.
- **Logging.** Structured JSON logger (`JsonLogger`). Pass `discardLogger` to silence the runtime.
- **Multi-core.** Use all machine cores effectively and efficiently with `Props` plus `registerActor` / `registerMessage`. Same `PID` API locally and across isolates.

The full API is in [Documentation](#documentation). What is not built yet is under [Not there yet](#not-there-yet).

## Install

```bash
pnpm add @tochemey/nodeakt
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

## Documentation

- [User guide](docs/README.md): actor system, actors, multi-core
- [Examples](examples/README.md): small programs that match those pages
- [Benchmarks](benchmark/README.md): tell and ask throughput, memory density, multi-core scaling
- [Contributing](CONTRIBUTING.md): setup, conventions, and what a change needs to merge

## Releases

nodeakt is pre-1.0. The API is settling and minor versions may still move it.

- **Nightly.** Every green push to `main` publishes a build to npm under the `nightly` dist-tag, versioned `X.Y.Z-nightly.<date>.<sha>`. Install it with `pnpm add @tochemey/nodeakt@nightly`. Nightlies never touch `latest`.
- **Stable.** A stable release is cut by pushing a version tag. The release pipeline folds the accumulated [changesets](https://github.com/changesets/changesets) into `CHANGELOG.md`, publishes to npm as `latest` with provenance, and creates the GitHub release from the changelog.

## Not there yet

nodeakt runs everything on one machine. Multi-core uses worker threads behind one logical actor system. No message ever crosses a network. The following do not exist yet:

- **Remoting and clustering.** No transport to another machine, no discovery, no cluster sharding. Remoting is planned as its own layer that reuses the existing envelope semantics and type registry with a wire codec of its own.
- **Persistence.** Actor state is in-memory only. There is no event sourcing or durable state.
- **Virtual actors (grains).** Actors are explicitly spawned and addressed. There is no on-demand activation model.
- **Scheduled messages.** No delayed or cron-style sends. Use timers inside an actor.
- **Routers.** No pool or broadcast routers as public API.
- **Metrics.** Structured logging exists. Metrics and tracing hooks do not.

If one of these matters to you, open an issue describing the use case. It helps order the roadmap.
