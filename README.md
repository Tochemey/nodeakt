<h2 align="center">
  <img src="assets/nodeakt.svg" alt="nodeakt" width="480"/><br/>
  Actor framework for Node, Bun, and Deno
</h2>

<p align="center">
  <a href="https://github.com/Tochemey/nodeakt/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Tochemey/nodeakt/ci.yml?branch=main" alt="build"/></a>
  <a href="https://codecov.io/gh/Tochemey/nodeakt"><img src="https://codecov.io/gh/Tochemey/nodeakt/graph/badge.svg" alt="codecov"/></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="license"/></a>
  <a href="https://join.slack.com/t/oss-r2l2029/shared_invite/zt-42zcqua8y-unSUH0tFlOQzwT_smzYfOQ"><img src="https://img.shields.io/badge/Slack-Join%20our%20community-4A154B?logo=slack&logoColor=white" alt="Join our Slack" /></a>
</p>

## Overview

NodeAkt is an actor runtime for Node.js, Bun, and Deno. An actor owns private state and a mailbox. The runtime delivers one message at a time, so that state needs no lock. Actors talk only by sending messages.

Requires Node.js 22+, Bun 1.3+, or Deno 2+. ESM only (`import`, not `require`). Every runtime is exercised in CI, multi-core placement included.

## Features

- **Actor system.** One logical runtime per process that owns every actor's lifecycle, from startup through graceful shutdown.
- **Typed messages.** Messages are plain classes, so a handler narrows them with ordinary type checks. Send and forget when no answer is needed, or ask and await a typed reply.
- **Hierarchy.** Actors spawn children and watch any other actor, receiving a message when the watched one stops. Stopping a parent drains its whole subtree cleanly.
- **Behaviors and stash.** An actor can swap its message handler at runtime, set aside messages it cannot serve yet, and replay them once it switches back.
- **Supervision.** When an actor fails, its parent decides: stop it, resume it, restart it, or escalate, for the one child or for all of them, with restart budgets and exponential backoff.
- **Routers.** Spread work over a pool of identical actors or broadcast to all of them: round robin, random, fan-out, and consistent-hash routing. The router supervises its routees, the pool resizes in place, and replies go straight back to the original sender.
- **Mailboxes.** Unbounded and bounded FIFO, segmented, fair per-sender, and priority variants, or bring your own implementation.
- **Passivation.** Actors live as long as you need them: keep them forever, or retire them automatically after an idle timeout or a processed-message count.
- **Reentrancy.** An actor can keep working through its mailbox while one of its own requests is still in flight, with control over which messages may overtake the pending reply.
- **Pipe to.** The result of any promise can be delivered to an actor's mailbox as an ordinary message once it settles; failures and timeouts become dead letters instead of crashing the actor.
- **Scheduling.** Send a message to an actor after a delay or on a repeating interval, and cancel, pause, or resume it by reference. A schedule created inside an actor is cancelled automatically when that actor stops.
- **Event stream.** Subscribe to what the runtime observes: dead letters and actor lifecycle events such as started, stopped, restarted, and passivated.
- **Logging.** Structured JSON logging out of the box, or silence the runtime entirely.
- **Multi-core.** Spawn actors across every machine core without touching workers or threads yourself. An actor's address works the same locally and across cores, on Node.js, Bun, and Deno alike.

The full API is in [Documentation](#documentation). What is not built yet is under [Not there yet](#not-there-yet).

## Install

```bash
npm  install @tochemey/nodeakt
pnpm add @tochemey/nodeakt
yarn add @tochemey/nodeakt
bun  add @tochemey/nodeakt
deno add npm:@tochemey/nodeakt
```

Then head to [Getting started](https://tochemey.github.io/nodeakt/guide/) for a first actor.

## Documentation

- [Documentation](https://tochemey.github.io/nodeakt/): getting started, the tour, and the full reference (source pages in [docs/](docs/))
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
- **Cron schedules.** `schedule` and `scheduleOnce` cover delayed and repeating sends; cron-expression schedules do not exist yet.
- **Metrics.** Structured logging exists. Metrics and tracing hooks do not.

If one of these matters to you, open an issue describing the use case. It helps order the roadmap.
