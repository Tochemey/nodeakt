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

nodeakt is an actor runtime for Node.js. An actor owns private state and a mailbox. The runtime delivers one message at a time, so that state needs no lock. Actors talk only by sending messages.

Requires Node.js 22 or newer. ESM only.

```bash
pnpm add nodeakt
```

```ts
import type { Actor, ReceiveContext } from "nodeakt";
import { ActorSystem, PostStart } from "nodeakt";

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

- [Documentation](docs/README.md): actor system, actors, multi-core
- [Examples](examples/README.md): small programs that match those pages
- [Benchmarks](benchmark/README.md): sustained tell and ask throughput, memory density, multi-core scaling
- [Contributing](CONTRIBUTING.md): setup, conventions, and what a change needs to merge

## Releases

nodeakt is pre-1.0: the API is settling and minor versions may still move it.

- **Nightly.** Every green push to `main` publishes a build to npm under the `nightly` dist-tag, versioned `X.Y.Z-nightly.<date>.<sha>`. Install it with `pnpm add nodeakt@nightly`. Nightlies never touch `latest`.
- **Stable.** A stable release is cut by pushing a version tag. The release pipeline folds the accumulated [changesets](https://github.com/changesets/changesets) into `CHANGELOG.md`, publishes to npm as `latest` with provenance, and creates the GitHub release from the changelog.

## Not there yet

nodeakt runs everything on one machine. Multi-core uses worker threads behind one logical actor system; no message ever crosses a network. The following do not exist yet:

- **Remoting and clustering.** No transport to another machine, no discovery, no cluster sharding. Remoting is planned as its own layer that reuses the existing envelope semantics and type registry with a wire codec of its own.
- **Persistence.** Actor state is in-memory only; there is no event sourcing or durable state.
- **Virtual actors (grains).** Actors are explicitly spawned and addressed; there is no on-demand activation model.
- **Scheduled messages.** No delayed or cron-style sends; use timers inside an actor.
- **Routers.** No pool or broadcast routers as public API.
- **Metrics.** Structured logging exists; metrics and tracing hooks do not.

If one of these matters to you, open an issue describing the use case; it helps order the roadmap.
