# Examples

These programs are the tour of nodeakt. Each one is small, self-contained,
and exists to show one thing the runtime does. Read them in the order
below; later examples assume the earlier ones. The guided walk-through,
with expected output for every program, is the documentation's
[Tour](https://tochemey.github.io/nodeakt/guide/tour).

They run under [tsx](https://tsx.is) and import the library from `src/`,
so there is no build step.

## Run

From the repository root:

```bash
make               # list the examples
make helloworld    # run one
```

Or without make:

```bash
pnpm example examples/helloworld/main.ts
```

## Programs

| Example                              | Shows                                                                       |
|--------------------------------------|-----------------------------------------------------------------------------|
| [helloworld](helloworld/main.ts)     | Actors own their state; `tell` and `ask`                                    |
| [behaviors](behaviors/main.ts)       | `become` / `unBecome`: an actor as a state machine                          |
| [stash](stash/main.ts)               | Defer messages until ready, then replay them                                |
| [watch](watch/main.ts)               | Death watch: `Terminated` as an ordinary message                            |
| [chat](chat/main.ts)                 | Many actors collaborating with no shared memory                             |
| [reentrancy](reentrancy/main.ts)     | `ctx.request`: ask without freezing                                         |
| [pipeto](pipeto/main.ts)             | `ctx.pipeTo`: deliver an async result as a message                          |
| [scheduling](scheduling/main.ts)     | `schedule` / `scheduleOnce`: send a message later or on a repeat            |
| [supervision](supervision/main.ts)   | Restart with backoff; let it crash                                          |
| [iot](iot/main.ts)                   | A device hierarchy; every query is a short-lived actor                      |
| [props](props/main.ts)               | Construction as data with `Props`                                           |
| [multicore](multicore/main.ts)       | One CPU-bound actor per core                                                |
| [metrics](metrics/main.ts)           | The runtime reports on itself; scrape `collectMetrics` on a timer           |
| [remoting](remoting/README.md)       | Checkout and payments on two nodes: remoting over TCP with Docker Compose   |
| [dns-cluster](dns-cluster/README.md) | DNS-discovered three-node cluster with Docker Compose                       |
| [dns-actors](dns-actors/README.md)   | Distributed three-node actor systems over a DNS cluster with Docker Compose |
| [k8s](k8s/README.md)                 | The distributed-actor cluster on Kubernetes with kind, discovered over DNS  |
