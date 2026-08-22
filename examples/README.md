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

| Example | Shows |
| --- | --- |
| [helloworld](helloworld/main.ts) | Actors own their state; `tell` and `ask` |
| [behaviors](behaviors/main.ts) | `become` / `unBecome`: an actor as a state machine |
| [stash](stash/main.ts) | Defer messages until ready, then replay them |
| [watch](watch/main.ts) | Death watch: `Terminated` as an ordinary message |
| [chat](chat/main.ts) | Many actors collaborating with no shared memory |
| [reentrancy](reentrancy/main.ts) | `ctx.request`: ask without freezing |
| [pipeto](pipeto/main.ts) | `ctx.pipeTo`: deliver an async result as a message |
| [supervision](supervision/main.ts) | Restart with backoff; let it crash |
| [props](props/main.ts) | Construction as data with `Props` |
| [multicore](multicore/main.ts) | One CPU-bound actor per core |
