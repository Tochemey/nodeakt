# Tour

The programs in [`examples/`](https://github.com/Tochemey/nodeakt/tree/main/examples) are the tour of nodeakt. Each one is small, self-contained, and exists to show one thing the runtime does. Read them in the order below; later examples assume the earlier ones.

They run under [tsx](https://tsx.is) and import the library from `src/`, so there is no build step. From a clone of the repository:

```bash
make               # list the examples
make helloworld    # run one
```

Or without make:

```bash
pnpm example examples/helloworld/main.ts
```

## Actors own their state: `helloworld`

An actor processes messages one at a time. Its state is private and is only ever mutated inside `receive`, so it needs no lock. `tell` is fire and forget; `ask` waits for `ctx.response`.

[helloworld/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/helloworld/main.ts), `make helloworld`

```text
Hello, Ada!
Hello, Alan!
Greeted 2 people.
```

## An actor is a state machine: `behaviors`

`become` swaps the message handler at runtime. State lives in which behavior is active, not in a pile of flags. `unBecome` reverts to the default `receive`.

[behaviors/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/behaviors/main.ts), `make behaviors`

```text
🔴 red    -> 🟢 green
🟢 green  -> 🟡 yellow
🟡 yellow -> 🔴 red
…
```

## Defer work until ready: `stash`

Requests that arrive before an actor has finished initializing are `stash`ed. When it is ready, `become` plus `unstashAll` replays them in arrival order to the new behavior. No lock, no busy wait.

[stash/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/stash/main.ts), `make stash`

```text
  stashing "a" (not ready yet)
  stashing "b" (not ready yet)
initialized; switching to serving and replaying the backlog
  processing "a"
  processing "b"
```

## Death is a message: `watch`

`watch` registers interest in another actor's stop. The runtime delivers `Terminated` as an ordinary message, handled on the watcher's own turn. No polling, no shared flag, no callback from another thread.

[watch/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/watch/main.ts), `make watch`

```text
sentinel is watching "worker"
sentinel saw "nodeakt://watch@…/user/worker" terminate
```

## Many actors, no shared memory: `chat`

Actors collaborate only by sending messages. A room fans posts out with `tell`, learns who joined from `ctx.sender`, and `watch`es each member so a stop drops them from the room automatically.

[chat/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/chat/main.ts), `make chat`

```text
* alice joined (1 present)
* bob joined (2 present)
  [alice] <alice> hello room
  [bob] <alice> hello room
  [alice] <bob> hi alice
  [bob] <bob> hi alice
* bob left (1 present)
  [alice] <alice> anyone still here?
```

## Ask without freezing: `reentrancy`

`ctx.ask` parks the actor until the reply lands. `ctx.request` does not: it returns a handle immediately, and `onReply` runs later on this actor's own turn, serialized with its other messages. The actor keeps serving while requests are in flight.

[reentrancy/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/reentrancy/main.ts), `make reentrancy`

```text
status while computing: 2 in flight
  alpha = 50 (1 still in flight)
  bravo = 50 (0 still in flight)
```

## Let it crash: `supervision`

A failure suspends the actor and asks its parent what to do. A `Supervisor` maps the error to `restart` with backoff: the runtime re-runs `preStart` (fresh state) and keeps the jobs that were already queued. Recovery is message-driven; nothing spins or holds a lock.

[supervision/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/supervision/main.ts), `make supervision`

```text
worker started with fresh state
  did job 1 (completed 1 since last start)
  did job 2 (completed 2 since last start)
worker started with fresh state
  did job 3 (completed 1 since last start)
  did job 4 (completed 2 since last start)
```

Job 13 throws. The counter resetting to 1 is the restart, not a patch.

## A small IoT system: `iot`

Everything so far, composed, in the shape of the classic Akka IoT tutorial. A device manager routes by group id and builds the hierarchy on demand: one group per home, one actor per sensor, each `watch`ed by its group. Reading the whole home is a short-lived actor per query: it fans `ReadTemperature` out with `tell`, watches every device so a death becomes an answer, and settles whatever is left when a deadline it `scheduleOnce`d to itself fires.

[iot/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/iot/main.ts), `make iot`

```text
registered nodeakt://iot@…/device-manager/group-home/device-kitchen
…
group home tracks: kitchen, bedroom, garage, attic

-- query 1: every sensor answers --
query #1 answered:
  kitchen  22.5°C
  bedroom  no reading yet
  garage   18.0°C
  attic    no reading yet

-- query 2: a jammed sensor and a dying one --
group home dropped garage (3 still tracked)
query #2 answered:
  kitchen  22.5°C
  bedroom  19.2°C
  garage   device stopped
  attic    no answer before the deadline
```

The second answer is the point: a reading, a sensor with nothing recorded, a death observed mid-query, and a deadline all come back as one uniform reply.

## Construction is data: `props`

`spawn` takes a live instance (always this core) or a `Props`, which captures the class and its constructor arguments as data. From that data the runtime can build the actor wherever it places it. The arguments are checked by the compiler against the constructor.

[props/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/props/main.ts), `make props`

```text
Bonjour, Ada!
Hola, Alan!
Hello, Grace!
```

This run forces `NODEAKT_PARALLELISM=1` so placement stays local. The spawn call is the same as in `multicore`.

## Every core, invisibly: `multicore`

Start a system, spawn one CPU-bound actor per core with `Props`, and `ask` them all. There is no worker, pool, or isolate wiring in the program. Messages that cross cores are registered classes, so `instanceof` still works on the far side.

[multicore/main.ts](https://github.com/Tochemey/nodeakt/blob/main/examples/multicore/main.ts), `make multicore`

```text
spawned 8 counters, each on its own core
  counter-0: 78498 primes below 1000000
  …
parallel (all 8 cores at once):  1200 ms
serial   (one core at a time):   6800 ms
speedup: 5.7x
```

Core count, timings, and speedup depend on the machine. Hybrid performance/efficiency cores will not scale linearly; that is honest.

## Across machines: `remoting`

The capstone: a checkout node and a payments node as two Docker Compose services, each one actor system. The checkout desk resolves the payments actor with `remoteLookup`, charges through it with an `ask` piped back to its own mailbox, and `watch`es it, so the payments actor stopping, or its whole node dying, arrives as the same `Terminated` message. The desk queues orders through the outage and flushes them when the node returns.

[remoting/README.md](https://github.com/Tochemey/nodeakt/blob/main/examples/remoting/README.md), `make remoting`

```text
checkout-1  | [checkout] order ord-0004: barista course (£899.00)
payments-1  | [payments] DECLINED ord-0004: over the charge limit
checkout-1  | [checkout] payments is GONE; queueing orders and re-resolving
checkout-1  | [checkout] ord-0016 queued; 1 order(s) waiting
checkout-1  | [checkout] payments connected: nodeakt://payments@172.19.0.2:5100/payments
checkout-1  | [checkout] flushing 3 queued order(s)
```

This one needs Docker; the example's README also shows a two-terminal run with plain `tsx`. Kill the payments service mid-run and bring it back: the failover is the demo.
