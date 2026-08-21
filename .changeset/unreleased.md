---
"nodeakt": minor
---

Initial implementation of nodeakt, a high-performance actor library for Node.js.

**Actor model core.** An `ActorSystem` hosts the actor tree, one logical system per machine. Actors implement `preStart`, `receive`, and `postStop`; every actor is addressed through its `PID` under a `nodeakt://system@host:port/name` path, receives a `PostStart` announcement as its first message, and processes messages strictly one at a time, awaiting asynchronous behaviors before dequeuing the next message. Top-level actors are created with `system.spawn` and resolved with `system.actorOf`; a guardian hierarchy supervises the runtime's own actors separately from user actors.

**Messaging.** `tell` is fire and forget and returns `Error | null` on the synchronous hot path. `ask` delivers a message and returns a promise settled by the receiver's `ctx.response(value)`, with a per-call timeout. Messages sent from outside an actor are attributed to the system's `NoSender` actor, so `ctx.sender` is always comparable by identity. `ReceiveContext` also carries `forward`, child management, and death watch.

**Behaviors and stashing.** `become` / `becomeStacked` / `unBecome` / `unBecomeStacked` switch an actor's message handler at runtime; `stash` / `unstash` / `unstashAll` buffer messages for re-delivery in arrival order.

**Hierarchy and lifecycle.** Actors spawn children with `spawnChild`; children stop with their parent. Graceful `shutdown` drains the accepted backlog, runs `postStop`, stops children first, and notifies watchers with `Terminated`. `restart` re-initializes an actor through its lifecycle hooks while keeping its queued messages; `reinstate` lifts a suspension in place. `PoisonPill` requests a graceful stop through the mailbox.

**Supervision.** A failing behavior suspends the actor and asks its parent for a decision. `Supervisor` maps error classes to directives (`stop`, `resume`, `restart`, `escalate`) under a `oneForOne` or `oneForAll` strategy, with retry budgets, exponential backoff, and a reset window. Escalation surfaces a `PanicSignal` to the grandparent's behavior; unsupervised guardians stop the system.

**Passivation.** Idle actors can be stopped automatically: `TimeBasedStrategy` schedules all participants on one shared unreferenced timer through a min-heap of deadlines, `MessagesCountBasedStrategy` stops an actor after a message budget, and the default `LongLivedStrategy` never passivates.

**Mailboxes.** The default `UnboundedMailbox` is a growable ring buffer that releases small buffers when an actor goes idle after start-up. `BoundedMailbox` rejects at capacity with `ErrMailboxFull`. `UnboundedSegmentedMailbox` stores the queue in linked fixed-size segments. `UnboundedFairMailbox` round-robins across per-sender sub-queues. Four priority mailboxes (`bounded` / `unbounded`, stable / unstable) order messages with a caller-supplied priority function, the stable variants preserving FIFO order among equal priorities.

**Reentrancy.** Actors spawned with a reentrancy configuration issue non-parking requests with `ctx.request(to, message, options)`: the call returns a `RequestCall` handle immediately, and the continuation registered with `onReply` runs on the requesting actor's own turn once the reply arrives, serialized with its message processing. `allowAll` keeps processing every message while replies are in flight, which also makes self-requests and call cycles (A requests B, B requests A) deadlock-free; `stashNonReentrant` stashes user messages until the last stash-mode request completes, preserving strict ordering while replies and runtime messages keep flowing. Requests support per-call timeout and mode overrides, cancellation with `cancel()`, and a per-actor `maxInFlight` cap; admission and delivery failures complete the handle with typed sentinels (`ErrReentrancyDisabled`, `ErrInvalidReentrancyMode`, `ErrReentrancyInFlightLimit`, `ErrRequestCanceled`). The handle is deliberately not awaitable: awaiting would park the behavior and resume outside the actor's turn, which is what a request avoids.

**Logging.** A structured `Logger` interface with `debug` / `info` / `warn` / `error` levels, per-entry fields, lazily computed payloads that are never built when the level is disabled, and `with(fields)` child loggers that bind stable context. The default `JsonLogger` writes one JSON line per entry to standard error at info level, serializing errors as name, message, and stack and surviving unserializable payloads; `discardLogger` silences the runtime entirely. Configure per system with `new ActorSystem(name, { logger })`; the runtime reports through it, and lifecycle hooks reach it via `ctx.logger()`.

**Errors.** Failures are typed sentinels (`ErrDead`, `ErrMailboxFull`, `ErrRequestTimeout`, `ErrActorAlreadyExists`, and friends) compared by identity; asynchronous APIs reject with them, the synchronous send path returns them.

**Performance.** Measured on a single event loop with the bundled benchmark suite: 23M+ tells/sec for a single producer and consumer pair, 24M+ across independent pairs, at 50 bytes allocated per message; 5.6M ask round trips/sec sequential, 7.2M with 10,000 in flight, 8.2M with concurrent askers; idle actors occupy under 512 bytes of heap each, about 0.4 GB per million actors. The receive loop stays fully synchronous while behaviors do, message processing yields to the event loop every 2048 messages, mailbox rings survive between bursts, and pending asks share one unreferenced timer per timeout duration instead of a timer object per call.

Two contracts worth knowing:

- The ask `timeout` is a lower bound with coarse expiry: an unanswered ask is rejected between one and two timeout periods after it was sent. Answered asks are unaffected.
- Receive contexts are managed by the runtime and may be reused once `receive` has fully settled. Call `response` before the behavior finishes and do not retain a context beyond the current message; holding a context to answer it during a later message remains supported while the ask is unanswered.
