# @tochemey/nodeakt

## 0.1.0

### Minor Changes

- [`f9b4e8a`](https://github.com/Tochemey/nodeakt/commit/f9b4e8a425be8c82debe2855198976afdee85ca3) Thanks [@Tochemey](https://github.com/Tochemey)! - First release of NodeAkt, a zero-dependency actor runtime for Node.js, Bun, and Deno. An actor owns private state and a mailbox; the runtime delivers one message at a time.
  
  **Actors.** Implement `preStart`, `receive`, and `postStop`. Spawn with `system.spawn` or `ctx.spawn`, look up with `system.actorOf`. Every actor has a `PID` and a `Path` (`nodeakt://system@host:port/name`) and receives `PostStart` as its first message.
  
  **Messaging.** `tell` is fire-and-forget (`Error | null` on the `PID` hot path). `ask` waits for `ctx.response(value)` and times out. `request` is the reentrant form: the actor keeps reading its mailbox while the reply is in flight. `forward` preserves the original sender. Messages sent from outside an actor are attributed to `NoSender`. `ctx.unhandled()` sends a message to dead letters without failing the actor.
  
  **pipeTo.** `pipeTo` / `pipeToName` (on `ReceiveContext` and `PID`) run a promise off the actor's turn and deliver the result as a mailbox message. Failures and timeouts become dead letters (`ErrPipeTimeout`, `ErrUndefinedTask`); they never fail the actor. Map a rejection to a message class if the actor should see the failure. Stopping the actor does not cancel an in-flight task; the task must not read or mutate actor state.
  
  **Scheduling.** `schedule` / `scheduleOnce` deliver after an interval or a delay; `cancelSchedule`, `pauseSchedule`, and `resumeSchedule` control a schedule by reference. A schedule created inside an actor is cancelled when that actor stops. Ticks use the normal send path, so a stopped target becomes a dead letter. Typed failures include `ErrActorSystemNotStarted`, `ErrInvalidInterval`, `ErrScheduleAlreadyExists`, `ErrScheduleNotFound`, and `ErrDead`.
  
  **Behaviors and stash.** `become` / `becomeStacked` / `unBecome` / `unBecomeStacked` swap the handler at runtime. `stash` / `unstash` / `unstashAll` hold messages and replay them in arrival order after a switch.
  
  **Hierarchy and watch.** Children (`spawnChild` / `ctx.spawn`) stop with their parent. `watch` / `unWatch` deliver `Terminated`. `shutdown` drains the mailbox, runs `postStop`, then stops children. `PoisonPill` is a graceful stop through the mailbox. `restart` re-runs lifecycle hooks and keeps queued messages; `reinstate` lifts a suspension in place.
  
  **Supervision.** A failing child asks its parent. `Supervisor` maps error classes to `stop`, `resume`, `restart`, or `escalate`, under `oneForOne` or `oneForAll`, with restart budgets and exponential backoff. Escalation surfaces a `PanicSignal`. Unsupervised failures stop the system.
  
  **Passivation.** `LongLivedStrategy` keeps an actor forever (the default). `TimeBasedStrategy` stops it after an idle timeout; `MessagesCountBasedStrategy` after a processed-message count.
  
  **Mailboxes.** Unbounded and bounded FIFO (`UnboundedMailbox`, `BoundedMailbox`), segmented, fair per-sender, and priority variants. Bounded mailboxes reject with `ErrMailboxFull`. Custom mailboxes are supported.
  
  **Reentrancy.** Spawn with a reentrancy configuration and issue `ctx.request(to, message, options)`. The call returns a `RequestCall` immediately; `onReply` runs on the actor's own turn. `allowAll` keeps processing every message while replies are in flight (self-requests and call cycles are deadlock-free); `stashNonReentrant` stashes user messages until the last stash-mode request completes. Per-call timeout and mode overrides, `cancel()`, and a `maxInFlight` cap. Failures complete the handle with `ErrReentrancyDisabled`, `ErrInvalidReentrancyMode`, `ErrReentrancyInFlightLimit`, or `ErrRequestCanceled`. The handle is not awaitable.
  
  **Routers.** `system.spawnRouter` runs a pool of identical routees: round-robin, random, fan-out, or consistent-hash. Replies go to the original sender. `GetRoutees` and `AdjustRouterPoolSize` inspect and resize the pool. Fan-out ask/request is refused (`ErrFanOutAsk`). A pool at zero routees dead-letters with `ErrDead`.
  
  **Dead letters and events.** Undeliverable messages become `Deadletter` events. Subscribe with `system.subscribe` / `unsubscribe`. Lifecycle events (started, stopped, restarted, passivated, and related) use the same stream.
  
  **Multi-core.** `Props.create(ActorClass, ...args)` lets the runtime place the actor on any isolate. `registerActor` / `registerMessage` at module scope. Same `PID` API across cores. Instance spawns stay on this isolate. The pool sizes to the machine; `NODEAKT_PARALLELISM` overrides it.
  
  **Remoting.** `{ remote: { host, port } }` binds a node. `remoteLookup`, `remoteSpawn`, `remoteReSpawn`, `remoteStop`, plus `tell` / `ask` / `request` / `watch` / `forward` / `pipeTo` on the returned `PID`. Optional TLS (`cert`, `key`, optional `ca`). Trust model: a private network whose nodes trust each other. Do not expose a remoting port to an untrusted network. Disabled remoting rejects with `ErrRemotingDisabled`.
  
  **Logging.** Structured `Logger` (`debug` / `info` / `warn` / `error`) with `with(fields)` child loggers. Default `JsonLogger` writes JSON lines to stderr; `discardLogger` silences the runtime. Configure with `{ logger }`; actors see `ctx.logger()`.
  
  **Errors.** Failures are typed sentinels compared by identity (`ErrDead`, `ErrMailboxFull`, `ErrRequestTimeout`, `ErrActorAlreadyExists`, and friends). Synchronous sends return them; async APIs reject with them.
  
  Two contracts:
  
  - The ask `timeout` is a lower bound: an unanswered ask is rejected between one and two timeout periods after it was sent. Answered asks are unaffected.
  - Receive contexts are owned by the runtime and may be reused once `receive` has settled. Call `response` before the behavior finishes; do not retain a context past the current message except to answer a still-open ask.
