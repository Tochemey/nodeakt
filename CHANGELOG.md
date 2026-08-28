# @tochemey/nodeakt

## 0.2.0

### Features

- **Clustering.** Turn a set of single-node systems into one cluster: actors spawn, are addressed, and are messaged by name across nodes, without a caller knowing which machine an actor runs on. Enable it with `{ cluster: { discovery } }`; `remote` is required on the same node (`ErrClusterRequiresRemote`), and a wildcard remoting host that peers cannot dial back is refused (`ErrClusterRequiresRoutableHost`). A system without a `cluster` option keeps its exact single-node behavior. Built on gossip-based membership, a distributed name registry, and the existing remoting transport.

  **Discovery.** How a node finds its seed peers at boot, consulted once and never again. `DnsDiscovery` resolves a hostname over `SRV`, `A`, or `AAAA` records, ideal for a headless service; `StaticDiscovery` takes a fixed `host:port` list; a custom `DiscoveryProvider` pulls seeds from any registry. All exported from the package.

  **Membership.** Every node keeps a live view of the cluster over a gossip protocol with failure detection: a member that misses its probes is suspected, corroborated, and confirmed dead, or revived if it answers again. The oldest live member is the coordinator, the deterministic single driver of cluster-wide work. `minimumMemberQuorum` enables the split-brain resolver so a minority half of a partition stops rather than minting duplicates.

  **Placement.** Top-level names are unique cluster-wide; a duplicate `spawn` is refused with `ErrActorAlreadyExists`. `spawn` keeps an actor local; `spawnOn(name, props, { strategy })` places it on the node a strategy chooses (`roundRobin` default, `random`, `local`, `leastLoad`) and returns a routed handle. The owning node performs the single name claim, so racing callers never double-write a record.

  **Singletons.** `spawnSingleton(name, props)` gives a name exactly one live instance cluster-wide, hosted on the coordinator and idempotent to create: a losing caller receives a handle to the existing instance rather than an error. At-most-one rests on the same name claim as every spawn, no lease or renewal clock, and depends on the split-brain resolver being on.

  **Location-transparent messaging.** `actorOf(name)` stays synchronous and non-blocking, returning a local or already-known routed handle, or `undefined`. `actorOfAsync(name)` awaits a single registry read to resolve a name owned anywhere in the cluster, the lookup application code uses to reach a cross-node actor on the first call. Routed handles keep every remoting contract: per-actor ordering, `ask`/`request` replies, cross-node `watch`, and re-resolution after a move.

  **Relocation and recovery.** When a node departs, gracefully via `stop()` or by crash, the coordinator recreates its relocatable actors on the survivors from their stored recipes, spread by a deterministic balanced fill. Recovery is idempotent and resumable: every recreate is a compare-and-set gated on the record still naming the dead node, a build that fails is retried rather than lost, and a periodic orphan sweep backstops the pass. A relocated actor is a fresh start that recovers its own state in `preStart`; messages in flight during a move are lost, by design. Relocation is on by default, opt out system-wide with `cluster.relocation: false` or per actor with `relocatable: false`.

  **Cluster events.** `subscribe` delivers `NodeJoined`, `NodeLeft`, `CoordinatorChanged`, `RebalanceStarted`, `RebalanceCompleted`, `RelocationStarted`, `RelocationCompleted`, and `RelocationFailed`, narrowed with `instanceof` like every other stream event.

  **Trust model.** As with remoting, a cluster is a private network whose nodes trust each other. Do not expose a gossip, data, or remoting port to an untrusted network.

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
