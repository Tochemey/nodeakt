# Remoting

One machine runs one logical actor system. Remoting connects those systems: a node opens a network endpoint, and actors on other machines become ordinary `PID`s. `tell`, `ask`, `request`, `watch`, `forward`, and `pipeTo` keep their call sites and their semantics; the address decides whether a message crosses the wire.

Messages travel over TCP in a compact binary encoding, on long-lived connections the runtime dials lazily and reuses; [TLS](tls.md) encrypts them.

Example: [`examples/remoting`](https://github.com/Tochemey/nodeakt/blob/main/examples/remoting/README.md), a checkout node and a payments node under Docker Compose, with a failover drill.

> [!WARNING]
> The trust model is a **private network whose nodes trust each other**. Without the `tls` option traffic is plaintext; with it, connections are encrypted and peers verified, but a sender's identity inside an envelope is still self-declared and nothing authorizes what a peer may do. Do not expose a remoting port to an untrusted network.

Encrypt connections with [TLS](tls.md). Enabling it hinders performance compared with plaintext: encryption is paid on every byte and a certificate handshake on every new connection.

## Enable it

Remoting is a construction-time option, `RemoteOptions`. A system created without it stays single-node and pays nothing for the transport.

```ts
const system = new ActorSystem("orders", {
  remote: { host: "10.0.0.5", port: 5100 },
});
await system.start();
```

- `host` is the concrete address the node binds **and advertises**: every local actor's path becomes `nodeakt://orders@10.0.0.5:5100/name`, which is what other nodes use to reach it and reply to it.
- `port` is the port to bind. `0` lets the operating system choose; read the bound port afterwards with `system.port()`. The advertised endpoint always carries the real bound port.
- `system.host()` and `system.port()` report the node's address either way.

A bind failure rejects `start()`, so a node whose endpoint cannot open fails to start rather than starting deaf. `stop()` closes the endpoint and every connection; in-flight remote asks fail cleanly.

Every remote method below rejects with `ErrRemotingDisabled` on a system created without a `remote` configuration.

## Reach a remote actor

`remoteLookup` resolves a top-level name on another node to a `PID`:

```ts
const charger = await system.remoteLookup("10.0.0.9", 5100, "charger");
if (charger !== undefined) {
  system.noSender().tell(charger, new Charge());  // crosses the wire like a local tell
  const state = await system.noSender().ask(charger, new Status(), 1_000);
}
```

| Call | Where | Effect |
| --- | --- | --- |
| `remoteLookup(host, port, name)` | `ActorSystem`, `ReceiveContext` | The named top-level actor's `PID`, or `undefined` when no running actor holds the name there. |
| `remoteSpawn(host, port, name, props, opts?)` | `ActorSystem` | Spawns on the remote node from a registered class; resolves to the new actor's `PID`. |
| `remoteReSpawn(host, port, name)` | `ActorSystem`, `ReceiveContext` | Restarts the named remote actor in place: same path, same incarnation, fresh state. |
| `remoteStop(host, port, name)` | `ActorSystem`, `ReceiveContext` | Stops the named remote actor gracefully. A name nobody holds is already stopped, so the call succeeds idempotently. |

Everything else needs no dedicated method: the `PID` a lookup or spawn returns carries the route, so `tell`, `ask`, `request`, `watch`, `unWatch`, `forward`, and `pipeTo` work on it directly. Replying to `ctx.sender` routes back to the sender's node, wherever that is.

## Messages across nodes

Class instances need the same registration that [multi-core](../multi-core/index.md#messages-that-cross-isolates) messaging uses: call `registerMessage` at module scope on both nodes, and `instanceof` narrowing survives the hop. An unregistered class instance is refused on the sending side: `tell` returns the error and dead-letters, `ask` rejects with it.

Two differences from the same-machine boundary:

- The payload crosses in a binary value encoding, not a structured clone. Primitives, plain objects, arrays, `Date`s, `Map`s, `Set`s, and typed arrays all travel; binary data is copied, never transferred.
- The runtime's sentinel errors cross by identity: an ask that fails remotely with `ErrDead` rejects locally with the very same `ErrDead`.

## What a remote PID does

The contract of a [routed handle](../multi-core/index.md#what-a-remote-handle-does) carries over, with the network's own edges:

- **`tell`** returns `null` when the transport accepted the envelope, not when the far mailbox did. An envelope nothing can receive, an unknown name, a full mailbox, a target that has since stopped, becomes a [dead letter](../actor-system/events.md) on the node that discovered it.
- **`ask` / `request`**: the reply crosses back, and a failure settles the call with the decoded reason, sentinel identity preserved. Timeouts reject with `ErrRequestTimeout`.
- **Incarnation pinning**: a looked-up `PID` addresses the incarnation it resolved. When that actor stops and a new one takes the name, sends through the stale handle dead-letter on the receiving node; look the name up again.
- **`isRunning()`** is always `false`; liveness across nodes is not synchronously knowable. [Watch](#death-watch-across-nodes) it instead.
- **`shutdown()`** rejects; stopping a remote actor is `remoteStop`.
- **`forward`** preserves the original sender across any number of hops: the receiving actor sees `ctx.sender` as the origin and can reply straight to it, even from a third node.
- **`pipeTo`** delivers a task's result to the remote target through the same route; an undeliverable result becomes a dead letter on the side that discovered it.

## Remote spawn

`remoteSpawn` constructs the actor **on the remote node**, so construction crosses by name, exactly as [`Props` placement](../multi-core/index.md#place-an-actor) does across isolates:

```ts
// Registered on both nodes, at module scope in the class's module.
registerActor(Charger);

const charger = await system.remoteSpawn("10.0.0.9", 5100, "charger", Props.create(Charger, "fast"));
```

- The class must be registered under the same name on both nodes: here to validate the `Props`, there to construct. An unregistered class rejects with `ActorNotRegisteredError`.
- Constructor arguments cross the wire codec, and the `reentrancy` spawn option travels; live-object options (`mailbox`, `supervisor`, `passivationStrategy`) are refused, as in every `Props` spawn.
- A spawn failure on the far node (the name already held, `preStart` throwing) settles the returned promise with that failure.

`remoteReSpawn` and `remoteStop` manage the actor afterwards by name. An actor the remote node placed on one of its worker isolates cannot be respawned or stopped remotely yet; those calls reject with an explanatory error.

## Death watch across nodes

`watch` and `unWatch` work on a remote `PID` with the [death watch](../actor/death-watch.md) semantics:

```ts
watcherPid.watch(charger);   // Terminated when it stops, or when its node dies
```

- A graceful stop on the far node delivers one `Terminated` to each remote watcher.
- **Node death and connection loss are indistinguishable by design.** When the connection to a node dies, every actor watched over it is reported terminated. A watch that cannot reach its node at all settles the same way: as the death it can no longer observe.
- Watching a remote actor that is already gone answers with an immediate `Terminated`: once a watch crossed the boundary, the watcher is always eventually notified of a death that is or becomes true.
- `unWatch` cancels on both sides; a `Terminated` no watch is waiting for is dropped, so a late or duplicate notification never reaches the actor.

## Failures

The failure taxonomy is uniform with the local one: synchronous refusals return, asynchronous ones reject, undeliverable messages dead-letter.

| Failure | When |
| --- | --- |
| `ErrRemotingDisabled` | Any remote method on a system without a `remote` configuration. |
| Dial failure | The remote endpoint is unreachable; the pending call rejects with the connection error. |
| `ErrRequestTimeout` | A remote `ask` or `request` unanswered in time. |
| `ErrDead` | The target is gone on the receiving node: unknown name, stopped actor, or stale incarnation. |
| `TypeNotRegisteredError` | An unregistered message class, refused on the sending side. |
| Decoded remote failure | The receiving side failed the ask (a throwing handler, an unencodable reply); the sender settles with that reason, sentinels by identity. |
| Node death mid-flight | Every pending ask to that node rejects; tells discovered undeliverable become dead letters. |

## Limits

- Inbound remote messages reach actors on the receiving node's **main isolate**. Combining remoting with worker placement on the same node works for outbound traffic, but a worker-placed actor is not yet reachable from other nodes, and cannot be remotely respawned or stopped.
- No authorization: [TLS](tls.md) encrypts and verifies certificates, but nothing checks what a verified peer may do, and a sender's identity inside an envelope is self-declared. Trusted networks only, as above.
- Clustering (discovery, membership, sharding) does not exist yet; remoting is point to point, addressed by `host:port`.
