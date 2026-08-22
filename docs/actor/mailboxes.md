# Mailboxes

Every actor owns a mailbox and drains it one message at a time, which is how single-threaded processing per actor works. A mailbox is never shared between actors.

Pass a mailbox in `SpawnOptions`. The default is `new UnboundedMailbox()`. Mailboxes are live objects: they cannot ride a [`Props`](../multi-core/index.md) spawn.

```ts
await system.spawn("worker", new Worker(), {
  mailbox: new BoundedMailbox(128),
});
```

`enqueue` returns `null` on accept, or an `Error` on reject. Compare sentinels by identity: `err === ErrMailboxFull`. A rejected **user** message is published as a [dead letter](../actor-system/events.md). After `dispose` (the actor has stopped), `enqueue` returns `ErrMailboxDisposed` and `dequeue` returns `undefined`.

You can implement `Mailbox` yourself. Document any departure from FIFO.

## FIFO

| Class | Bound | Notes |
| --- | --- | --- |
| `UnboundedMailbox` | No | Default. Growable ring. Idle mailboxes at or below 64 slots release the buffer. |
| `BoundedMailbox` | Yes | Fixed ring. Constructor `new BoundedMailbox(capacity)` throws `RangeError` unless `capacity` is a positive integer. Full → `ErrMailboxFull` (non-blocking; the message is not stored). |
| `UnboundedSegmentedMailbox` | No | Linked segments of 256 messages. Amortized O(1), no per-message allocation. A drained mailbox hands memory back. Use for high-throughput, bursty fan-in. |

## Fair

`UnboundedFairMailbox` gives each sender its own sub-queue. Messages from the same sender stay FIFO; the consumer round-robins one message per sender per turn, so a chatty sender cannot starve quieter peers.

```ts
new UnboundedFairMailbox(); // key = sender path; no-sender messages share one queue
new UnboundedFairMailbox((msg) => msg.sender?.name() ?? "");
```

`SenderKeyFunc` is `(msg: ReceiveContext) => string`. A sender's sub-queue is created on first use and dropped when it drains.

When raw throughput of a single hot sender matters more than fairness, use `UnboundedMailbox`.

## Priority

`PriorityFunc` is `(msg1, msg2) => boolean`: return `true` when `msg1` should run before `msg2`. The function receives the **message payloads** (`ctx.message`), not the contexts.

| Class | Bound | Equal-priority order |
| --- | --- | --- |
| `UnboundedPriorityMailbox` | No | Unspecified |
| `UnboundedStablePriorityMailbox` | No | FIFO (arrival sequence) |
| `BoundedPriorityMailbox` | Yes | Unspecified |
| `BoundedStablePriorityMailbox` | Yes | FIFO |

```ts
new UnboundedStablePriorityMailbox((a, b) => (a as Job).priority > (b as Job).priority);
new BoundedPriorityMailbox(64, (a, b) => /* ... */);
```

Bounded constructors throw `RangeError` unless `capacity` is a positive integer. Full → `ErrMailboxFull`. `enqueue` / `dequeue` are O(log n).

Stable variants stamp each message with a monotonic sequence and are slightly slower (up to two priority calls per comparison).
