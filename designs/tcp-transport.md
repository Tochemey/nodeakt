# The TCP transport design

This is the reliable, fast TCP layer that remoting rides on: the socket, the wire protocol, the serializer, and the connection engine. Remoting keeps the contracts the runtime already defines: every machine runs one logical actor system, a send whose path addresses another node travels over this transport to that node's system, and tell, ask, and watch mean there exactly what they mean locally. The payload codec is a custom binary serializer defined here, with zero external dependencies.

## Goals

- **Reliable.** Every failure has a defined outcome: an ask always settles, a tell that cannot be delivered becomes a dead letter with a reason, a dead or half-open connection is detected and torn down, and teardown fails every in-flight request exactly once. Malformed or hostile bytes can never crash the process or hang a connection; every decode path validates before it allocates.
- **Fast.** Small messages coalesce into few syscalls, large messages move without copies where the runtime allows it, framing is fixed-offset arithmetic on contiguous buffers, and repeated strings (actor paths, type names) shrink to varint references after first use. Backpressure is byte-based and end-to-end, so a slow receiver slows the sender instead of ballooning memory.
- **Dependency-free and freestanding.** The entire layer uses only the platform: `node:net`, `node:tls`, `DataView`, `TextEncoder`/`TextDecoder`. No wire-format library, no serialization library, no compression library, and no imports from the actor runtime either: the transport knows nothing about actors, registries, or mailboxes, and the actor system plugs into it from above.
- **Cross-runtime.** Works on Node 22+, Bun 1.3+, and Deno 2+ through their `node:net` compatibility layers, verified by the existing smoke harness (`test/smoke/run.sh`).

## Non-goals

Discovery, membership, and placement across nodes (a future cluster plan). HTTP or WebSocket compatibility. Browser support. Cross-version wire compatibility promises before the format is declared stable; until then the capability revision and version byte exist so incompatibility fails fast and cleanly.

## Where it lives

The transport is a freestanding package in `src/net/`, the one folder in an otherwise flat `src/`. It is self-contained by rule, not by taste: modules in `net/` import platform modules and each other, and nothing else. No envelope types, no message registry, no error sentinels, no scheduler; the transport deals in frames, opaque ref strings, serializer ids, payload bytes, numeric codes, and callbacks. The actor system plugs in through one seam module, `remoting.ts`, which lives outside the folder in flat `src/` and is the only module allowed to import from `net/`.

Dependencies therefore flow one way by construction and a circular import is impossible: `net/` cannot reach the runtime, and the runtime reaches `net/` only through the seam. A guard test enforces both rules by scanning import statements, so a violation fails CI instead of surviving as convention. A system that never enables remoting loads none of this and pays nothing.

| Module            | Holds                                                                                                                                                                                              |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `net/frame.ts`    | Frame header codec, frame type and flag constants, validation                                                                                                                                      |
| `net/values.ts`   | The binary value serializer (payloads)                                                                                                                                                             |
| `net/envelope.ts` | DATA, REPLY, ERROR, and HELLO body codecs; refs as opaque strings                                                                                                                                  |
| `net/chunk.ts`    | Chunk split and reassembly                                                                                                                                                                         |
| `net/table.ts`    | String-interning tables, TABLE body codec                                                                                                                                                          |
| `net/conn.ts`     | Framed connection: read parser, write coalescing, socket backpressure                                                                                                                              |
| `net/session.ts`  | Session engine: handshake, credits, liveness, pending table, dispatch                                                                                                                              |
| `net/server.ts`   | Listener lifecycle: bind, accept, drain, shutdown                                                                                                                                                  |
| `net/peer.ts`     | Outbound peer: lanes, dialing, reconnect, redelivery                                                                                                                                               |
| `net/timers.ts`   | The transport's timer facility: coarse clock, deadline scheduling                                                                                                                                  |
| `remoting.ts`     | The seam, outside the folder: maps envelopes both ways, restores message prototypes through the registry, translates sentinel indexes, records dead letters, turns session close into `Terminated` |

Where this document says a tell dead-letters or a watcher receives `Terminated`, that is the seam acting on an outcome the transport reports; inside `net/` those are callbacks and typed rejections, nothing more.

Tests mirror the modules in `test/`, one file per module, plus protocol-level fault-injection tests against real sockets on port 0.

## Shape of the layer

One wire protocol, no variants. A connection is a long-lived, full-duplex byte stream carrying self-describing frames in both directions. There is no request-per-connection mode and no protocol sniffing: the first bytes a dialer sends are a HELLO frame, and anything else closes the connection.

```
socket (node:net / node:tls)
  └─ framed connection        frame parser in, write coalescing out
       └─ session             handshake, credits, liveness, correlation, chunking, tables
            └─ remoting seam  envelopes in and out of the actor system
```

Concurrency inside one connection comes from correlation ids, not streams: many asks are in flight at once, each REPLY or ERROR carrying the correlation of the request it settles. Parallelism across the wire comes from **lanes**: a peer may hold several connections to the same remote system, one per lane, so a large transfer on the large lane never delays small messages on an ordinary lane. Per-destination ordering holds within a lane because a lane is one TCP stream written by one flush loop.

The socket endpoint lives on the main isolate, where the control plane already runs. An inbound envelope addressed to an actor on a worker isolate hops across the existing in-process transport; an outbound remote send from a worker hops to the main isolate first, then out the socket. That keeps one connection set per machine, which is what one logical actor system per machine implies. The session engine takes a duplex byte stream and callbacks, not a socket, so it can move to a dedicated isolate later without a protocol change if the main loop ever saturates.

## The wire protocol

### Frame header

Every frame is a fixed 16-byte header followed by a body of `length` bytes.

```
byte  0        1        2        3        4..7           8..15
      +--------+--------+--------+--------+---------------+--------------------+
      | version| type   | flags  | lane   | length (BE32) | correlation (BE64) |
      +--------+--------+--------+--------+---------------+--------------------+
```

- **version** is `0x01`. Any other value is a protocol error: the receiver answers with a connection-scoped ERROR naming the mismatch, then closes.
- **type** is one of the frame types below; unknown types are a protocol error.
- **flags** is a bitset: bit 0 `expectsReply`, bit 1 `firstChunk`, bit 2 `lastChunk`; bits 3 to 7 are reserved and must be zero on the wire, and the decoder rejects them so they stay available.
- **lane** is the connection's lane byte (below); a frame whose lane byte does not match the connection's negotiated lane is a protocol error.
- **length** bounds are validated before the body is read; a length above the negotiated `maxFrameSize` is a protocol error, and the floor on `maxFrameSize` means a peer can never advertise a limit of zero to disable the check.
- **correlation** links an ask to the REPLY or ERROR that settles it and names a chunk group. Zero means none. It must be nonzero on REPLY, on CHUNK, and on DATA with `expectsReply`; a connection-scoped ERROR uses zero. On the wire it is eight bytes; in the runtime it is an ordinary number, allocated monotonically from 1 per connection, which stays below 2^53 for longer than any connection lives. Each side allocates from its own counter with opposite parities, odd for the dialer and even for the acceptor, and replies echo; the parity split keeps chunk-group keys from the two directions disjoint, since a connection's reassembler keys groups by correlation alone.

Frame types:

| Type      | Value | Body                                                     |
|-----------|-------|----------------------------------------------------------|
| HELLO     | 0x01  | handshake parameters (dialer to acceptor)                |
| HELLO_ACK | 0x02  | negotiated parameters (acceptor to dialer)               |
| DATA      | 0x03  | a data envelope: tell, ask, watch, or unwatch            |
| REPLY     | 0x04  | a reply envelope settling an ask                         |
| ERROR     | 0x05  | an error body; request-scoped or connection-scoped       |
| CHUNK     | 0x06  | one fragment of a logical frame                          |
| CREDIT    | 0x07  | a flow-control grant, uvarint byte count                 |
| TABLE     | 0x08  | a string-table registration                              |
| PING      | 0x09  | liveness probe, empty body, correlated                   |
| PONG      | 0x0A  | liveness answer, empty body, echoes the PING correlation |

Lane bytes: `0x00` control, `0x01`..`0xFE` ordinary lanes (byte is index plus one, so up to 254 ordinary lanes), `0xFF` large. The dialer picks the lane when it dials; the acceptor echoes it in HELLO_ACK, and the dialer adopts the echoed identity.

Integers inside frame bodies are unsigned LEB128 varints ("uvarint") unless a layout says otherwise; strings are a uvarint byte length followed by UTF-8 bytes. The two header integers are fixed-width big-endian on purpose: fixed offsets keep the parser branch-free.

### Handshake and negotiation

The dialer's first frame is HELLO; the acceptor answers HELLO_ACK or a connection-scoped ERROR and closes. Both HELLO bodies share one layout, encoded with the field codecs above in fixed order:

```
revision      uvarint    highest capability revision the sender supports
systemName    string     the actor system's name
host          string     the sender's advertised host
port          uvarint    the sender's advertised port
lane          1 byte     same encoding as the header lane byte
compression   1 byte     0 none; other values reserved
maxFrameSize  uvarint    largest frame body the sender will accept
maxMessageSize uvarint   largest reassembled logical frame the sender will accept
initialCredits uvarint   the sender's receive window in bytes
maxLargeTransfers uvarint concurrent chunk groups the sender will reassemble
```

A decoder reads the fields it knows and ignores trailing bytes, so a newer peer can append fields without breaking an older one; the revision governs whether the newer peer may then use them.

Negotiation is pairwise minimum on every numeric field and on the revision, with every size floored so an advertisement cannot produce an unusable effective set: `maxFrameSize` at 16 KiB, `maxMessageSize` at the effective `maxFrameSize`, and `maxLargeTransfers` at one. Compression is agreed only when both sides configured the same codec, otherwise none; the field is carried from day one so adding a codec later (via `node:zlib`, still dependency-free) is a negotiation change, not a format change. The handshake is bounded by a 10-second deadline on both sides, so a peer that connects and never speaks cannot pin resources.

Capability revisions gate features so the two sides always agree on what may appear on the wire:

| Revision | Enables                                         |
|----------|-------------------------------------------------|
| 1        | DATA, REPLY, ERROR, PING, PONG                  |
| 2        | CHUNK: logical messages larger than a frame     |
| 3        | TABLE: interned path and type refs in envelopes |
| 4        | CREDIT: receiver-granted send windows           |

The first shipped version implements all four and advertises revision 4; the ladder exists so the protocol can grow and so a build can be tested at a lower revision.

### Envelopes

The DATA body carries the same fields the in-process `Envelope` carries, minus what the frame header already holds (`cid` rides as the header correlation) and minus `senderWorkerId`, which is a same-machine concept that does not cross the network.

```
kind        1 byte     0 tell, 1 ask, 2 watch, 3 unwatch
toRef       ref        target actor path
uid         string     target incarnation, empty addresses whoever lives at the path
senderRef   ref        sender path, empty inline ref when the send carried none
senderUid   string     sender incarnation
timeout     uvarint    remaining milliseconds of the ask budget, 0 for none
serializerId 1 byte    0 is the binary value codec; 255 reserved for custom
typeRef     ref        registered message type id, empty inline ref for passthrough
payload     bytes      value-codec encoding of the message, to end of body
```

Watches and unwatches carry an empty payload and empty type ref, exactly as they do in process. The timeout travels as remaining time, never as an absolute deadline, so enforcement never depends on clocks agreeing across machines: the receiver re-derives its own deadline on arrival.

The REPLY body is `serializerId`, `typeRef`, `payload`. A failed reply does not use REPLY at all: it travels as a request-scoped ERROR frame echoing the ask's correlation.

A **ref** is either a nonzero uvarint naming an entry in the connection's string table, or a zero byte followed by an inline string (uvarint length plus bytes). An empty inline ref costs two bytes. Actor paths go through the path table, type ids through the type table; incarnation uids stay inline.

### Errors on the wire

The ERROR body:

```
code      1 byte    1 protocol, 2 badRequest, 3 unavailable, 4 internal, 5 application
sentinel  uvarint   0 none, else 1 + index into the sentinel error list
name      string    error name when sentinel is 0
message   string    error message when sentinel is 0
```

Code 5 carries an application failure settling an ask. The transport treats `sentinel` as an opaque number; the seam maps it through the append-only sentinel list `codec.ts` already defines, so identity-compared errors like `ErrRequestTimeout` cross the wire as an index and come back as the identical instance, and any other error comes back reconstructed from name and message. Codes 1 to 4 are transport-level: a request-scoped ERROR (nonzero correlation) settles one ask and the connection lives on; a connection-scoped ERROR (correlation 0) is the peer's last words before it closes.

What is request-scoped versus connection-scoped is fixed by rule, not judgment: a failure attributable to one message (undecodable envelope, unregistered type, oversize logical message, handler failure) is request-scoped; a failure that poisons the shared stream state (bad version, unknown frame type, reserved flags, lane mismatch, table violations, chunk sequence violations) is connection-scoped, because after it the stream cannot be trusted.

### Chunking

A DATA or REPLY whose encoded logical frame (header plus body) exceeds the connection's `chunkSize` is split into CHUNK frames sharing one group correlation: a chunked ask or reply keys its group by the inner frame's correlation (so a receiver rejecting the group can settle the waiting ask at once), and a chunked tell allocates a fresh id. The inner frame's own header rides at the start of the logical bytes, so the receiver recovers it whole. Each CHUNK body is a uvarint index, plus a uvarint total logical size on the first chunk only, plus a slice of the logical bytes; the first and last carry the `firstChunk` and `lastChunk` flags, and `expectsReply` rides only on the first. Indexes start at zero and must be contiguous: TCP already guarantees order, so a gap is corruption and is connection-scoped. Chunk bodies are capped at exactly `chunkSize` so receive buffers stay uniform.

The reassembler keys groups by correlation and allocates one exact-size buffer per group on the first chunk. It enforces two soft limits that reject the message but keep the connection: a declared total above `maxMessageSize`, and more concurrent groups than `maxLargeTransfers` (both answered with a request-scoped ERROR so a waiting ask settles). Everything else, duplicate first chunk, index gaps, overflow past the declared total, is connection-scoped. A sender that fails mid-group emits a short `lastChunk` frame with no data, which aborts the group on the receiver; a continuation chunk for an unknown correlation is silently ignored so the tail of an aborted group cannot kill the connection. There is no reassembly timer: partial groups are bounded by the concurrency cap times `maxMessageSize` and are freed by abort, violation, or connection teardown.

### Credits and admission

Flow control is two independent mechanisms.

**Admission** is local and always on: each connection tracks the bytes of frames accepted for sending but not yet written, capped at the credit window size. A peer that advertises zero credits opts out of the window but not of this bound: admission then falls back to the locally configured budget, and only an explicit local zero disables it. DATA, REPLY, CHUNK, and TABLE count; CREDIT, PING, PONG, and ERROR are exempt so control traffic can never be locked out. When admission is full, a tell is refused with a backpressure reason (the seam records the dead letter) and an ask rejects with a backpressure error; nothing ever blocks, because nothing may block an isolate's loop. An empty pipe admits one message of any size, mirroring the window's oversize allowance below, so a message the size caps permit can never be wedged by a window smaller than itself.

**Credits** are end-to-end: the handshake grants each direction an initial window (`initialCredits`, default 16 MiB), DATA and CHUNK frames consume it by their wire cost (16 plus body length), and the receiver grants it back with CREDIT frames as it disposes of frames. Grants are batched: the receiver accumulates owed bytes and flushes a CREDIT once the accumulator reaches a quarter of the window, so grant traffic stays negligible; a remainder still below the batch is repaid within a short deadline (50 ms), because repayment held forever would leave a sender whose next frame cannot afford the partially spent window parked with no grant ever falling due. When the window is empty, windowed frames wait in the outbound queue while exempt frames overtake them; a single frame larger than the whole window may be sent into an empty queue so an undersized peer still makes progress. Version one grants at dispatch handoff, when the frame reaches the inbound seam; deferring repayment until the message leaves its mailbox (so a slow actor slows its senders, not just the wire) is a designed extension, since the grant point is a policy inside the receiver and needs no wire change.

The receiver never trusts the sender's arithmetic: its own admission cap, `maxFrameSize`, `maxMessageSize`, and reassembly caps bound memory regardless of what the peer does with its window. The sender is equally untrusting: a CREDIT below revision 4 or with a malformed count poisons flow-control state and is connection-scoped, and a well-formed grant replenishes the window only up to its capacity, so a peer's arithmetic can never inflate it.

### String tables

At revision 3 or above, each direction of a connection interns actor paths and type ids independently. The sender assigns ids from 1, announces each with a TABLE frame (`kind` byte: 0 path, 1 type; uvarint id; the literal string), and thereafter encodes the ref as the bare id. Tables are per-connection, per-direction, per-kind, capped at 8192 entries; a full table just means new literals go inline. Receiving rules are strict because a corrupt table corrupts every later message: id zero, an empty literal, a conflicting re-registration, or an over-capacity install are connection-scoped; an identical re-registration is idempotent. A ref naming an unknown id, or any nonzero ref below revision 3, is likewise connection-scoped. Tables die with the connection and a reconnect starts empty.

The receiver may cache a resolved handle (the local actor ref for a path) on the table entry, so steady-state delivery skips path resolution entirely.

### Liveness and idle

Half-open TCP connections look healthy forever without probing, so the read side owns liveness. Each connection keeps a read-idle timer (default 10 s): when it fires with no inbound frame, the session sends a correlated PING; a peer answers PONG immediately. Two consecutive intervals with an outstanding unanswered probe kill the connection. Any inbound frame of any type resets the miss count and the timer, so an active connection never probes. A PING that could not even be admitted locally does not count as a miss, because the peer never saw it.

Separately, a server-side connection idle timeout (default 20 minutes, measured from the last inbound frame) reclaims connections nobody is using; probe traffic keeps a watched-but-quiet connection alive on purpose, since the peer holding it open is saying it wants it. Every socket write is bounded by a write timeout (default 10 s) via a timer that destroys the socket on expiry, so a peer that stops reading cannot wedge the flush loop.

Timers come from the transport's own facility (`net/timers.ts`): a coarse clock and deadline scheduling over plain `setTimeout`, behind an interface small enough that the seam could back it with the runtime's shared wheel later without `net/` importing anything. Nothing on the per-frame path calls `Date.now()`.

## The serializer

The payload codec is a self-contained binary encoding of the value domain the in-process codec already enforces, written by hand in `net/values.ts`. Its contract matches `codec.ts` v0 exactly, so a message that crosses isolates today crosses the network tomorrow with the same semantics: a payload is passthrough data or an instance of a registered class, and encoding fails on the sending side for anything that cannot survive (functions, symbols, unregistered class instances). The duties split so the transport stays actor-blind: `net/values.ts` encodes and decodes plain value trees only, and the seam performs the registration check on the way out and the prototype restoration on the way in (from the type ref through the registry, without running constructors, dropping `__proto__` and `constructor` keys).

Every value is a tag byte followed by tag-specific data:

| Tag       | Value | Encoding after the tag                                                                            |
|-----------|-------|---------------------------------------------------------------------------------------------------|
| NULL      | 0x00  | nothing                                                                                           |
| UNDEFINED | 0x01  | nothing                                                                                           |
| FALSE     | 0x02  | nothing                                                                                           |
| TRUE      | 0x03  | nothing                                                                                           |
| INT       | 0x04  | zigzag varint; integers in the 32-bit signed range except -0                                      |
| F64       | 0x05  | 8 bytes IEEE 754 BE; every other number (exact for all safe integers, -0, NaN, infinities)        |
| STRING    | 0x06  | uvarint length, UTF-8 bytes                                                                       |
| BIGINT    | 0x07  | sign byte, uvarint magnitude byte length, magnitude BE                                            |
| DATE      | 0x08  | 8 bytes F64 epoch milliseconds                                                                    |
| BYTES     | 0x09  | subtype byte (0 ArrayBuffer, 1 Uint8Array, other typed views numbered), uvarint length, raw bytes |
| ARRAY     | 0x0A  | uvarint element count, elements                                                                   |
| OBJECT    | 0x0B  | uvarint entry count, entries as string key then value                                             |
| MAP       | 0x0C  | uvarint entry count, entries as key value pairs                                                   |
| SET       | 0x0D  | uvarint element count, elements                                                                   |
| REF       | 0x0E  | uvarint back-reference to the nth previously encoded container                                    |

BYTES subtypes cover the whole typed-view family (0 ArrayBuffer, 1 Uint8Array, then Int8Array, Uint8ClampedArray, Int16Array, Uint16Array, Int32Array, Uint32Array, Float32Array, Float64Array, BigInt64Array, BigUint64Array, 12 DataView), contents as raw bytes in native element order; every supported runtime is little-endian. Nesting is bounded at depth 1024 on encode and decode alike, so a runaway or hostile tree fails with a typed error instead of the stack. Container values (array, object, map, set, bytes, date) are numbered in encode order; a value already seen encodes as REF, which preserves aliasing and makes cycles terminate, matching what structured clone gives the in-process path. Non-plain objects that are not the registered top-level message encode as OBJECT from their own enumerable string-keyed properties, which is the same silent prototype loss v0 documents for nested class instances. Object keys use the STRING encoding without a tag. The tag space is append-only; unknown tags are a decode error, and a payload that fails to decode settles the ask (or dead-letters the tell) as request-scoped badRequest without touching the connection.

Two implementation rules, both bench-gated rather than assumed: short ASCII strings may take a manual char-loop fast path past `TextEncoder`, and the encoder builds into one growable scratch buffer per connection that is retained across messages, because the profiling history of this codebase says buffer churn and cadence dominate, not raw allocation count. Nothing is pooled beyond that until a benchmark proves it pays.

The same primitives (uvarint, string, the scratch writer) are what `net/envelope.ts`, `net/table.ts`, and the HELLO codec build on, so the byte-level code exists once.

## The framed connection

`net/conn.ts` turns a socket into frames in and frames out, and is the only module that touches socket events.

**Read side.** A parser state machine consumes `data` events: accumulate until 16 header bytes exist, validate, then accumulate until `length` body bytes exist, then emit one frame and continue; partial header and partial body state persist across events by construction. The parser reads out of a rolling view over the received chunks and hands each frame a body that is a subarray view when the frame lies within one chunk (the overwhelmingly common case under a 64 KiB receive buffer), copying into a fresh buffer only when a body spans chunks or must outlive dispatch. Validation happens on the header before any body handling, so an oversize or malformed frame costs 16 bytes of inspection, an ERROR, and a close.

**Write side.** Callers enqueue frames; a flush scheduled at microtask boundary drains the queue in one batch per turn. Small frames are coalesced into one retained per-connection write buffer (a single `socket.write` per batch); a payload above a copy threshold is written as its own buffer instead, letting the runtime's vectored write path take it without copying. When `socket.write` returns false the flush loop stops scheduling and resumes on `drain`, so kernel backpressure propagates into admission (the queue's byte budget) and from there to callers. Frame headers are encoded into the retained buffer directly; steady-state writing allocates nothing.

The engine never yields a frame to dispatch without bounding its own cadence: inbound frames dispatch in bounded batches per macrotask, because a hot socket must not starve the rest of the isolate. The batch size is a tunable verified by benchmark, not a guess.

## The server

`net/server.ts` owns exactly the listener lifecycle; everything after accept is a session.

- **Bind and listen** on a configured host and port (port 0 supported for tests; the bound address is queryable). TLS is the same server with a `node:tls` listener and is configuration, not a protocol change.
- **Accept** wires the socket into a framed connection, arms the 10-second handshake deadline, and runs the acceptor side of HELLO. A connection that fails the handshake is closed and never reaches the session table. `keepAlive` is enabled on every accepted socket.
- **Accounting** tracks accepted and active connections, exposes both, and supports an optional max-connections cap that refuses beyond the limit.
- **Shutdown** is one method with one duration semantic: positive waits up to that long for active sessions to drain, zero waits indefinitely, negative closes everything now. Draining means the listener closes first (no new connections), sessions get a connection-scoped ERROR flushed best-effort, and each session's teardown fails its pending asks. Shutdown is idempotent.

There is deliberately no accept-loop pool, no worker pool, no ballast, and no thread pinning: those solve scheduler and GC problems of a different runtime. Here accept concurrency is the event loop's job, and the multi-core story is the isolate architecture described above.

## The peer

`net/peer.ts` owns the outbound side for one remote system: the lane set, dialing, and failure policy.

- **Lanes.** One control lane for system traffic, N ordinary lanes for user messages (default 1, because per-destination FIFO is the contract and more lanes trade ordering for parallelism), one large lane that carries chunked transfers so they never queue behind small messages. Watches and unwatches route to the control lane; a message whose payload reaches the chunk threshold routes to the large lane; everything else picks an ordinary lane by a stable hash of the target path when N is above 1, keeping per-actor order.
- **Dialing** is lazy per lane, with a 5-second dial timeout bounding TCP connect and handshake together, and single-flight per lane (concurrent senders share the dial in progress). The dialer sends HELLO and adopts the negotiated parameters.
- **Reconnect** is per-lane exponential backoff, starting at one second and doubling per consecutive failure to the 30-second cap; a use inside the open window fails fast with a backoff error rather than queueing, and the first use after it redials. Sessions are use-driven: a failed lane redials on next use, not on a background loop, and a successful dial resets the lane's backoff.
- **Redelivery.** A tell confirms once the kernel accepts its last frame; a tell admitted but still unconfirmed when the connection dies is redelivered at most once, in order, on the lane's next session, then dead-letters. Retrying in place preserves lane FIFO, and one retry covers the overwhelmingly common case (peer restarted) without inventing a queueing discipline the actor model does not promise; a frame already handed to the kernel is never resent, so a tell is duplicated at most by that single redelivery. An ask is never silently retried: its pending entry fails with the connection and the caller decides.
- **Asks** register in the session's pending table keyed by correlation, with the caller's timeout armed on the transport's timer facility. Settlement is: REPLY resolves, ERROR rejects, timeout abandons the entry (a late reply then finds no entry and is dropped by design), connection teardown fails every entry with a connection-closed error. Whoever removes the entry settles the promise; there is no path that settles twice or leaks an entry.
- **Teardown.** Closing the peer closes every lane, bumps a generation counter so a dial still in flight parks itself, and clears backoff state.

## Reliability rules

The failure taxonomy, in one place:

| Failure                                                                | Outcome                                                            |
|------------------------------------------------------------------------|--------------------------------------------------------------------|
| Ask settled by peer error                                              | rejects with the decoded error (sentinel identity preserved)       |
| Ask timeout                                                            | rejects with `ErrRequestTimeout`; late reply dropped silently      |
| Connection lost with asks in flight                                    | every pending ask rejects with a connection-closed error           |
| Tell over a full admission budget                                      | dead letter, backpressure reason                                   |
| Tell admitted, connection died before write                            | one redelivery on a fresh session, then dead letter                |
| Undecodable inbound envelope or payload                                | request-scoped ERROR; connection lives                             |
| Protocol violation (version, type, flags, lane, table, chunk sequence) | connection-scoped ERROR, then close                                |
| Handshake timeout or malformed HELLO                                   | close; dialer surfaces a typed dial error                          |
| Liveness probes missed twice                                           | connection torn down as dead                                       |
| Local shutdown                                                         | drain best-effort within the budget, then close; pending asks fail |

Connection loss and remote death are indistinguishable by design, and that must be documented as part of the remoting contract: the remoting seam above this layer turns a session's close event into `Terminated` for every remote actor watched over it, and undeliverable inbound envelopes into dead letters on the receiving system. This document only guarantees the transport delivers those two events reliably: close fires exactly once per session, after the last frame that will ever be dispatched.

Teardown ordering inside a session is fixed: mark closing, stop admitting, fail pending asks and drop partial chunk groups, flush already-admitted frames within the write-timeout budget so a final ERROR reaches the peer, then destroy the socket. A locally initiated close never reports itself as a peer failure.

## Performance plan

What makes this fast is mostly what it refuses to do per message: no allocation in the frame header path (retained scratch buffers, fixed offsets), no `Date.now()` (coarse clock), no unbounded microtask chains (batched flush out, batched dispatch in), no string re-encoding for hot paths (interned refs after first send), no copies for large payloads (subarray views in, vectored writes out).

Every optimization beyond the baseline design is gated by `pnpm bench` before it stays, because this codebase has already measured its intuitions failing in both directions: a state bitmask that looked free cost 40% of tell throughput, and releasing hot buffers that looked like hygiene cost 20%. The bench suite for this layer measures, at minimum: framed round-trip throughput for small tells over a loopback socket pair, ask latency distribution under pipelining, chunked transfer throughput at 1 MiB and at `maxMessageSize`, value-codec encode and decode against `JSON` on representative messages, and the cost of the dispatch batch size. Buffer pooling, string fast paths, and cadence values are adopted or rejected on those numbers, per runtime, since Node, Bun, and Deno will not agree.

## Defaults and limits

| Parameter                    | Default                             | Bounds                            |
|------------------------------|-------------------------------------|-----------------------------------|
| maxFrameSize                 | 16 MiB                              | floor 16 KiB                      |
| maxMessageSize               | 16 MiB                              | at least maxFrameSize, below 2^32 |
| chunkSize                    | 256 KiB                             | 16 KiB to maxFrameSize            |
| initialCredits (window)      | 16 MiB                              | at least chunkSize                |
| credit grant batch           | window / 4                          |                                   |
| maxLargeTransfers            | 4                                   | at least 1                        |
| table capacity               | 8192 entries per kind per direction |                                   |
| table literal cap            | 16 MiB                              |                                   |
| ordinary lanes               | 1                                   | 1 to 254                          |
| handshake deadline           | 10 s                                |                                   |
| dial timeout                 | 5 s                                 |                                   |
| TCP keepAlive                | 15 s                                |                                   |
| write timeout                | 10 s                                |                                   |
| read-idle (probe) interval   | 10 s                                |                                   |
| liveness miss limit          | 2                                   |                                   |
| connection idle reclaim      | 20 min                              | server side                       |
| reconnect backoff cap        | 30 s                                |                                   |
| tell delivery attempts       | 2                                   |                                   |
| write coalesce batch         | 32 frames or 64 KiB per flush       | bench-tuned                       |
| dispatch batch per macrotask | bench-tuned                         |                                   |
