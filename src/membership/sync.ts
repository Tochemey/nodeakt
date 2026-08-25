/*
 * MIT License
 *
 * Copyright (c) 2026 GoAkt Team
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import type { BroadcastQueue } from "./broadcast";
import type { Clock, ClockTimer } from "./clock";
import type { Random } from "./random";
import type { MembershipStream, MembershipTransport } from "./transport";
import { isProbeEligibleState, MAX_INCARNATION, type MembershipView } from "./view";
import {
  decodeSyncChunks,
  decodeSyncMessage,
  encodeMessage,
  encodeSyncChunks,
  MAX_MEMBERS,
  MAX_SYNC_MESSAGE_BYTES,
  MESSAGE_GOSSIP,
  MESSAGE_SYNC_REQUEST,
  MESSAGE_SYNC_RESPONSE,
  type MembershipUpdate,
  ProtocolError,
  STATE_ALIVE,
  STATE_LEFT,
  STATE_SUSPECT,
  type SyncMessageType,
} from "./wire";

/** Maximum injected-clock duration, in milliseconds, allowed to open a sync stream. @internal */
export const SYNC_CONNECT_TIMEOUT_MS = 1_000;
/** Maximum duration, in milliseconds, from first frame work through merge completion. @internal */
export const SYNC_EXCHANGE_TIMEOUT_MS = 5_000;
/** Fixed start-to-start anti-entropy cadence, in milliseconds. @internal */
export const ANTI_ENTROPY_INTERVAL_MS = 30_000;
/** Post-fanout inbound-drain duration, in milliseconds, before transport shutdown. @internal */
export const LEAVE_DRAIN_MS = 1_000;

/** Deadline whose expiration is represented by {@link SyncTimeoutError}. @internal */
export type SyncTimeoutPhase = "connect" | "exchange";

/**
 * Indicates that an injected-clock deadline elapsed; transport or protocol
 * errors remain available as their original error types.
 *
 * @internal
 */
export class SyncTimeoutError extends Error {
  /** Operation segment whose timer won the settlement race. */
  readonly phase: SyncTimeoutPhase;
  /** Configured relative deadline in milliseconds, not an absolute timestamp. */
  readonly timeoutMs: number;

  /** Captures the elapsed phase and configured duration in a stable diagnostic shape. */
  constructor(phase: SyncTimeoutPhase, timeoutMs: number) {
    super(`sync ${phase} timed out after ${timeoutMs}ms`);
    this.name = "SyncTimeoutError";
    this.phase = phase;
    this.timeoutMs = timeoutMs;
  }
}

/** Indicates framing, merge-preflight, or exchange-state failure after stream acquisition. @internal */
export class SyncExchangeError extends Error {
  /** Creates a domain exchange error while preserving an optional lower-level cause. */
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "SyncExchangeError";
  }
}

/** Indicates that every distinct, non-self join seed failed its complete exchange. @internal */
export class JoinError extends Error {
  /** Defensive copy of all attempted seeds in first-occurrence caller order. */
  readonly seeds: readonly string[];

  /** Captures attempted identities; `cause` is normally the first observed seed failure. */
  constructor(seeds: readonly string[], options?: ErrorOptions) {
    super("no join seed completed a sync exchange", options);
    this.name = "JoinError";
    this.seeds = Array.from(seeds);
  }
}

/**
 * Optional synchronous callbacks that keep suspicion, retention, and Lifeguard
 * state coherent with mutations performed by {@link mergeSyncTable}.
 *
 * @internal
 */
export interface SyncCallbacks {
  /** Runs after an applied/refuted record is enqueued, before cancellation and self-refutation hooks. */
  readonly applied?: (update: MembershipUpdate) => void;
  /** Cancels suspicion through the inclusive incarnation supplied by merge ordering rules. */
  readonly cancelSuspicion?: (member: string, incarnation: number) => void;
  /** Receives equal-incarnation, distinct-reporter evidence without disseminating a new record. */
  readonly confirmSuspicion?: (member: string, incarnation: number, reporter: string) => void;
  /** Runs last for generated alive self-defense truth so the owner can penalize local health. */
  readonly selfRefuted?: (update: MembershipUpdate) => void;
}

/**
 * Engine-wide tracker of in-flight outbound exchanges.
 *
 * Sharing one instance across join and anti-entropy lets the scheduler honor
 * the no-overlap rule against every outbound sync, not only its own.
 *
 * @internal
 */
export class SyncActivity {
  /** Number of currently unsettled outbound exchanges. */
  #count = 0;

  /** Whether any outbound exchange is currently unsettled. */
  get active(): boolean {
    return this.#count > 0;
  }

  /** Records one outbound exchange as started. */
  begin(): void {
    this.#count += 1;
  }

  /** Records one outbound exchange as settled, never going below zero. */
  end(): void {
    this.#count = Math.max(0, this.#count - 1);
  }
}

/** Shared dependencies and composition policy for outbound and inbound push-pull sync. @internal */
export interface SyncOptions {
  /** Mutable local table sent as snapshots and atomically updated after remote validation. */
  readonly view: MembershipView;
  /** Shared queue receiving each newly applied or generated self-refutation record. */
  readonly broadcasts: BroadcastQueue;
  /** Monotonic clock used for millisecond deadlines, observations, and fallback state times. */
  readonly clock: Clock;
  /** Random source used for nonzero 64-bit exchange IDs and anti-entropy peer choice. */
  readonly random: Random;
  /** Stream transport; each acquired stream is exclusively owned and closed by the exchange. */
  readonly transport: MembershipTransport;
  /** Optional synchronous hooks for state coupled to view mutations. */
  readonly callbacks?: SyncCallbacks;
  /** Filters complete-table entries before capacity preflight, typically to protect leaving self truth. */
  readonly acceptUpdate?: (update: MembershipUpdate) => boolean;
  /** Supplies monotonic origin time for generated self-refutations; fallback is epoch clock time. */
  readonly stateChangeTime?: () => bigint;
  /** Shared outbound-exchange tracker consulted by the anti-entropy no-overlap rule. */
  readonly activity?: SyncActivity;
  /** Gates outbound exchanges; a `false` reading fails the exchange before I/O or merge. */
  readonly outboundAllowed?: () => boolean;
  /** Gates inbound merge and response work; a `false` reading abandons the exchange. */
  readonly inboundAllowed?: () => boolean;
}

/** One canonically framed and fully decoded remote membership-table snapshot. @internal */
export interface SyncExchange {
  /** Transport-reported remote stream address, not a membership-record identity assertion. */
  readonly peer: string;
  /** Nonzero request correlation ID shared by every chunk and matching response. */
  readonly exchangeId: bigint;
  /** Decoded updates in wire order; returned only after all chunks validate. */
  readonly updates: readonly MembershipUpdate[];
}

/** Outcome after attempting every distinct non-self join seed exactly once. @internal */
export interface JoinResult {
  /** First successful seed in caller order, or `undefined` only when no seed was attempted. */
  readonly seed: string | undefined;
  /** Successful canonical transport addresses in attempted order. */
  readonly succeeded: readonly string[];
  /** Failed canonical transport addresses in attempted order; their individual errors are not retained. */
  readonly failed: readonly string[];
}

/** Prefixes one encoded sync message with its big-endian unsigned 32-bit byte length. */
function frame(message: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(4 + message.length);
  new DataView(bytes.buffer).setUint32(0, message.length);
  bytes.set(message, 4);
  return bytes;
}

/**
 * Writes one canonically chunked table as u32-length-prefixed stream frames.
 * Frames are awaited sequentially, preserving chunk order and backpressure.
 *
 * @throws Any encoder validation error or stream write failure.
 * @internal
 */
export async function writeSyncFrames(
  stream: MembershipStream,
  type: SyncMessageType,
  exchangeId: bigint,
  updates: readonly MembershipUpdate[],
): Promise<void> {
  const chunks = encodeSyncChunks(type, exchangeId, updates);
  for (const chunk of chunks) {
    await stream.write(frame(chunk));
  }
}

/** Concatenates unread stream bytes into fresh owned storage. */
function append(left: Uint8Array, right: Uint8Array): Uint8Array {
  if (left.length === 0) {
    return right.slice();
  }

  const joined = new Uint8Array(left.length + right.length);
  joined.set(left);
  joined.set(right, left.length);
  return joined;
}

/** Per-exchange unread-byte ownership used to tolerate split and coalesced stream reads. */
interface SyncFrameReader {
  /** Bytes received from the stream but not yet consumed as a complete frame. */
  buffered: Uint8Array<ArrayBufferLike>;
}

/**
 * Reads until at least `minimum` bytes are buffered.
 *
 * @throws {SyncExchangeError} If the stream ends before the requested byte count.
 */
async function fillBuffer(
  stream: MembershipStream,
  reader: SyncFrameReader,
  minimum: number,
  endedMessage: string,
): Promise<void> {
  while (reader.buffered.length < minimum) {
    const input = await stream.read();
    if (input === undefined) {
      throw new SyncExchangeError(endedMessage);
    }

    reader.buffered = append(reader.buffered, input);
  }
}

/** Decodes and validates the current frame prefix, returning payload bytes. */
function frameLength(reader: SyncFrameReader): number {
  const length = new DataView(
    reader.buffered.buffer,
    reader.buffered.byteOffset,
    reader.buffered.byteLength,
  ).getUint32(0);
  if (length < 4 || length > MAX_SYNC_MESSAGE_BYTES) {
    throw new ProtocolError("sync stream frame length is out of range");
  }

  return length;
}

/**
 * Consumes exactly one length-prefixed frame while retaining coalesced following bytes.
 *
 * @throws {ProtocolError} For an out-of-range length.
 * @throws {SyncExchangeError} For premature stream end.
 */
async function readSyncFrame(
  stream: MembershipStream,
  reader: SyncFrameReader,
): Promise<Uint8Array> {
  await fillBuffer(stream, reader, 4, "sync stream ended before the frame prefix");
  const length = frameLength(reader);
  await fillBuffer(stream, reader, 4 + length, "sync stream ended inside a frame");

  const message = reader.buffered.slice(4, 4 + length);
  reader.buffered = reader.buffered.slice(4 + length);
  return message;
}

/**
 * Validates one decoded chunk against exchange role, correlation, ordering,
 * and the chunk-count invariant; returns the declared total chunk count.
 */
function validateSyncChunk(
  message: Uint8Array,
  chunkIndex: number,
  expectedType: SyncMessageType,
  expectedExchangeId: bigint | undefined,
  expectedChunks: number | undefined,
): number {
  const decoded = decodeSyncMessage(message);
  if (decoded.type !== expectedType) {
    throw new ProtocolError("sync message type does not match exchange role");
  }

  if (expectedExchangeId !== undefined && decoded.exchangeId !== expectedExchangeId) {
    throw new ProtocolError("sync response exchange ID does not match request");
  }

  if (
    chunkIndex !== decoded.chunkIndex ||
    (expectedChunks !== undefined && expectedChunks !== decoded.chunkCount)
  ) {
    throw new ProtocolError("sync chunks are reordered or mismatched");
  }

  return decoded.chunkCount;
}

/**
 * Reads exactly one complete sync table. Stream reads may split or coalesce
 * frame bytes; no decoded update escapes until every canonical chunk exists.
 *
 * @returns The remote address, correlation ID, and complete updates in chunk order.
 * @throws {ProtocolError} For malformed, mismatched, reordered, or trailing bytes.
 * @throws {SyncExchangeError} If the stream ends before the table is complete.
 * @internal
 */
export async function readSyncFrames(
  stream: MembershipStream,
  expectedType: SyncMessageType,
  expectedExchangeId?: bigint,
): Promise<SyncExchange> {
  const reader: SyncFrameReader = {
    buffered: new Uint8Array(0),
  };

  let expectedChunks: number | undefined;
  const chunks: Uint8Array[] = [];

  while (expectedChunks === undefined || chunks.length < expectedChunks) {
    const message = await readSyncFrame(stream, reader);
    expectedChunks = validateSyncChunk(
      message,
      chunks.length,
      expectedType,
      expectedExchangeId,
      expectedChunks,
    );
    chunks.push(message);
  }

  if (reader.buffered.length !== 0) {
    throw new ProtocolError("trailing bytes after complete sync exchange");
  }

  const decoded = decodeSyncChunks(chunks);
  return {
    peer: stream.remoteAddress,
    exchangeId: decoded.exchangeId,
    updates: decoded.updates,
  };
}

/**
 * Races a promise against an injected-clock deadline. The losing promise is
 * not canceled; callers must use `onTimeout` to close its underlying resource.
 */
function timeout<T>(
  clock: Clock,
  promise: Promise<T>,
  phase: SyncTimeoutPhase,
  timeoutMs: number,
  onTimeout?: () => void,
): Promise<T> {
  return new Promise<T>((resolve, reject): void => {
    let settled = false;
    let timer: ClockTimer;
    const finish = (action: () => void): void => {
      if (settled) {
        return;
      }

      settled = true;
      clock.cancel(timer);
      action();
    };

    timer = clock.schedule(timeoutMs, (): void => {
      if (settled) {
        return;
      }

      settled = true;
      onTimeout?.();
      reject(new SyncTimeoutError(phase, timeoutMs));
    });

    void promise.then(
      (value: T): void => finish((): void => resolve(value)),
      (error: unknown): void => finish((): void => reject(error)),
    );
  });
}

/** Produces a nonzero 64-bit exchange correlation ID from two unsigned 32-bit draws. */
function exchangeId(random: Random): bigint {
  let value =
    (BigInt(random.integer(0x1_0000_0000)) << 32n) | BigInt(random.integer(0x1_0000_0000));
  if (value === 0n) {
    value = 1n;
  }

  return value;
}

/**
 * Validates whole-table capacity and self-refutation headroom before mutation,
 * so these exchange-level failures cannot leave a partially merged table.
 */
function preflight(view: MembershipView, updates: readonly MembershipUpdate[]): void {
  let additions = 0;
  let greatestSelfAccusation = -1;
  for (const incoming of updates) {
    if (view.stateOf(incoming.member) === undefined) {
      additions += 1;
    }

    if (view.wouldRefute(incoming)) {
      greatestSelfAccusation = Math.max(greatestSelfAccusation, incoming.incarnation);
    }
  }

  if (view.size + additions > MAX_MEMBERS) {
    throw new SyncExchangeError("merged membership table exceeds 1024 retained members");
  }

  const self = view.self();
  if (
    greatestSelfAccusation >= MAX_INCARNATION ||
    (greatestSelfAccusation >= 0 && (self?.incarnation ?? 0) >= MAX_INCARNATION)
  ) {
    throw new SyncExchangeError("self-refutation would exhaust the incarnation");
  }
}

/**
 * Applies the suspicion-supersession rule for one accepted truth record:
 * suspect at incarnation N preserves N while cancelling through N-1; every
 * other state cancels through N.
 *
 * @internal
 */
export function cancelSupersededSuspicion(
  update: MembershipUpdate,
  cancel: (member: string, incarnation: number) => void,
): void {
  if (update.state === STATE_SUSPECT) {
    if (update.incarnation > 0) {
      cancel(update.member, update.incarnation - 1);
    }

    return;
  }

  cancel(update.member, update.incarnation);
}

/**
 * Applies a fully validated remote table and disseminates every accepted truth.
 * Filtering and whole-table preflight precede mutation. For each mutation the
 * ordering is enqueue, `applied`, suspicion cancellation, then `selfRefuted`.
 *
 * @returns Newly applied records, including generated self-refutations, in input order.
 * @throws {SyncExchangeError} Before mutation if capacity or self-incarnation would be exhausted.
 * @internal
 */
export function mergeSyncTable(
  options: Pick<
    SyncOptions,
    "view" | "broadcasts" | "clock" | "callbacks" | "stateChangeTime" | "acceptUpdate"
  >,
  updates: readonly MembershipUpdate[],
): readonly MembershipUpdate[] {
  const accepted =
    options.acceptUpdate === undefined
      ? updates
      : updates.filter(
          (update: MembershipUpdate): boolean => options.acceptUpdate?.(update) !== false,
        );
  preflight(options.view, accepted);
  const callbacks = options.callbacks ?? {};
  const applied: MembershipUpdate[] = [];
  for (const update of accepted) {
    const result = options.view.apply(
      update,
      options.clock.now(),
      options.stateChangeTime?.() ?? BigInt(options.clock.epochMilliseconds()),
    );
    if (result.kind === "confirmed") {
      callbacks.confirmSuspicion?.(result.member, result.incarnation, result.reporter);
      continue;
    }

    if (result.kind === "ignored") {
      continue;
    }

    const truth = result.record;
    options.broadcasts.enqueue(
      truth,
      options.view.aliveOrSuspectCount(),
      result.kind === "refuted",
    );
    applied.push(truth);
    callbacks.applied?.(truth);
    cancelSupersededSuspicion(truth, (member, incarnation): void => {
      callbacks.cancelSuspicion?.(member, incarnation);
    });
    if (result.kind === "refuted") {
      callbacks.selfRefuted?.(truth);
    }
  }

  return applied;
}

/**
 * Sends a complete request before reading its matching response. The local view
 * changes only after the complete response passes framing and canonical checks.
 *
 * The acquired stream is always closed. Connect timeout also closes a stream
 * that resolves late; exchange timeout closes the active stream before rejection.
 *
 * @returns The validated remote table that was merged.
 * @throws {SyncTimeoutError} When connect or exchange duration expires.
 * @throws {ProtocolError|SyncExchangeError} For invalid framing, protocol, or merge preflight.
 * @internal
 */
export async function syncWith(options: SyncOptions, peer: string): Promise<SyncExchange> {
  if (options.outboundAllowed?.() === false) {
    throw new SyncExchangeError("engine lifecycle no longer permits outbound sync");
  }

  let stream: MembershipStream | undefined;
  let aborted = false;
  options.activity?.begin();
  try {
    const opening = options.transport.stream(peer);
    stream = await timeout(options.clock, opening, "connect", SYNC_CONNECT_TIMEOUT_MS, (): void => {
      aborted = true;
      void opening.then(
        (lateStream: MembershipStream): void => lateStream.close(),
        (): void => undefined,
      );
    });

    aborted = false;
    const activeStream = stream;
    const id = exchangeId(options.random);
    const exchanging = (async (): Promise<SyncExchange> => {
      await writeSyncFrames(activeStream, MESSAGE_SYNC_REQUEST, id, options.view.updates());
      const response = await readSyncFrames(activeStream, MESSAGE_SYNC_RESPONSE, id);
      if (aborted) {
        throw new SyncTimeoutError("exchange", SYNC_EXCHANGE_TIMEOUT_MS);
      }

      if (options.outboundAllowed?.() === false) {
        throw new SyncExchangeError("engine lifecycle no longer permits outbound sync");
      }

      mergeSyncTable(options, response.updates);
      return response;
    })();
    return await timeout(
      options.clock,
      exchanging,
      "exchange",
      SYNC_EXCHANGE_TIMEOUT_MS,
      (): void => {
        aborted = true;
        activeStream.close();
      },
    );
  } finally {
    options.activity?.end();
    stream?.close();
  }
}

/**
 * Handles one inbound request atomically and responds with the post-merge table.
 * The response therefore includes accepted request truth and any generated
 * self-refutation. The borrowed inbound stream is always closed before settlement.
 *
 * @throws {SyncTimeoutError} If the whole request/merge/response exchange exceeds its deadline.
 * @throws {ProtocolError|SyncExchangeError} For invalid request or merge preflight.
 * @internal
 */
export async function respondToSync(options: SyncOptions, stream: MembershipStream): Promise<void> {
  let aborted = false;
  try {
    await timeout(
      options.clock,
      (async (): Promise<void> => {
        const request = await readSyncFrames(stream, MESSAGE_SYNC_REQUEST);
        if (aborted || options.inboundAllowed?.() === false) {
          return;
        }

        mergeSyncTable(options, request.updates);
        await writeSyncFrames(
          stream,
          MESSAGE_SYNC_RESPONSE,
          request.exchangeId,
          options.view.updates(),
        );

        // Hold the connection until the initiator's EOF so a half-open inbound
        // socket cannot linger past the exchange deadline.
        while ((await stream.read()) !== undefined) {
          // Post-response bytes are discarded; framing validation rejects garbage.
        }
      })(),
      "exchange",
      SYNC_EXCHANGE_TIMEOUT_MS,
      (): void => {
        aborted = true;
        stream.close();
      },
    );
  } finally {
    stream.close();
  }
}

/** Removes self and duplicate seed addresses while preserving first-occurrence order. */
function distinctSeeds(self: string, seeds: readonly string[]): string[] {
  const seen = new Set<string>();
  const distinct: string[] = [];
  for (const seed of seeds) {
    if (seed === self || seen.has(seed)) {
      continue;
    }

    seen.add(seed);
    distinct.push(seed);
  }

  return distinct;
}

/**
 * Tries distinct seeds in caller order. Before first success failures are
 * significant; afterwards each remaining seed is attempted once best-effort.
 *
 * @returns Partitioned success/failure addresses and the first successful seed.
 * @throws {JoinError} If at least one seed was attempted and none succeeded.
 * @internal
 */
export async function join(options: SyncOptions, seeds: readonly string[]): Promise<JoinResult> {
  const ordered = distinctSeeds(options.view.selfName, seeds);
  if (ordered.length === 0) {
    return { seed: undefined, succeeded: [], failed: [] };
  }

  const succeeded: string[] = [];
  const failed: string[] = [];
  let firstError: unknown;
  for (const seed of ordered) {
    if (options.outboundAllowed?.() === false) {
      break;
    }

    try {
      await syncWith(options, seed);
      succeeded.push(seed);
    } catch (error) {
      firstError ??= error;
      failed.push(seed);
    }
  }

  const first = succeeded[0];
  if (first === undefined) {
    throw new JoinError(ordered, { cause: firstError });
  }

  return { seed: first, succeeded, failed };
}

/**
 * Fixed-cadence scheduler for non-overlapping push-pull exchanges with a
 * uniformly selected alive peer. Individual exchange failures are intentionally swallowed.
 *
 * @internal
 */
export class AntiEntropy {
  /** Shared options borrowed by each scheduled exchange. */
  readonly #options: SyncOptions;
  /** Owned timer for the next cadence boundary, or `undefined` between scheduling steps. */
  #timer: ClockTimer | undefined;
  /** Whether future ticks should continue to schedule. */
  #running = false;
  /** Whether one exchange promise is currently unsettled. */
  #active = false;
  /** Epoch that invalidates callbacks from a prior start/stop cycle. */
  #generation = 0;

  /** Captures dependencies without scheduling work or opening a stream. */
  constructor(options: SyncOptions) {
    this.#options = options;
  }

  /** Whether an exchange started by this scheduler is currently in flight. */
  get active(): boolean {
    return this.#active;
  }

  /** Idempotently starts scheduling; the first exchange is attempted after one full interval. */
  start(): void {
    if (this.#running) {
      return;
    }

    this.#running = true;
    this.#generation += 1;
    this.#schedule(this.#generation);
  }

  /** Stops future ticks and cancels the cadence timer without aborting an active exchange. */
  stop(): void {
    if (!this.#running) {
      return;
    }

    this.#running = false;
    this.#generation += 1;
    if (this.#timer !== undefined) {
      this.#options.clock.cancel(this.#timer);
      this.#timer = undefined;
    }
  }

  /**
   * Arms the next cadence boundary before selecting work, preserving fixed
   * cadence even when no peer exists or the previous exchange remains active.
   */
  #schedule(generation: number): void {
    this.#timer = this.#options.clock.schedule(ANTI_ENTROPY_INTERVAL_MS, (): void => {
      this.#timer = undefined;
      if (!this.#running || generation !== this.#generation) {
        return;
      }

      this.#schedule(generation);
      if (this.#active || this.#options.activity?.active === true) {
        return;
      }

      const peers = this.#options.view
        .members()
        .filter(
          (member): boolean =>
            member.member !== this.#options.view.selfName && member.state === STATE_ALIVE,
        )
        .map((member): string => member.member);
      if (peers.length === 0) {
        return;
      }

      this.#active = true;
      void syncWith(this.#options, this.#options.random.pick(peers)).then(
        (): void => {
          this.#active = false;
        },
        (): void => {
          this.#active = false;
        },
      );
    });
  }
}

/** Dependencies and lifecycle hook for the standalone graceful-leave operation. @internal */
export interface LeaveOptions {
  /** Mutable view that must already contain the local member record. */
  readonly view: MembershipView;
  /** Shared queue receiving newly applied local left truth before final fanout. */
  readonly broadcasts: BroadcastQueue;
  /** Injected clock used for observation time and the millisecond drain delay. */
  readonly clock: Clock;
  /** Transport retained for ordered final packets, inbound drain, then shutdown. */
  readonly transport: MembershipTransport;
  /** Optional monotonic origin timestamp source; fallback is truncated clock time. */
  readonly stateChangeTime?: () => bigint;
  /** Invoked first so composed components cannot race new outbound work with leave. */
  readonly stopStarting?: () => void;
}

/** Immutable summary returned only after fanout, drain, and transport shutdown complete. @internal */
export interface LeaveResult {
  /** Local left record used for fanout; may be a pre-existing left record. */
  readonly update: MembershipUpdate;
  /** Deterministically byte-ordered eligible peer names targeted exactly once. */
  readonly targets: readonly string[];
}

/** Resolves after an injected-clock delay measured in milliseconds. */
function delay(clock: Clock, milliseconds: number): Promise<void> {
  return new Promise<void>((resolve): void => {
    clock.schedule(milliseconds, resolve);
  });
}

/** Shared UTF-8 encoder used solely for deterministic member-name byte ordering. */
const memberNameEncoder = new TextEncoder();

/** Compares two identities by their already-encoded unsigned UTF-8 bytes. */
function compareNameBytes(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const difference = (left[index] as number) - (right[index] as number);
    if (difference !== 0) {
      return difference;
    }
  }

  return left.length - right.length;
}

/**
 * Returns non-self probe-eligible identities sorted lexicographically by UTF-8
 * bytes, independent of locale and view insertion order.
 *
 * Each name is encoded once and sorted by that decoration, so the comparator
 * allocates no per-comparison byte arrays.
 *
 * @internal
 */
export function leaveTargets(view: MembershipView): readonly string[] {
  return view
    .members()
    .filter(
      (member): boolean => member.member !== view.selfName && isProbeEligibleState(member.state),
    )
    .map((member): { name: string; bytes: Uint8Array } => ({
      name: member.member,
      bytes: memberNameEncoder.encode(member.member),
    }))
    .sort((left, right): number => compareNameBytes(left.bytes, right.bytes))
    .map((entry): string => entry.name);
}

/**
 * Sends one left record to every supplied target. Sends are initiated in the
 * given deterministic order but settle concurrently, so a set of unreachable
 * targets costs one connect timeout rather than one per target. Per-target
 * synchronous or asynchronous transport failures are swallowed.
 *
 * @internal
 */
export async function fanoutLeave(
  transport: MembershipTransport,
  update: MembershipUpdate,
  targets: readonly string[],
): Promise<void> {
  const bytes = encodeMessage({ type: MESSAGE_GOSSIP, updates: [update] });
  const sends: Promise<void>[] = [];
  for (const target of targets) {
    try {
      sends.push(
        transport.packet(target, bytes).catch((): void => {
          // Graceful leave dissemination is best-effort.
        }),
      );
    } catch {
      // A synchronous transport rejection is equally best-effort.
    }
  }

  await Promise.all(sends);
}

/**
 * Applies leave, fanouts deterministically, drains, then closes the transport.
 * Existing local left truth is reused; otherwise new truth is applied and
 * enqueued before any packet. Transport shutdown is skipped if an earlier
 * unexpected operation rejects.
 *
 * @returns The disseminated record and exact ordered target snapshot.
 * @throws {SyncExchangeError} If the local member has not been installed.
 * @throws Any error from the drain clock or transport shutdown.
 * @internal
 */
export async function leave(options: LeaveOptions): Promise<LeaveResult> {
  options.stopStarting?.();
  const current = options.view.self();
  if (current === undefined) {
    throw new SyncExchangeError("cannot leave before self is installed in the membership view");
  }

  const update: MembershipUpdate =
    current.state === STATE_LEFT
      ? current
      : {
          state: STATE_LEFT,
          selfOriginated: true,
          incarnation: current.incarnation,
          stateChangeTime: options.stateChangeTime?.() ?? BigInt(options.clock.epochMilliseconds()),
          member: current.member,
          reporter: "",
          metadata: new Uint8Array(0),
        };
  if (current.state !== STATE_LEFT) {
    const result = options.view.applyLocal(update, options.clock.now());
    if (result.kind === "applied") {
      options.broadcasts.enqueue(result.record, options.view.aliveOrSuspectCount());
    }
  }

  const targets = leaveTargets(options.view);
  await fanoutLeave(options.transport, update, targets);
  await delay(options.clock, LEAVE_DRAIN_MS);
  await options.transport.stop();
  return { update, targets };
}
