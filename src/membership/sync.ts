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
import {
  ApplyKind,
  type ApplyResult,
  isProbeEligibleState,
  isTerminalState,
  MAX_INCARNATION,
  type MemberRecord,
  type MembershipView,
} from "./view";
import {
  assertSyncExchangeBudget,
  combineSyncChunks,
  decodeSyncMessage,
  encodeMessage,
  encodeSyncChunks,
  MAX_MEMBERS,
  MESSAGE_GOSSIP,
  MESSAGE_SYNC_REQUEST,
  MESSAGE_SYNC_RESPONSE,
  type MembershipUpdate,
  ProtocolError,
  STATE_ALIVE,
  STATE_SUSPECT,
  SyncFrameAssembler,
  type SyncMessage,
  type SyncMessageType,
} from "./wire";

/** Maximum injected-clock duration, in milliseconds, allowed to open a sync stream. @internal */
export const SYNC_CONNECT_TIMEOUT_MS: number = 1_000;
/** Maximum duration, in milliseconds, from first frame work through merge completion. @internal */
export const SYNC_EXCHANGE_TIMEOUT_MS: number = 5_000;
/** Fixed start-to-start anti-entropy cadence, in milliseconds. @internal */
export const ANTI_ENTROPY_INTERVAL_MS: number = 30_000;
/** Post-fanout inbound-drain duration, in milliseconds, before transport shutdown. @internal */
export const LEAVE_DRAIN_MS: number = 1_000;

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
  /**
   * Runs after an applied/refuted record is enqueued and owns all post-apply
   * maintenance: suspicion supersession (see {@link cancelSupersededSuspicion}),
   * terminal-record retention, and any timer arming.
   */
  readonly applied?: (update: MembershipUpdate) => void;
  /**
   * Receives the full equal-incarnation, distinct-reporter suspect record so the owner
   * can shorten its timer and re-disseminate the corroboration to the rest of the cluster.
   */
  readonly confirmSuspicion?: (update: MembershipUpdate) => void;
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
  #count: number = 0;

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
  const bytes: Uint8Array = new Uint8Array(4 + message.length);
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
  const chunks: readonly Uint8Array[] = encodeSyncChunks(type, exchangeId, updates);
  for (const chunk of chunks) {
    await stream.write(frame(chunk));
  }
}

/**
 * Validates one decoded chunk against exchange role, correlation, ordering,
 * and the chunk-count invariant; returns the declared total chunk count.
 */
function validateSyncChunk(
  decoded: SyncMessage,
  chunkIndex: number,
  expectedType: SyncMessageType,
  expectedExchangeId: bigint | undefined,
  expectedChunks: number | undefined,
): number {
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
 * frame bytes; the wire module's shared assembler owns the framing, each chunk
 * is decoded exactly once on arrival, and no decoded update escapes until
 * every canonical chunk exists.
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
  const assembler: SyncFrameAssembler = new SyncFrameAssembler();
  const chunks: Uint8Array[] = [];
  const decoded: SyncMessage[] = [];
  let expectedChunks: number | undefined;

  while (expectedChunks === undefined || decoded.length < expectedChunks) {
    const input: Uint8Array | undefined = await stream.read();
    if (input === undefined) {
      throw new SyncExchangeError(
        assembler.awaitingPrefix
          ? "sync stream ended before the frame prefix"
          : "sync stream ended inside a frame",
      );
    }

    for (const frame of assembler.push(input)) {
      const message: SyncMessage = decodeSyncMessage(frame);
      expectedChunks = validateSyncChunk(
        message,
        decoded.length,
        expectedType,
        expectedExchangeId,
        expectedChunks,
      );
      chunks.push(frame);
      decoded.push(message);
    }
  }

  if (!assembler.complete) {
    throw new ProtocolError("trailing bytes after complete sync exchange");
  }

  assertSyncExchangeBudget(chunks);
  const exchange: ReturnType<typeof combineSyncChunks> = combineSyncChunks(chunks, decoded);
  return {
    peer: stream.remoteAddress,
    exchangeId: exchange.exchangeId,
    updates: exchange.updates,
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
    let settled: boolean = false;
    let timer: ClockTimer;
    const finish: (action: () => void) => void = (action: () => void): void => {
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
  let value: bigint =
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
  let additions: number = 0;
  let greatestSelfAccusation: number = -1;
  for (const incoming of updates) {
    // Terminal truth about an unknown identity is ignored by the view, so it
    // cannot contribute to table growth.
    if (view.stateOf(incoming.member) === undefined && !isTerminalState(incoming.state)) {
      additions += 1;
    }

    if (view.wouldRefute(incoming)) {
      greatestSelfAccusation = Math.max(greatestSelfAccusation, incoming.incarnation);
    }
  }

  if (view.size + additions > MAX_MEMBERS) {
    throw new SyncExchangeError("merged membership table exceeds 1024 retained members");
  }

  const self: MemberRecord | undefined = view.self();
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
 * Applies one remote update through the single shared ingestion pipeline used
 * by both the packet piggyback path and the sync merge path: apply under
 * precedence, route confirmations, enqueue accepted truth with self-defense
 * priority, then run the `applied` and `selfRefuted` hooks in that order.
 * Post-apply maintenance, such as suspicion supersession and terminal-record
 * retention, is owned entirely by the `applied` callback so it can never run
 * twice for one record.
 *
 * @returns The newly applied record, or `undefined` for confirmations and
 * ignored truth.
 * @throws {IncarnationExhaustedError} If self-refutation cannot increment incarnation.
 * @throws {MembershipCapacityError} If accepting a new identity would exceed capacity.
 * @internal
 */
export function applyRemoteTruth(
  options: Pick<SyncOptions, "view" | "broadcasts" | "clock" | "callbacks" | "stateChangeTime">,
  update: MembershipUpdate,
): MembershipUpdate | undefined {
  const callbacks: SyncCallbacks = options.callbacks ?? {};
  const result: ApplyResult = options.view.apply(
    update,
    options.clock.now(),
    options.stateChangeTime?.() ?? BigInt(options.clock.epochMilliseconds()),
  );
  if (result.kind === ApplyKind.confirmed) {
    callbacks.confirmSuspicion?.(update);
    return undefined;
  }

  if (result.kind === ApplyKind.ignored) {
    return undefined;
  }

  const truth: MemberRecord = result.record;
  options.broadcasts.enqueue(
    truth,
    options.view.aliveOrSuspectCount(),
    result.kind === ApplyKind.refuted,
  );
  callbacks.applied?.(truth);
  if (result.kind === ApplyKind.refuted) {
    callbacks.selfRefuted?.(truth);
  }

  return truth;
}

/**
 * Applies a fully validated remote table and disseminates every accepted truth.
 * Filtering and whole-table preflight precede mutation; each accepted record
 * then flows through {@link applyRemoteTruth}.
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
  const accepted: readonly MembershipUpdate[] =
    options.acceptUpdate === undefined
      ? updates
      : updates.filter(
          (update: MembershipUpdate): boolean => options.acceptUpdate?.(update) !== false,
        );
  preflight(options.view, accepted);
  const applied: MembershipUpdate[] = [];
  for (const update of accepted) {
    const truth: MembershipUpdate | undefined = applyRemoteTruth(options, update);
    if (truth !== undefined) {
      applied.push(truth);
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
  options.activity?.begin();
  try {
    const opening: Promise<MembershipStream> = options.transport.stream(peer);
    stream = await timeout(options.clock, opening, "connect", SYNC_CONNECT_TIMEOUT_MS, (): void => {
      void opening.then(
        (lateStream: MembershipStream): void => lateStream.close(),
        (): void => undefined,
      );
    });

    // A connect timeout rejects before the flag could ever be read, so only
    // the exchange phase participates in abort tracking.
    let aborted: boolean = false;
    const activeStream: MembershipStream = stream;
    const id: bigint = exchangeId(options.random);
    const exchanging: Promise<SyncExchange> = (async (): Promise<SyncExchange> => {
      await writeSyncFrames(activeStream, MESSAGE_SYNC_REQUEST, id, options.view.updates());
      const response: SyncExchange = await readSyncFrames(activeStream, MESSAGE_SYNC_RESPONSE, id);
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
  let aborted: boolean = false;
  try {
    await timeout(
      options.clock,
      (async (): Promise<void> => {
        const request: SyncExchange = await readSyncFrames(stream, MESSAGE_SYNC_REQUEST);
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
  const seen: Set<string> = new Set<string>();
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
  const ordered: string[] = distinctSeeds(options.view.selfName, seeds);
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

  const first: string | undefined = succeeded[0];
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
  #running: boolean = false;
  /** Whether one exchange promise is currently unsettled. */
  #active: boolean = false;
  /** Epoch that invalidates callbacks from a prior start/stop cycle. */
  #generation: number = 0;

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

      const peers: string[] = this.#options.view
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

/** Shared UTF-8 encoder used solely for deterministic member-name byte ordering. */
const memberNameEncoder: TextEncoder = new TextEncoder();

/** Compares two identities by their already-encoded unsigned UTF-8 bytes. */
function compareNameBytes(left: Uint8Array, right: Uint8Array): number {
  const length: number = Math.min(left.length, right.length);
  for (let index: number = 0; index < length; index += 1) {
    const difference: number = (left[index] as number) - (right[index] as number);
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
  const bytes: Uint8Array = encodeMessage({ type: MESSAGE_GOSSIP, updates: [update] });
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
