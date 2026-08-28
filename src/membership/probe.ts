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

import type { BroadcastQueue, BroadcastSelection } from "./broadcast";
import type { Clock, ClockTimer } from "./clock";
import type { Random } from "./random";
import type { SuspicionManager } from "./suspicion";
import type { MembershipStream, MembershipTransport } from "./transport";
import { isProbeEligibleState, type MemberRecord, type MembershipView } from "./view";
import {
  type AckMessage,
  decodePacketMessage,
  encodeMessage,
  MAX_PACKET_BYTES,
  MESSAGE_ACK,
  MESSAGE_GOSSIP,
  MESSAGE_NACK,
  MESSAGE_PING,
  MESSAGE_PING_REQ,
  type MemberState,
  type MembershipUpdate,
  membershipUpdateSize,
  type NackMessage,
  type PacketMessage,
  type PingMessage,
  type PingReqMessage,
  packetOverheadLength,
  STATE_ALIVE,
  UPDATE_LIST_MAX_RECORDS,
} from "./wire";

/** Base duration, in milliseconds, of one SWIM period at zero awareness penalty. @internal */
export const BASE_PROTOCOL_PERIOD_MS: number = 1_000;
/** Direct-ping budget, in milliseconds, before an owner asks indirect helpers. @internal */
export const BASE_DIRECT_TIMEOUT_MS: number = 500;
/** Maximum distinct alive, non-owner, non-target members selected as indirect helpers. @internal */
export const INDIRECT_HELPER_COUNT: number = 3;
/** Fraction of a helper's Lifeguard-scaled relay window elapsed before it reports a NACK. @internal */
export const HELPER_NACK_FRACTION: number = 0.8;
/** Inclusive upper bound for the local Lifeguard awareness penalty. @internal */
export const AWARENESS_MAX: number = 8;
/** Wall-clock interval, in milliseconds, between dedicated gossip attempts. @internal */
export const GOSSIP_INTERVAL_MS: number = 200;
/** Maximum eligible peers contacted by one dedicated gossip tick. @internal */
export const GOSSIP_FANOUT: number = 3;

/**
 * Stable evidence captured when an owner probe reaches its period deadline
 * without an ACK. Consumers must revalidate the target incarnation before
 * converting this observation into suspect truth.
 *
 * @internal
 */
export interface ProbeFailure {
  /** Canonical membership name of the member that did not acknowledge. */
  readonly target: string;
  /** Target incarnation captured at period start; it identifies the accused version. */
  readonly incarnation: number;
  /** Unsigned 32-bit owner-local sequence used to correlate direct and relayed responses. */
  readonly sequence: number;
  /** Lifeguard-scaled period duration, in milliseconds. */
  readonly effectivePeriod: number;
  /** Injected-clock timestamp, in milliseconds, at which this period began. */
  readonly periodStart: number;
}

/**
 * Synchronous integration seam through which probing delegates membership
 * mutations to the engine that owns the view.
 *
 * @internal
 */
export interface ProbeCallbacks {
  /** Applies decoded piggybacked updates before the enclosing packet is dispatched. */
  readonly updates: (updates: readonly MembershipUpdate[]) => void;
  /**
   * Attempts to install suspect truth for a failed probe. Returns `true` only
   * when the exact accused incarnation was applied and a suspicion timer should start.
   */
  readonly suspect: (failure: ProbeFailure) => boolean;
  /** Receives an inbound stream when this probe owns the transport listener; otherwise closes it. */
  readonly stream?: (from: string, stream: MembershipStream) => void | Promise<void>;
}

/** Long-lived dependencies and transport-listener ownership for one probe detector. @internal */
export interface ProbeOptions {
  /** Mutable view whose canonical member names and current states drive peer selection. */
  readonly view: MembershipView;
  /** Shared dissemination queue; selections remain owned by it until send settlement. */
  readonly broadcasts: BroadcastQueue;
  /** Monotonic injected clock; timestamps and delays throughout this component are milliseconds. */
  readonly clock: Clock;
  /** Random source used for the fair target walk, helper sampling, and initial sequence. */
  readonly random: Random;
  /** Shared manager started only after `callbacks.suspect` accepts a failure. */
  readonly suspicion: SuspicionManager;
  /** Packet transport, and optionally listener, whose address is the local member identity. */
  readonly transport: MembershipTransport;
  /** Synchronous callbacks invoked while handling packets or completing periods. */
  readonly callbacks: ProbeCallbacks;
  /** Whether `start`/`stop` bind and close the transport; defaults to `true`. */
  readonly manageTransport?: boolean;
}

/** Defensive, read-only diagnostic projection of the single active owner probe. @internal */
export interface OutstandingProbeSnapshot {
  /** Canonical member name currently being probed. */
  readonly target: string;
  /** Unsigned 32-bit owner-local correlation sequence. */
  readonly sequence: number;
  /** Injected-clock period start, in milliseconds. */
  readonly periodStart: number;
  /** Absolute injected-clock deadline, in milliseconds, for the direct phase. */
  readonly directDeadline: number;
  /** Absolute injected-clock deadline, in milliseconds, for the complete probe. */
  readonly periodDeadline: number;
  /** Lifeguard-scaled period duration, in milliseconds. */
  readonly effectivePeriod: number;
  /** Whether helper requests have already been issued for this probe. */
  readonly indirectStarted: boolean;
  /** Defensive array of selected helper member names, irrespective of response state. */
  readonly helpers: readonly string[];
  /** Whether any valid direct or indirect ACK arrived by the period deadline. */
  readonly succeeded: boolean;
}

/** Internal detector lifecycle; `paused` still accepts inbound packets during leave drain. */
type Lifecycle = "stopped" | "starting" | "started" | "paused" | "stopping";
/** Owner-side accounting for one selected helper's response. */
type HelperResponse = "waiting" | "ack" | "nack";

/** Mutable state for the sole probe initiated by this member in the current period. */
interface OwnerProbe {
  /** Canonical identity of the probed member. */
  readonly target: string;
  /** Target incarnation captured before the direct ping was sent. */
  readonly incarnation: number;
  /** Owner-local unsigned 32-bit correlation sequence. */
  readonly sequence: number;
  /** Absolute injected-clock start timestamp, in milliseconds. */
  readonly periodStart: number;
  /** Absolute deadline for switching to indirect probing, in milliseconds. */
  readonly directDeadline: number;
  /** Absolute deadline for deciding success or suspicion, in milliseconds. */
  readonly periodDeadline: number;
  /** Awareness-scaled period duration, in milliseconds. */
  readonly effectivePeriod: number;
  /** Selected helper identities and their latest terminal/waiting response. */
  readonly helpers: Map<string, HelperResponse>;
  /** Cancelable direct-phase deadline; cleared once fired or canceled. */
  directTimer: ClockTimer | undefined;
  /** Cancelable full-period deadline; owned until it fires or lifecycle cancellation. */
  periodTimer: ClockTimer | undefined;
  /** Guards helper selection and PING_REQ transmission against duplicate triggers. */
  indirectStarted: boolean;
  /** Sticky success bit set by the first matching ACK within the period. */
  succeeded: boolean;
}

/** Helper-side state for relaying one owner's indirect probe to one target. */
interface RelayProbe {
  /** Collision-free composite key for owner, target, and sequence. */
  readonly key: string;
  /** Canonical member that requested the indirect probe and receives ACK/NACK. */
  readonly owner: string;
  /** Canonical member being pinged on the owner's behalf. */
  readonly target: string;
  /** Owner-supplied sequence preserved across relay messages. */
  readonly sequence: number;
  /** Absolute injected-clock expiry of this relay attempt, in milliseconds. */
  readonly deadline: number;
  /** Timer for proactive NACK transmission before relay expiry. */
  nackTimer: ClockTimer | undefined;
  /** Timer that removes relay correlation state at the deadline. */
  cleanupTimer: ClockTimer | undefined;
  /** Sticky guard ensuring at most one NACK is attempted. */
  nackSent: boolean;
  /** Sticky guard set after a valid target ACK has been forwarded. */
  answered: boolean;
}

/** Builds an unambiguous in-memory correlation key; NUL cannot alias concatenated fields. */
function relayKey(owner: string, target: string, sequence: number): string {
  return `${owner}\0${target}\0${sequence}`;
}

/** Restricts a local-health score to the protocol's inclusive awareness range. */
function clampAwareness(score: number): number {
  return Math.max(0, Math.min(AWARENESS_MAX, score));
}

/**
 * SWIM probe, relay, awareness, and dedicated-gossip state machine.
 *
 * The transport promise represents local packet acceptance only. Every packed
 * broadcast selection is acknowledged from that promise, never from a remote
 * response.
 *
 * @internal
 */
export class Probe {
  /** Shared mutable membership view; this class reads but delegates truth mutation. */
  readonly #view: MembershipView;
  /** Shared queue supplying and tracking piggyback selections. */
  readonly #broadcasts: BroadcastQueue;
  /** Sole source of detector time and timers. */
  readonly #clock: Clock;
  /** Sole source of nondeterministic selection and initial sequencing. */
  readonly #random: Random;
  /** Timer owner for accepted local suspicions. */
  readonly #suspicion: SuspicionManager;
  /** Packet I/O dependency and optional listener lifecycle resource. */
  readonly #transport: MembershipTransport;
  /** Synchronous bridge to composed membership state. */
  readonly #callbacks: ProbeCallbacks;
  /** Whether this instance starts and stops `#transport`. */
  readonly #manageTransport: boolean;
  /** Active helper-side relay attempts keyed by owner/target/sequence. */
  readonly #relayProbes: Map<string, RelayProbe> = new Map<string, RelayProbe>();
  /** Every timer currently owned by this detector, for generation-safe cancellation. */
  readonly #timers: Set<ClockTimer> = new Set<ClockTimer>();
  /** Current lifecycle gate for scheduling and packet acceptance. */
  #lifecycle: Lifecycle = "stopped";
  /** Shared promise returned to concurrent callers while transport binding is pending. */
  #startPromise: Promise<void> | undefined;
  /** Shared promise returned to concurrent callers while shutdown is pending. */
  #stopPromise: Promise<void> | undefined;
  /** Epoch invalidating callbacks scheduled by an earlier lifecycle. */
  #generation: number = 0;
  /** Local Lifeguard penalty in `[0, AWARENESS_MAX]`. */
  #awareness: number = 0;
  /** Next owner-local unsigned 32-bit sequence, wrapping modulo 2^32. */
  #nextSequence: number;
  /** At most one owner probe exists per protocol period. */
  #outstanding: OwnerProbe | undefined;
  /** Randomized queue of eligible names not yet visited in the current walk. */
  #walk: string[] = [];
  /** Names admitted to the current walk, including names already visited. */
  #walkMembers: Set<string> = new Set<string>();

  /**
   * Captures shared dependencies and seeds the owner-local probe sequence.
   * No listener, timer, or packet I/O is started by construction.
   */
  constructor(options: ProbeOptions) {
    this.#view = options.view;
    this.#broadcasts = options.broadcasts;
    this.#clock = options.clock;
    this.#random = options.random;
    this.#suspicion = options.suspicion;
    this.#transport = options.transport;
    this.#callbacks = options.callbacks;
    this.#manageTransport = options.manageTransport ?? true;
    this.#nextSequence = this.#random.integer(0x1_0000_0000);
  }

  /** Whether this generation accepts packets; `true` for both active and leave-drain states. */
  get started(): boolean {
    return this.#lifecycle === "started" || this.#lifecycle === "paused";
  }

  /** Current bounded Lifeguard local-health penalty; larger values lengthen probe deadlines. */
  get awareness(): number {
    return this.#awareness;
  }

  /** Multiplicative deadline scale, always `awareness + 1` and therefore in `[1, 9]`. */
  get scale(): number {
    return this.#awareness + 1;
  }

  /** Defensive snapshot of eligible names not yet selected in the current randomized walk. */
  get walkRemaining(): readonly string[] {
    return Array.from(this.#walk);
  }

  /** Defensive diagnostic snapshot, or `undefined` between periods and while inactive. */
  get outstanding(): OutstandingProbeSnapshot | undefined {
    const probe: OwnerProbe | undefined = this.#outstanding;
    if (probe === undefined) {
      return undefined;
    }

    return {
      target: probe.target,
      sequence: probe.sequence,
      periodStart: probe.periodStart,
      directDeadline: probe.directDeadline,
      periodDeadline: probe.periodDeadline,
      effectivePeriod: probe.effectivePeriod,
      indirectStarted: probe.indirectStarted,
      helpers: Array.from(probe.helpers.keys()),
      succeeded: probe.succeeded,
    };
  }

  /** Increments the bounded local-health penalty after self-refutation and returns the new score. */
  selfRefute(): number {
    this.#changeAwareness(1);
    return this.#awareness;
  }

  /**
   * Stops scheduled detector and gossip work while preserving inbound packet
   * handling for a graceful-leave drain. Idempotent outside `started`; drops
   * owner/relay correlation state but does not stop transport or suspicion.
   */
  pause(): void {
    if (this.#lifecycle !== "started") {
      return;
    }

    this.#lifecycle = "paused";
    this.#generation += 1;
    this.#cancelAllTimers();
    this.#outstanding = undefined;
    this.#relayProbes.clear();
  }

  /**
   * Starts optional listener ownership, then the probe-period and gossip loops.
   * Concurrent starts share one promise; rejects while stopping and propagates
   * transport-start failure after restoring `stopped`.
   */
  start(): Promise<void> {
    if (this.#lifecycle === "started") {
      return Promise.resolve();
    }

    if (this.#lifecycle === "starting") {
      return this.#startPromise as Promise<void>;
    }

    if (this.#lifecycle === "stopping") {
      return Promise.reject(new Error("probe is stopping"));
    }

    this.#lifecycle = "starting";
    const generation: number = this.#generation + 1;
    this.#generation = generation;
    const bind: Promise<void> = this.#manageTransport
      ? this.#transport.start({
          packet: (from: string, bytes: Uint8Array): void => {
            this.receivePacket(from, bytes);
          },
          stream: (from: string, stream: MembershipStream): void | Promise<void> => {
            const handler: ProbeCallbacks["stream"] = this.#callbacks.stream;
            if (handler === undefined) {
              stream.close();
              return;
            }

            return handler(from, stream);
          },
        })
      : Promise.resolve();
    const start: Promise<void> = bind
      .then((): void => {
        if (this.#lifecycle !== "starting" || this.#generation !== generation) {
          return;
        }

        this.#lifecycle = "started";
        this.#startPeriod(generation);
        this.#scheduleGossip(generation);
      })
      .catch((error: unknown): never => {
        if (this.#lifecycle === "starting") {
          this.#lifecycle = "stopped";
        }

        throw error;
      });
    this.#startPromise = start;
    return start;
  }

  /**
   * Invalidates scheduled work, cancels all suspicion, and optionally stops the
   * transport. Concurrent stops share one promise; start failure is swallowed
   * so resource cleanup still runs, while transport-stop failure is propagated.
   */
  stop(): Promise<void> {
    if (this.#lifecycle === "stopped") {
      return Promise.resolve();
    }

    if (this.#lifecycle === "stopping") {
      return this.#stopPromise as Promise<void>;
    }

    const starting: Promise<void> | undefined =
      this.#lifecycle === "starting" ? this.#startPromise : undefined;
    this.#lifecycle = "stopping";
    this.#generation += 1;
    this.#cancelAllTimers();
    this.#suspicion.cancelAll();
    this.#outstanding = undefined;
    this.#relayProbes.clear();
    const stop: Promise<void> = (starting ?? Promise.resolve())
      .catch((): void => undefined)
      .then(
        (): Promise<void> => (this.#manageTransport ? this.#transport.stop() : Promise.resolve()),
      )
      .finally((): void => {
        this.#lifecycle = "stopped";
        this.#startPromise = undefined;
        this.#stopPromise = undefined;
      });
    this.#stopPromise = stop;
    return stop;
  }

  /**
   * Routes bytes delivered by an engine-owned listener through the current
   * generation. Inactive or malformed packets are silently discarded.
   */
  receivePacket(from: string, bytes: Uint8Array): void {
    this.#receivePacket(this.#generation, from, bytes);
  }

  /** Applies a signed Lifeguard score delta while preserving the bounded invariant. */
  #changeAwareness(delta: number): void {
    this.#awareness = clampAwareness(this.#awareness + delta);
  }

  /** Schedules a millisecond delay and removes the timer from ownership before callback entry. */
  #schedule(delay: number, callback: () => void): ClockTimer {
    let timer: ClockTimer;
    timer = this.#clock.schedule(delay, (): void => {
      this.#timers.delete(timer);
      callback();
    });

    this.#timers.add(timer);
    return timer;
  }

  /** Cancels one optional owned timer and removes it from lifecycle tracking. */
  #cancel(timer: ClockTimer | undefined): void {
    if (timer === undefined) {
      return;
    }

    this.#clock.cancel(timer);
    this.#timers.delete(timer);
  }

  /** Cancels every detector timer and clears relay timer handles without changing lifecycle. */
  #cancelAllTimers(): void {
    for (const timer of this.#timers) {
      this.#clock.cancel(timer);
    }

    this.#timers.clear();
    for (const relay of this.#relayProbes.values()) {
      relay.nackTimer = undefined;
      relay.cleanupTimer = undefined;
    }
  }

  /** Returns current non-self members whose states permit failure detection. */
  #eligibleProbeNames(): string[] {
    const self: string = this.#view.selfName;
    const names: string[] = [];
    this.#view.eachMember((member: string, state: MemberState): void => {
      if (member !== self && isProbeEligibleState(state)) {
        names.push(member);
      }
    });

    return names;
  }

  /**
   * Selects the next eligible target without replacement within a randomized
   * walk, admitting newly discovered members at random positions. A pending
   * entry that turned ineligible is forgotten entirely, so a member that
   * becomes eligible again mid-walk is reinserted uniformly instead of waiting
   * for the next reshuffle.
   */
  #nextTarget(): MemberRecord | undefined {
    const eligible: string[] = this.#eligibleProbeNames();
    const eligibleSet: Set<string> = new Set(eligible);
    const retained: string[] = [];
    for (const name of this.#walk) {
      if (eligibleSet.has(name)) {
        retained.push(name);
      } else {
        this.#walkMembers.delete(name);
      }
    }

    this.#walk = retained;
    for (const name of eligible) {
      if (this.#walkMembers.has(name)) {
        continue;
      }

      const position: number = this.#random.integer(this.#walk.length + 1);
      this.#walk.splice(position, 0, name);
      this.#walkMembers.add(name);
    }

    if (this.#walk.length === 0) {
      this.#walk = this.#random.shuffle(eligible);
      this.#walkMembers = new Set(eligible);
    }

    while (this.#walk.length > 0) {
      const target: string = this.#walk.shift() as string;
      const record: MemberRecord | undefined = this.#view.get(target);
      if (record !== undefined && isProbeEligibleState(record.state)) {
        return record;
      }
    }

    return undefined;
  }

  /** Starts one scaled protocol period, or schedules an idle period when no target exists. */
  #startPeriod(generation: number): void {
    if (!this.#active(generation)) {
      return;
    }

    const periodStart: number = this.#clock.now();
    const scale: number = this.scale;
    const effectivePeriod: number = BASE_PROTOCOL_PERIOD_MS * scale;
    const record: MemberRecord | undefined = this.#nextTarget();
    if (record === undefined) {
      this.#scheduleNextPeriod(effectivePeriod, generation);
      return;
    }

    const probe: OwnerProbe = this.#createOwnerProbe(
      record.member,
      record.incarnation,
      periodStart,
      scale,
      effectivePeriod,
    );
    this.#outstanding = probe;
    this.#sendDirectProbe(probe, generation);
    this.#scheduleProbeDeadlines(probe, generation);
  }

  /** Schedules the next period after `delay` milliseconds in the same generation. */
  #scheduleNextPeriod(delay: number, generation: number): void {
    this.#schedule(delay, (): void => {
      this.#startPeriod(generation);
    });
  }

  /** Allocates owner correlation state and advances the wrapping sequence counter. */
  #createOwnerProbe(
    target: string,
    incarnation: number,
    periodStart: number,
    scale: number,
    effectivePeriod: number,
  ): OwnerProbe {
    const sequence: number = this.#nextSequence;
    this.#nextSequence = (this.#nextSequence + 1) >>> 0;

    return {
      target,
      incarnation,
      sequence,
      periodStart,
      directDeadline: periodStart + BASE_DIRECT_TIMEOUT_MS * scale,
      periodDeadline: periodStart + effectivePeriod,
      effectivePeriod,
      helpers: new Map<string, HelperResponse>(),
      directTimer: undefined,
      periodTimer: undefined,
      indirectStarted: false,
      succeeded: false,
    };
  }

  /**
   * Sends the initial PING with target-specific buddy evidence. A local send
   * rejection immediately begins indirect probing; remote ACK is unrelated to send settlement.
   */
  #sendDirectProbe(probe: OwnerProbe, generation: number): void {
    const ping: PingMessage = {
      type: MESSAGE_PING,
      sequence: probe.sequence,
      owner: this.#view.selfName,
      relay: "",
      updates: [],
    };

    this.#sendPacked(probe.target, ping, this.#indictmentFor(probe.target), (): void => {
      if (this.#outstanding === probe && !probe.succeeded) {
        this.#beginIndirect(probe, generation);
      }
    });
  }

  /**
   * Arms independent direct-phase and whole-period deadlines for an owner
   * probe. Delays clamp at zero so a deadline that already passed fires on the
   * next timer turn instead of faulting the period loop with a negative delay.
   */
  #scheduleProbeDeadlines(probe: OwnerProbe, generation: number): void {
    const now: number = this.#clock.now();
    probe.directTimer = this.#schedule(Math.max(0, probe.directDeadline - now), (): void => {
      probe.directTimer = undefined;
      this.#beginIndirect(probe, generation);
    });

    probe.periodTimer = this.#schedule(Math.max(0, probe.periodDeadline - now), (): void => {
      probe.periodTimer = undefined;
      this.#finishPeriod(probe, generation);
    });
  }

  /** Selects alive helpers once and sends each a PING_REQ for the remaining period. */
  #beginIndirect(probe: OwnerProbe, generation: number): void {
    if (
      !this.#active(generation) ||
      this.#outstanding !== probe ||
      probe.succeeded ||
      probe.indirectStarted
    ) {
      return;
    }

    probe.indirectStarted = true;
    this.#cancel(probe.directTimer);
    probe.directTimer = undefined;
    const helpers: string[] = this.#random
      .shuffle(
        this.#view
          .members()
          .filter(
            (member): boolean =>
              member.state === STATE_ALIVE &&
              member.member !== this.#view.selfName &&
              member.member !== probe.target,
          )
          .map((member): string => member.member),
      )
      .slice(0, INDIRECT_HELPER_COUNT);
    for (const helper of helpers) {
      probe.helpers.set(helper, "waiting");
      const request: PingReqMessage = {
        type: MESSAGE_PING_REQ,
        sequence: probe.sequence,
        owner: this.#view.selfName,
        target: probe.target,
        updates: [],
      };

      this.#sendPacked(helper, request);
    }
  }

  /**
   * Finalizes the current period: scores local health, conditionally reports
   * failure, then starts the next period immediately from callback time.
   */
  #finishPeriod(probe: OwnerProbe, generation: number): void {
    if (!this.#active(generation) || this.#outstanding !== probe) {
      return;
    }

    this.#cancel(probe.directTimer);
    this.#outstanding = undefined;
    this.#scoreCompletedPeriod(probe);
    this.#applyProbeFailure(probe);
    this.#startPeriod(generation);
  }

  /**
   * Rewards an ACK with a single improvement and penalizes a failed probe.
   * A failure with selected helpers is scored solely by how many stayed silent:
   * responsive helpers prove local timing was healthy, so an all-NACK failure
   * costs nothing, while a failure with no helpers costs the base penalty.
   * Successful periods never charge for slow helpers.
   */
  #scoreCompletedPeriod(probe: OwnerProbe): void {
    if (probe.succeeded) {
      this.#changeAwareness(-1);
      return;
    }

    if (probe.helpers.size === 0) {
      this.#changeAwareness(1);
      return;
    }

    let silent: number = 0;
    for (const response of probe.helpers.values()) {
      if (response === "waiting") {
        silent += 1;
      }
    }

    this.#changeAwareness(silent);
  }

  /**
   * Reports an unanswered probe to the engine and starts the local suspicion
   * timer only when the engine accepted the evidence. The callback is the sole
   * validator of the captured incarnation against current truth; the detector
   * performs no revalidation of its own, so the rule cannot drift between two
   * copies.
   */
  #applyProbeFailure(probe: OwnerProbe): void {
    if (probe.succeeded) {
      return;
    }

    const applied: boolean = this.#callbacks.suspect({
      target: probe.target,
      incarnation: probe.incarnation,
      sequence: probe.sequence,
      effectivePeriod: probe.effectivePeriod,
      periodStart: probe.periodStart,
    });

    if (!applied) {
      return;
    }

    this.#suspicion.start({
      member: probe.target,
      incarnation: probe.incarnation,
      reporter: this.#view.selfName,
      memberCount: this.#view.aliveOrSuspectCount(),
      effectivePeriod: probe.effectivePeriod,
    });
  }

  /** Arms the recurring dedicated-gossip tick for the supplied lifecycle generation. */
  #scheduleGossip(generation: number): void {
    this.#schedule(GOSSIP_INTERVAL_MS, (): void => {
      if (!this.#active(generation)) {
        return;
      }

      this.#gossip(generation);
      this.#scheduleGossip(generation);
    });
  }

  /** Sends queued or buddy truth to at most `GOSSIP_FANOUT` randomized eligible peers. */
  #gossip(generation: number): void {
    const now: number = this.#clock.now();
    const candidates: string[] = [];
    this.#view.eachMember((member: string, state: MemberState): void => {
      if (
        member !== this.#view.selfName &&
        this.#view.isGossipEligible(member, now) &&
        (this.#broadcasts.size > 0 || state !== STATE_ALIVE)
      ) {
        candidates.push(member);
      }
    });

    const targets: string[] = this.#random.shuffle(candidates).slice(0, GOSSIP_FANOUT);
    for (const target of targets) {
      if (!this.#active(generation)) {
        break;
      }

      const buddy: MembershipUpdate | undefined = this.#indictmentFor(target);
      if (this.#broadcasts.size === 0 && buddy === undefined) {
        continue;
      }

      this.#sendPacked(target, { type: MESSAGE_GOSSIP, updates: [] }, buddy);
    }
  }

  /** Returns current non-alive truth about `member` for direct buddy dissemination. */
  #indictmentFor(member: string): MembershipUpdate | undefined {
    const current: { readonly state: MemberState; readonly incarnation: number } | undefined =
      this.#view.stateOf(member);
    if (current === undefined || current.state === STATE_ALIVE) {
      return undefined;
    }

    // Only a non-alive record ships, so the detached snapshot is built on demand.
    return this.#view.get(member);
  }

  /**
   * Packs queue-owned updates into the packet byte budget, optionally forcing
   * buddy truth, and transfers send-settlement accounting to `#sendSelection`.
   */
  #sendPacked(
    to: string,
    message: PacketMessage,
    buddy?: MembershipUpdate,
    onRejected?: () => void,
  ): void {
    const baseLength: number = packetOverheadLength(message);
    const selection: BroadcastSelection =
      buddy === undefined
        ? this.#broadcasts.pack(MAX_PACKET_BYTES - baseLength)
        : this.#broadcasts.pack(MAX_PACKET_BYTES - baseLength, { buddy });
    const bytes: Uint8Array = encodeMessage({ ...message, updates: selection.updates });
    this.#sendSelection(to, bytes, selection, onRejected);
  }

  /**
   * Attempts packet delivery and acknowledges the queue selection from local
   * transport settlement. Synchronous throws and promise rejection are equivalent.
   */
  #sendSelection(
    to: string,
    bytes: Uint8Array,
    selection: BroadcastSelection,
    onRejected?: () => void,
  ): void {
    let sending: Promise<void>;
    try {
      sending = this.#transport.packet(to, bytes);
    } catch {
      this.#broadcasts.acknowledge(selection, false);
      onRejected?.();
      return;
    }

    void sending.then(
      (): void => {
        this.#broadcasts.acknowledge(selection, true);
      },
      (): void => {
        this.#broadcasts.acknowledge(selection, false);
        onRejected?.();
      },
    );
  }

  /**
   * Decodes and applies piggybacked updates before dispatching packet semantics.
   * Unknown generations, inactive lifecycle, and malformed bytes have no side effects.
   */
  #receivePacket(generation: number, from: string, bytes: Uint8Array): void {
    if (!this.#active(generation)) {
      return;
    }

    let message: PacketMessage;
    try {
      message = decodePacketMessage(bytes);
    } catch {
      return;
    }

    this.#callbacks.updates(message.updates);
    switch (message.type) {
      case MESSAGE_PING:
        this.#receivePing(from, message);
        break;
      case MESSAGE_PING_REQ:
        this.#receivePingReq(message, generation);
        break;
      case MESSAGE_ACK:
        this.#receiveAck(from, message);
        break;
      case MESSAGE_NACK:
        this.#receiveNack(message);
        break;
      case MESSAGE_GOSSIP:
        break;
    }
  }

  /** Replies to a direct or relayed PING, including buddy evidence only for a direct owner. */
  #receivePing(from: string, message: PingMessage): void {
    const destination: string = message.relay.length === 0 ? message.owner : message.relay;
    const ack: AckMessage = {
      type: MESSAGE_ACK,
      sequence: message.sequence,
      owner: message.owner,
      target: this.#view.selfName,
      updates: [],
    };

    const buddy: MembershipUpdate | undefined =
      message.relay.length === 0 && from === message.owner ? this.#indictmentFor(from) : undefined;
    this.#sendPacked(destination, ack, buddy);
  }

  /**
   * Creates one deduplicated helper relay attempt, pings the target, schedules
   * an early NACK, and retains correlation state through the relay deadline.
   */
  #receivePingReq(message: PingReqMessage, generation: number): void {
    const key: string = relayKey(message.owner, message.target, message.sequence);
    const previous: RelayProbe | undefined = this.#relayProbes.get(key);
    if (previous !== undefined) {
      return;
    }

    const relayWindow: number = BASE_DIRECT_TIMEOUT_MS * this.scale;
    const relay: RelayProbe = {
      key,
      owner: message.owner,
      target: message.target,
      sequence: message.sequence,
      deadline: this.#clock.now() + relayWindow,
      nackTimer: undefined,
      cleanupTimer: undefined,
      nackSent: false,
      answered: false,
    };

    this.#relayProbes.set(key, relay);
    const ping: PingMessage = {
      type: MESSAGE_PING,
      sequence: relay.sequence,
      owner: relay.owner,
      relay: this.#view.selfName,
      updates: [],
    };

    this.#sendPacked(relay.target, ping, this.#indictmentFor(relay.target), (): void => {
      this.#sendRelayNack(relay, generation);
    });

    relay.nackTimer = this.#schedule(Math.floor(HELPER_NACK_FRACTION * relayWindow), (): void => {
      relay.nackTimer = undefined;
      this.#sendRelayNack(relay, generation);
    });

    relay.cleanupTimer = this.#schedule(relayWindow, (): void => {
      relay.cleanupTimer = undefined;
      if (this.#relayProbes.get(key) === relay) {
        this.#relayProbes.delete(key);
      }
    });
  }

  /** Sends at most one NACK for an unanswered, current relay attempt. */
  #sendRelayNack(relay: RelayProbe, generation: number): void {
    if (
      !this.#active(generation) ||
      this.#relayProbes.get(relay.key) !== relay ||
      relay.answered ||
      relay.nackSent
    ) {
      return;
    }

    relay.nackSent = true;
    const nack: NackMessage = {
      type: MESSAGE_NACK,
      sequence: relay.sequence,
      owner: relay.owner,
      target: relay.target,
      helper: this.#view.selfName,
      updates: [],
    };

    this.#sendPacked(relay.owner, nack);
  }

  /** Classifies a decoded ACK as owner completion first, otherwise as helper relay completion. */
  #receiveAck(from: string, message: AckMessage): void {
    if (this.#receiveOwnerAck(from, message)) {
      return;
    }

    this.#receiveRelayAck(message);
  }

  /**
   * Accepts an ACK only for the current owner/target/sequence before deadline.
   * Returns whether it matched, and records helper attribution when `from` is selected.
   */
  #receiveOwnerAck(from: string, message: AckMessage): boolean {
    const probe: OwnerProbe | undefined = this.#outstanding;
    if (
      probe === undefined ||
      message.owner !== this.#view.selfName ||
      message.target !== probe.target ||
      message.sequence !== probe.sequence ||
      this.#clock.now() > probe.periodDeadline
    ) {
      return false;
    }

    probe.succeeded = true;
    this.#cancel(probe.directTimer);
    probe.directTimer = undefined;
    if (probe.helpers.has(from)) {
      probe.helpers.set(from, "ack");
    }

    return true;
  }

  /**
   * Forwards a valid target ACK to the relay owner once, preserving received
   * updates before filling the remaining packet budget with local broadcasts.
   */
  #receiveRelayAck(message: AckMessage): void {
    const key: string = relayKey(message.owner, message.target, message.sequence);
    const relay: RelayProbe | undefined = this.#relayProbes.get(key);
    if (
      relay === undefined ||
      relay.target !== message.target ||
      relay.owner !== message.owner ||
      relay.sequence !== message.sequence ||
      relay.answered ||
      this.#clock.now() > relay.deadline
    ) {
      return;
    }

    relay.answered = true;
    this.#cancel(relay.nackTimer);
    relay.nackTimer = undefined;
    const forwarded: AckMessage = {
      type: MESSAGE_ACK,
      sequence: message.sequence,
      owner: message.owner,
      target: message.target,
      updates: message.updates,
    };

    const baseLength: number = packetOverheadLength(forwarded);
    const forwardedBytes: number = message.updates.reduce(
      (total: number, update: MembershipUpdate): number => total + membershipUpdateSize(update),
      0,
    );
    const selection: BroadcastSelection = this.#broadcasts.pack(
      MAX_PACKET_BYTES - baseLength - forwardedBytes,
      {
        maxRecords: UPDATE_LIST_MAX_RECORDS - message.updates.length,
      },
    );

    this.#sendSelection(
      relay.owner,
      encodeMessage({ ...forwarded, updates: [...message.updates, ...selection.updates] }),
      selection,
    );
  }

  /** Records the first timely NACK from a helper selected for the current owner probe. */
  #receiveNack(message: NackMessage): void {
    const probe: OwnerProbe | undefined = this.#outstanding;
    if (
      probe === undefined ||
      message.owner !== this.#view.selfName ||
      message.target !== probe.target ||
      message.sequence !== probe.sequence ||
      !probe.helpers.has(message.helper) ||
      this.#clock.now() > probe.periodDeadline
    ) {
      return;
    }

    if (probe.helpers.get(message.helper) === "waiting") {
      probe.helpers.set(message.helper, "nack");
    }
  }

  /** Tests lifecycle packet acceptance and callback epoch equality. */
  #active(generation: number): boolean {
    return (
      (this.#lifecycle === "started" || this.#lifecycle === "paused") &&
      this.#generation === generation
    );
  }
}
