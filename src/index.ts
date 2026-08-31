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

export type { Actor } from "./actor";
export { ActorSystem } from "./actor.system";
export type { ActorSystemOptions } from "./actor.system.options";
export type { Behavior } from "./behavior.stack";
export { BoundedMailbox } from "./bounded.mailbox";
export {
  CoordinatorChanged,
  NodeJoined,
  NodeLeft,
  RebalanceCompleted,
  RebalanceStarted,
  RelocationCompleted,
  RelocationFailed,
  RelocationStarted,
} from "./cluster.events";
export type { ClusterOptions } from "./cluster.options";
export type { PlacementStrategy } from "./clustering.strategy";
export { Context } from "./context";
export { discardLogger } from "./discard.logger";
export {
  DnsDiscovery,
  type DnsDiscoveryOptions,
  DnsRecordType,
  type DnsRecordTypeValue,
  type DnsResolver,
  type DnsSrvRecord,
} from "./discovery/dns";
export type { DiscoveryProvider } from "./discovery/provider";
export { StaticDiscovery } from "./discovery/static";
export {
  ActorInitializationError,
  ActorNotFoundError,
  ActorNotRegisteredError,
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrClusteringDisabled,
  ErrClusterRequiresRemote,
  ErrClusterRequiresRoutableHost,
  ErrDead,
  ErrExtensionAlreadyExists,
  ErrFanOutAsk,
  ErrInvalidActorName,
  ErrInvalidActorSystemName,
  ErrInvalidExtensionId,
  ErrInvalidInterval,
  ErrInvalidPoolSize,
  ErrInvalidReentrancyMode,
  ErrInvalidRoutingStrategy,
  ErrInvalidTimeout,
  ErrMailboxDisposed,
  ErrMailboxFull,
  ErrNameRequired,
  ErrPipeTimeout,
  ErrReentrancyDisabled,
  ErrReentrancyInFlightLimit,
  ErrRemotingDisabled,
  ErrRequestCanceled,
  ErrRequestTimeout,
  ErrReservedName,
  ErrRoutingKeyRequired,
  ErrScheduleAlreadyExists,
  ErrScheduleNotFound,
  ErrStashBufferEmpty,
  ErrUndefinedActor,
  ErrUndefinedTask,
  ErrUnhandled,
} from "./errors";
export { EventStream, type StreamSubscriber } from "./eventstream";
export type { Extension } from "./extension/extension";
export {
  type SenderKeyFunc,
  UnboundedFairMailbox,
} from "./fair.mailbox";
export { JsonLogger, type JsonLoggerOptions } from "./json.logger";
export type { EntryLevel, Fields, LazyFields, Level, Logger } from "./logger";
export type { Mailbox } from "./mailbox";
export {
  ActorChildCreated,
  ActorPassivated,
  ActorReinstated,
  ActorRestarted,
  ActorStarted,
  ActorStopped,
  ActorSuspended,
  Deadletter,
  PanicSignal,
  PoisonPill,
  PostStart,
  Terminated,
} from "./messages";
export type { MetricsOptions } from "./observability/metric.options";
export type {
  ActorFleetMetrics,
  ActorMetrics,
  HistogramBucket,
  HistogramData,
  MailboxMetrics,
  MessageMetrics,
  MetricsSnapshot,
} from "./observability/metric.snapshot";
export {
  DefaultPassivationTimeout,
  LongLivedStrategy,
  MessagesCountBasedStrategy,
  type PassivationStrategy,
  TimeBasedStrategy,
} from "./passivation";
export type { Path } from "./path";
export { PID } from "./pid";
export type { PipeTask } from "./pipe";
export type { PipeOptions } from "./pipe.options";
export {
  BoundedPriorityMailbox,
  BoundedStablePriorityMailbox,
  type PriorityFunc,
  UnboundedPriorityMailbox,
  UnboundedStablePriorityMailbox,
} from "./priority.mailbox";
export { Props } from "./props";
export { ReceiveContext } from "./receive.context";
export type {
  Reentrancy,
  ReentrancyMode,
  RequestCall,
  RequestOptions,
} from "./reentrancy";
export { registerActor, registerMessage } from "./registration";
export type { RemoteOptions, TlsOptions } from "./remote.options";
export { AdjustRouterPoolSize, GetRoutees, Routees } from "./router.messages";
export {
  ConsistentHashRouting,
  FanOutRouting,
  RandomRouting,
  RoundRobinRouting,
  type RouterOptions,
  type RoutingKeyFunc,
  type RoutingStrategy,
} from "./router.options";
export type { ScheduleOptions } from "./schedule.options";
export { UnboundedSegmentedMailbox } from "./segmented.mailbox";
export type { SpawnOnOptions, SpawnOptions } from "./spawn.options";
export {
  type Directive,
  type ErrorClass,
  EscalateDirective,
  OneForAllStrategy,
  OneForOneStrategy,
  RestartDirective,
  ResumeDirective,
  StopDirective,
  type Strategy,
  Supervisor,
  type SupervisorOptions,
} from "./supervisor";
export { defaultLogger, TextLogger, type TextLoggerOptions } from "./text.logger";
export { UnboundedMailbox } from "./unbounded.mailbox";
