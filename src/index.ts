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
export { Context } from "./context";
export { discardLogger } from "./discard.logger";
export {
  ActorInitializationError,
  ActorNotFoundError,
  ActorNotRegisteredError,
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrDead,
  ErrFanOutAsk,
  ErrInvalidActorName,
  ErrInvalidActorSystemName,
  ErrInvalidInterval,
  ErrInvalidPoolSize,
  ErrInvalidReentrancyMode,
  ErrInvalidRouteeDirective,
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
export {
  type SenderKeyFunc,
  UnboundedFairMailbox,
} from "./fair.mailbox";
export { defaultLogger, JsonLogger, type JsonLoggerOptions } from "./json.logger";
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
  type RouteeDirective,
  type RouterOptions,
  type RoutingKeyFunc,
  type RoutingStrategy,
} from "./router.options";
export type { ScheduleOptions } from "./schedule.options";
export { UnboundedSegmentedMailbox } from "./segmented.mailbox";
export type { SpawnOptions } from "./spawn.options";
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
export { UnboundedMailbox } from "./unbounded.mailbox";
