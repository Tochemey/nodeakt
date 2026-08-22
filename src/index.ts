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

export type { Actor } from "./actor/actor";
export { ActorSystem } from "./actor/actor.system";
export type { ActorSystemOptions } from "./actor/actor.system.options";
export type { Behavior } from "./actor/behavior.stack";
export { BoundedMailbox } from "./actor/bounded.mailbox";
export { Context } from "./actor/context";
export {
  type SenderKeyFunc,
  UnboundedFairMailbox,
} from "./actor/fair.mailbox";
export type { Mailbox } from "./actor/mailbox";
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
} from "./actor/messages";
export {
  DefaultPassivationTimeout,
  LongLivedStrategy,
  MessagesCountBasedStrategy,
  type PassivationStrategy,
  TimeBasedStrategy,
} from "./actor/passivation";
export type { Path } from "./actor/path";
export { PID } from "./actor/pid";
export type { PipeTask } from "./actor/pipe";
export type { PipeOptions } from "./actor/pipe.options";
export {
  BoundedPriorityMailbox,
  BoundedStablePriorityMailbox,
  type PriorityFunc,
  UnboundedPriorityMailbox,
  UnboundedStablePriorityMailbox,
} from "./actor/priority.mailbox";
export { Props } from "./actor/props";
export { ReceiveContext } from "./actor/receive.context";
export type {
  Reentrancy,
  ReentrancyMode,
  RequestCall,
  RequestOptions,
} from "./actor/reentrancy";
export { UnboundedSegmentedMailbox } from "./actor/segmented.mailbox";
export type { SpawnOptions } from "./actor/spawn.options";
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
} from "./actor/supervisor";
export { UnboundedMailbox } from "./actor/unbounded.mailbox";
export {
  ActorInitializationError,
  ActorNotFoundError,
  ActorNotRegisteredError,
  ErrActorAlreadyExists,
  ErrActorSystemNotStarted,
  ErrDead,
  ErrInvalidActorName,
  ErrInvalidActorSystemName,
  ErrInvalidReentrancyMode,
  ErrInvalidTimeout,
  ErrMailboxDisposed,
  ErrMailboxFull,
  ErrNameRequired,
  ErrPipeTimeout,
  ErrReentrancyDisabled,
  ErrReentrancyInFlightLimit,
  ErrRequestCanceled,
  ErrRequestTimeout,
  ErrReservedName,
  ErrStashBufferEmpty,
  ErrUndefinedActor,
  ErrUndefinedTask,
  ErrUnhandled,
} from "./errors/errors";
export { EventStream, type StreamSubscriber } from "./eventstream/eventstream";
export { discardLogger } from "./logger/discard.logger";
export { defaultLogger, JsonLogger, type JsonLoggerOptions } from "./logger/json.logger";
export type { EntryLevel, Fields, LazyFields, Level, Logger } from "./logger/logger";
export { registerActor, registerMessage } from "./runtime/registration";
