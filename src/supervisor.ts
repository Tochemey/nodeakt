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

/**
 * Strategy determines the scope of a supervision decision: whether a
 * directive applies to the failing actor alone or to the failing actor and
 * all of its siblings.
 */
export type Strategy = "oneForOne" | "oneForAll";

/** Applies the directive to the failing actor only; its siblings keep
 * running unaffected. */
export const OneForOneStrategy: Strategy = "oneForOne";

/**
 * Applies the directive to the failing actor and every sibling under the
 * same parent. Appropriate when the children are tightly coupled and the
 * malfunction of one compromises the ensemble.
 */
export const OneForAllStrategy: Strategy = "oneForAll";

/**
 * Directive is the action a supervisor takes when an actor fails while
 * processing a message.
 */
export type Directive = "stop" | "resume" | "restart" | "escalate";

/** Stops the failing actor. Used when a failure is irrecoverable or the
 * actor's state cannot be safely resumed. */
export const StopDirective: Directive = "stop";

/** Resumes the failing actor without a state reset. Used for transient
 * failures the actor can simply move past. */
export const ResumeDirective: Directive = "resume";

/** Restarts the failing actor: its state is torn down and re-initialized
 * through its lifecycle hooks. */
export const RestartDirective: Directive = "restart";

/** Escalates the failure to the failing actor's grandparent level: the
 * parent surfaces a `PanicSignal` to its own behavior and the failing
 * actor stays suspended. */
export const EscalateDirective: Directive = "escalate";

/** An error class usable as a directive rule key. */
export type ErrorClass = new (...args: never[]) => Error;

/** Options configuring a {@link Supervisor}. */
export interface SupervisorOptions {
  /** The supervision strategy; {@link OneForOneStrategy} when omitted. */
  strategy?: Strategy;

  /**
   * Directive rules keyed by error class. A rule matches a thrown error
   * whose constructor is exactly the given class; subclasses do not
   * match. Ignored when {@link anyErrorDirective} is set.
   */
  directives?: Iterable<readonly [ErrorClass, Directive]>;

  /**
   * The directive applied to every failure regardless of its error type.
   * Setting it clears any error-specific rules.
   */
  anyErrorDirective?: Directive;

  /**
   * The maximum number of consecutive restarts within the reset window
   * when using {@link RestartDirective}; one more fault suspends the
   * actor instead. It also bounds the attempts made when a restart itself
   * keeps failing. `0` (the default) leaves restarts unbounded.
   */
  maxRetries?: number;

  /**
   * The fault-free period, in milliseconds, after which the consecutive
   * fault counter resets. Also the constant delay between attempts when a
   * restart itself keeps failing. A non-positive value (the default)
   * disables the budget: restarts are then unbounded.
   */
  timeout?: number;

  /**
   * Enables exponential backoff between consecutive restarts: the nth
   * consecutive restart is delayed by `min(initialDelay * 2^(n-1),
   * maxDelay)` milliseconds. A non-positive value (the default) disables
   * backoff.
   */
  initialDelay?: number;

  /** The upper bound of the backoff delay, in milliseconds; values lower
   * than `initialDelay` are raised to it. */
  maxDelay?: number;

  /**
   * The fault-free period, in milliseconds, after which the consecutive
   * fault counter resets when backoff is enabled; defaults to `maxDelay`.
   * Takes precedence over {@link timeout} as the reset window.
   */
  backoffResetAfter?: number;
}

/**
 * Supervisor defines how a parent reacts when one of its actors fails
 * while processing a message.
 *
 * It combines a {@link Strategy} (one-for-one or one-for-all) with
 * directive rules mapping error classes to actions. Rules match on the
 * error's exact constructor; when no rule matches, the catch-all
 * directive applies, and when there is none either, the failing actor is
 * suspended until it is reinstated or restarted explicitly.
 *
 * A supervisor created without options treats every failure as fatal for
 * the failing actor: any error maps to {@link StopDirective}. Configuring
 * error-specific rules replaces that catch-all, so unmatched errors then
 * suspend the actor.
 *
 * Instances are immutable and can safely be shared between actors.
 */
export class Supervisor {
  private readonly _strategy: Strategy;
  private readonly _directives: Map<ErrorClass, Directive> | null;
  private readonly _anyErrorDirective: Directive | undefined;
  private readonly _maxRetries: number;
  private readonly _timeout: number;
  private readonly _initialDelay: number;
  private readonly _maxDelay: number;
  private readonly _backoffResetAfter: number;

  constructor(options?: SupervisorOptions) {
    this._strategy = options?.strategy ?? OneForOneStrategy;
    this._maxRetries = options?.maxRetries ?? 0;
    this._timeout = options?.timeout ?? -1;

    let initialDelay = options?.initialDelay ?? 0;
    let maxDelay = 0;
    let backoffResetAfter = 0;

    if (initialDelay > 0) {
      maxDelay = Math.max(options?.maxDelay ?? initialDelay, initialDelay);
      const resetAfter = options?.backoffResetAfter ?? 0;
      backoffResetAfter = resetAfter > 0 ? resetAfter : maxDelay;
    } else {
      initialDelay = 0;
    }

    this._initialDelay = initialDelay;
    this._maxDelay = maxDelay;
    this._backoffResetAfter = backoffResetAfter;

    const anyError = options?.anyErrorDirective;

    if (anyError !== undefined) {
      // The catch-all overrides every error-specific rule.
      this._anyErrorDirective = anyError;
      this._directives = null;
    } else {
      const rules = options?.directives;
      this._directives = rules === undefined ? null : new Map(rules);
      // Without any rule, every failure is fatal for the failing actor.
      this._anyErrorDirective = this._directives === null ? StopDirective : undefined;
    }
  }

  /** Returns the configured supervision strategy. */
  strategy(): Strategy {
    return this._strategy;
  }

  /**
   * Resolves the directive for the given failure: the rule matching the
   * error's exact constructor when one exists, the catch-all directive
   * otherwise, or `undefined` when neither is configured.
   */
  directive(err: unknown): Directive | undefined {
    if (this._directives !== null && err instanceof Error) {
      const found = this._directives.get(err.constructor as ErrorClass);

      if (found !== undefined) {
        return found;
      }
    }

    return this._anyErrorDirective;
  }

  /** Returns the catch-all directive, or `undefined` when only
   * error-specific rules are configured. */
  anyErrorDirective(): Directive | undefined {
    return this._anyErrorDirective;
  }

  /** Returns the restart budget used with {@link RestartDirective}. */
  maxRetries(): number {
    return this._maxRetries;
  }

  /** Returns the retry window, in milliseconds, used with
   * {@link RestartDirective}. */
  timeout(): number {
    return this._timeout;
  }

  /** Returns the initial backoff delay in milliseconds; `0` when backoff
   * is disabled. */
  initialDelay(): number {
    return this._initialDelay;
  }

  /** Returns the upper bound of the backoff delay in milliseconds. */
  maxDelay(): number {
    return this._maxDelay;
  }

  /** Returns the fault-free period, in milliseconds, after which the
   * consecutive fault counter resets when backoff is enabled. */
  backoffResetAfter(): number {
    return this._backoffResetAfter;
  }
}

/** The supervisor applied to actors spawned without one: any failure
 *
 * @internal
 * stops the failing actor. */
export const defaultSupervisor = new Supervisor();

/**
 * Computes the exponential backoff delay for the nth consecutive fault:
 * `min(initialDelay * 2^(n-1), maxDelay)` milliseconds. Returns `0` when
 * backoff is disabled or there is no fault.
 *
 * @internal
 */
export function backoffDelay(faults: number, initialDelay: number, maxDelay: number): number {
  if (initialDelay <= 0 || faults < 1) {
    return 0;
  }

  return Math.min(initialDelay * 2 ** (faults - 1), maxDelay);
}
