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

import { ErrActorAlreadyExists } from "../errors/errors";

/** The worker id of the main isolate, which hosts the control plane
 * and every actor spawned from a constructed instance.
 *
 * @internal
 */
export const mainWorkerId = 0;

/**
 * ControlPlane is the single authority the pool needs: which top-level
 * name lives on which worker, where the next placed actor goes, and
 * what to clean up when a worker dies. It lives on the main isolate
 * and owns bookkeeping only; application messages never route through
 * it.
 *
 * Worker ids: {@link mainWorkerId} (zero) is the main isolate, pool
 * workers carry the positive ids handed to the constructor. Placement
 * round-robins over the live pool and falls back to the main isolate
 * once every pool worker is gone, so a system whose pool died keeps
 * accepting spawns the way a poolless system does.
 *
 * Eviction is the name-registry half of worker-exit handling: it
 * frees the dead worker's names and removes it from rotation. The
 * per-peer half, settling pending asks and requests, is
 * `Mesh.disconnect` on every surviving isolate.
 *
 * @internal
 */
export class ControlPlane {
  /** Live pool worker ids, in rotation order. */
  private _pool: number[];

  /** Top-level actor name to owning worker id. */
  private readonly _names = new Map<string, number>();

  private _next = 0;

  constructor(poolIds: readonly number[]) {
    this._pool = [...poolIds];
  }

  /**
   * Chooses the worker for the next placed actor: round-robin over the
   * live pool, or {@link mainWorkerId} when no pool worker is left.
   */
  place(): number {
    const pool = this._pool;
    if (pool.length === 0) {
      return mainWorkerId;
    }

    const id = pool[this._next % pool.length] as number;
    this._next = (this._next + 1) % pool.length;
    return id;
  }

  /**
   * Records a top-level actor name as living on the given worker.
   *
   * @returns `null`, or the {@link ErrActorAlreadyExists} sentinel
   * when the name is already registered; top-level names are unique
   * across the whole pool.
   */
  register(name: string, workerId: number): Error | null {
    if (this._names.has(name)) {
      return ErrActorAlreadyExists;
    }

    this._names.set(name, workerId);
    return null;
  }

  /** Returns the worker id owning the top-level name, or undefined. */
  resolve(name: string): number | undefined {
    return this._names.get(name);
  }

  /** Frees a top-level name, making it spawnable again; unknown names
   * are a no-op. */
  free(name: string): void {
    this._names.delete(name);
  }

  /** Returns the live pool worker ids, in rotation order. */
  workers(): number[] {
    return [...this._pool];
  }

  /**
   * Evicts a dead worker: removes it from the placement rotation and
   * frees every top-level name it owned.
   *
   * @returns The freed names, so the caller can act on what was lost;
   * empty when the worker owned nothing or was never in the pool.
   */
  evict(workerId: number): string[] {
    this._pool = this._pool.filter((id) => id !== workerId);

    const freed: string[] = [];
    for (const [name, owner] of this._names) {
      if (owner === workerId) {
        freed.push(name);
      }
    }

    for (const name of freed) {
      this._names.delete(name);
    }

    return freed;
  }
}
