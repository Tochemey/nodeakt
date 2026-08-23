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

import { describe, expect, it } from "vitest";
import {
  backoffDelay,
  defaultSupervisor,
  OneForAllStrategy,
  OneForOneStrategy,
  ResumeDirective,
  StopDirective,
  Supervisor,
} from "../src/supervisor";

describe("Supervisor", () => {
  it("defaults to one-for-one with a stop-anything rule", () => {
    const supervisor = new Supervisor();

    expect(supervisor.strategy()).toBe(OneForOneStrategy);
    expect(supervisor.anyErrorDirective()).toBe(StopDirective);
    expect(supervisor.directive(new Error("x"))).toBe(StopDirective);
    expect(supervisor.directive("not an error")).toBe(StopDirective);
    expect(supervisor.maxRetries()).toBe(0);
    expect(supervisor.timeout()).toBe(-1);
    expect(supervisor.initialDelay()).toBe(0);
    expect(supervisor.maxDelay()).toBe(0);
    expect(supervisor.backoffResetAfter()).toBe(0);
    expect(defaultSupervisor.directive(new Error("x"))).toBe(StopDirective);
  });

  it("matches directive rules on the exact error class", () => {
    class Transient extends Error {}

    class Fatal extends Error {}

    class SubTransient extends Transient {}

    const supervisor = new Supervisor({
      directives: [
        [Transient, ResumeDirective],
        [Fatal, StopDirective],
      ],
    });

    expect(supervisor.directive(new Transient())).toBe(ResumeDirective);
    expect(supervisor.directive(new Fatal())).toBe(StopDirective);
    expect(supervisor.directive(new SubTransient())).toBeUndefined();
    expect(supervisor.directive(new RangeError("other"))).toBeUndefined();
    expect(supervisor.directive("thrown string")).toBeUndefined();
    expect(supervisor.anyErrorDirective()).toBeUndefined();
  });

  it("lets the catch-all directive override error-specific rules", () => {
    class Transient extends Error {}

    const supervisor = new Supervisor({
      directives: [[Transient, StopDirective]],
      anyErrorDirective: ResumeDirective,
    });

    expect(supervisor.directive(new Transient())).toBe(ResumeDirective);
    expect(supervisor.directive("anything")).toBe(ResumeDirective);
  });

  it("normalizes the backoff configuration", () => {
    const supervisor = new Supervisor({
      strategy: OneForAllStrategy,
      maxRetries: 3,
      timeout: 500,
      initialDelay: 100,
      maxDelay: 50,
      backoffResetAfter: 0,
    });

    expect(supervisor.strategy()).toBe(OneForAllStrategy);
    expect(supervisor.maxRetries()).toBe(3);
    expect(supervisor.timeout()).toBe(500);
    expect(supervisor.initialDelay()).toBe(100);
    expect(supervisor.maxDelay()).toBe(100);
    expect(supervisor.backoffResetAfter()).toBe(100);
  });

  it("derives the backoff bounds from the initial delay alone", () => {
    const supervisor = new Supervisor({ initialDelay: 40 });

    expect(supervisor.maxDelay()).toBe(40);
    expect(supervisor.backoffResetAfter()).toBe(40);
  });

  it("keeps an explicit backoff reset window", () => {
    const supervisor = new Supervisor({ initialDelay: 10, maxDelay: 80, backoffResetAfter: 300 });

    expect(supervisor.maxDelay()).toBe(80);
    expect(supervisor.backoffResetAfter()).toBe(300);
  });

  it("ignores a non-positive initial delay", () => {
    const supervisor = new Supervisor({ initialDelay: -5, maxDelay: 100, backoffResetAfter: 100 });

    expect(supervisor.initialDelay()).toBe(0);
    expect(supervisor.maxDelay()).toBe(0);
    expect(supervisor.backoffResetAfter()).toBe(0);
  });
});

describe("backoffDelay", () => {
  it("doubles the delay per consecutive fault up to the cap", () => {
    expect(backoffDelay(1, 100, 1000)).toBe(100);
    expect(backoffDelay(2, 100, 1000)).toBe(200);
    expect(backoffDelay(4, 100, 1000)).toBe(800);
    expect(backoffDelay(5, 100, 1000)).toBe(1000);
    expect(backoffDelay(2000, 100, 1000)).toBe(1000);
  });

  it("is disabled without an initial delay or a fault", () => {
    expect(backoffDelay(3, 0, 1000)).toBe(0);
    expect(backoffDelay(0, 100, 1000)).toBe(0);
  });
});
