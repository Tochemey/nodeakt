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

import { type ChildProcess, fork } from "node:child_process";
import { STATE_ALIVE, STATE_DEAD, STATE_LEFT } from "../../src/membership/wire";

interface Member {
  readonly member: string;
  readonly state: number;
  readonly incarnation: number;
}

interface Response {
  readonly type: "ready" | "response";
  readonly id?: number;
  readonly address?: string;
  readonly ok?: boolean;
  readonly value?: unknown;
  readonly error?: string;
}

class MembershipProcess {
  readonly child: ChildProcess;
  readonly address: Promise<string>;
  #nextId = 1;
  readonly #pending = new Map<
    number,
    { resolve: (value: unknown) => void; reject: (error: Error) => void }
  >();

  constructor(seed: number, address?: string) {
    this.child = fork(new URL("membership-process.ts", import.meta.url), {
      execArgv: ["--import", "tsx"],
      env: {
        ...process.env,
        NODEAKT_MEMBERSHIP_SEED: String(seed),
        ...(address === undefined ? {} : { NODEAKT_MEMBERSHIP_ADDRESS: address }),
      },
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    this.address = new Promise<string>((resolve, reject): void => {
      const failed = (code: number | null): void => {
        reject(new Error(`membership child exited before ready with code ${String(code)}`));
      };
      this.child.once("exit", failed);
      this.child.on("message", (message: Response): void => {
        if (message.type === "ready" && message.address !== undefined) {
          this.child.removeListener("exit", failed);
          resolve(message.address);
        }
      });
    });
    this.child.on("message", (message: Response): void => {
      if (message.type !== "response" || message.id === undefined) {
        return;
      }
      const pending = this.#pending.get(message.id);
      if (pending === undefined) {
        return;
      }
      this.#pending.delete(message.id);
      if (message.ok === true) {
        pending.resolve(message.value);
      } else {
        pending.reject(new Error(message.error ?? "membership child operation failed"));
      }
    });
    this.child.once("exit", (code, signal): void => {
      for (const pending of this.#pending.values()) {
        pending.reject(
          new Error(
            `membership child exited with code ${String(code)} and signal ${String(signal)}`,
          ),
        );
      }
      this.#pending.clear();
    });
  }

  request<T>(operation: "join" | "leave" | "members" | "stop", seeds?: string[]): Promise<T> {
    const id = this.#nextId;
    this.#nextId += 1;
    return new Promise<T>((resolve, reject): void => {
      this.#pending.set(id, {
        resolve: (value): void => resolve(value as T),
        reject,
      });
      this.child.send({ id, operation, seeds }, (error): void => {
        if (error === null) {
          return;
        }
        this.#pending.delete(id);
        reject(error);
      });
    });
  }

  members(): Promise<Member[]> {
    return this.request<Member[]>("members");
  }

  async kill(): Promise<void> {
    const exited = new Promise<void>((resolve): void => {
      this.child.once("exit", (): void => resolve());
    });
    this.child.kill("SIGKILL");
    await exited;
  }
}

function fail(label: string): never {
  throw new Error(label);
}

async function waitUntil(
  label: string,
  read: () => Promise<boolean>,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await read()) {
      return;
    }
    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 200);
    });
  }
  fail(`timed out waiting for ${label}`);
}

function stateOf(members: readonly Member[], address: string): number | undefined {
  return members.find((member): boolean => member.member === address)?.state;
}

const watchdog = setTimeout((): void => {
  console.error("FAIL: multi-process membership smoke timed out after 45s");
  process.exit(1);
}, 45_000);

const a = new MembershipProcess(11);
const b = new MembershipProcess(12);
let c = new MembershipProcess(13);
const live = new Set<MembershipProcess>([a, b, c]);

try {
  const [addressA, addressB, addressC] = await Promise.all([a.address, b.address, c.address]);
  await Promise.all([b.request("join", [addressA]), c.request("join", [addressA])]);
  await waitUntil(
    "initial join convergence",
    async (): Promise<boolean> =>
      (await Promise.all([a.members(), b.members(), c.members()])).every(
        (members): boolean =>
          members.length === 3 && members.every((member): boolean => member.state === STATE_ALIVE),
      ),
    5_000,
  );

  await c.kill();
  live.delete(c);
  await waitUntil(
    "SIGKILL death convergence",
    async (): Promise<boolean> =>
      (await Promise.all([a.members(), b.members()])).every(
        (members): boolean => stateOf(members, addressC) === STATE_DEAD,
      ),
    28_000,
  );

  c = new MembershipProcess(14, addressC);
  live.add(c);
  if ((await c.address) !== addressC) {
    fail("restarted child did not reclaim its advertised address");
  }
  await c.request("join", [addressA]);
  await waitUntil(
    "kill/restart rejoin convergence",
    async (): Promise<boolean> =>
      (await Promise.all([a.members(), b.members(), c.members()])).every(
        (members): boolean => stateOf(members, addressC) === STATE_ALIVE,
      ),
    5_000,
  );
  const restarted = (await a.members()).find((member): boolean => member.member === addressC);
  if (restarted?.incarnation !== 1) {
    fail(`restarted member used incarnation ${String(restarted?.incarnation)}, expected 1`);
  }

  await b.request("leave");
  live.delete(b);
  await waitUntil(
    "graceful leave convergence",
    async (): Promise<boolean> =>
      (await Promise.all([a.members(), c.members()])).every(
        (members): boolean => stateOf(members, addressB) === STATE_LEFT,
      ),
    5_000,
  );
} finally {
  await Promise.all(
    Array.from(live, async (member): Promise<void> => {
      try {
        await member.request("stop");
      } catch {
        await member.kill();
      }
    }),
  );
}

clearTimeout(watchdog);
console.log("PASS: multi-process membership join, SIGKILL, rejoin, and leave");
