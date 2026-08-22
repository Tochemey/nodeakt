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
 * Test fixture: a worker entry identical to the production one, plus a
 * relay actor exposing the isolate's mesh to the test. The relay is
 * what lets a test on the main isolate prove worker-to-worker traffic
 * with main off the data path: "ask|workerId|path|payload" performs a
 * mesh ask from this isolate and answers with the outcome, and
 * "isolates" answers with the mesh's connected worker ids.
 */

import { type MessagePort, parentPort, workerData } from "node:worker_threads";
import { ActorSystem } from "../../src/actor/actor.system";
import { parsePath } from "../../src/actor/path";
import type { ReceiveContext } from "../../src/actor/receive.context";
import { ErrDead } from "../../src/errors/errors";
import { discardLogger } from "../../src/logger/discard.logger";
import { MessageRegistry } from "../../src/runtime/message.registry";
import type { WorkerBootData } from "../../src/runtime/protocol";
import { applySetup, WorkerRuntime } from "../../src/runtime/worker.runtime";

const boot = workerData as WorkerBootData;

void (async () => {
  const registry = new MessageRegistry();
  if (boot.setup !== null) {
    await applySetup(registry, boot.setup);
  }

  const system = new ActorSystem(boot.systemName, boot.quiet ? { logger: discardLogger } : {});
  await system.start();
  const runtime = new WorkerRuntime(system, registry, parentPort as MessagePort, boot.workerId);
  const mesh = runtime.mesh();

  await system.spawn(`relay-${boot.workerId}`, {
    preStart(): void {},

    async receive(ctx: ReceiveContext): Promise<void> {
      const message = ctx.message;
      if (typeof message !== "string") {
        return;
      }

      if (message === "isolates") {
        ctx.response(mesh.isolates().sort());
        return;
      }

      if (!message.startsWith("ask|")) {
        return;
      }

      const [, workerId, path, payload] = message.split("|") as [string, string, string, string];
      try {
        const reply = await mesh.ask(Number(workerId), parsePath(path), payload, 10_000);
        ctx.response({ ok: true, reply });
      } catch (err) {
        ctx.response({ ok: false, isDead: err === ErrDead, error: (err as Error).message });
      }
    },

    postStop(): void {},
  });

  runtime.announce();
})();
