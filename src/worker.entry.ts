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
 * The worker isolate's entry point: builds the facade actor system and
 * hands control to {@link WorkerRuntime}. Deliberately nothing but
 * wiring, and excluded from coverage, because code running inside a
 * worker escapes the main isolate's instrumentation; everything with
 * logic lives in modules the in-process tests cover.
 */

import { type MessagePort, parentPort, workerData } from "node:worker_threads";
import { ActorSystem } from "./actor.system";
import { discardLogger } from "./discard.logger";
import type { WorkerBootData } from "./protocol";
import { defaultMessageRegistry } from "./registration";
import { applySetup, WorkerRuntime } from "./worker.runtime";

const boot = workerData as WorkerBootData | null;

// Bundling can share this module's chunk with main-isolate code, so it
// may be evaluated outside a worker, where there is no parent port and
// no boot data. Booting is meaningful only as an actual worker entry;
// anywhere else this module must load as an inert collection of exports.
if (parentPort !== null && boot !== null) {
  void (async () => {
    // The isolate's transports must read from the very registry the
    // module-scope `registerMessage` lines write to, so that importing an
    // actor's module (which imports its message modules) is what makes the
    // registrations propagate here. A private registry would leave every
    // registered message undecodable on this side.
    const registry = defaultMessageRegistry;
    if (boot.setup !== null) {
      await applySetup(registry, boot.setup);
    }

    const system = new ActorSystem(boot.systemName, boot.quiet ? { logger: discardLogger } : {});
    // The facade adopts the node's advertised address before any actor
    // exists, so paths minted on this isolate are canonical for the
    // whole node and inbound envelopes resolve here.
    system.adoptAddress(boot.host, boot.port);
    await system.start();
    const runtime = new WorkerRuntime(system, registry, parentPort as MessagePort, boot.workerId);
    runtime.announce();
  })();
}
