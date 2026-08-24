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

import { ActorSystem } from "../../src/actor.system";
import { discardLogger } from "../../src/discard.logger";

/** A system with remoting enabled on an ephemeral loopback port. */
export function remoteSystem(name: string): ActorSystem {
  return new ActorSystem(name, {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0 },
  });
}

/** Polls until `read` reports true, failing loudly after four seconds:
 * every positive cross-node assertion rides this rather than a bare
 * sleep, so a slow machine stretches the wait instead of flaking. */
export async function until(label: string, read: () => boolean): Promise<void> {
  for (let i: number = 0; i < 800; i++) {
    if (read()) {
      return;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 5);
    });
  }

  throw new Error(`timed out waiting for ${label}`);
}

/** A bare wait, for negative assertions only: proving something did
 * not happen within a window. */
export function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve): void => {
    setTimeout(resolve, ms);
  });
}

/** Runs `fn` against two started remote systems, alpha and beta, and
 * stops both however it ends. */
export async function withSystems(
  fn: (a: ActorSystem, b: ActorSystem) => Promise<void>,
): Promise<void> {
  const a: ActorSystem = remoteSystem("alpha");
  const b: ActorSystem = remoteSystem("beta");
  await a.start();
  await b.start();

  try {
    await fn(a, b);
  } finally {
    await a.stop();
    await b.stop();
  }
}
