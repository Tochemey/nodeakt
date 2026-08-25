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

import { wallClock } from "../../src/membership/clock";
import { SeededRandom } from "../../src/membership/random";
import { Swim } from "../../src/membership/swim";
import { TcpMembershipTransport } from "../../src/membership/transport";
import { STATE_ALIVE, STATE_LEFT } from "../../src/membership/wire";

function fail(label: string): never {
  console.error(`FAIL: ${label}`);
  process.exit(1);
}

async function waitUntil(label: string, read: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (read()) {
      return;
    }
    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 25);
    });
  }
  fail(`timed out waiting for ${label}`);
}

async function create(seed: number): Promise<Swim> {
  const transport = await TcpMembershipTransport.bind({ host: "127.0.0.1", port: 0 });
  const swim = new Swim({
    address: transport.address,
    metadata: Uint8Array.of(seed),
    transport,
    clock: wallClock,
    random: new SeededRandom(seed),
  });
  await swim.start();
  return swim;
}

const watchdog = setTimeout((): void => {
  fail("membership smoke timed out after 15s");
}, 15_000);

const runtime =
  "bun" in process.versions
    ? `bun ${process.versions.bun}`
    : "deno" in process.versions
      ? `deno ${(process.versions as { deno?: string }).deno}`
      : `node ${process.versions.node}`;

const a = await create(1);
const b = await create(2);
const c = await create(3);

try {
  await b.join([a.self()?.member as string]);
  await c.join([a.self()?.member as string]);
  await waitUntil("three-member convergence", (): boolean =>
    [a, b, c].every(
      (node): boolean =>
        node.members().length === 3 &&
        node.members().every((member): boolean => member.state === STATE_ALIVE),
    ),
  );

  const leaving = b.self()?.member as string;
  await b.leave();
  await waitUntil("graceful leave", (): boolean =>
    [a, c].every(
      (node): boolean =>
        node.members().find((member): boolean => member.member === leaving)?.state === STATE_LEFT,
    ),
  );
} finally {
  await Promise.all([a.stop(), b.stop(), c.stop()]);
}

clearTimeout(watchdog);
console.log(`PASS: membership smoke on ${runtime}`);
