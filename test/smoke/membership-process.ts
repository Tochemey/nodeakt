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

interface Request {
  readonly id: number;
  readonly operation: "join" | "leave" | "members" | "stop";
  readonly seeds?: readonly string[];
}

function send(message: unknown): void {
  process.send?.(message);
}

function portOf(address: string | undefined): number {
  if (address === undefined) {
    return 0;
  }
  const separator = address.lastIndexOf(":");
  const port = Number(address.slice(separator + 1));
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`invalid requested membership address: ${address}`);
  }
  return port;
}

const requestedAddress = process.env.NODEAKT_MEMBERSHIP_ADDRESS;
const transport = await TcpMembershipTransport.bind({
  host: "127.0.0.1",
  port: portOf(requestedAddress),
});
const swim = new Swim({
  address: transport.address,
  metadata: Uint8Array.of(Number(process.env.NODEAKT_MEMBERSHIP_SEED ?? 1) & 0xff),
  transport,
  clock: wallClock,
  random: new SeededRandom(Number(process.env.NODEAKT_MEMBERSHIP_SEED ?? 1)),
});
await swim.start();

send({ type: "ready", address: transport.address });

process.on("message", (request: Request): void => {
  void (async (): Promise<void> => {
    try {
      let value: unknown;
      switch (request.operation) {
        case "join":
          value = await swim.join(request.seeds ?? []);
          break;
        case "leave":
          value = await swim.leave();
          break;
        case "members":
          value = swim.members().map((member) => ({
            member: member.member,
            state: member.state,
            incarnation: member.incarnation,
          }));
          break;
        case "stop":
          value = await swim.stop();
          break;
      }
      send({ type: "response", id: request.id, ok: true, value });
      if (request.operation === "stop" || request.operation === "leave") {
        process.disconnect?.();
      }
    } catch (error) {
      send({
        type: "response",
        id: request.id,
        ok: false,
        error: error instanceof Error ? error.stack : String(error),
      });
    }
  })();
});
