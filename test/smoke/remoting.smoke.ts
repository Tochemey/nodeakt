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

import type { Actor } from "../../src/actor";
import { ActorSystem } from "../../src/actor.system";
import { discardLogger } from "../../src/discard.logger";
import { PostStart, Terminated } from "../../src/messages";
import type { PID } from "../../src/pid";
import type { ReceiveContext } from "../../src/receive.context";
import { registerMessage } from "../../src/registration";

/**
 * The remoting smoke, run per runtime by test/smoke/net.sh alongside
 * the transport smoke: two actor systems on loopback, a remote lookup,
 * typed tells and an ask round trip through routed PIDs, and death
 * watch delivering a Terminated across the boundary, ending in a
 * remote stop. Remote spawn is deliberately absent: it places on the
 * receiving node's worker pool, which needs built fixture modules the
 * from-source smoke does not carry; the vitest suite covers it. Exits
 * nonzero on the first broken expectation or after the watchdog
 * deadline.
 */

const TELLS: number = 50;

function fail(label: string): never {
  console.error(`FAIL: ${label}`);
  process.exit(1);
}

function check(condition: boolean, label: string): void {
  if (!condition) {
    fail(label);
  }
}

async function waitUntil(label: string, read: () => boolean): Promise<void> {
  for (let i: number = 0; i < 1000; i++) {
    if (read()) {
      return;
    }

    await new Promise<void>((resolve: () => void): void => {
      setTimeout(resolve, 5);
    });
  }

  fail(`timed out waiting for ${label}`);
}

class Ping {
  constructor(readonly n: number) {}
}

class Ask {
  constructor(readonly n: number) {}
}

class Answer {
  constructor(readonly n: number) {}
}

registerMessage(Ping);
registerMessage(Ask);
registerMessage(Answer);

/** Counts pings and answers asks. */
class Greeter implements Actor {
  static pings: number = 0;

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof Ping) {
      Greeter.pings++;
      return;
    }

    if (ctx.message instanceof Ask) {
      ctx.response(new Answer(ctx.message.n + 1));
    }
  }

  postStop(): void {}
}

/** Records the Terminated notifications it receives. */
class Watcher implements Actor {
  readonly terminated: string[] = [];

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    if (ctx.message instanceof PostStart) {
      return;
    }

    if (ctx.message instanceof Terminated) {
      this.terminated.push(ctx.message.actorPath);
    }
  }

  postStop(): void {}
}

const watchdog: NodeJS.Timeout = setTimeout((): void => {
  fail("remoting smoke timed out after 20s");
}, 20_000);

const runtime: string =
  "bun" in process.versions
    ? `bun ${process.versions.bun}`
    : "deno" in process.versions
      ? `deno ${(process.versions as { deno?: string }).deno}`
      : `node ${process.versions.node}`;

function system(name: string): ActorSystem {
  return new ActorSystem(name, {
    logger: discardLogger,
    remote: { host: "127.0.0.1", port: 0 },
  });
}

const alpha: ActorSystem = system("alpha");
const beta: ActorSystem = system("beta");
await alpha.start();
await beta.start();

await beta.spawn("greeter", new Greeter());
const found: PID | undefined = await alpha.remoteLookup("127.0.0.1", beta.port(), "greeter");
check(found !== undefined, "lookup resolves the remote actor");
const greeter: PID = found as PID;

// Fire-and-forget tells, delivered and counted across the wire.
for (let i: number = 0; i < TELLS; i++) {
  check(alpha.noSender().tell(greeter, new Ping(i)) === null, "tell accepted");
}

await waitUntil("tells", (): boolean => Greeter.pings >= TELLS);
check(Greeter.pings === TELLS, "exact tell count");

// An ask round trip with the prototype intact on both hops.
const answer: unknown = await alpha.noSender().ask(greeter, new Ask(41), 5000);
check(answer instanceof Answer, "ask reply prototype");
check((answer as Answer).n === 42, "ask reply value");

// Death watch across the boundary: a remote stop delivers Terminated.
const watcher: Watcher = new Watcher();
const watcherPid: PID = await alpha.spawn("watcher", watcher);
watcherPid.watch(greeter);
await alpha.remoteStop("127.0.0.1", beta.port(), "greeter");
await waitUntil("the Terminated", (): boolean => watcher.terminated.length === 1);
check(watcher.terminated[0] === greeter.path().toString(), "Terminated names the target");

await alpha.stop();
await beta.stop();

clearTimeout(watchdog);
console.log(`PASS: remoting smoke on ${runtime}`);
process.exit(0);
