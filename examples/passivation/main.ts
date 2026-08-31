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
 * Showcase: passivation reclaims idle actors automatically.
 *
 * A presence server keeps one Session actor per online user, holding that
 * user's in-memory state. Leave a session idle and it passivates itself and
 * frees its memory; keep it active and it stays put; come back later and you
 * get a fresh one. This is the pattern for per-entity actors, a session, a
 * connection, a device shadow, a cart, so the idle ones cost nothing.
 *
 * The default passivation is time-based at two minutes; this demo sets a short
 * window so a session comes and goes while you watch.
 *
 * Run: make passivation
 */

import type { Actor, Context, PID, ReceiveContext } from "../../src/index";
import {
  ActorPassivated,
  ActorStopped,
  ActorSystem,
  PostStart,
  TextLogger,
  TimeBasedStrategy,
} from "../../src/index";

/** One thing the user just did. */
class Track {
  constructor(readonly action: string) {}
}

/** "How many actions has this session seen?" */
class HowActive {}

/** How long a session may sit idle before it passivates. Two minutes is the
 * framework default; 150 ms keeps this demo watchable. */
const IDLE_MS: number = 150;

class Session implements Actor {
  /** Per-user in-memory state: exactly what passivation reclaims when the user
   * goes idle. A real session would hold a cart, a cursor, a socket. */
  private actions = 0;

  constructor(private readonly user: string) {}

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message = ctx.message;

    if (message instanceof PostStart) {
      ctx.logger().info(`session opened for ${this.user}`);
      return;
    }

    if (message instanceof Track) {
      this.actions++;
      return;
    }

    if (message instanceof HowActive) {
      ctx.response(this.actions);
    }
  }

  postStop(ctx: Context): void {
    // Runs on passivation as well as an explicit stop: the one place to flush
    // or persist before the in-memory state is gone.
    ctx.logger().info(`session closed for ${this.user}; ${this.actions} action(s) reclaimed`);
  }
}

/** Resolves after `ms`. */
function delay(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

const logger: TextLogger = new TextLogger({ level: "info" });
const system: ActorSystem = new ActorSystem("presence", { logger });
await system.start();

const outside: PID = system.noSender();

// Alice connects: her session passivates after IDLE_MS of no activity.
const session: PID = await system.spawn("session-alice", new Session("alice"), {
  passivationStrategy: new TimeBasedStrategy(IDLE_MS),
});
const sessionPath: string = session.path().toString();

// Watch the event stream: ActorPassivated marks the idle stop, and the
// name is free only once ActorStopped follows, so a re-open can reuse it.
const stopped: Promise<void> = new Promise<void>((resolve) => {
  system.subscribe((event: unknown) => {
    if (event instanceof ActorPassivated && event.actorPath === sessionPath) {
      logger.info("passivated: alice was idle, so her session's memory is reclaimed");
    }

    if (event instanceof ActorStopped && event.actorPath === sessionPath) {
      resolve();
    }
  });
});

// Alice is active: three actions, then a check.
outside.tell(session, new Track("open app"));
outside.tell(session, new Track("view feed"));
outside.tell(session, new Track("like post"));
logger.info(
  `alice active: ${(await outside.ask(session, new HowActive(), 1_000)) as number} actions`,
);

// Staying active keeps the session alive: each action lands inside the idle
// window, so the passivation clock keeps resetting.
for (let i = 0; i < 2; i++) {
  await delay(IDLE_MS - 60);
  outside.tell(session, new Track("scroll"));
}

const active: number = (await outside.ask(session, new HowActive(), 1_000)) as number;
logger.info(`still open after staying active: ${active} actions`);

// Alice goes idle: no messages for longer than the window. Nobody stops the
// session by hand; the runtime passivates it and reclaims its memory. The
// passivation timer is deliberately unref'd, so it never keeps a process alive
// on its own; a real server has a socket holding the loop open, so here we hold
// it with a plain timer while the idle stop lands.
logger.info("alice goes idle...");
const keepAlive: NodeJS.Timeout = setTimeout(() => {}, IDLE_MS * 4);
await stopped;
clearTimeout(keepAlive);

// The name is free again, so actorOf finds nothing.
logger.info(
  `actorOf("session-alice") -> ${system.actorOf("session-alice") === undefined ? "absent" : "present"}`,
);

// Alice comes back: a brand-new session, its state reset to zero. Her old
// actions did not survive, which is the point, they were never durable state.
const returning: PID = await system.spawn("session-alice", new Session("alice"), {
  passivationStrategy: new TimeBasedStrategy(IDLE_MS),
});
const fresh: number = (await outside.ask(returning, new HowActive(), 1_000)) as number;
logger.info(`alice returns: ${fresh} actions on a fresh session`);

await system.stop();
