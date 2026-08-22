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
 * Showcase: many actors, no shared memory.
 *
 * Users join a room; posts fan out with `tell`. The room learns who joined
 * from `ctx.sender` and `watch`es each member, so a stop drops them from
 * the room. The member list is the room's private state, mutated only on
 * its own turn.
 *
 * Run: make chat
 */

import type { Actor, PID } from "../../src/index";
import { ActorSystem, type ReceiveContext, Terminated } from "../../src/index";

// --- protocol -------------------------------------------------------------

/** Driver -> user: here is the room you belong to. */
class Connect {
  constructor(readonly room: PID) {}
}

/** Driver -> user: post this line to the room. */
class Say {
  constructor(readonly text: string) {}
}

/** User -> room: I am joining (the room reads the user from ctx.sender). */
class JoinRoom {}

/** User -> room: broadcast this line to everyone. */
class Post {
  constructor(readonly text: string) {}
}

/** Room -> user: a line to display. */
class Deliver {
  constructor(
    readonly from: string,
    readonly text: string,
  ) {}
}

// --- actors ---------------------------------------------------------------

class Room implements Actor {
  private readonly members = new Set<PID>();

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message = ctx.message;

    if (message instanceof JoinRoom) {
      const user = ctx.sender;
      if (user === undefined) {
        return;
      }

      this.members.add(user);
      ctx.watch(user); // drop the user automatically when it stops
      console.log(`* ${user.name()} joined (${this.members.size} present)`);
      return;
    }

    if (message instanceof Post) {
      const from = ctx.sender;
      if (from === undefined) {
        return;
      }

      for (const member of this.members) {
        ctx.tell(member, new Deliver(from.name(), message.text));
      }

      return;
    }

    if (message instanceof Terminated) {
      // A watched member stopped; find and forget it by path.
      for (const member of this.members) {
        if (member.path().toString() === message.actorPath) {
          this.members.delete(member);
          console.log(`* ${member.name()} left (${this.members.size} present)`);
          break;
        }
      }
    }
  }

  postStop(): void {}
}

class User implements Actor {
  private room: PID | null = null;

  constructor(readonly name: string) {}

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message = ctx.message;

    if (message instanceof Connect) {
      this.room = message.room;
      ctx.tell(message.room, new JoinRoom()); // room sees ctx.sender === this user
      return;
    }

    if (message instanceof Say && this.room !== null) {
      ctx.tell(this.room, new Post(message.text));
      return;
    }

    if (message instanceof Deliver) {
      console.log(`  [${this.name}] <${message.from}> ${message.text}`);
    }
  }

  postStop(): void {}
}

// --- driver ---------------------------------------------------------------

const settle = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

const system = new ActorSystem("chat");
await system.start();

const room = await system.spawn("room", new Room());
const alice = await system.spawn("alice", new User("alice"));
const bob = await system.spawn("bob", new User("bob"));

const outside = system.noSender();
outside.tell(alice, new Connect(room));
outside.tell(bob, new Connect(room));
await settle(20);

outside.tell(alice, new Say("hello room"));
outside.tell(bob, new Say("hi alice"));
await settle(20);

// Bob leaves; death watch removes him from the room.
await bob.shutdown();
await settle(20);

outside.tell(alice, new Say("anyone still here?"));
await settle(20);

await system.stop();
