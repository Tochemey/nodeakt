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
 * Showcase: a device hierarchy, in the shape of the classic Akka IoT tutorial.
 *
 * A device manager owns one group per home; each group owns one actor per
 * sensor and `watch`es it. Reading the whole home is a short-lived actor of
 * its own: it fans `ReadTemperature` out with `tell`, watches every device
 * so a death becomes an answer, and settles whatever is left when a
 * deadline it `scheduleOnce`d to itself fires. Registration builds the
 * tree on demand. Nothing polls and nothing locks.
 *
 * Run: make iot
 */

import type { Actor, PID } from "../../src/index";
import { ActorSystem, PostStart, type ReceiveContext, Terminated } from "../../src/index";

// query outcomes

/** The device answered with its last recorded temperature. */
class Temperature {
  constructor(readonly value: number) {}
}

/** The device answered, but no temperature has been recorded yet. */
class TemperatureNotAvailable {}

/** The device stopped before answering; death watch reported it. */
class DeviceNotAvailable {}

/** The device did not answer before the query deadline. */
class DeviceTimedOut {}

type Reading = Temperature | TemperatureNotAvailable | DeviceNotAvailable | DeviceTimedOut;

// protocol

/** Driver -> manager (ask): track a device, creating its group on demand. */
class TrackDevice {
  constructor(
    readonly groupId: string,
    readonly deviceId: string,
  ) {}
}

/** Manager -> asker: the device actor, freshly spawned or already known. */
class DeviceRegistered {
  constructor(readonly device: PID) {}
}

/** Driver -> manager (ask): which devices does this group track? */
class ListDevices {
  constructor(readonly groupId: string) {}
}

/** Group -> asker: the tracked device ids. */
class DeviceList {
  constructor(readonly deviceIds: string[]) {}
}

/** Driver -> device (ask): store a reading. Acknowledged. */
class RecordTemperature {
  constructor(readonly value: number) {}
}

/** Device -> asker: the reading was stored. */
class TemperatureRecorded {}

/** Query -> device (tell): report your last reading to the sender. */
class ReadTemperature {}

/** Device -> query (tell): the last reading, or null when none exists. */
class RespondTemperature {
  constructor(
    readonly deviceId: string,
    readonly value: number | null,
  ) {}
}

/**
 * Driver -> manager (tell), forwarded to the group: collect one reading
 * per device and tell `RespondAllTemperatures` to `replyTo`, settling
 * everything unanswered once `deadline` milliseconds have passed.
 */
class QueryAllTemperatures {
  constructor(
    readonly groupId: string,
    readonly requestId: number,
    readonly replyTo: PID,
    readonly deadline: number,
  ) {}
}

/** Query -> replyTo: one outcome for every device the group tracked. */
class RespondAllTemperatures {
  constructor(
    readonly requestId: number,
    readonly readings: Map<string, Reading>,
  ) {}
}

/** Driver -> device (tell): simulate a fault; stop answering reads. */
class Jam {}

/** Query -> itself (scheduleOnce): the collection deadline fired. */
class CollectionTimeout {}

// actors

/** One sensor. It keeps only its last reading. */
class Device implements Actor {
  private lastReading: number | null = null;
  private jammed = false;

  constructor(readonly deviceId: string) {}

  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message = ctx.message;

    if (message instanceof RecordTemperature) {
      this.lastReading = message.value;
      ctx.response(new TemperatureRecorded());
      return;
    }

    if (message instanceof ReadTemperature) {
      const asker = ctx.sender;
      if (this.jammed || asker === undefined) {
        return; // a jammed sensor never answers; the query's deadline covers it
      }

      ctx.tell(asker, new RespondTemperature(this.deviceId, this.lastReading));
      return;
    }

    if (message instanceof Jam) {
      this.jammed = true;
      console.log(`${this.deviceId} jammed (stops answering reads)`);
    }
  }

  postStop(): void {}
}

/**
 * One query, one actor. It lives for a single collection round, so the
 * group stays free while the round runs against its deadline, and every
 * way a device can fail to answer resolves to a `Reading`.
 */
class DeviceGroupQuery implements Actor {
  private readonly readings = new Map<string, Reading>();
  private readonly pending = new Set<string>();
  private done = false;

  constructor(
    private readonly devices: Map<string, PID>,
    private readonly requestId: number,
    private readonly replyTo: PID,
    private readonly deadline: number,
  ) {}

  preStart(): void {}

  async receive(ctx: ReceiveContext): Promise<void> {
    const message = ctx.message;

    if (message instanceof PostStart) {
      const self = ctx.self;
      if (self === undefined) {
        return;
      }

      for (const [deviceId, device] of this.devices) {
        this.pending.add(deviceId);
        ctx.watch(device); // a device dying mid-query is an answer too
        ctx.tell(device, new ReadTemperature());
      }

      // The deadline arrives as an ordinary message on this actor's own
      // turn; stopping the actor cancels it if the round finishes early.
      await ctx.scheduleOnce(new CollectionTimeout(), self, this.deadline);
      return;
    }

    if (message instanceof RespondTemperature) {
      const reading: Reading =
        message.value === null ? new TemperatureNotAvailable() : new Temperature(message.value);
      this.settle(ctx, message.deviceId, reading);
      return;
    }

    if (message instanceof Terminated) {
      const deviceId = this.deviceIdOf(message.actorPath);
      if (deviceId !== undefined) {
        this.settle(ctx, deviceId, new DeviceNotAvailable());
      }

      return;
    }

    if (message instanceof CollectionTimeout) {
      for (const deviceId of this.pending) {
        this.readings.set(deviceId, new DeviceTimedOut());
      }

      this.pending.clear();
      this.finish(ctx);
    }
  }

  postStop(): void {}

  /** Records one device's outcome. The first answer wins: a reading that
   * arrived before the device died stays a reading. */
  private settle(ctx: ReceiveContext, deviceId: string, reading: Reading): void {
    if (!this.pending.delete(deviceId)) {
      return;
    }

    this.readings.set(deviceId, reading);
    if (this.pending.size === 0) {
      this.finish(ctx);
    }
  }

  /** Replies in registration order, then stops: this actor was one query. */
  private finish(ctx: ReceiveContext): void {
    if (this.done) {
      return;
    }

    this.done = true;
    const ordered = new Map<string, Reading>();
    for (const deviceId of this.devices.keys()) {
      ordered.set(deviceId, this.readings.get(deviceId) as Reading);
    }

    ctx.tell(this.replyTo, new RespondAllTemperatures(this.requestId, ordered));
    ctx.shutdown(); // one query, one life
  }

  private deviceIdOf(actorPath: string): string | undefined {
    for (const [deviceId, device] of this.devices) {
      if (device.path().toString() === actorPath) {
        return deviceId;
      }
    }

    return undefined;
  }
}

/** One home. It owns its device actors and forgets the ones that stop. */
class DeviceGroup implements Actor {
  private readonly devices = new Map<string, PID>();
  private queries = 0;

  constructor(readonly groupId: string) {}

  preStart(): void {}

  async receive(ctx: ReceiveContext): Promise<void> {
    const message = ctx.message;

    if (message instanceof TrackDevice) {
      const existing = this.devices.get(message.deviceId);
      if (existing !== undefined) {
        ctx.response(new DeviceRegistered(existing)); // registration is idempotent
        return;
      }

      const device = await ctx.spawn(`device-${message.deviceId}`, new Device(message.deviceId));
      this.devices.set(message.deviceId, device);
      ctx.watch(device); // forget the device when it stops
      ctx.response(new DeviceRegistered(device));
      return;
    }

    if (message instanceof ListDevices) {
      ctx.response(new DeviceList([...this.devices.keys()]));
      return;
    }

    if (message instanceof QueryAllTemperatures) {
      // Hand the round to a fresh child built from a snapshot of the
      // membership; devices registered after this line join the next query.
      this.queries++;
      const snapshot = new Map(this.devices);
      await ctx.spawn(
        `query-${this.queries}`,
        new DeviceGroupQuery(snapshot, message.requestId, message.replyTo, message.deadline),
      );
      return;
    }

    if (message instanceof Terminated) {
      for (const [deviceId, device] of this.devices) {
        if (device.path().toString() === message.actorPath) {
          this.devices.delete(deviceId);
          console.log(
            `group ${this.groupId} dropped ${deviceId} (${this.devices.size} still tracked)`,
          );
          break;
        }
      }
    }
  }

  postStop(): void {}
}

/** The root of the hierarchy: routes by group id, spawning groups on demand. */
class DeviceManager implements Actor {
  private readonly groups = new Map<string, PID>();

  preStart(): void {}

  async receive(ctx: ReceiveContext): Promise<void> {
    const message = ctx.message;

    if (message instanceof TrackDevice) {
      let group = this.groups.get(message.groupId);
      if (group === undefined) {
        group = await ctx.spawn(`group-${message.groupId}`, new DeviceGroup(message.groupId));
        this.groups.set(message.groupId, group);
      }

      // An ask relays by asking: ask the group, hand its answer back.
      ctx.response(await ctx.ask(group, message, 1_000));
      return;
    }

    if (message instanceof ListDevices) {
      const group = this.groups.get(message.groupId);
      if (group === undefined) {
        ctx.response(new DeviceList([]));
        return;
      }

      ctx.response(await ctx.ask(group, message, 1_000));
      return;
    }

    if (message instanceof QueryAllTemperatures) {
      const group = this.groups.get(message.groupId);
      if (group === undefined) {
        ctx.unhandled();
        return;
      }

      ctx.forward(group); // a tell relays by forwarding; the sender survives
    }
  }

  postStop(): void {}
}

/** Where query results land; queries answer an actor, not a caller. */
class Reporter implements Actor {
  preStart(): void {}

  receive(ctx: ReceiveContext): void {
    const message = ctx.message;

    if (message instanceof RespondAllTemperatures) {
      console.log(`query #${message.requestId} answered:`);
      for (const [deviceId, reading] of message.readings) {
        console.log(`  ${deviceId.padEnd(8)} ${describe(reading)}`);
      }
    }
  }

  postStop(): void {}
}

const describe = (reading: Reading): string => {
  if (reading instanceof Temperature) {
    return `${reading.value.toFixed(1)}°C`;
  }

  if (reading instanceof TemperatureNotAvailable) {
    return "no reading yet";
  }

  if (reading instanceof DeviceNotAvailable) {
    return "device stopped";
  }

  return "no answer before the deadline";
};

// driver

const settle = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

const system = new ActorSystem("iot");
await system.start();

const manager = await system.spawn("device-manager", new DeviceManager());
const reporter = await system.spawn("reporter", new Reporter());
const outside = system.noSender();

// Registration builds the tree on demand: the first ask creates the home
// group, and every device lands under it. The paths show the hierarchy.
const register = async (deviceId: string): Promise<PID> => {
  const registered = (await outside.ask(
    manager,
    new TrackDevice("home", deviceId),
    1_000,
  )) as DeviceRegistered;
  console.log(`registered ${registered.device.path().toString()}`);
  return registered.device;
};

const kitchen = await register("kitchen");
const bedroom = await register("bedroom");
const garage = await register("garage");
const attic = await register("attic");

const again = (await outside.ask(
  manager,
  new TrackDevice("home", "kitchen"),
  1_000,
)) as DeviceRegistered;
console.log(`registering kitchen again hands back the same actor: ${again.device === kitchen}`);

const listed = (await outside.ask(manager, new ListDevices("home"), 1_000)) as DeviceList;
console.log(`group home tracks: ${listed.deviceIds.join(", ")}`);

await outside.ask(kitchen, new RecordTemperature(21.0), 1_000);
await outside.ask(kitchen, new RecordTemperature(22.5), 1_000); // the last reading wins
await outside.ask(garage, new RecordTemperature(18.0), 1_000);
console.log("recorded: kitchen 22.5°C, garage 18.0°C");

console.log("\n-- query 1: every sensor answers --");
outside.tell(manager, new QueryAllTemperatures("home", 1, reporter, 250));
await settle(100); // all four answer at once, so the round finishes early

await outside.ask(bedroom, new RecordTemperature(19.2), 1_000);
console.log("recorded: bedroom 19.2°C");

console.log("\n-- query 2: a jammed sensor and a dying one --");
outside.tell(garage, new Jam());
outside.tell(attic, new Jam());
await settle(20);

outside.tell(manager, new QueryAllTemperatures("home", 2, reporter, 250));
await settle(80);
await garage.shutdown(); // mid-query: death watch turns the stop into an answer
await settle(300); // let the deadline settle the silent attic

const remaining = (await outside.ask(manager, new ListDevices("home"), 1_000)) as DeviceList;
console.log(`group home tracks: ${remaining.deviceIds.join(", ")}`);

await system.stop();
