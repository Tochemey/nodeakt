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

import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { MessageChannel } from "node:worker_threads";
import { describe, expect, it } from "vitest";
import type { Actor } from "../src/actor";
import { ActorSystem } from "../src/actor.system";
import { discardLogger } from "../src/discard.logger";
import { ActorNotRegisteredError } from "../src/errors";
import { PostStart } from "../src/messages";
import { PortTransport } from "../src/port.transport";
import { type ActorClass, Props } from "../src/props";
import type { ReceiveContext } from "../src/receive.context";
import {
  callerScript,
  defaultMessageRegistry,
  recipeOf,
  registerActor,
  registerMessage,
  scriptUrlOf,
} from "../src/registration";

describe("registerActor", () => {
  it("infers the registering module from the call site", () => {
    class Inferred implements Actor {
      preStart(): void {}

      receive(): void {}

      postStop(): void {}
    }

    registerActor(Inferred);

    const recipe = recipeOf(Props.create(Inferred));
    expect(recipe.actor).toBe("Inferred");
    expect(new URL(recipe.module).pathname.endsWith("/test/registration.test.ts")).toBe(true);
    expect(recipe.args).toBeUndefined();
  });

  it("honors an explicit module URL over inference", () => {
    class Explicit implements Actor {
      preStart(): void {}

      receive(): void {}

      postStop(): void {}
    }

    registerActor(Explicit, "file:///elsewhere/explicit.actor.mjs");

    expect(recipeOf(Props.create(Explicit)).module).toBe("file:///elsewhere/explicit.actor.mjs");
  });

  it("is idempotent for the same class and module, and refuses a move", () => {
    class Fixed implements Actor {
      preStart(): void {}

      receive(): void {}

      postStop(): void {}
    }

    registerActor(Fixed, "file:///a.mjs");
    registerActor(Fixed, "file:///a.mjs");

    expect(() => registerActor(Fixed, "file:///b.mjs")).toThrow(
      'actor class "Fixed" is already registered from "file:///a.mjs"',
    );
  });

  it("refuses an anonymous class", () => {
    // A class expression in an array literal gets no inferred name.
    const list: Array<new () => object> = [class {}];
    const anonymous = list[0] as unknown as ActorClass;

    expect(() => registerActor(anonymous, "file:///x.mjs")).toThrow(
      "an anonymous class cannot be registered; export it as a named class",
    );
  });

  it("refuses registration from a module it cannot infer", () => {
    const attempt = new Function(
      "registerActor",
      "class Sneaky { preStart() {} receive() {} postStop() {} } return () => registerActor(Sneaky);",
    )(registerActor) as () => void;

    expect(attempt).toThrow('cannot infer the module of actor class "Sneaky"');
  });
});

describe("call-site helpers", () => {
  it("picks the registering module's frame", () => {
    expect(callerScript([])).toBe("");
    expect(
      callerScript([{ scriptName: "helper" }, { scriptName: "api" }, { scriptName: "caller" }]),
    ).toBe("caller");
  });

  it("normalizes script names to module URLs", () => {
    const absoluteScript = resolve("app", "greeter.actor.js");

    expect(scriptUrlOf("file:///app/greeter.actor.js")).toBe("file:///app/greeter.actor.js");
    expect(scriptUrlOf(absoluteScript)).toBe(pathToFileURL(absoluteScript).href);
    expect(scriptUrlOf("")).toBeUndefined();
    expect(scriptUrlOf("[eval]")).toBeUndefined();
    expect(scriptUrlOf("evalmachine.<anonymous>")).toBeUndefined();
  });
});

describe("recipeOf", () => {
  it("refuses an unregistered class by name", () => {
    class Stray implements Actor {
      preStart(): void {}

      receive(): void {}

      postStop(): void {}
    }

    expect(() => recipeOf(Props.create(Stray))).toThrow(ActorNotRegisteredError);
    expect(() => recipeOf(Props.create(Stray))).toThrow(
      'actor class "Stray" is not registered; call registerActor(Stray) in its module',
    );
  });

  it("carries cloneable constructor arguments", () => {
    class Configured implements Actor {
      constructor(readonly limit: number) {}

      preStart(): void {}

      receive(): void {}

      postStop(): void {}
    }

    registerActor(Configured, "file:///app/configured.actor.mjs");

    const recipe = recipeOf(Props.create(Configured, 7));
    expect(recipe.args).toEqual([7]);
  });

  it("refuses constructor arguments that cannot cross an isolate", () => {
    class Hooked implements Actor {
      constructor(readonly onDone: () => void) {}

      preStart(): void {}

      receive(): void {}

      postStop(): void {}
    }

    registerActor(Hooked, "file:///app/hooked.actor.mjs");

    expect(() => recipeOf(Props.create(Hooked, () => {}))).toThrow(
      'Props arguments for "Hooked" must be structured-cloneable data',
    );
  });
});

describe("registerMessage", () => {
  it("registers into the isolate's default registry", () => {
    class Wave {
      constructor(readonly n: number) {}
    }

    class Splash {
      constructor(readonly n: number) {}
    }

    registerMessage(Wave);
    registerMessage(Splash, "test.Splash");

    expect(defaultMessageRegistry.idOf(Wave)).toBe("Wave");
    expect(defaultMessageRegistry.idOf(Splash)).toBe("test.Splash");
  });

  it("feeds the transport layer, so instanceof survives a hop", async () => {
    class Ripple {
      constructor(readonly n: number) {}

      doubled(): number {
        return this.n * 2;
      }
    }

    registerMessage(Ripple);

    const system = new ActorSystem("reg", { logger: discardLogger });
    await system.start();
    const channel = new MessageChannel();
    const near = new PortTransport(system, defaultMessageRegistry, channel.port1, 0, 0);
    const far = new PortTransport(system, defaultMessageRegistry, channel.port2, 0, 0);

    const target = await system.spawn("mirror", {
      preStart(): void {},
      receive(ctx: ReceiveContext): void {
        if (ctx.message instanceof PostStart) {
          return;
        }

        if (ctx.message instanceof Ripple) {
          ctx.response(new Ripple(ctx.message.n + 1));
        }
      },
      postStop(): void {},
    });

    const reply = await near.ask(target.path(), new Ripple(1), 5000);
    expect(reply).toBeInstanceOf(Ripple);
    expect((reply as Ripple).doubled()).toBe(4);

    near.close();
    far.close();
    await system.stop();
  });
});
