import { describe, expect, it } from "vitest";
import { ActorSystem } from "../src/actor/actor.system";
import { discardLogger } from "../src/logger/discard.logger";
import { defaultLogger, JsonLogger } from "../src/logger/json.logger";
import type { Fields, Level } from "../src/logger/logger";

/** A sink that captures each written line for assertions. */
class CaptureStream {
  readonly lines: string[] = [];

  write(chunk: string): boolean {
    this.lines.push(chunk);
    return true;
  }
}

function makeLogger(level?: Level): { logger: JsonLogger; sink: CaptureStream } {
  const sink = new CaptureStream();
  const stream = sink as unknown as NodeJS.WritableStream;
  const logger =
    level === undefined ? new JsonLogger({ stream }) : new JsonLogger({ level, stream });
  return { logger, sink };
}

function parse(line: string): Fields {
  return JSON.parse(line) as Fields;
}

describe("JsonLogger", () => {
  it("writes one JSON line with time, level, and message", () => {
    const { logger, sink } = makeLogger();

    logger.info("started", { actor: "greeter" });

    expect(sink.lines).toHaveLength(1);
    const entry = parse(sink.lines[0] as string);
    expect(entry.level).toBe("info");
    expect(entry.msg).toBe("started");
    expect(entry.actor).toBe("greeter");
    expect(typeof entry.time).toBe("string");
    expect(Number.isNaN(Date.parse(entry.time as string))).toBe(false);
    expect((sink.lines[0] as string).endsWith("\n")).toBe(true);
  });

  it("emits entries at its level and above and drops the rest", () => {
    const { logger, sink } = makeLogger();

    logger.debug("hidden");
    logger.info("kept");
    logger.warn("kept");
    logger.error("kept");

    expect(sink.lines).toHaveLength(3);
    expect(logger.level()).toBe("info");
    expect(logger.enabled("debug")).toBe(false);
    expect(logger.enabled("info")).toBe(true);
    expect(logger.enabled("error")).toBe(true);
  });

  it("emits everything at debug level and nothing at off", () => {
    const debug = makeLogger("debug");
    debug.logger.debug("visible");
    expect(debug.sink.lines).toHaveLength(1);
    expect(parse(debug.sink.lines[0] as string).level).toBe("debug");

    const off = makeLogger("off");
    off.logger.debug("dropped");
    off.logger.info("dropped");
    off.logger.warn("dropped");
    off.logger.error("dropped");
    expect(off.sink.lines).toHaveLength(0);
    expect(off.logger.enabled("error")).toBe(false);
  });

  it("suppresses warnings below the error level", () => {
    const { logger, sink } = makeLogger("error");

    logger.warn("hidden");
    logger.error("kept");

    expect(sink.lines).toHaveLength(1);
    expect(parse(sink.lines[0] as string).level).toBe("error");
  });

  it("computes lazy fields only when the entry is emitted", () => {
    const { logger, sink } = makeLogger();
    let computed = 0;
    const lazy = (): Fields => {
      computed++;
      return { size: 3 };
    };

    logger.debug("hidden", lazy);
    expect(computed).toBe(0);

    logger.info("kept", lazy);
    expect(computed).toBe(1);
    expect(parse(sink.lines[0] as string).size).toBe(3);
  });

  it("binds fields with `with` and lets entry fields win", () => {
    const { logger, sink } = makeLogger();
    const bound = logger.with({ actor: "greeter", node: "a" }).with({ node: "b" });

    bound.info("hello");
    bound.info("hello", { node: "c" });

    expect(parse(sink.lines[0] as string)).toMatchObject({ actor: "greeter", node: "b" });
    expect(parse(sink.lines[1] as string)).toMatchObject({ actor: "greeter", node: "c" });
  });

  it("serializes errors as name, message, and stack", () => {
    const { logger, sink } = makeLogger();

    logger.error("postStop failed", { error: new RangeError("cleanup failed") });

    const entry = parse(sink.lines[0] as string) as { error: Fields };
    expect(entry.error.name).toBe("RangeError");
    expect(entry.error.message).toBe("cleanup failed");
    expect(typeof entry.error.stack).toBe("string");
  });

  it("still emits a line when the payload cannot be serialized", () => {
    const { logger, sink } = makeLogger();
    const circular: Fields = {};
    circular.self = circular;

    logger.info("survived", { circular });

    const entry = parse(sink.lines[0] as string);
    expect(entry.msg).toBe("survived");
    expect(entry.unserializable).toBe(true);
  });

  it("defaults to info on standard error", () => {
    expect(defaultLogger.level()).toBe("info");
    expect(defaultLogger.enabled("debug")).toBe(false);
  });
});

describe("discardLogger", () => {
  it("drops everything and reports itself off", () => {
    expect(() => {
      discardLogger.debug("x");
      discardLogger.info("x");
      discardLogger.warn("x");
      discardLogger.error("x");
    }).not.toThrow();

    expect(discardLogger.level()).toBe("off");
    expect(discardLogger.enabled("error")).toBe(false);
    expect(discardLogger.with({ actor: "greeter" })).toBe(discardLogger);
  });
});

describe("ActorSystem logging", () => {
  it("hands out the default logger unless one is configured", () => {
    expect(new ActorSystem("plain").logger()).toBe(defaultLogger);

    const system = new ActorSystem("quiet", { logger: discardLogger });
    expect(system.logger()).toBe(discardLogger);
  });

  it("exposes the logger to lifecycle hooks", async () => {
    const sink = new CaptureStream();
    const system = new ActorSystem("hooks", {
      logger: new JsonLogger({ stream: sink as unknown as NodeJS.WritableStream }),
    });
    await system.start();

    await system.spawn("worker", {
      preStart(ctx): void {
        ctx.logger().info("acquiring", { actor: ctx.actorName() });
      },
      receive(): void {},
      postStop(): void {},
    });

    await system.stop();

    const entry = parse(sink.lines[0] as string);
    expect(entry.msg).toBe("acquiring");
    expect(entry.actor).toBe("worker");
  });
});
