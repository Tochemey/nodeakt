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

import { describe, expect, it } from "vitest";
import { ActorSystem } from "../src/actor.system";
import type { CallSiteScript } from "../src/call.sites";
import { discardLogger } from "../src/discard.logger";
import { JsonLogger } from "../src/json.logger";
import type { Fields, Level } from "../src/logger";
import {
  defaultLogger,
  firstScriptName,
  resolveCaller,
  TextLogger,
  threadName,
} from "../src/text.logger";

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

  it("defaults to info on standard error with no options", () => {
    const logger = new JsonLogger();

    expect(logger.level()).toBe("info");
    expect(logger.enabled("debug")).toBe(false);
  });
});

function makeText(level?: Level): { logger: TextLogger; sink: CaptureStream } {
  const sink = new CaptureStream();
  const stream = sink as unknown as NodeJS.WritableStream;
  const logger =
    level === undefined ? new TextLogger({ stream }) : new TextLogger({ level, stream });
  return { logger, sink };
}

/** The first physical line of a written chunk, before any appended stack. */
function textLine(chunk: string): string {
  return (chunk.split("\n")[0] ?? "") as string;
}

/** The contents of each bracketed column, in order. */
function columnsOf(chunk: string): string[] {
  const line = textLine(chunk);
  const dash = line.indexOf(" - ");
  const head = dash === -1 ? line : line.slice(0, dash);
  return [...head.matchAll(/\[([^\]]*)\]/g)].map((match) => match[1] as string);
}

/** The message, between the columns and any fields segment. */
function msgOf(chunk: string): string {
  const line = textLine(chunk);
  const dash = line.indexOf(" - ");
  const tail = dash === -1 ? line : line.slice(dash + 3);
  const fields = / \{.*\}$/.exec(tail);
  return fields === null ? tail : tail.slice(0, fields.index);
}

/** The field text, or `null` when the line carries no fields segment. */
function fieldsOf(chunk: string): string | null {
  const fields = / \{(.*)\}$/.exec(textLine(chunk));
  return fields === null ? null : (fields[1] as string);
}

describe("TextLogger", () => {
  it("renders a clean line with a lifted logger column and fields", () => {
    const { logger, sink } = makeText();

    logger.with({ logger: "orders" }).info("actor started", { actor: "user/greeter" });

    expect(sink.lines).toHaveLength(1);
    const chunk = sink.lines[0] as string;
    const columns = columnsOf(chunk);
    expect(columns[1]).toBe("INF");
    expect(columns).toContain("orders");
    expect(columns).toContain("main");
    expect(textLine(chunk)).not.toContain("[]");
    expect(msgOf(chunk)).toBe("actor started");
    expect(fieldsOf(chunk)).toBe("actor=user/greeter");
    expect(Number.isNaN(Date.parse(columns[0] as string))).toBe(false);
    expect(chunk.endsWith("\n")).toBe(true);
  });

  it("reports the call site as dir/file:line in a column", () => {
    const { logger, sink } = makeText();

    logger.info("here");

    const caller = columnsOf(sink.lines[0] as string).find((c) => c.includes("logger.test"));
    expect(caller).toBeDefined();
    expect(caller).toMatch(/:\d+$/);
  });

  it("lifts the marker column and omits the absent logger column", () => {
    const { logger, sink } = makeText();

    logger.info("mark", { marker: "audit" });

    const chunk = sink.lines[0] as string;
    expect(columnsOf(chunk)).toContain("audit");
    expect(textLine(chunk)).not.toContain("[]");
    expect(fieldsOf(chunk)).toBeNull();
  });

  it("renders bound and per-entry fields, per-entry winning", () => {
    const { logger, sink } = makeText();
    const bound = logger.with({ node: "a", region: "eu" });

    bound.info("hello", { node: "b" });

    expect(fieldsOf(sink.lines[0] as string)).toBe("node=b, region=eu");
  });

  it("carries bound fields alone when an entry adds none", () => {
    const { logger, sink } = makeText();

    logger.with({ node: "a" }).info("hello");

    expect(fieldsOf(sink.lines[0] as string)).toBe("node=a");
  });

  it("merges fields across chained with, later binds winning", () => {
    const { logger, sink } = makeText();

    logger.with({ node: "a", region: "eu" }).with({ node: "b" }).info("hello");

    expect(fieldsOf(sink.lines[0] as string)).toBe("node=b, region=eu");
  });

  it("omits reserved columns whose value is explicitly undefined", () => {
    const { logger, sink } = makeText();

    logger.info("mark", { logger: undefined, marker: undefined, actor: "greeter" });

    const chunk = sink.lines[0] as string;
    expect(textLine(chunk)).not.toContain("[]");
    expect(fieldsOf(chunk)).toBe("actor=greeter");
  });

  it("omits the fields segment when there are none", () => {
    const { logger, sink } = makeText();

    logger.info("bare");

    const chunk = sink.lines[0] as string;
    expect(fieldsOf(chunk)).toBeNull();
    expect(textLine(chunk)).not.toContain("{");
    expect(textLine(chunk)).not.toContain("[]");
    expect(msgOf(chunk)).toBe("bare");
  });

  it("emits entries at its level and above and drops the rest", () => {
    const { logger, sink } = makeText();

    logger.debug("hidden");
    logger.info("kept");
    logger.warn("kept");
    logger.error("kept");

    expect(sink.lines).toHaveLength(3);
    expect(logger.level()).toBe("info");
    expect(logger.enabled("debug")).toBe(false);
    expect(logger.enabled("error")).toBe(true);
  });

  it("emits everything at debug level and nothing at off", () => {
    const debug = makeText("debug");
    debug.logger.debug("visible");
    expect(columnsOf(debug.sink.lines[0] as string)[1]).toBe("DBG");

    const off = makeText("off");
    off.logger.debug("dropped");
    off.logger.info("dropped");
    off.logger.warn("dropped");
    off.logger.error("dropped");
    expect(off.sink.lines).toHaveLength(0);
  });

  it("suppresses warnings below the error level", () => {
    const { logger, sink } = makeText("error");

    logger.warn("hidden");
    logger.error("kept");

    expect(sink.lines).toHaveLength(1);
    expect(columnsOf(sink.lines[0] as string)[1]).toBe("ERR");
  });

  it("computes lazy fields only when the entry is emitted", () => {
    const { logger, sink } = makeText();
    let computed = 0;
    const lazy = (): Fields => {
      computed++;
      return { size: 3 };
    };

    logger.debug("hidden", lazy);
    expect(computed).toBe(0);

    logger.info("kept", lazy);
    expect(computed).toBe(1);
    expect(fieldsOf(sink.lines[0] as string)).toBe("size=3");
  });

  it("renders an error as name: message and appends its stack", () => {
    const { logger, sink } = makeText();

    logger.error("postStop failed", { error: new RangeError("cleanup failed") });

    const chunk = sink.lines[0] as string;
    expect(fieldsOf(chunk)).toBe("error=RangeError: cleanup failed");
    expect(chunk).toContain("RangeError: cleanup failed\n");
    expect(chunk.split("\n").length).toBeGreaterThan(2);
  });

  it("renders an error that carries no stack without appending lines", () => {
    const { logger, sink } = makeText();
    const stackless = new Error("no trace");
    delete (stackless as { stack?: string }).stack;
    const empty = new Error("blank trace");
    empty.stack = "";

    logger.info("first", { error: stackless });
    logger.info("second", { error: empty });

    expect((sink.lines[0] as string).trimEnd().split("\n")).toHaveLength(1);
    expect((sink.lines[1] as string).trimEnd().split("\n")).toHaveLength(1);
    expect(fieldsOf(sink.lines[0] as string)).toBe("error=Error: no trace");
  });

  it("serializes object fields as JSON and stringifies scalars", () => {
    const { logger, sink } = makeText();

    logger.info("mixed", { config: { retries: 3 }, count: 7, ready: true });

    expect(fieldsOf(sink.lines[0] as string)).toBe('config={"retries":3}, count=7, ready=true');
  });

  it("falls back to a string when an object serializes to nothing", () => {
    const { logger, sink } = makeText();

    logger.info("empty json", { value: { toJSON: (): undefined => undefined } });

    expect(fieldsOf(sink.lines[0] as string)).toBe("value=[object Object]");
  });

  it("still emits a line when a field cannot be serialized", () => {
    const { logger, sink } = makeText();
    const circular: Fields = {};
    circular.self = circular;

    logger.info("survived", { circular });

    const chunk = sink.lines[0] as string;
    expect(msgOf(chunk)).toBe("survived");
    expect(fieldsOf(chunk)).toBe("circular=[unserializable]");
  });

  it("defaults to info on standard error", () => {
    expect(defaultLogger.level()).toBe("info");
    expect(defaultLogger.enabled("debug")).toBe(false);
  });
});

describe("threadName", () => {
  it("names the main thread and worker threads", () => {
    expect(threadName(true, 0)).toBe("main");
    expect(threadName(false, 3)).toBe("worker-3");
  });
});

describe("firstScriptName", () => {
  it("answers frame 0's script, or empty when there is none", () => {
    expect(firstScriptName([{ scriptName: "/src/app.ts", lineNumber: 4 }])).toBe("/src/app.ts");
    expect(firstScriptName([])).toBe("");
  });
});

describe("resolveCaller", () => {
  function frame(scriptName: string, lineNumber: number): CallSiteScript {
    return { scriptName, lineNumber };
  }

  it("skips this module and empty frames, then reports dir/file:line", () => {
    const frames = [frame("own", 1), frame("", 2), frame("/project/src/app.ts", 42)];

    expect(resolveCaller(frames, "own")).toBe("src/app.ts:42");
  });

  it("keeps a single-segment path and a bare script name intact", () => {
    expect(resolveCaller([frame("/main.ts", 9)], "own")).toBe("main.ts:9");
    expect(resolveCaller([frame("[eval]", 5)], "own")).toBe("[eval]:5");
  });

  it("returns empty when no frame is available or the module is unknown", () => {
    expect(resolveCaller([frame("own", 1)], "own")).toBe("");
    expect(resolveCaller([frame("/project/src/app.ts", 1)], "")).toBe("");
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

    const acquiring = sink.lines
      .map((line: string): Fields => parse(line))
      .find((entry: Fields): boolean => entry.msg === "acquiring");
    expect(acquiring).toBeDefined();
    expect(acquiring?.actor).toBe("worker");
  });
});
