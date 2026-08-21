import { describe, expect, it } from "vitest";
import { type Behavior, BehaviorStack } from "../src/actor/behavior.stack";

const behavior = (name: string): Behavior => {
  const fn: Behavior = () => {};
  Object.defineProperty(fn, "name", { value: name });
  return fn;
};

describe("BehaviorStack", () => {
  it("starts empty", () => {
    const stack = new BehaviorStack();
    expect(stack.isEmpty()).toBe(true);
    expect(stack.len()).toBe(0);
    expect(stack.peek()).toBeUndefined();
    expect(stack.pop()).toBeUndefined();
  });

  it("pushes and pops in LIFO order", () => {
    const stack = new BehaviorStack();
    const [a, b, c] = [behavior("a"), behavior("b"), behavior("c")];
    stack.push(a);
    stack.push(b);
    stack.push(c);
    expect(stack.len()).toBe(3);

    expect(stack.pop()).toBe(c);
    expect(stack.pop()).toBe(b);
    expect(stack.pop()).toBe(a);
    expect(stack.pop()).toBeUndefined();
    expect(stack.isEmpty()).toBe(true);
  });

  it("peek returns the top behavior without removing it", () => {
    const stack = new BehaviorStack();
    const a = behavior("a");
    const b = behavior("b");
    stack.push(a);
    stack.push(b);

    expect(stack.peek()).toBe(b);
    expect(stack.peek()).toBe(b);
    expect(stack.len()).toBe(2);

    stack.pop();
    expect(stack.peek()).toBe(a);
  });

  it("reset empties the stack", () => {
    const stack = new BehaviorStack();
    stack.push(behavior("a"));
    stack.push(behavior("b"));

    stack.reset();
    expect(stack.isEmpty()).toBe(true);
    expect(stack.len()).toBe(0);
    expect(stack.peek()).toBeUndefined();

    // remains usable after reset
    const c = behavior("c");
    stack.push(c);
    expect(stack.peek()).toBe(c);
  });
});
