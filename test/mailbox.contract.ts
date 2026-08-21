import { describe, expect, it } from "vitest";
import { ErrMailboxDisposed } from "../src/actor/errors";
import type { Mailbox } from "../src/actor/mailbox";
import { createReceiveContext, type ReceiveContext } from "../src/actor/receive.context";

export function ctx(message: unknown): ReceiveContext {
  return createReceiveContext(message);
}

/** Dequeues every message and returns their payloads in dequeue order. */
export function drain(mailbox: Mailbox): unknown[] {
  const out: unknown[] = [];
  for (let msg = mailbox.dequeue(); msg !== undefined; msg = mailbox.dequeue()) {
    out.push(msg.message);
  }

  return out;
}

/** Ranks messages of shape `{ urgency: number }`, highest urgency first. */
export const byUrgency = (a: unknown, b: unknown): boolean =>
  (a as { urgency: number }).urgency > (b as { urgency: number }).urgency;

/** Contract tests every mailbox implementation must satisfy. */
export function describeMailboxContract(create: () => Mailbox): void {
  describe("mailbox contract", () => {
    it("starts empty", () => {
      const mb = create();
      expect(mb.isEmpty()).toBe(true);
      expect(mb.len()).toBe(0);
      expect(mb.dequeue()).toBeUndefined();
    });

    it("tracks len and isEmpty across enqueue/dequeue", () => {
      const mb = create();
      expect(mb.enqueue(ctx(1))).toBeNull();
      expect(mb.enqueue(ctx(2))).toBeNull();
      expect(mb.isEmpty()).toBe(false);
      expect(mb.len()).toBe(2);

      mb.dequeue();
      expect(mb.len()).toBe(1);
      mb.dequeue();
      expect(mb.isEmpty()).toBe(true);
      expect(mb.dequeue()).toBeUndefined();
    });

    it("is reusable after being drained", () => {
      const mb = create();
      mb.enqueue(ctx("a"));
      mb.dequeue();
      expect(mb.enqueue(ctx("b"))).toBeNull();
      expect(mb.dequeue()?.message).toBe("b");
    });

    it("rejects enqueue and drops messages after dispose", () => {
      const mb = create();
      mb.enqueue(ctx(1));
      mb.dispose();
      expect(mb.enqueue(ctx(2))).toBe(ErrMailboxDisposed);
      expect(mb.dequeue()).toBeUndefined();
      expect(mb.isEmpty()).toBe(true);
      expect(mb.len()).toBe(0);
      mb.dispose(); // disposing twice is a no-op
    });
  });
}

/** FIFO tests shared by the order-preserving mailboxes. */
export function describeFifo(create: () => Mailbox): void {
  describe("FIFO ordering", () => {
    it("dequeues in arrival order", () => {
      const mb = create();
      for (let i = 0; i < 100; i++) {
        expect(mb.enqueue(ctx(i))).toBeNull();
      }

      expect(drain(mb)).toEqual([...Array(100).keys()]);
    });

    it("preserves order across interleaved enqueue/dequeue", () => {
      const mb = create();
      const out: unknown[] = [];
      let next = 0;
      for (let round = 0; round < 50; round++) {
        mb.enqueue(ctx(next++));
        mb.enqueue(ctx(next++));
        out.push(mb.dequeue()?.message);
      }

      out.push(...drain(mb));
      expect(out).toEqual([...Array(100).keys()]);
    });
  });
}
