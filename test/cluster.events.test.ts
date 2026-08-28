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
import {
  type ClusterLifecycleEvent,
  CoordinatorChanged,
  NodeJoined,
  NodeLeft,
  RebalanceCompleted,
  RebalanceStarted,
  translateClusterEvent,
} from "../src/cluster.events";
import { ClusterEventType } from "../src/clustering.events";

describe("translateClusterEvent", () => {
  it("maps a node-joined event to its public class", () => {
    const event: ClusterLifecycleEvent = translateClusterEvent(
      { type: ClusterEventType.nodeJoined, address: "a:1" },
      100,
    );

    expect(event).toBeInstanceOf(NodeJoined);
    expect((event as NodeJoined).address).toBe("a:1");
    expect((event as NodeJoined).timestamp).toBe(100);
  });

  it("maps a node-left event to its public class", () => {
    const event: ClusterLifecycleEvent = translateClusterEvent(
      { type: ClusterEventType.nodeLeft, address: "b:1" },
      200,
    );

    expect(event).toBeInstanceOf(NodeLeft);
    expect((event as NodeLeft).address).toBe("b:1");
  });

  it("maps a coordinator-changed event to its public class", () => {
    const event: ClusterLifecycleEvent = translateClusterEvent(
      { type: ClusterEventType.coordinatorChanged, coordinator: "c:1" },
      300,
    );

    expect(event).toBeInstanceOf(CoordinatorChanged);
    expect((event as CoordinatorChanged).coordinator).toBe("c:1");
  });

  it("maps a rebalance-started event to its public class", () => {
    const event: ClusterLifecycleEvent = translateClusterEvent(
      { type: ClusterEventType.rebalanceStarted },
      400,
    );

    expect(event).toBeInstanceOf(RebalanceStarted);
    expect((event as RebalanceStarted).timestamp).toBe(400);
  });

  it("maps a rebalance-completed event to its public class", () => {
    const event: ClusterLifecycleEvent = translateClusterEvent(
      { type: ClusterEventType.rebalanceCompleted },
      500,
    );

    expect(event).toBeInstanceOf(RebalanceCompleted);
    expect((event as RebalanceCompleted).timestamp).toBe(500);
  });
});
