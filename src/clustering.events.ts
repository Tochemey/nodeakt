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
 * The cluster's own lifecycle events, the topology transitions a node observes
 * and reports so the actor runtime above it can react to the cluster changing
 * shape and surface its own events to an application.
 *
 * The engine emits these through an injected {@link ClusterEventSink}, so it names
 * the transitions without knowing where they go; the runtime that owns the cluster
 * routes them onto its event stream. A `node-left` is reported only after the
 * departed node's partitions have been repaired, so a consumer that reacts to it
 * never reads the registry before a dead node's records are promoted.
 *
 * This is cluster-runtime infrastructure the actor system consumes, not a public
 * surface an application uses directly.
 *
 * @internal
 */

/** A node became a member of the cluster. @internal */
export interface NodeJoined {
  readonly type: "node-joined";
  /** The joined node's data endpoint. */
  readonly address: string;
}

/** A node left the cluster, reported only once its partitions have been repaired. @internal */
export interface NodeLeft {
  readonly type: "node-left";
  /** The departed node's data endpoint. */
  readonly address: string;
}

/** The coordinator, the oldest member, changed. @internal */
export interface CoordinatorChanged {
  readonly type: "coordinator-changed";
  /** The new coordinator's data endpoint. */
  readonly coordinator: string;
}

/** The coordinator began recomputing and pushing the routing table. @internal */
export interface RebalanceStarted {
  readonly type: "rebalance-started";
}

/** The coordinator finished recomputing and installing the routing table. @internal */
export interface RebalanceCompleted {
  readonly type: "rebalance-completed";
}

/** One cluster lifecycle transition. @internal */
export type ClusterEvent =
  | NodeJoined
  | NodeLeft
  | CoordinatorChanged
  | RebalanceStarted
  | RebalanceCompleted;

/**
 * The five {@link ClusterEvent} discriminants as named values. `satisfies`
 * validates each against the union while `as const` keeps the literal type.
 *
 * @internal
 */
export const ClusterEventType = {
  nodeJoined: "node-joined",
  nodeLeft: "node-left",
  coordinatorChanged: "coordinator-changed",
  rebalanceStarted: "rebalance-started",
  rebalanceCompleted: "rebalance-completed",
} as const satisfies Record<string, ClusterEvent["type"]>;

/** Receives each cluster lifecycle event in the order the engine observes it. @internal */
export type ClusterEventSink = (event: ClusterEvent) => void;

/** The event-stream topic a node's cluster lifecycle events publish onto. @internal */
export const CLUSTER_EVENT_TOPIC: string = "cluster";
