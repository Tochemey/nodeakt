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

/*
 * Cluster lifecycle events, published on the actor system's event stream and
 * consumed through `ActorSystem.subscribe` by `instanceof`, the same convention
 * the actor lifecycle events follow. The first five re-publish the cluster
 * runtime's own membership and rebalance signals; the relocation family is defined
 * here, marking when actors on a departed node are recreated on survivors.
 */

import { type ClusterEvent, ClusterEventType } from "./clustering.events";

/** Published when a node joins the cluster this node can see. */
export class NodeJoined {
  constructor(
    /** The joined node's cluster address. */
    readonly address: string,
    /** When it was observed, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/** Published when a node leaves the cluster, gracefully or by crash, after its
 * partitions have been repaired onto the survivors. */
export class NodeLeft {
  constructor(
    /** The departed node's cluster address. */
    readonly address: string,
    /** When the departure was reported, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/** Published when the cluster coordinator, the oldest live member, changes. */
export class CoordinatorChanged {
  constructor(
    /** The new coordinator's cluster address. */
    readonly coordinator: string,
    /** When the change was observed, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/** Published when the store begins redistributing its partitions after a
 * membership change. Transparent to actors, which keep resolving names. */
export class RebalanceStarted {
  constructor(
    /** When the rebalance began, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/** Published when a partition rebalance has settled. */
export class RebalanceCompleted {
  constructor(
    /** When the rebalance completed, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/** Published by the coordinator when it begins recreating the relocatable actors
 * of a departed node on the survivors. */
export class RelocationStarted {
  constructor(
    /** The departed node whose actors are being relocated. */
    readonly departed: string,
    /** When the relocation pass began, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/** Published by the coordinator when every relocatable actor of a departed node
 * has a new owner. */
export class RelocationCompleted {
  constructor(
    /** The departed node whose actors were relocated. */
    readonly departed: string,
    /** The names given a new owner in this pass. */
    readonly relocated: readonly string[],
    /** When the relocation pass completed, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/** Published by the coordinator when a relocation pass could not place some names,
 * which survive as records the next sweep retries; not terminal. */
export class RelocationFailed {
  constructor(
    /** The departed node whose actors were being relocated. */
    readonly departed: string,
    /** The names the pass could not place, retried by a later sweep. */
    readonly names: readonly string[],
    /** When the pass gave up on these names, in milliseconds since the epoch. */
    readonly timestamp: number,
  ) {}
}

/** A cluster runtime membership or rebalance signal in its public, `instanceof`
 * form. @internal */
export type ClusterLifecycleEvent =
  | NodeJoined
  | NodeLeft
  | CoordinatorChanged
  | RebalanceStarted
  | RebalanceCompleted;

/** Translates one cluster runtime event into the public event class subscribers
 * observe, stamped with `timestamp`. The runtime's five membership and rebalance
 * signals each map to one class; the relocation family is published directly by the
 * relocation actor, not translated here. @internal */
export function translateClusterEvent(
  event: ClusterEvent,
  timestamp: number,
): ClusterLifecycleEvent {
  switch (event.type) {
    case ClusterEventType.nodeJoined:
      return new NodeJoined(event.address, timestamp);
    case ClusterEventType.nodeLeft:
      return new NodeLeft(event.address, timestamp);
    case ClusterEventType.coordinatorChanged:
      return new CoordinatorChanged(event.coordinator, timestamp);
    case ClusterEventType.rebalanceStarted:
      return new RebalanceStarted(timestamp);
    case ClusterEventType.rebalanceCompleted:
      return new RebalanceCompleted(timestamp);
  }
}
