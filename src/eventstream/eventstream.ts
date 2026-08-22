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

/** A stream subscriber: called once per event published to a topic it
 * is subscribed to. The function's identity is its subscription key. */
export type StreamSubscriber = (event: unknown) => void;

/**
 * EventStream is a synchronous topic-based publish and subscribe
 * channel: subscribers register a callback under a topic, and every
 * event published to that topic invokes each of them on the publisher's
 * own stack.
 *
 * A subscriber that throws never disturbs the publisher or the other
 * subscribers: the failure is handed to the stream's error handler.
 * The stream itself is domain agnostic; the actor system routes its
 * runtime events, dead letters among them, through one instance.
 */
export class EventStream {
  private readonly _topics = new Map<string, StreamSubscriber[]>();
  private readonly _onError: ((err: unknown) => void) | null;

  /** @param onError - Receives the failure of a throwing subscriber;
   * failures are swallowed when omitted. */
  constructor(onError?: (err: unknown) => void) {
    this._onError = onError ?? null;
  }

  /** Subscribes the callback to a topic; subscribing the same function
   * to the same topic twice is a no-op. */
  subscribe(subscriber: StreamSubscriber, topic: string): void {
    let subscribers = this._topics.get(topic);
    if (subscribers === undefined) {
      subscribers = [];
      this._topics.set(topic, subscribers);
    }

    if (!subscribers.includes(subscriber)) {
      subscribers.push(subscriber);
    }
  }

  /** Removes the callback from a topic; unknown subscriptions are
   * ignored. */
  unsubscribe(subscriber: StreamSubscriber, topic: string): void {
    const subscribers = this._topics.get(topic);
    if (subscribers === undefined) {
      return;
    }

    const at = subscribers.indexOf(subscriber);
    if (at !== -1) {
      subscribers.splice(at, 1);
    }
  }

  /** Publishes one event to every subscriber of the topic. */
  publish(topic: string, event: unknown): void {
    const subscribers = this._topics.get(topic);
    if (subscribers === undefined) {
      return;
    }

    for (const subscriber of subscribers) {
      try {
        subscriber(event);
      } catch (err) {
        this._onError?.(err);
      }
    }
  }

  /** Publishes one event to every given topic. */
  broadcast(event: unknown, topics: string[]): void {
    for (const topic of topics) {
      this.publish(topic, event);
    }
  }

  /** Returns how many subscribers the topic has. */
  subscribersCount(topic: string): number {
    return this._topics.get(topic)?.length ?? 0;
  }

  /** Drops every subscription; the stream stays usable afterwards. */
  close(): void {
    this._topics.clear();
  }
}
