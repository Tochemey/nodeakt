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

import type { ClusterOptions } from "./cluster.options";
import type { Logger } from "./logger";
import type { RemoteOptions } from "./remote.options";

/** Options customizing an actor system. */
export interface ActorSystemOptions {
  /** The logger the runtime reports through; one JSON line per entry on
   * standard error at info level by default. */
  logger?: Logger;

  /** Enables remoting on the system. Absent by default, in which case
   * the system is single-node and the transport never loads. */
  remote?: RemoteOptions;

  /** Enables clustering on the system: the node joins a cluster of peers
   * and its registry is distributed. Requires `remote`, since a clustered
   * node must be reachable for actor messages.
   *
   * @internal Not yet public: the option and its discovery types are exported,
   * and documented, once the distributed actor API that rides on them lands. */
  cluster?: ClusterOptions;

  /**
   * The default deadline, in milliseconds, applied to an `ask` or
   * `request` whose own timeout is omitted or non-positive, so no
   * reply-bearing call ever waits without a bound. A positive integer;
   * 5000 when omitted. A call that passes its own positive timeout
   * keeps it; this is only the fallback.
   */
  askTimeout?: number;
}
