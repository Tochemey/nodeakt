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

import { lookup } from "node:dns/promises";
import { hostname } from "node:os";

/**
 * The concrete IPv4 address this node binds and advertises. Remoting
 * needs a real address, not a name: every local actor's path carries it,
 * and other nodes dial it back to reply. Inside a Docker Compose
 * network the container's own hostname resolves to its network address,
 * so the lookup is the service-discovery step; outside Docker, set
 * `NODEAKT_HOST` (the local runs in the README use `127.0.0.1`).
 */
export async function advertisedHost(): Promise<string> {
  const override: string | undefined = process.env.NODEAKT_HOST;
  if (override !== undefined && override !== "") {
    return override;
  }

  const { address } = await lookup(hostname(), { family: 4 });
  return address;
}
