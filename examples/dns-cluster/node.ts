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
 * A three-node cluster that finds its peers through DNS, forms over TCP, and
 * exercises the distributed key/value store end to end, the way a headless service
 * does in a container orchestrator.
 *
 * Every replica runs this same program. On boot it resolves one DNS name that
 * returns every replica's address, joins the cluster over those addresses, and then:
 *
 * - serves an HTTP API for distributed writes and reads that route to the owning
 *   partition, a conditional write that keeps a key unique, and a cluster-wide scan;
 * - writes its own heartbeat every few seconds and scans the whole cluster, so the
 *   logs show each node reading keys written by the others;
 * - logs every cluster lifecycle event, join, leave, coordinator change, and
 *   rebalance, as it happens;
 * - leaves gracefully on a stop signal, draining its partitions first.
 *
 * This is the actor-agnostic store the actor system builds its distributed registry
 * and placement on; this program drives it directly to prove the layer underneath
 * end to end. The imports carry a `.js` extension because this script runs through
 * `tsx`, whose runtime resolver treats a bare `clustering.node` as a native addon.
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { CLUSTER_EVENT_TOPIC, type ClusterEvent } from "../../src/clustering.events.js";
import { ClusterNode } from "../../src/clustering.host.js";
import { DnsDiscovery, DnsRecordType } from "../../src/discovery/dns.js";
import { EventStream } from "../../src/eventstream.js";
import { PutCondition, WriteKind } from "../../src/kv/discriminants.js";
import type { Entry, ScanEntry, WriteResult } from "../../src/kv/ports.js";

/** How often, in milliseconds, a node re-writes its heartbeat and scans the cluster. */
const HEARTBEAT_INTERVAL_MS: number = 5_000;

/** The key prefix each node writes its own heartbeat under, one entry per host. */
const HEARTBEAT_PREFIX: string = "heartbeat:";

/** Codecs for the store's opaque byte values, which this example writes as UTF-8 text. */
const encoder: TextEncoder = new TextEncoder();
const decoder: TextDecoder = new TextDecoder();

/** Reads a required string environment variable, failing fast when it is absent. */
function requireEnv(name: string): string {
  const value: string | undefined = process.env[name];
  if (value === undefined || value.trim().length === 0) {
    throw new Error(`missing required environment variable ${name}`);
  }

  return value.trim();
}

/** Reads a port environment variable, or the given default when it is absent. */
function portEnv(name: string, fallback: number): number {
  const raw: string | undefined = process.env[name];
  if (raw === undefined) {
    return fallback;
  }

  const port: number = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`environment variable ${name} is not a valid port: ${raw}`);
  }

  return port;
}

/** Timestamped log line, prefixed with this replica so a reader can follow each one. */
function log(message: string): void {
  process.stdout.write(`[${host}] ${message}\n`);
}

// The advertised host is this replica's own resolvable name; the discovery hostname
// resolves to every replica's address, and both endpoints run on fixed ports, so a
// resolved address plus the gossip port is one complete peer to join.
const host: string = requireEnv("NODEAKT_HOST");
const discoveryHostname: string = requireEnv("NODEAKT_DISCOVERY_HOSTNAME");
const gossipPort: number = portEnv("NODEAKT_GOSSIP_PORT", 7946);
const dataPort: number = portEnv("NODEAKT_DATA_PORT", 8080);
const httpPort: number = portEnv("NODEAKT_HTTP_PORT", 3000);
const memberQuorum: number = portEnv("NODEAKT_MEMBER_QUORUM", 2);

const events: EventStream = new EventStream();
events.subscribe((event: unknown): void => {
  log(`event ${JSON.stringify(event as ClusterEvent)}`);
}, CLUSTER_EVENT_TOPIC);

log(
  `booting: resolving peers from "${discoveryHostname}", gossip :${gossipPort}, data :${dataPort}`,
);
const node: ClusterNode = await ClusterNode.start({
  discovery: new DnsDiscovery({
    hostname: discoveryHostname,
    recordType: DnsRecordType.address,
    port: gossipPort,
  }),
  host,
  gossipPort,
  dataPort,
  minimumMemberQuorum: memberQuorum,
  events,
});
log(`joined=${node.joined} address=${node.address}`);

/** The current coordinator, the oldest member, as this node sees the cluster. */
function coordinator(): string | undefined {
  return node.members()[0]?.name;
}

/** Writes `value` at `key`; a conditional write keeps `key` unique across the cluster. */
function put(key: string, value: string, unique: boolean): Promise<WriteResult> {
  return node.write({
    kind: WriteKind.put,
    key,
    value: encoder.encode(value),
    condition: unique ? PutCondition.ifAbsent : PutCondition.none,
  });
}

/**
 * Writes this node's heartbeat, then scans the whole cluster. The heartbeat routes
 * to whichever partition owns its key, usually a different node, and the scan reads
 * every partition from its primary. The log names the peers whose heartbeats this
 * node can read, its own excluded: reading a heartbeat another node wrote is the
 * proof that the store is distributed. A stopped minority half cannot write or scan
 * and logs that instead.
 */
async function heartbeat(): Promise<void> {
  try {
    const own: string = `${HEARTBEAT_PREFIX}${host}`;
    await put(own, new Date().toISOString(), false);
    const entries: ScanEntry[] = await node.scan();
    const peers: string[] = entries
      .map((entry: ScanEntry): string => entry.key)
      .filter((key: string): boolean => key.startsWith(HEARTBEAT_PREFIX) && key !== own)
      .map((key: string): string => key.slice(HEARTBEAT_PREFIX.length))
      .sort();
    log(
      `members=${node.members().length} coordinator=${coordinator()} ` +
        `keys=${entries.length} reads peer heartbeats from [${peers.join(", ")}]`,
    );
  } catch (error: unknown) {
    log(`cluster not serving (split-brain minority or forming): ${String(error)}`);
  }
}

const beating: ReturnType<typeof setInterval> = setInterval((): void => {
  void heartbeat();
}, HEARTBEAT_INTERVAL_MS);

/** Writes a JSON body with the given status. */
function json(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

/**
 * Answers the API. Writes and reads route to the partition that owns the key, so a
 * value written through one node is readable through any other, and the scan
 * gathers every partition from its primary.
 */
async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url: URL = new URL(request.url ?? "/", `http://${host}`);
  const method: string = request.method ?? "GET";
  const key: string | null = url.searchParams.get("key");

  if (method === "GET" && url.pathname === "/health") {
    json(response, 200, {
      host,
      address: node.address,
      joined: node.joined,
      coordinator: coordinator(),
    });
    return;
  }

  if (method === "GET" && url.pathname === "/members") {
    json(
      response,
      200,
      node.members().map((member): string => member.name),
    );
    return;
  }

  if (method === "PUT" && url.pathname === "/kv") {
    const value: string | null = url.searchParams.get("value");
    if (key === null || value === null) {
      json(response, 400, {
        error: "query parameters 'key' and 'value' are required",
      });
      return;
    }

    // ?unique=1 makes it a claim: it applies only if the key is unset, the way a
    // cluster keeps a name unique under a partition.
    const unique: boolean = url.searchParams.get("unique") === "1";
    const result: WriteResult = await put(key, value, unique);
    json(response, 200, {
      key,
      value,
      applied: result.applied,
      writtenBy: host,
    });
    return;
  }

  if (method === "GET" && url.pathname === "/kv") {
    if (key === null) {
      json(response, 400, { error: "query parameter 'key' is required" });
      return;
    }

    const entry: Entry | undefined = await node.read(key);
    const value: string | null = entry?.value === undefined ? null : decoder.decode(entry.value);
    json(response, 200, { key, value, readFrom: host });
    return;
  }

  if (method === "GET" && url.pathname === "/keys") {
    const entries: ScanEntry[] = await node.scan();
    json(
      response,
      200,
      entries.map((entry: ScanEntry): { key: string; value: string } => ({
        key: entry.key,
        value: decoder.decode(entry.value),
      })),
    );
    return;
  }

  json(response, 404, { error: "not found" });
}

const server: Server = createServer((request: IncomingMessage, response: ServerResponse): void => {
  handle(request, response).catch((error: unknown): void => {
    json(response, 500, { error: String(error) });
  });
});

server.listen(httpPort, (): void => {
  log(
    `http api on :${httpPort} (GET /health /members /keys, PUT|GET /kv?key= [&value= &unique=1])`,
  );
});

// A container stop signals SIGTERM: leave the cluster gracefully so the survivors
// take over this node's partitions before it goes, then close the server.
let leaving: boolean = false;
async function shutdown(): Promise<void> {
  if (leaving) {
    return;
  }

  leaving = true;
  clearInterval(beating);
  log("received stop signal, leaving the cluster gracefully");
  await node.leave();
  server.close();
  log("left the cluster");
}

process.on("SIGTERM", (): void => {
  void shutdown();
});
process.on("SIGINT", (): void => {
  void shutdown();
});
