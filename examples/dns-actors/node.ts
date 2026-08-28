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
 * A three-node distributed-actor cluster over DNS discovery, driven by an HTTP API.
 *
 * Each node runs a full ActorSystem with clustering enabled. Actors are spawned with
 * cluster-wide unique names, placed across the cluster with a strategy, or claimed as
 * singletons; a caller on any node reaches an actor on any other through its name.
 * When a node departs, gracefully or by a hard kill, the coordinator recreates its
 * relocatable actors on the survivors, and lookups on the survivors reach them at
 * their new home. The framework imports carry a `.js` extension, the ESM specifier
 * `tsx` resolves back to the TypeScript source it runs.
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { ActorSystem } from "../../src/actor.system.js";
import { DnsDiscovery, DnsRecordType } from "../../src/discovery/dns.js";
import { Props } from "../../src/props.js";
import { Greet, WhereAreYou, Worker } from "./worker.actor.js";

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

const host: string = requireEnv("NODEAKT_HOST");
const discoveryHostname: string = requireEnv("NODEAKT_DISCOVERY_HOSTNAME");
const remotingPort: number = portEnv("NODEAKT_REMOTING_PORT", 4000);
const gossipPort: number = portEnv("NODEAKT_GOSSIP_PORT", 7946);
const dataPort: number = portEnv("NODEAKT_DATA_PORT", 8080);
const httpPort: number = portEnv("NODEAKT_HTTP_PORT", 3000);
const memberQuorum: number = portEnv("NODEAKT_MEMBER_QUORUM", 2);

/** Timestamped log line, prefixed with this node so a reader can follow each one. */
function log(message: string): void {
  process.stdout.write(`[${host}] ${message}\n`);
}

// The bind host is the wildcard so the container accepts connections on any interface;
// the advertised host is this node's own resolvable service name, and the discovery
// hostname resolves to every node, so a peer dials each node directly after it joins.
const system: ActorSystem = new ActorSystem("dns-actors", {
  remote: { host: "0.0.0.0", advertisedHost: host, port: remotingPort },
  cluster: {
    discovery: new DnsDiscovery({
      hostname: discoveryHostname,
      recordType: DnsRecordType.address,
      port: gossipPort,
    }),
    gossipPort,
    dataPort,
    minimumMemberQuorum: memberQuorum,
  },
});

log(
  `booting: resolving peers from "${discoveryHostname}", remoting :${remotingPort}, gossip :${gossipPort}`,
);
await system.start();
log(`started at ${system.host()}:${system.port()}`);

// Log every cluster lifecycle and relocation event, so a reader can watch membership
// changes and actors moving between nodes. Subscribing requires the system running.
system.subscribe((event: unknown): void => {
  log(`event ${event?.constructor?.name} ${JSON.stringify(event)}`);
});

/** The reply to a one-off ask of the named worker, or undefined when no worker holds
 * the name anywhere in the cluster. The lookup is location-transparent: it returns a
 * local worker, a routed handle to one on another node, or nothing. */
async function ask(name: string, message: Greet | WhereAreYou): Promise<string | undefined> {
  const pid = await system.actorOfAsync(name);
  if (pid === undefined) {
    return undefined;
  }

  return (await system.noSender().ask(pid, message, 5_000)) as string;
}

/** Writes a JSON response with the given status. */
function json(response: ServerResponse, status: number, body: unknown): void {
  const payload: string = JSON.stringify(body);
  response.writeHead(status, { "content-type": "application/json" });
  response.end(payload);
}

/** Routes one HTTP request to a cluster-actor operation. Paths:
 * `PUT /workers/:name`   spawn a worker on this node,
 * `PUT /spread/:name`    place a worker on the node a strategy chooses,
 * `PUT /singletons/:name` claim a cluster-wide singleton worker,
 * `GET /where/:name`     where the named worker runs now,
 * `GET /greet/:name`     a greeting from the named worker,
 * `GET /health`          readiness. */
async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url: URL = new URL(request.url ?? "/", `http://${host}`);
  const parts: string[] = url.pathname
    .split("/")
    .filter((part: string): boolean => part.length > 0);
  const [route, name]: (string | undefined)[] = parts;
  const region: string = url.searchParams.get("region") ?? host;

  if (route === "health") {
    json(response, 200, { host, members: system.clusterNode()?.members().length ?? 0 });
    return;
  }

  if (request.method === "PUT" && route === "workers" && name !== undefined) {
    const pid = await system.spawn(name, Props.create(Worker, region));
    json(response, 201, { name, host: system.host(), path: pid.path().toString() });
    return;
  }

  if (request.method === "PUT" && route === "spread" && name !== undefined) {
    const pid = await system.spawnOn(name, Props.create(Worker, region), {
      strategy: "roundRobin",
    });
    json(response, 201, { name, path: pid.path().toString() });
    return;
  }

  if (request.method === "PUT" && route === "singletons" && name !== undefined) {
    const pid = await system.spawnSingleton(name, Props.create(Worker, region));
    json(response, 201, { name, path: pid.path().toString() });
    return;
  }

  if (request.method === "GET" && route === "where" && name !== undefined) {
    const where: string | undefined = await ask(name, new WhereAreYou());
    json(response, where === undefined ? 404 : 200, { name, host: where ?? null });
    return;
  }

  if (request.method === "GET" && route === "greet" && name !== undefined) {
    const greeting: string | undefined = await ask(
      name,
      new Greet(url.searchParams.get("who") ?? "world"),
    );
    json(response, greeting === undefined ? 404 : 200, { name, greeting: greeting ?? null });
    return;
  }

  json(response, 404, { error: "unknown route" });
}

const server: Server = createServer((request: IncomingMessage, response: ServerResponse): void => {
  handle(request, response).catch((error: unknown): void => {
    // The failure is logged on the node; the HTTP client gets no internal detail.
    log(`request failed: ${error instanceof Error ? error.message : String(error)}`);
    json(response, 500, { error: "internal error" });
  });
});
server.listen(httpPort, (): void => log(`http listening on :${httpPort}`));

/** Leaves the cluster gracefully on a signal: the departing node hands its relocatable
 * actors to survivors through the coordinator's recovery, then the process exits. */
async function shutdown(signal: string): Promise<void> {
  log(`${signal}: leaving the cluster`);
  server.close();
  await system.stop();
  log("stopped");
  process.exit(0);
}

process.on("SIGTERM", (): void => void shutdown("SIGTERM"));
process.on("SIGINT", (): void => void shutdown("SIGINT"));
