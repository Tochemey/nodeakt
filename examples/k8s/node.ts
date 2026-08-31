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
 * A distributed-actor cluster on Kubernetes, discovered over DNS, driven by an HTTP API.
 *
 * Every pod of the StatefulSet in deploy/k8s.yaml runs this same program. On boot it
 * resolves the headless service's DNS name, which returns every pod's address, joins
 * the cluster over those addresses, and serves the API. Actors are spawned with
 * cluster-wide unique names, placed across the cluster with a strategy, or claimed as
 * singletons; a caller on any pod reaches an actor on any other through its name, and
 * when a pod departs the coordinator recreates its relocatable actors on the survivors.
 *
 * The pod's identity arrives through the environment, set in the manifest from the
 * downward API: NODEAKT_HOST is the pod's own IP, so member identity dies with the
 * process and a replaced pod joins as a new member while the old one is detected
 * dead and its actors relocated. NODEAKT_DISCOVERY_HOSTNAME is the headless
 * service's fully qualified name, because the discovery resolver queries DNS records
 * directly and does not walk the resolv.conf search path. The framework is imported
 * from its package entry point; `tsx` runs the TypeScript sources directly.
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import {
  ActorSystem,
  DnsDiscovery,
  DnsRecordType,
  LongLivedStrategy,
  Props,
  TextLogger,
} from "../../src/index";
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

/** The pod's name from the downward API, used as the log prefix; the advertised host
 * is the pod's IP, which tells a reader little. */
const podName: string = process.env.POD_NAME?.trim() || host;

/** Timestamped log line, prefixed with this pod so a reader can follow each one. */
function log(message: string): void {
  process.stdout.write(`[${podName}] ${message}\n`);
}

// The bind host is the wildcard so the container accepts connections on any interface;
// the advertised host is this pod's own IP, and the discovery hostname resolves to
// every pod's IP, so a peer dials each pod directly after it joins.
const system: ActorSystem = new ActorSystem("k8s-actors", {
  logger: new TextLogger({ level: "debug" }),
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
// changes and actors moving between pods. Subscribing requires the system running.
system.subscribe((event: unknown): void => {
  log(`event ${event?.constructor?.name} ${JSON.stringify(event)}`);
});

/** The reply to a one-off ask of the named worker, or undefined when no worker holds
 * the name anywhere in the cluster. The lookup is location-transparent: it returns a
 * local worker, a routed handle to one on another pod, or nothing. */
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
 * `PUT /workers/:name`    spawn a worker on this pod,
 * `PUT /spread/:name`     place a worker on the pod a strategy chooses,
 * `PUT /singletons/:name` claim a cluster-wide singleton worker,
 * `GET /where/:name`      where the named worker runs now,
 * `GET /greet/:name`      a greeting from the named worker,
 * `GET /health`           this pod's identity and the member count it sees,
 * `GET /ready`            200 once this pod sees the member quorum, 503 before;
 *                         the manifest's readiness probe, so the API service routes
 *                         around a pod that is still forming or cut off. */
async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url: URL = new URL(request.url ?? "/", `http://${host}`);
  const parts: string[] = url.pathname
    .split("/")
    .filter((part: string): boolean => part.length > 0);
  const [route, name]: (string | undefined)[] = parts;
  const region: string = url.searchParams.get("region") ?? podName;

  if (route === "health") {
    json(response, 200, { host, members: system.clusterNode()?.members().length ?? 0 });
    return;
  }

  if (route === "ready") {
    const members: number = system.clusterNode()?.members().length ?? 0;
    const serving: boolean = members >= memberQuorum;
    json(response, serving ? 200 : 503, { host, members, quorum: memberQuorum });
    return;
  }

  if (request.method === "PUT" && route === "workers" && name !== undefined) {
    // Workers are reached by name across the cluster long after they are
    // created, so they are long-lived: an idle window must not passivate one.
    const pid = await system.spawn(name, Props.create(Worker, region), {
      passivationStrategy: new LongLivedStrategy(),
    });
    json(response, 201, { name, host: system.host(), path: pid.path().toString() });
    return;
  }

  if (request.method === "PUT" && route === "spread" && name !== undefined) {
    const pid = await system.spawnOn(name, Props.create(Worker, region), {
      strategy: "roundRobin",
      passivationStrategy: new LongLivedStrategy(),
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
    // The failure is logged on the pod; the HTTP client gets no internal detail.
    log(`request failed: ${error instanceof Error ? error.message : String(error)}`);
    json(response, 500, { error: "internal error" });
  });
});
server.listen(httpPort, (): void => log(`http listening on :${httpPort}`));

/** Leaves the cluster gracefully on a signal: Kubernetes sends SIGTERM when it stops
 * the pod, the departing node hands its relocatable actors to survivors through the
 * coordinator's recovery, then the process exits. */
async function shutdown(signal: string): Promise<void> {
  log(`${signal}: leaving the cluster`);
  server.close();
  await system.stop();
  log("stopped");
  process.exit(0);
}

process.on("SIGTERM", (): void => void shutdown("SIGTERM"));
process.on("SIGINT", (): void => void shutdown("SIGINT"));
