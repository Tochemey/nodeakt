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

import { promises as dnsPromises } from "node:dns";
import type { DiscoveryProvider } from "./provider";

/**
 * The DNS record kinds a {@link DnsDiscovery} can resolve. `srv` reads an `SRV`
 * record, which already carries a port per target; `address` reads an IPv4 `A`
 * record and `address6` an IPv6 `AAAA` record, neither of which carries a port,
 * so a fixed port is attached to each address.
 */
export const DnsRecordType = {
  srv: "srv",
  address: "a",
  address6: "aaaa",
} as const;

/** One of the {@link DnsRecordType} values. */
export type DnsRecordTypeValue = (typeof DnsRecordType)[keyof typeof DnsRecordType];

/** The one field of an `SRV` answer discovery needs: a target host and its port. */
export interface DnsSrvRecord {
  /** The target host the record points at. */
  readonly name: string;
  /** The port advertised for that target. */
  readonly port: number;
}

/**
 * The subset of a DNS resolver {@link DnsDiscovery} depends on, injected so a
 * test or an advanced deployment can supply its own instead of the platform's.
 */
export interface DnsResolver {
  /** Resolves the `SRV` records for a hostname, or rejects with a coded DNS error. */
  resolveSrv(hostname: string): Promise<readonly DnsSrvRecord[]>;
  /** Resolves the IPv4 `A` records for a hostname, or rejects with a coded DNS error. */
  resolve4(hostname: string): Promise<readonly string[]>;
  /** Resolves the IPv6 `AAAA` records for a hostname, or rejects with a coded DNS error. */
  resolve6(hostname: string): Promise<readonly string[]>;
}

/** Configuration for a {@link DnsDiscovery}. */
export interface DnsDiscoveryOptions {
  /** The hostname to resolve, for example a headless service's cluster DNS name. */
  readonly hostname: string;
  /** Which record kind to read; defaults to {@link DnsRecordType.srv}. */
  readonly recordType?: DnsRecordTypeValue;
  /** The port attached to each address in an `address` or `address6` mode; ignored for `srv`. */
  readonly port?: number;
  /** The resolver to query; defaults to the platform's `node:dns`. */
  readonly resolver?: DnsResolver;
}

/** Largest port number the wire permits. */
const MAX_PORT: number = 65_535;

/** The platform resolver used when a caller supplies none. */
const nodeDnsResolver: DnsResolver = {
  resolveSrv(hostname: string): Promise<readonly DnsSrvRecord[]> {
    return dnsPromises.resolveSrv(hostname);
  },
  resolve4(hostname: string): Promise<readonly string[]> {
    return dnsPromises.resolve4(hostname);
  },
  resolve6(hostname: string): Promise<readonly string[]> {
    return dnsPromises.resolve6(hostname);
  },
};

/**
 * Joins a host and port into the `host:port` seed form the bootstrap expects.
 *
 * A trailing root dot on a fully qualified DNS name is dropped, and an IPv6
 * literal, which itself contains colons, is bracketed so the port stays
 * unambiguous.
 */
function formatHostPort(host: string, port: number): string {
  const bare: string = host.endsWith(".") ? host.slice(0, -1) : host;
  return bare.includes(":") ? `[${bare}]:${port}` : `${bare}:${port}`;
}

/**
 * Whether a rejection is a DNS resolution failure, which resolve maps to an
 * empty result so the boot sequence retries rather than aborting.
 *
 * A coded rejection is a resolver outcome: the name is absent, has no records,
 * or the resolver itself timed out or refused. All of these mean "no seeds from
 * DNS right now", and the boot sequence retries until they appear or the
 * deadline anchors a fresh cluster; failing the whole boot on a momentary
 * resolver blip would be worse. An uncoded rejection is a programming error and
 * propagates.
 */
function isDnsResolutionError(error: unknown): boolean {
  if (typeof error !== "object" || error === null) {
    return false;
  }

  return typeof (error as { code?: unknown }).code === "string";
}

/**
 * Validates the port required by an `address` or `address6` mode.
 *
 * @throws {RangeError} If the port is missing or outside the wire's range.
 */
function validatePort(port: number | undefined): number {
  if (port === undefined) {
    throw new RangeError("dns discovery in address mode requires a port");
  }

  if (!Number.isSafeInteger(port) || port < 1 || port > MAX_PORT) {
    throw new RangeError("dns discovery port must be between 1 and 65535");
  }

  return port;
}

/**
 * Discovery over a DNS `SRV`, `A`, or `AAAA` lookup through `node:dns`, the
 * zero-dependency way a headless service in a container orchestrator exposes its
 * members.
 *
 * A lookup that fails for any resolver reason, the name not registered yet or a
 * momentary resolver error while an environment comes up, resolves to an empty
 * list rather than rejecting, so the boot sequence retries; only a non-DNS
 * programming error propagates. The record kind is chosen once at construction,
 * so resolving does no branching on configuration.
 */
export class DnsDiscovery implements DiscoveryProvider {
  /** The hostname queried on every resolve. */
  readonly #hostname: string;
  /** The resolver the queries run against. */
  readonly #resolver: DnsResolver;
  /** The lookup bound to the configured record kind at construction. */
  readonly #lookup: () => Promise<readonly string[]>;

  /**
   * @param options The hostname, record kind, address-mode port, and optional resolver.
   * @throws {TypeError} If the hostname is blank.
   * @throws {RangeError} If an address mode is chosen without a valid port.
   */
  constructor(options: DnsDiscoveryOptions) {
    const hostname: string = options.hostname.trim();
    if (hostname.length === 0) {
      throw new TypeError("dns discovery requires a non-blank hostname");
    }

    this.#hostname = hostname;
    this.#resolver = options.resolver ?? nodeDnsResolver;

    const recordType: DnsRecordTypeValue = options.recordType ?? DnsRecordType.srv;
    switch (recordType) {
      case DnsRecordType.address: {
        const port: number = validatePort(options.port);
        const query: (host: string) => Promise<readonly string[]> = (
          host: string,
        ): Promise<readonly string[]> => this.#resolver.resolve4(host);
        this.#lookup = (): Promise<readonly string[]> => this.#resolveAddresses(port, query);
        break;
      }

      case DnsRecordType.address6: {
        const port: number = validatePort(options.port);
        const query: (host: string) => Promise<readonly string[]> = (
          host: string,
        ): Promise<readonly string[]> => this.#resolver.resolve6(host);
        this.#lookup = (): Promise<readonly string[]> => this.#resolveAddresses(port, query);
        break;
      }

      default: {
        this.#lookup = (): Promise<readonly string[]> => this.#resolveSrv();
        break;
      }
    }
  }

  /** Resolves the configured lookup, mapping any DNS resolution failure to empty. */
  async resolve(): Promise<readonly string[]> {
    try {
      return await this.#lookup();
    } catch (error: unknown) {
      if (isDnsResolutionError(error)) {
        return [];
      }

      throw error;
    }
  }

  /** Reads `SRV` records and formats each target and port as a seed. */
  async #resolveSrv(): Promise<readonly string[]> {
    const records: readonly DnsSrvRecord[] = await this.#resolver.resolveSrv(this.#hostname);
    return records.map((record: DnsSrvRecord): string => formatHostPort(record.name, record.port));
  }

  /** Reads address records with `query` and attaches the configured port to each. */
  async #resolveAddresses(
    port: number,
    query: (host: string) => Promise<readonly string[]>,
  ): Promise<readonly string[]> {
    const addresses: readonly string[] = await query(this.#hostname);
    return addresses.map((address: string): string => formatHostPort(address, port));
  }
}
