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

/** The nodeakt addressing scheme used in canonical path strings. */
const SCHEME = "nodeakt";

/**
 * Actor and system names must start with an alphanumeric character and may
 * contain alphanumerics, '-', '_' or '.'.
 */
const NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9\-_.]*$/;

/** Maximum length of an actor name. */
const MAX_NAME_LENGTH = 255;

let nextUid = 1;

/**
 * Path is the logical address of an actor: where it lives and how it is
 * reached, independent of the in-memory object that implements it.
 *
 * A path identifies an actor by the actor system it belongs to, the node
 * that system runs on (host and port), its own name, and its position in
 * the supervision hierarchy via {@link parent}. Because messaging is
 * addressed by path rather than by object reference, actors stay
 * location-transparent: a path names an actor the same way whether it lives
 * in this process or, once remoting exists, on another node.
 *
 * The canonical textual representation (see {@link toString}) is:
 *
 * ```
 * nodeakt://<system>@<host>:<port>/<name>
 * ```
 *
 * and, when the actor descends from a chain of parents:
 *
 * ```
 * nodeakt://<system>@<host>:<port>/<ancestorName>/.../<parentName>/<name>
 * ```
 *
 * The string encodes the full ancestor chain, so two actors carrying the
 * same local name under different parents always have distinct canonical
 * paths, at any depth. That string is the wire and storage format of a
 * path: registries persist it and {@link parsePath} restores it, so paths
 * round-trip across process boundaries.
 *
 * Paths are immutable: an actor keeps the same path for its entire life,
 * including across supervisor restarts. Two references with equal paths
 * (see {@link equals}) designate the same actor name and location; to ask
 * whether they designate the same living instance, use
 * {@link sameUid}.
 *
 * Developers receive a path from {@link PID.path}; they do not construct
 * one.
 */
export interface Path {
  /**
   * Returns the hostname of the node where the actor resides.
   */
  host(): string;

  /**
   * Returns the `host:port` endpoint of the actor's node as a single
   * string. The host is rendered raw, without IPv6 brackets, exactly as it
   * appears inside {@link toString}.
   */
  hostPort(): string;

  /**
   * Returns the port on which the actor's node listens.
   */
  port(): number;

  /**
   * Returns the actor's own name, which is the last segment of the path.
   *
   * The name is unique among the children of the same parent, and it is
   * the identifier used to look the actor up within its system.
   */
  name(): string;

  /**
   * Returns the path of the actor's parent in the supervision hierarchy,
   * or `undefined` for a root actor that has no parent.
   */
  parent(): Path | undefined;

  /**
   * Returns the name of the actor system the actor belongs to.
   */
  system(): string;

  /**
   * Returns the unique id of the actor instance this path was minted for.
   *
   * A live actor's path carries a fresh process-local counter, so
   * successive actors spawned under the same name are distinguishable.
   * That is how stale references are detected once actors are tracked
   * across restarts, registries, or nodes. A path restored from its
   * string form is a mere reference and carries an empty id unless one
   * was supplied.
   */
  uid(): string;

  /**
   * Returns the canonical string representation of the path. Built on
   * first call and reused afterwards.
   */
  toString(): string;

  /**
   * Reports whether this path and `other` designate the same actor name
   * and location: name, system, host, port, and parent chain match. The
   * {@link uid} is not compared. A child shares its parent's node, so
   * {@link system} follows the parent's spelling.
   */
  equals(other: Path): boolean;

  /**
   * Reports whether this path and `other` identify the same living actor
   * instance: they are {@link equals} and both carry the same non-empty
   * {@link uid}.
   */
  sameUid(other: Path): boolean;
}

/** System, host, and port of one actor system node. Shared by every
 * path minted in that system; paths do not copy the three fields.
 *
 * @internal
 */
export interface PathAddress {
  readonly system: string;
  readonly host: string;
  readonly port: number;
}

/** @internal */
class ActorPath implements Path {
  private readonly _name: string;
  private readonly _address: PathAddress;
  private readonly _parent: Path | undefined;
  /** 0 is a restored reference with no living instance. */
  private readonly _uid: number;
  private _id: string | undefined;

  constructor(name: string, address: PathAddress, parent: Path | undefined, uid: number) {
    this._name = name;
    this._address = address;
    this._parent = parent;
    this._uid = uid;
  }

  host(): string {
    return this._address.host;
  }

  hostPort(): string {
    return `${this._address.host}:${this._address.port}`;
  }

  port(): number {
    return this._address.port;
  }

  name(): string {
    return this._name;
  }

  parent(): Path | undefined {
    return this._parent;
  }

  system(): string {
    return this._address.system;
  }

  /** @internal */
  address(): PathAddress {
    return this._address;
  }

  uid(): string {
    return this._uid === 0 ? "" : String(this._uid);
  }

  toString(): string {
    if (this._id === undefined) {
      this._id =
        this._parent === undefined
          ? `${SCHEME}://${this._address.system}@${this._address.host}:${this._address.port}/${this._name}`
          : `${this._parent.toString()}/${this._name}`;
    }

    return this._id;
  }

  equals(other: Path): boolean {
    if (this === other) {
      return true;
    }

    if (
      this._name !== other.name() ||
      this._address.system !== other.system() ||
      this._address.host !== other.host() ||
      this._address.port !== other.port()
    ) {
      return false;
    }

    const parent = this._parent;
    const otherParent = other.parent();
    if (parent === undefined || otherParent === undefined) {
      return parent === otherParent;
    }

    return parent.equals(otherParent);
  }

  sameUid(other: Path): boolean {
    return this._uid !== 0 && this.equals(other) && this.uid() === other.uid();
  }
}

/**
 * Reports whether the given value is a valid actor name: it starts with an
 * alphanumeric character, contains only alphanumerics, `-`, `_` or `.`,
 * and is at most 255 characters long.
 *
 * @internal
 */
export function isValidActorName(name: string): boolean {
  return name.length > 0 && name.length <= MAX_NAME_LENGTH && NAME_PATTERN.test(name);
}

/** Throws unless `value` is a well-formed actor or system name. */
function checkName(kind: string, value: string): void {
  if (value.length === 0) {
    throw new TypeError(`${kind} must not be empty`);
  }

  if (value.length > MAX_NAME_LENGTH) {
    throw new RangeError(`${kind} is too long. Maximum length is ${MAX_NAME_LENGTH}`);
  }

  if (!NAME_PATTERN.test(value)) {
    throw new TypeError(
      `${kind} "${value}" is invalid: it must start with an alphanumeric character and contain only alphanumerics, '-', '_' or '.'`,
    );
  }
}

/**
 * Creates the {@link Path} of an actor after validating all constituents.
 *
 * When `uid` is omitted, a fresh {@link Path.uid | uid} is minted, which
 * is the right behavior where an actor is being created. Pass an explicit
 * uid (or an empty string for a mere reference) when restoring a path
 * that designates an actor living elsewhere, as {@link parsePath} does.
 *
 * @internal
 */
export function newPath(
  name: string,
  system: string,
  host: string,
  port: number,
  parent?: Path,
  uid?: string,
): Path {
  return newPathAt(name, { system, host, port }, parent, uid);
}

/**
 * Creates a path on a shared node address. Every actor in one system
 * should pass the same {@link PathAddress} object so idle paths do not
 * each store system, host, and port.
 *
 * @internal
 */
export function newPathAt(name: string, address: PathAddress, parent?: Path, uid?: string): Path {
  checkName("actor name", name);
  checkName("actor system name", address.system);

  if (address.host.length === 0) {
    throw new TypeError("host must not be empty");
  }

  if (!Number.isInteger(address.port) || address.port < 0 || address.port > 65535) {
    throw new RangeError(`port must be an integer in [0, 65535], got ${address.port}`);
  }

  if (parent !== undefined) {
    if (parent.system().toLowerCase() !== address.system.toLowerCase()) {
      throw new TypeError(
        `parent path belongs to actor system "${parent.system()}", not "${address.system}"`,
      );
    }

    if (parent.host() !== address.host || parent.port() !== address.port) {
      throw new TypeError(
        `parent path resides on ${parent.hostPort()}, not ${address.host}:${address.port}`,
      );
    }

    if (parent.name() === name) {
      throw new TypeError(`actor name "${name}" must differ from its parent's name`);
    }
  }

  const node = parent instanceof ActorPath ? parent.address() : address;

  return new ActorPath(name, node, parent, parseUid(uid));
}

/** Returns the shared node of `path`, or a new address built from its
 * fields when the path was not minted by this module.
 *
 * @internal
 */
export function addressOf(path: Path): PathAddress {
  return (path as ActorPath).address();
}

function parseUid(id: string | undefined): number {
  if (id === undefined) {
    return nextUid++;
  }

  if (id === "") {
    return 0;
  }

  if (!/^[1-9]\d*$/.test(id)) {
    throw new TypeError(`uid "${id}" is invalid: it must be a positive integer`);
  }

  return Number(id);
}

/**
 * Restores a {@link Path} from its canonical string representation, e.g.
 * `nodeakt://system@host:port/name` or
 * `nodeakt://system@host:port/grandParentName/parentName/name` for an
 * actor at any depth of the hierarchy.
 *
 * The result is a reference: its {@link Path.uid | uid} is empty unless
 * `uid` is supplied. When the string carries ancestor segments, the
 * returned path's parent chain is rebuilt as references on the same
 * system, host, and port.
 *
 * @internal
 */
export function parsePath(value: string, uid?: string): Path {
  const prefix = `${SCHEME}://`;
  const malformed = (): TypeError =>
    new TypeError(
      `"${value}" is not a well-formed actor path; expected ${prefix}<system>@<host>:<port>/[<ancestorName>/...]<name>`,
    );

  if (!value.startsWith(prefix)) {
    throw malformed();
  }

  const rest = value.slice(prefix.length);
  const at = rest.indexOf("@");
  const slash = rest.indexOf("/", at + 1);
  if (at <= 0 || slash < 0) {
    throw malformed();
  }

  const system = rest.slice(0, at);
  const hostPort = rest.slice(at + 1, slash);
  const colon = hostPort.lastIndexOf(":");
  if (colon <= 0) {
    throw malformed();
  }

  const host = hostPort.slice(0, colon);
  const portStr = hostPort.slice(colon + 1);
  // The port must be canonical: no leading zeros, so a parsed path
  // stringifies back to exactly the input. Round-tripping is a
  // contract callers rely on to key a path by its string form, and a
  // leading-zero port ("007") would normalize to a different string
  // ("7") on the way back out.
  if (!/^(0|[1-9]\d*)$/.test(portStr)) {
    throw malformed();
  }

  const port = Number(portStr);

  const segments = rest.slice(slash + 1).split("/");
  if (segments.some((s) => s.length === 0)) {
    throw malformed();
  }

  const address: PathAddress = { system, host, port };

  // Rebuild the ancestor chain left to right; each ancestor is a mere
  // reference with an empty uid.
  let parent: Path | undefined;

  for (let i = 0; i < segments.length - 1; i++) {
    parent = newPathAt(segments[i] as string, address, parent, "");
  }

  const name = segments[segments.length - 1] as string;
  return newPathAt(name, address, parent, uid ?? "");
}
