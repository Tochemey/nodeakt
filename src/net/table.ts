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

import type { RefInterner, RefResolver } from "./envelope";
import { ProtocolError } from "./frame";
import type { ByteReader, ByteWriter } from "./values";

/**
 * String tables: each direction of a connection interns actor paths
 * and type ids independently, so a ref that repeats on every message
 * travels as one or two bytes instead of the literal. The sender
 * assigns ids from 1 per kind, announces each with a TABLE frame, and
 * thereafter encodes the bare id; a full table just means new
 * literals go inline. Receiving is strict because a corrupt table
 * corrupts every later message: id zero, an empty literal, a
 * conflicting re-registration, or an over-capacity install are
 * connection-scoped, and only an identical re-registration is
 * idempotent. Tables die with the connection; a reconnect starts
 * empty.
 *
 * @internal
 */

/** TABLE body kinds. */
export const TABLE_PATH: number = 0;
export const TABLE_TYPE: number = 1;

/** Entries per kind per direction; beyond it, literals go inline. */
export const TABLE_CAPACITY: number = 8192;

/** One TABLE frame body: which table, which id, which literal. */
export interface TableAnnouncement {
  readonly kind: number;
  readonly id: number;
  readonly literal: string;
}

export function encodeTableBody(writer: ByteWriter, announcement: TableAnnouncement): void {
  writer.writeU8(announcement.kind);
  writer.writeUvarint(announcement.id);
  writer.writeString(announcement.literal);
}

/**
 * Reads a TABLE body. Strict on shape as well as content: an unknown
 * kind or trailing bytes mean the sender and receiver already
 * disagree about the stream.
 *
 * @throws A {@link ProtocolError} on an unknown kind or trailing
 * bytes; the reader's own bounds errors escape for truncated bodies.
 */
export function decodeTableBody(reader: ByteReader): TableAnnouncement {
  const kind: number = reader.readU8();
  if (kind !== TABLE_PATH && kind !== TABLE_TYPE) {
    throw new ProtocolError(`unknown table kind ${kind}`);
  }

  const id: number = reader.readUvarint();
  const literal: string = reader.readString();
  if (reader.remaining !== 0) {
    throw new ProtocolError("trailing bytes after a table registration");
  }

  return { kind, id, literal };
}

/**
 * The sending side of one direction's tables: hands out ids for
 * literals worth interning and announces each assignment exactly once
 * through the owner's callback, before the id ever appears in a ref.
 * Empty literals and literals past capacity report id zero, which the
 * envelope codec encodes inline.
 */
export class OutboundTables implements RefInterner {
  private readonly _announce: (announcement: TableAnnouncement) => void;
  private readonly _paths: Map<string, number> = new Map<string, number>();
  private readonly _types: Map<string, number> = new Map<string, number>();

  constructor(announce: (announcement: TableAnnouncement) => void) {
    this._announce = announce;
  }

  pathId(literal: string): number {
    return this.intern(TABLE_PATH, this._paths, literal);
  }

  typeId(literal: string): number {
    return this.intern(TABLE_TYPE, this._types, literal);
  }

  private intern(kind: number, table: Map<string, number>, literal: string): number {
    if (literal === "") {
      return 0;
    }

    const known: number | undefined = table.get(literal);
    if (known !== undefined) {
      return known;
    }

    if (table.size >= TABLE_CAPACITY) {
      return 0;
    }

    const id: number = table.size + 1;
    table.set(literal, id);
    this._announce({ kind, id, literal });
    return id;
  }
}

/**
 * One installed entry. The handle slot is opaque storage for the
 * layer above: it caches whatever a path resolves to locally, so
 * steady-state delivery skips resolution. The transport never reads
 * it.
 */
interface TableEntry {
  readonly literal: string;
  handle: unknown;
}

/**
 * The receiving side of one direction's tables: installs the peer's
 * announcements under the strict rules and resolves inbound refs.
 * Every violation throws {@link ProtocolError}, which the session
 * turns into a connection-scoped failure.
 */
export class InboundTables implements RefResolver {
  /** Indexed by id: ids are small and dense, so resolving one ref is
      an array load instead of a hash probe on the receive path. */
  private readonly _paths: (TableEntry | undefined)[] = [];
  private readonly _types: (TableEntry | undefined)[] = [];
  private readonly _byPath: Map<string, TableEntry> = new Map<string, TableEntry>();

  install(announcement: TableAnnouncement): void {
    if (announcement.id === 0) {
      throw new ProtocolError("table id zero is reserved for inline refs");
    }

    if (announcement.literal === "") {
      throw new ProtocolError(`table entry ${announcement.id} registers an empty literal`);
    }

    if (announcement.id > TABLE_CAPACITY) {
      throw new ProtocolError(`table entry ${announcement.id} is over capacity`);
    }

    const table: (TableEntry | undefined)[] =
      announcement.kind === TABLE_PATH ? this._paths : this._types;
    const existing: TableEntry | undefined = table[announcement.id];
    if (existing !== undefined) {
      if (existing.literal === announcement.literal) {
        return;
      }

      throw new ProtocolError(`table entry ${announcement.id} re-registered with a new literal`);
    }

    const entry: TableEntry = { literal: announcement.literal, handle: undefined };
    table[announcement.id] = entry;
    if (announcement.kind === TABLE_PATH) {
      this._byPath.set(announcement.literal, entry);
    }
  }

  path(id: number): string {
    const entry: TableEntry | undefined = this._paths[id];
    if (entry === undefined) {
      throw new ProtocolError(`unknown path table ref ${id}`);
    }

    return entry.literal;
  }

  type(id: number): string {
    const entry: TableEntry | undefined = this._types[id];
    if (entry === undefined) {
      throw new ProtocolError(`unknown type table ref ${id}`);
    }

    return entry.literal;
  }

  /**
   * Attaches the owner's resolved handle to an interned path; false
   * when the path is not in the table, in which case there is nothing
   * to pin a cache to and the owner just resolves again next time.
   */
  cacheHandle(path: string, handle: unknown): boolean {
    const entry: TableEntry | undefined = this._byPath.get(path);
    if (entry === undefined) {
      return false;
    }

    entry.handle = handle;
    return true;
  }

  /** The handle cached for an interned path, undefined otherwise. */
  handleOf(path: string): unknown {
    return this._byPath.get(path)?.handle;
  }
}
