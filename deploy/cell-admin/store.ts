// Where a cell's state lives (public-beta issuer triage B1.1). Every write
// is a compare-and-swap on the document's ETag, so cell-admin and the
// interim issuer can share one state without losing an update: a writer
// that read an older copy gets a Conflict, re-reads and re-applies its
// change. A local directory (DirStore) also holds its `.lock` for the
// whole command, as cell-admin always did.
import { createHash, randomBytes } from "node:crypto";
import { appendFileSync, closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, writeSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { AdminError } from "./errors";
import type { Bundle } from "./feeds";
import {
  BUNDLE_FILE,
  type CellState,
  parseDenylist,
  parseState,
  STATE_FILE,
  stateText,
  withLock,
  writeDurable,
} from "./state";

/// A compare-and-swap lost: the document is no longer the one read.
export class Conflict extends Error {}

/// The bundle a cell's wrapper polls, as last published.
export interface PublishedBundle {
  version: number;
  etag: string;
  bundle: Bundle;
}

export interface StateStore {
  /// Where the state and the bundle live, for messages.
  readonly statePath: string;
  readonly bundlePath: string;
  /// Where `init` records the denylist: `--denylist`'s file, or the
  /// store's own place for it.
  denylistAt(flag: string | undefined): string;
  /// Run `fn` as the store's only writer where the store needs that (a
  /// directory's lock); compare-and-swap covers every other store.
  exclusive<T>(fn: () => Promise<T>): Promise<T>;
  /// The state and its ETag; refuses a missing state or another format.
  load(): Promise<{ state: CellState; etag: string }>;
  /// Replace the state if it still has `etag`, or create it when `etag`
  /// is null (refused with a Conflict when one exists). Returns its ETag.
  save(state: CellState, etag: string | null): Promise<string>;
  readBundle(): Promise<PublishedBundle | null>;
  /// Replace the bundle if it still has `etag`, or create it when null.
  publishBundle(text: string, etag: string | null): Promise<string>;
  /// The denylist named by the loaded state (or by the one being created).
  readDenylist(): Promise<{ ids: Set<string>; etag: string | null }>;
  /// Append `id` to the denylist if it still has `etag`, durably.
  appendDenylist(id: string, etag: string | null): Promise<string>;
  /// Write a new audit object at `key` (auditKey()); never replaces one.
  putAudit(key: string, json: unknown): Promise<void>;
}

/// `audit/YYYY/MM/DD/<epoch-ms>-<rand>.json`: one object per record, so
/// writers never contend and a lifecycle rule can age the prefix.
export function auditKey(now = new Date()): string {
  const day = now.toISOString().slice(0, 10).replaceAll("-", "/");
  return `audit/${day}/${now.getTime()}-${randomBytes(6).toString("hex")}.json`;
}

const etagOf = (text: string) => `"${createHash("sha256").update(text).digest("hex")}"`;

/// The published bundle's text as a bundle and its version, or a refusal.
export function parsePublished(text: string, etag: string, where: string): PublishedBundle {
  let bundle: Bundle;
  try {
    bundle = JSON.parse(text) as Bundle;
  } catch {
    throw new AdminError(`${where} is not JSON`);
  }
  const version = bundle?.keys?.feed_version;
  if (!Number.isSafeInteger(version)) throw new AdminError(`${where} carries no keys.feed_version`);
  return { version, etag, bundle };
}

function readText(path: string): string | null {
  return existsSync(path) ? readFileSync(path, "utf8") : null;
}

/// A cell's state in a local directory: `cell-state.json`, the bundle
/// `feeds-bundle.json` beside it and the denylist the state names, each
/// replaced atomically and durably. The ETag is the content's sha256.
export class DirStore implements StateStore {
  readonly statePath: string;
  readonly bundlePath: string;
  private readonly defaultDenylist: string;
  private denylist?: string;

  constructor(
    private readonly dir: string,
    private readonly create = false,
  ) {
    this.statePath = join(dir, STATE_FILE);
    this.bundlePath = join(dir, BUNDLE_FILE);
    this.defaultDenylist = join(dir, "denylist.txt");
  }

  denylistAt(flag: string | undefined): string {
    return resolve(flag ?? this.defaultDenylist);
  }

  async exclusive<T>(fn: () => Promise<T>): Promise<T> {
    if (this.create) {
      if (existsSync(this.statePath)) throw new AdminError(`${this.statePath} exists: a cell is initialised once`);
      mkdirSync(this.dir, { recursive: true, mode: 0o700 });
    }
    return withLock(this.dir, fn);
  }

  async load(): Promise<{ state: CellState; etag: string }> {
    const text = readText(this.statePath);
    if (text === null) throw new AdminError(`${this.statePath} does not exist: run cell-admin init first`);
    const state = parseState(text, this.statePath);
    this.denylist = state.cell.denylist;
    return { state, etag: etagOf(text) };
  }

  /// Under the directory's lock nothing else writes, so the check and the
  /// rename cannot interleave with another cell-admin.
  private swap(path: string, text: string, etag: string | null): string {
    const current = readText(path);
    if (etag === null ? current !== null : current === null || etagOf(current) !== etag) {
      throw new Conflict(`${path} changed`);
    }
    writeDurable(path, text);
    return etagOf(text);
  }

  async save(state: CellState, etag: string | null): Promise<string> {
    const tag = this.swap(this.statePath, stateText(state), etag);
    this.denylist = state.cell.denylist;
    return tag;
  }

  async readBundle(): Promise<PublishedBundle | null> {
    const text = readText(this.bundlePath);
    return text === null ? null : parsePublished(text, etagOf(text), this.bundlePath);
  }

  async publishBundle(text: string, etag: string | null): Promise<string> {
    return this.swap(this.bundlePath, text, etag);
  }

  private denylistPath(): string {
    if (this.denylist === undefined) throw new Error("the denylist is read after the state");
    return this.denylist;
  }

  async readDenylist(): Promise<{ ids: Set<string>; etag: string | null }> {
    const text = readText(this.denylistPath());
    return { ids: parseDenylist(text ?? ""), etag: text === null ? null : etagOf(text) };
  }

  /// An append, not a rewrite: other cells' tools may share the file.
  async appendDenylist(id: string, etag: string | null): Promise<string> {
    const path = this.denylistPath();
    const current = readText(path);
    if ((current === null ? null : etagOf(current)) !== etag) throw new Conflict(`${path} changed`);
    appendFileSync(path, `${id}\n`, { mode: 0o600 });
    const fd = openSync(path, "r");
    try {
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    return etagOf(readFileSync(path, "utf8"));
  }

  /// Under the state directory, created exclusively and fsync'd.
  async putAudit(key: string, json: unknown): Promise<void> {
    const path = join(this.dir, key);
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    const fd = openSync(path, "wx", 0o600);
    try {
      writeSync(fd, `${JSON.stringify(json)}\n`);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
  }
}
