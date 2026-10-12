// A cell's state in buckets (public-beta issuer triage B1.1, B1.2), the
// store the interim issuer and the owner's cell-admin share:
//
//   control bucket   cells/<cell_id>/cell-state.json   the state
//                    denylist.txt                       ids never placed again, across cells
//                    beta/allowlist.json                the workspaces that may create projects
//   feeds bucket     cells/<cell_id>/feeds-bundle.json  what the cell's wrapper polls
//
// Every write is conditional: `If-None-Match: *` creates, `If-Match: <etag>`
// replaces, and a 412 is a Conflict the writer resolves by re-reading.
// Bun's S3 client has no conditional PUT, so each request is presigned
// (60 s) and sent with `fetch`, the precondition as a plain header, as the
// cell's wrapper already reads the bundle (deploy/app-server/feeds.ts).
// Nothing proves a store honours those headers on that path, so open()
// tests it first and refuses to run on a store that ignores them: a
// precondition silently ignored would make every write last-writer-wins.
//
// The bucket keys come from a file only its owner can read
// (`--s3-credentials`), never from the command line:
//   {"endpoint", "bucket", "region", "accessKeyId", "secretAccessKey",
//    "feeds": {the same five, for the bucket the cell reads its bundle from}}
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { S3Client } from "bun";
import { AdminError } from "./errors";
import { BUNDLE_FILE, type CellState, parseDenylist, parseState, STATE_FILE, stateText } from "./state";
import {
  type Allowlist,
  allowlistText,
  Conflict,
  type PublishedBundle,
  parseAllowlist,
  parsePublished,
  type StateStore,
} from "./store";
import { checkCustody } from "./tokens";

/// The owner's question D3 (a): the bundle lives in its own `feeds`
/// bucket, which the cell reads with a read-only key, apart from the
/// control bucket (state, denylist, allow-list, audit) the cell holds no
/// key to. Answer (b) (one bucket for both) is `false` here: the bundle
/// then goes to the control bucket and the file needs no `feeds` member.
export const BUNDLE_IN_FEEDS_BUCKET = true;

export interface S3Target {
  endpoint: string;
  bucket: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
}

const TARGET_FIELDS = ["endpoint", "bucket", "region", "accessKeyId", "secretAccessKey"] as const;
const REQUEST_TIMEOUT_MS = 10_000;

function targetOf(doc: unknown, path: string, where: string): S3Target {
  const t = (doc ?? {}) as Record<string, unknown>;
  for (const field of TARGET_FIELDS) {
    if (typeof t[field] !== "string" || t[field] === "") {
      throw new AdminError(`credentials file ${path} lacks ${where}${field}`);
    }
  }
  return Object.fromEntries(TARGET_FIELDS.map((f) => [f, t[f]])) as unknown as S3Target;
}

/// The control and feeds targets from a mode-0600 credentials file.
export function readS3Credentials(path: string): { control: S3Target; feeds: S3Target } {
  checkCustody(path, "credentials file", "the bucket keys");
  let doc: Record<string, unknown>;
  try {
    doc = JSON.parse(readFileSync(path, "utf8"));
  } catch {
    throw new AdminError(`credentials file ${path} is not JSON`);
  }
  const control = targetOf(doc, path, "");
  if (!BUNDLE_IN_FEEDS_BUCKET) return { control, feeds: control };
  if (doc.feeds === undefined) {
    throw new AdminError(`credentials file ${path} lacks feeds: the bundle lives in its own bucket (D3 (a))`);
  }
  return { control, feeds: targetOf(doc.feeds, path, "feeds.") };
}

/// `s3://<bucket>/cells/<cell_id>`, or a refusal.
export function parseStateUrl(url: string): { bucket: string; cellId: string } {
  const m = /^s3:\/\/([a-z0-9][a-z0-9.-]{1,62})\/cells\/([A-Za-z0-9_-]{1,128})$/.exec(url);
  if (!m) throw new AdminError(`--state ${url} is not s3://<bucket>/cells/<cell id>`, 2);
  return { bucket: m[1], cellId: m[2] };
}

/// The S3 error code of a failed answer, for messages.
async function codeOf(res: Response): Promise<string> {
  const text = await res.text().catch(() => "");
  return /<Code>([^<]+)<\/Code>/.exec(text)?.[1] ?? "";
}

/// One object, read and written through presigned URLs.
class S3Object {
  readonly where: string;
  private readonly file: ReturnType<S3Client["file"]>;

  constructor(client: S3Client, target: S3Target, readonly key: string) {
    this.file = client.file(key);
    this.where = `s3://${target.bucket}/${key}`;
  }

  private async send(method: "GET" | "PUT" | "DELETE", headers: Record<string, string>, body?: string) {
    try {
      return await fetch(this.file.presign({ method, expiresIn: 60 }), {
        method,
        headers,
        body,
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch (e) {
      const why = (e as Error).name === "TimeoutError" ? "timed out" : "unreachable";
      throw new AdminError(`${method} ${this.where}: the store is ${why}`);
    }
  }

  async get(): Promise<{ text: string; etag: string } | null> {
    const res = await this.send("GET", {});
    if (res.status === 404) return null;
    if (!res.ok) throw new AdminError(`GET ${this.where} answered ${res.status} ${await codeOf(res)}`.trimEnd());
    const etag = res.headers.get("etag");
    if (!etag) throw new AdminError(`GET ${this.where} answered without an ETag`);
    return { text: await res.text(), etag };
  }

  /// Create (`etag` null) or replace the object only if it still has
  /// `etag`; a refused precondition is a Conflict. Returns the new ETag.
  async put(text: string, etag: string | null): Promise<string> {
    const res = await this.putRaw(text, etag === null ? { "if-none-match": "*" } : { "if-match": etag });
    if (res.status === 412 || (res.status === 404 && etag !== null)) throw new Conflict(`${this.where} changed`);
    if (!res.ok) throw new AdminError(`PUT ${this.where} answered ${res.status} ${await codeOf(res)}`.trimEnd());
    const tag = res.headers.get("etag");
    if (!tag) throw new AdminError(`PUT ${this.where} answered without an ETag`);
    return tag;
  }

  private putRaw(text: string, headers: Record<string, string>): Promise<Response> {
    return this.send("PUT", { "content-type": "application/json", ...headers }, text);
  }

  /// A PUT's status alone, its body drained.
  async putStatus(text: string, headers: Record<string, string>): Promise<number> {
    const res = await this.putRaw(text, headers);
    await res.arrayBuffer().catch(() => undefined);
    return res.status;
  }

  async delete(): Promise<void> {
    const res = await this.send("DELETE", {});
    if (!res.ok && res.status !== 404) {
      throw new AdminError(`DELETE ${this.where} answered ${res.status} ${await codeOf(res)}`.trimEnd());
    }
  }
}

const clientOf = (t: S3Target) =>
  new S3Client({
    endpoint: t.endpoint,
    bucket: t.bucket,
    region: t.region,
    accessKeyId: t.accessKeyId,
    secretAccessKey: t.secretAccessKey,
  });

/// Refuse a bucket that does not honour the two preconditions every write
/// rests on (skeptic S5d): a create over an existing object and a replace
/// under a wrong ETag must both answer 412. The probe key is the run's
/// own, so two writers testing at once cannot fail each other.
async function selfTest(target: S3Target, cellId: string): Promise<void> {
  const probe = new S3Object(
    clientOf(target),
    target,
    `cells/${cellId}/.cas-probe-${randomBytes(8).toString("hex")}`,
  );
  const where = `${target.endpoint} bucket ${target.bucket}`;
  const refuse = (what: string, status: number) =>
    new AdminError(
      `${where} does not honour conditional writes (${what} answered ${status}, not 412): ` +
        "cell-admin refuses to run without them",
    );
  const created = await probe.putStatus("{}", { "if-none-match": "*" });
  if (created < 200 || created > 299) {
    throw new AdminError(`${where} refused a probe write (If-None-Match: * on a new key answered ${created})`);
  }
  try {
    const again = await probe.putStatus("{}", { "if-none-match": "*" });
    if (again !== 412) throw refuse("If-None-Match: * over an existing object", again);
    const wrong = await probe.putStatus("{}", { "if-match": '"cas-probe-wrong"' });
    if (wrong !== 412) throw refuse('If-Match: "cas-probe-wrong"', wrong);
  } finally {
    await probe.delete();
  }
}

export class S3Store implements StateStore {
  readonly statePath: string;
  readonly bundlePath: string;
  private readonly state: S3Object;
  private readonly bundle: S3Object;
  private readonly denylist: S3Object;
  private readonly allowlist: S3Object;

  private readonly control: S3Client;

  private constructor(
    private readonly targets: { control: S3Target; feeds: S3Target },
    private readonly cellId: string,
  ) {
    this.control = clientOf(targets.control);
    const feeds = clientOf(targets.feeds);
    this.state = new S3Object(this.control, targets.control, `cells/${cellId}/${STATE_FILE}`);
    this.bundle = new S3Object(feeds, targets.feeds, `cells/${cellId}/${BUNDLE_FILE}`);
    this.denylist = new S3Object(this.control, targets.control, "denylist.txt");
    this.allowlist = new S3Object(this.control, targets.control, "beta/allowlist.json");
    [this.statePath, this.bundlePath] = [this.state.where, this.bundle.where];
  }

  /// The store `url` names, after the precondition self-test on each bucket.
  static async open(url: string, targets: { control: S3Target; feeds: S3Target }): Promise<S3Store> {
    const { bucket, cellId } = parseStateUrl(url);
    if (bucket !== targets.control.bucket) {
      throw new AdminError(`--state ${url} names bucket ${bucket}, the credentials file ${targets.control.bucket}`, 2);
    }
    await selfTest(targets.control, cellId);
    const same = targets.feeds.endpoint === targets.control.endpoint && targets.feeds.bucket === targets.control.bucket;
    if (!same) await selfTest(targets.feeds, cellId);
    return new S3Store(targets, cellId);
  }

  /// The denylist is the control bucket's, shared by every cell in it.
  denylistAt(flag: string | undefined): string {
    if (flag !== undefined) throw new AdminError(`--denylist: an s3 state keeps its denylist at ${this.denylist.where}`, 2);
    return this.denylist.where;
  }

  exclusive<T>(fn: () => Promise<T>): Promise<T> {
    return fn();
  }

  async load(): Promise<{ state: CellState; etag: string }> {
    const got = await this.state.get();
    if (got === null) throw new AdminError(`${this.statePath} does not exist: run cell-admin init first`);
    const state = parseState(got.text, this.statePath);
    if (state.cell.cell_id !== this.cellId) {
      throw new AdminError(`${this.statePath} holds cell ${state.cell.cell_id}, not ${this.cellId}`);
    }
    return { state, etag: got.etag };
  }

  save(state: CellState, etag: string | null): Promise<string> {
    if (state.cell.cell_id !== this.cellId) {
      throw new AdminError(`${this.statePath} is the state of cell ${this.cellId}, not ${state.cell.cell_id}`);
    }
    return this.state.put(stateText(state), etag);
  }

  async readBundle(): Promise<PublishedBundle | null> {
    const got = await this.bundle.get();
    return got === null ? null : parsePublished(got.text, got.etag, this.bundlePath);
  }

  publishBundle(text: string, etag: string | null): Promise<string> {
    return this.bundle.put(text, etag);
  }

  async readDenylist(): Promise<{ ids: Set<string>; etag: string | null }> {
    const got = await this.denylist.get();
    return { ids: parseDenylist(got?.text ?? ""), etag: got?.etag ?? null };
  }

  async readAllowlist(): Promise<{ doc: Allowlist; etag: string } | null> {
    const got = await this.allowlist.get();
    return got === null ? null : { doc: parseAllowlist(got.text, this.allowlist.where), etag: got.etag };
  }

  writeAllowlist(doc: Allowlist, etag: string | null): Promise<string> {
    return this.allowlist.put(allowlistText(doc), etag);
  }

  /// In the control bucket, created with If-None-Match: *.
  async putAudit(key: string, json: unknown): Promise<void> {
    await new S3Object(this.control, this.targets.control, key).put(`${JSON.stringify(json)}\n`, null);
  }

  async appendDenylist(id: string, etag: string | null): Promise<string> {
    const got = await this.denylist.get();
    if ((got?.etag ?? null) !== etag) throw new Conflict(`${this.denylist.where} changed`);
    const text = got === null || got.text === "" || got.text.endsWith("\n") ? `${got?.text ?? ""}${id}\n` : `${got.text}\n${id}\n`;
    return this.denylist.put(text, etag);
  }
}
