// The auth feed bundle (FEEDS_S3_KEY) kept current on a running cell
// (owner decision of 2026-10-08, README "Shared cells Q6" (a), option 4(b)).
//
// The bundle is one JSON document {keys, policies, grants} that cell-admin
// (or a campaign) writes; the server reads the three STREAMS_AUTH_*_FILE
// files and re-reads them every STREAMS_AUTH_REFRESH_SECS. The wrapper
// polls the bundle every POLL_MS with If-None-Match, so an unchanged bundle
// costs one 304, and rewrites the three files atomically (tmp + rename,
// exactly as a platform projector would) when it changed: a published
// admission or revocation reaches the cell without a restart.
//
// A cell that cannot fetch the bundle must not serve feeds it can no longer
// see revoked: after STALE_MS without a successful poll the wrapper deletes
// the three files, and the server refuses every request once its feeds'
// freshness window passes, until a poll succeeds and restores them. A poll
// that fetches a body that is not JSON or lacks a feed has failed too: it
// replaces nothing, and a malformed bundle left published deletes the feeds
// after STALE_MS exactly as an unreachable store does, since the cell cannot
// see a revocation it was meant to carry either. Only a poll's own failure
// deletes, so the first thing a cell does after any wake is poll. The 120 s
// count awake time only: the timer is monotonic, and a gap between two
// polls longer than WAKE_GAP_MS (the VM slept) starts the count again at
// the wake, so a sleep longer than 120 s never deletes the feeds of a cell
// whose store is up.
import { mkdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { artifactClient } from "./downloader";

/// How often the wrapper polls the bundle; also the longest a published
/// change waits before the server's next refresh reads it.
export const POLL_MS = 15_000;
/// Awake time without a successful poll after which the feeds are deleted.
export const STALE_MS = 120_000;
/// A gap between two polls this long means the instance slept.
const WAKE_GAP_MS = 2 * POLL_MS;
/// The three feed files, by the bundle member each one holds.
const FEEDS = ["keys", "policies", "grants"] as const;

export type Fetched = { status: "unchanged" } | { status: "changed"; etag: string | null; body: string };
/// One conditional fetch of the bundle: `etag` is the last one seen, or
/// null for an unconditional GET. Throws when the store cannot answer.
export type FetchBundle = (etag: string | null) => Promise<Fetched>;

/// The three feeds of a bundle's text, or a reason it is not a bundle.
function parseBundle(body: string): Record<(typeof FEEDS)[number], unknown> | string {
  let doc: any;
  try {
    doc = JSON.parse(body);
  } catch (e) {
    return `not JSON: ${e}`;
  }
  const missing = FEEDS.filter((f) => typeof doc?.[f] !== "object" || doc[f] === null);
  return missing.length ? `no ${missing.join(", ")}` : doc;
}

/// Write the bundle's three feeds into `dir`, each atomically; returns the
/// keys feed's version. Refuses (throws) a body that is not a whole bundle,
/// before writing anything.
export function materialize(dir: string, body: string): unknown {
  const bundle = parseBundle(body);
  if (typeof bundle === "string") throw new Error(`feeds bundle refused: ${bundle}`);
  mkdirSync(dir, { recursive: true });
  for (const name of FEEDS) {
    const path = join(dir, `${name}.json`);
    writeFileSync(`${path}.tmp`, JSON.stringify(bundle[name]));
    renameSync(`${path}.tmp`, path);
  }
  return (bundle.keys as { feed_version?: unknown }).feed_version;
}

/// See the module documentation. `now` is a monotonic clock in ms.
export class FeedPoller {
  private etag: string | null = null;
  /// Start of the current count towards STALE_MS: the last successful
  /// poll, or the wake after it.
  private since: number;
  private last: number;
  private deleted = false;

  constructor(
    private readonly fetch: FetchBundle,
    private readonly dir: string,
    private readonly log: (line: string) => void,
    private readonly now: () => number = () => performance.now(),
  ) {
    this.since = this.last = this.now();
  }

  /// One poll: rewrite the feeds if the bundle changed; on a failure, delete
  /// them once STALE_MS of awake time passed without a success.
  async poll(): Promise<void> {
    const now = this.now();
    if (now - this.last > WAKE_GAP_MS) this.since = now;
    this.last = now;
    try {
      const got = await this.fetch(this.etag);
      if (got.status === "changed") {
        const version = materialize(this.dir, got.body);
        this.etag = got.etag;
        this.log(`feeds updated: ${this.dir}/{keys,policies,grants}.json gen=${version}`);
      }
      this.since = now;
      this.deleted = false;
    } catch (e) {
      this.log(`feeds poll failed: ${e}`);
      if (!this.deleted && now - this.since >= STALE_MS) {
        for (const name of FEEDS) rmSync(join(this.dir, `${name}.json`), { force: true });
        // The next success rewrites the files whatever the store's ETag.
        this.etag = null;
        this.deleted = true;
        this.log(`feeds deleted: no successful poll for ${Math.round((now - this.since) / 1000)} s`);
      }
    }
  }

  /// Poll every POLL_MS until the returned function is called.
  start(): () => void {
    let running = false;
    const timer = setInterval(() => {
      if (running) return;
      running = true;
      this.poll()
        .catch((e) => this.log(`feeds poll failed: ${e}`))
        .finally(() => (running = false));
    }, POLL_MS);
    return () => clearInterval(timer);
  }
}

/// The bundle at `key` in the artifact bucket, fetched with If-None-Match
/// through a presigned GET (the S3 client has no conditional read).
export function s3BundleFetcher(
  key: string,
  env: Record<string, string | undefined> = process.env,
): FetchBundle {
  const file = artifactClient(env).file(key);
  return async (etag) => {
    const url = file.presign({ method: "GET", expiresIn: 60 });
    const res = await fetch(url, {
      headers: etag === null ? {} : { "if-none-match": etag },
      signal: AbortSignal.timeout(10_000),
    });
    if (res.status === 304) return { status: "unchanged" };
    if (!res.ok) throw new Error(`GET ${key}: ${res.status}`);
    return { status: "changed", etag: res.headers.get("etag"), body: await res.text() };
  };
}
