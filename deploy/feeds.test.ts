// The deploy wrapper's auth-feed poller (owner decision of 2026-10-08,
// README "Shared cells Q6" (a), option 4(b)): a running cell picks up a
// republished bundle without a restart, and a cell that cannot reach the
// bundle for 120 s of awake time stops serving stale feeds. Run with
// `bun test ./deploy/feeds.test.ts`. The poller is driven by hand over a
// scripted fetcher and a fake monotonic clock; the S3 fetcher and the boot
// download run against local fake stores, one for the bundle's bucket
// (FEEDS_S3_*) and a decoy for every other bucket the wrapper knows.
import { afterEach, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { downloadFile } from "./app-server/downloader";
import { type FetchBundle, FeedPoller, POLL_MS, STALE_MS, s3BundleFetcher } from "./app-server/feeds";

const dirs: string[] = [];
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
});
function feedDir(): string {
  const d = mkdtempSync(join(tmpdir(), "feeds-"));
  dirs.push(d);
  return d;
}

const bundle = (version: number) =>
  JSON.stringify({
    keys: { feed_version: version, keys: [] },
    policies: { feed_version: version, projects: [] },
    grants: { feed_version: version, credentials: [] },
  });
const FILES = ["grants.json", "keys.json", "policies.json"];
const version = (dir: string) => JSON.parse(readFileSync(join(dir, "keys.json"), "utf8")).feed_version;

/// A fetcher that answers from a script: a bundle (with its ETag), 304, or
/// a failure; it records the ETag each call sent.
function scripted() {
  const sent: (string | null)[] = [];
  let next: { body: string; etag: string } | "unchanged" | "fail" = "fail";
  const fetch: FetchBundle = async (etag) => {
    sent.push(etag);
    if (next === "fail") throw new Error("store unreachable");
    if (next === "unchanged" || etag === next.etag) return { status: "unchanged" };
    return { status: "changed", etag: next.etag, body: next.body };
  };
  return {
    fetch,
    sent,
    serve(v: number) {
      next = { body: bundle(v), etag: `"etag-${v}"` };
    },
    /// Publish any body under `etag`, a bundle or not.
    publish(body: string, etag: string) {
      next = { body, etag };
    },
    fail() {
      next = "fail";
    },
  };
}

/// A poller over `store` whose monotonic clock the test moves.
function rig(dir: string) {
  const store = scripted();
  const clock = { now: 0 };
  const lines: string[] = [];
  const poller = new FeedPoller(store.fetch, dir, (l) => lines.push(l), () => clock.now);
  /// One poll `ms` after the previous one.
  const pollAfter = (ms: number) => {
    clock.now += ms;
    return poller.poll();
  };
  return { store, clock, lines, poller, pollAfter };
}

test("a changed bundle is rewritten whole and atomically, then polled with its ETag", async () => {
  const dir = feedDir();
  const { store, pollAfter } = rig(dir);
  store.serve(1);
  await pollAfter(0);
  expect(readdirSync(dir).sort()).toEqual(FILES);
  expect(version(dir)).toBe(1);
  const inode = statSync(join(dir, "keys.json")).ino;
  await pollAfter(POLL_MS);
  expect(statSync(join(dir, "keys.json")).ino).toBe(inode);
  store.serve(2);
  await pollAfter(POLL_MS);
  expect(version(dir)).toBe(2);
  expect(JSON.parse(readFileSync(join(dir, "grants.json"), "utf8")).feed_version).toBe(2);
  expect(readdirSync(dir).sort()).toEqual(FILES);
  expect(store.sent).toEqual([null, '"etag-1"', '"etag-1"']);
});

test("polls that fail for 120 s of awake time delete the feeds, and the next success restores them", async () => {
  const dir = feedDir();
  const { store, lines, pollAfter } = rig(dir);
  store.serve(1);
  await pollAfter(0);
  store.fail();
  for (let t = POLL_MS; t < STALE_MS; t += POLL_MS) {
    await pollAfter(POLL_MS);
    expect(readdirSync(dir).sort()).toEqual(FILES);
  }
  await pollAfter(POLL_MS);
  expect(readdirSync(dir)).toEqual([]);
  expect(lines.at(-1)).toContain("deleted");
  store.serve(1);
  await pollAfter(POLL_MS);
  expect(version(dir)).toBe(1);
  expect(store.sent.at(-1)).toBeNull();
});

test("a wake restarts the 120 s: a failed poll after a long sleep keeps the feeds until 120 s after it", async () => {
  const dir = feedDir();
  const { store, pollAfter } = rig(dir);
  store.serve(1);
  await pollAfter(0);
  store.fail();
  await pollAfter(10 * 60_000);
  expect(readdirSync(dir).sort()).toEqual(FILES);
  for (let t = POLL_MS; t < STALE_MS; t += POLL_MS) await pollAfter(POLL_MS);
  expect(readdirSync(dir).sort()).toEqual(FILES);
  await pollAfter(POLL_MS);
  expect(readdirSync(dir)).toEqual([]);
});

test("a bundle that does not parse, or lacks a feed, is a failed poll: it never replaces the feeds, and left published for 120 s of awake time it deletes them", async () => {
  for (const body of ['{"keys":', '{"keys":{}}']) {
    const dir = feedDir();
    const { store, lines, pollAfter } = rig(dir);
    store.serve(1);
    await pollAfter(0);
    store.publish(body, '"bad"');
    for (let t = POLL_MS; t < STALE_MS; t += POLL_MS) {
      await pollAfter(POLL_MS);
      expect(version(dir)).toBe(1);
      expect(readdirSync(dir).sort()).toEqual(FILES);
      expect(lines.at(-1)).toContain("failed");
    }
    await pollAfter(POLL_MS);
    expect(readdirSync(dir)).toEqual([]);
    expect(lines.at(-1)).toContain("deleted");
    store.serve(2);
    await pollAfter(POLL_MS);
    expect(version(dir)).toBe(2);
    expect(readdirSync(dir).sort()).toEqual(FILES);
  }
});

const KEY = "feeds/cell.json";

/// A fake S3 endpoint serving `body` (ETag "v7", 304 on a match) at
/// `/feeds-bucket/<KEY>` and 404 elsewhere. It records every request: its
/// path, its If-None-Match, and the access key id and region it is signed
/// with (from a presigned URL's X-Amz-Credential or a signed header).
function fakeStore(body: string) {
  const seen: { path: string; etag: string | null; credential: string | null }[] = [];
  const server = Bun.serve({
    port: 0,
    fetch(req) {
      const url = new URL(req.url);
      const signed = `${url.searchParams.get("X-Amz-Credential") ?? ""} ${req.headers.get("authorization") ?? ""}`;
      const scope = signed.match(/(?:^|Credential=)([^/\s]+)\/\d{8}\/([^/]+)\/s3\/aws4_request/);
      const etag = req.headers.get("if-none-match");
      seen.push({ path: url.pathname, etag, credential: scope ? `${scope[1]}/${scope[2]}` : null });
      if (url.pathname !== `/feeds-bucket/${KEY}`) return new Response("", { status: 404 });
      if (etag === '"v7"') return new Response(null, { status: 304 });
      return new Response(body, { headers: { etag: '"v7"' } });
    },
  });
  return { seen, endpoint: `http://127.0.0.1:${server.port}`, stop: () => server.stop(true) };
}

/// The bundle's bucket, by the only names the wrapper reads it with
/// (FEEDS_S3_REGION unset: the region defaults to "auto").
const feedsEnv = (endpoint: string): Record<string, string> => ({
  FEEDS_S3_ENDPOINT: endpoint,
  FEEDS_S3_BUCKET: "feeds-bucket",
  FEEDS_S3_ACCESS_KEY_ID: "feeds-key-id",
  FEEDS_S3_SECRET_ACCESS_KEY: "feeds-secret",
});

/// Every other bucket a wrapper's environment names (the binary's BIN_S3_*,
/// the data bucket's SLATE_S3_*, the generator's S3_*), all on `endpoint`.
function otherBuckets(endpoint: string): Record<string, string> {
  const env: Record<string, string> = {};
  for (const p of ["BIN_S3_", "SLATE_S3_", "S3_"]) {
    env[`${p}ENDPOINT`] = endpoint;
    env[`${p}BUCKET`] = "artifacts";
    env[`${p}REGION`] = "eu-central-1";
    env[`${p}ACCESS_KEY_ID`] = "other-key-id";
    env[`${p}SECRET_ACCESS_KEY`] = "other-secret";
  }
  return env;
}

test("the S3 fetcher reads the bundle with FEEDS_S3_* only, sends If-None-Match and reads a 304 as unchanged", async () => {
  const body = bundle(7);
  const feeds = fakeStore(body);
  const decoy = fakeStore(body);
  try {
    const fetch = s3BundleFetcher(KEY, { ...otherBuckets(decoy.endpoint), ...feedsEnv(feeds.endpoint) });
    expect(await fetch(null)).toEqual({ status: "changed", etag: '"v7"', body });
    expect(await fetch('"v7"')).toEqual({ status: "unchanged" });
    const asked = { path: `/feeds-bucket/${KEY}`, credential: "feeds-key-id/auto" };
    expect(feeds.seen).toEqual([{ ...asked, etag: null }, { ...asked, etag: '"v7"' }]);
    expect(decoy.seen).toEqual([]);
  } finally {
    feeds.stop();
    decoy.stop();
  }
});

test("the boot download reads the bundle with FEEDS_S3_* only", async () => {
  const body = bundle(3);
  const feeds = fakeStore(body);
  const decoy = fakeStore(body);
  const dest = join(feedDir(), "bundle.json");
  try {
    await downloadFile(KEY, dest, () => {}, { ...otherBuckets(decoy.endpoint), ...feedsEnv(feeds.endpoint) });
    expect(readFileSync(dest, "utf8")).toBe(body);
    expect(feeds.seen).toEqual([{ path: `/feeds-bucket/${KEY}`, etag: null, credential: "feeds-key-id/auto" }]);
    expect(decoy.seen).toEqual([]);
  } finally {
    feeds.stop();
    decoy.stop();
  }
});

test("FEEDS_S3_REGION names the region the fetcher and the boot download sign for, never BIN_S3_REGION's", async () => {
  const body = bundle(5);
  const feeds = fakeStore(body);
  const dest = join(feedDir(), "bundle.json");
  try {
    // The other buckets sit on the same store and name eu-central-1.
    const env = { ...otherBuckets(feeds.endpoint), ...feedsEnv(feeds.endpoint), FEEDS_S3_REGION: "us-east-1" };
    expect(await s3BundleFetcher(KEY, env)(null)).toEqual({ status: "changed", etag: '"v7"', body });
    await downloadFile(KEY, dest, () => {}, env);
    expect(readFileSync(dest, "utf8")).toBe(body);
    const asked = { path: `/feeds-bucket/${KEY}`, etag: null, credential: "feeds-key-id/us-east-1" };
    expect(feeds.seen).toEqual([asked, asked]);
  } finally {
    feeds.stop();
  }
});

test("a BIN_S3_*-only environment is refused by the fetcher and the boot download before any request, naming each missing FEEDS_S3_* name", async () => {
  const decoy = fakeStore(bundle(1));
  const dest = join(feedDir(), "bundle.json");
  const lines: string[] = [];
  try {
    const others = otherBuckets(decoy.endpoint);
    const refused =
      "feeds bucket refused: FEEDS_S3_ENDPOINT, FEEDS_S3_BUCKET, FEEDS_S3_ACCESS_KEY_ID, FEEDS_S3_SECRET_ACCESS_KEY unset";
    expect(() => s3BundleFetcher(KEY, others)).toThrow(refused);
    await expect(downloadFile(KEY, dest, (l) => lines.push(l), others)).rejects.toThrow(refused);
    // One name missing is refused alone: no name falls back to its BIN_S3_* twin.
    const noSecret = { ...others, ...feedsEnv(decoy.endpoint) };
    delete noSecret.FEEDS_S3_SECRET_ACCESS_KEY;
    expect(() => s3BundleFetcher(KEY, noSecret)).toThrow("feeds bucket refused: FEEDS_S3_SECRET_ACCESS_KEY unset");
    expect(decoy.seen).toEqual([]);
    expect(existsSync(dest)).toBe(false);
    expect(lines).toEqual([]);
  } finally {
    decoy.stop();
  }
});
