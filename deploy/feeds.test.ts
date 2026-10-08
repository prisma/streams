// The deploy wrapper's auth-feed poller (owner decision of 2026-10-08,
// README "Shared cells Q6" (a), option 4(b)): a running cell picks up a
// republished bundle without a restart, and a cell that cannot reach the
// bundle for 120 s of awake time stops serving stale feeds. Run with
// `bun test ./deploy/feeds.test.ts`. The poller is driven by hand over a
// scripted fetcher and a fake monotonic clock; the S3 fetcher runs against a
// local fake store.
import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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

test("the S3 fetcher sends If-None-Match and reads a 304 as unchanged", async () => {
  const seen: (string | null)[] = [];
  const body = bundle(7);
  const server = Bun.serve({
    port: 0,
    fetch(req) {
      const tag = req.headers.get("if-none-match");
      seen.push(tag);
      if (!new URL(req.url).pathname.endsWith("/feeds/cell.json")) return new Response("", { status: 404 });
      if (tag === '"v7"') return new Response(null, { status: 304 });
      return new Response(body, { headers: { etag: '"v7"' } });
    },
  });
  try {
    const fetch = s3BundleFetcher("feeds/cell.json", {
      BIN_S3_ENDPOINT: `http://127.0.0.1:${server.port}`,
      BIN_S3_BUCKET: "artifacts",
      BIN_S3_ACCESS_KEY_ID: "test",
      BIN_S3_SECRET_ACCESS_KEY: "test",
    });
    expect(await fetch(null)).toEqual({ status: "changed", etag: '"v7"', body });
    expect(await fetch('"v7"')).toEqual({ status: "unchanged" });
    expect(seen).toEqual([null, '"v7"']);
  } finally {
    server.stop(true);
  }
});
