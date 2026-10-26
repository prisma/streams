import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import { StreamsClient, StreamsError } from "../dist/index.js";

// The multiplexed wait (layout 6A package 2): a client's subscribe() tails,
// consumer iterators and watch subscriptions wait through ONE
// `POST /v1/projects/{p}/live:wait` at a time; a cell without the route
// (404) or a stream it refuses there (`not_owner`) is waited on per stream.
const KEY = "A".repeat(43);

/** A cell: catch-up reads answer "up to date at c0"; each live wait is
 * handed to `onWait(body)`, which answers its results (or a status); the
 * per-stream long-poll answers one record then 204s. */
async function cell({ onWait, waitStatus } = {}) {
  const requests = [];
  const waits = [];
  let open = 0;
  let maxOpen = 0;
  const server = createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;
    const url = new URL(req.url, "http://cell");
    requests.push({ method: req.method, path: url.pathname, query: url.search, body });
    res.setHeader("content-type", "application/json");
    if (url.pathname === "/v1/projects/p1/live:wait") {
      if (waitStatus) {
        res.writeHead(waitStatus);
        return res.end(JSON.stringify({ error: { code: "unknown_route", message: "no", retryable: false } }));
      }
      const parsed = JSON.parse(body);
      waits.push(parsed);
      open++;
      maxOpen = Math.max(maxOpen, open);
      try {
        const results = await onWait(parsed, req);
        return res.end(JSON.stringify({ results }));
      } finally {
        open--;
      }
    }
    if (req.method === "GET" && url.pathname.endsWith("/records")) {
      res.setHeader("streams-next-cursor", "c0");
      res.setHeader("streams-up-to-date", "true");
      return res.end("[]");
    }
    if (req.method === "GET" && url.pathname.endsWith("/records:long-poll")) {
      const cursor = url.searchParams.get("cursor");
      if (cursor === "c0") {
        res.setHeader("streams-next-cursor", "c1");
        res.setHeader("streams-up-to-date", "true");
        return res.end(JSON.stringify([{ perStream: url.pathname }]));
      }
      res.writeHead(204, { "streams-next-cursor": cursor, "streams-up-to-date": "true" });
      return res.end();
    }
    if (req.method === "POST" && url.pathname.endsWith(":settle")) {
      return res.end(JSON.stringify({ acked: JSON.parse(body).acks.length, retried: 0, extended: 0, dlq: 0, stale: 0, backlog: 0 }));
    }
    res.writeHead(404);
    res.end(JSON.stringify({ error: { code: "not_found", message: url.pathname, retryable: false } }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const url = `http://127.0.0.1:${server.address().port}`;
  return {
    requests,
    waits,
    maxOpen: () => maxOpen,
    client: (options = {}) => new StreamsClient({ url, project: "p1", token: "t", ...options }),
    close() {
      server.closeAllConnections();
      server.close();
    },
  };
}

/** Resolve after `ms`. */
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("twenty tails wait through one request and each gets only its own records", async () => {
  // A request answers only once it carries all twenty; an earlier one,
  // carrying the tails whose catch-up had finished, is aborted by the
  // client when the next tail joins.
  let answered = false;
  const c = await cell({
    onWait: async (body, req) => {
      if (body.subs.length === 20 && !answered) {
        answered = true;
        const s7 = body.subs.find((s) => s.stream === "s7");
        return [{ id: s7.id, records: [{ n: 7 }], cursor: "c7", upToDate: true, sealed: false }];
      }
      await new Promise((resolve) => {
        req.on("close", resolve);
        setTimeout(resolve, 3000);
      });
      return [];
    },
  });
  const client = c.client();
  const controller = new AbortController();
  const streams = Array.from({ length: 20 }, (_, i) => client.stream(`s${i}`, { encryptionKey: KEY }));
  const firsts = streams.map((s) => s.subscribe({ signal: controller.signal })[Symbol.asyncIterator]().next());
  const got = await Promise.race([firsts[7], pause(5000).then(() => "timeout")]);
  assert.deepEqual(got, { done: false, value: { n: 7 } });
  const all = c.waits.findIndex((w) => w.subs.length === 20);
  assert.deepEqual(
    c.waits[all].subs.map((s) => [s.kind, s.stream, s.cursor, s.key]).sort(),
    streams.map((s, i) => ["tail", `s${i}`, "c0", KEY]).sort(),
  );
  // Tail 7 is still yielding its page: the next request carries the other 19.
  await pause(50);
  const next = c.waits[all + 1];
  assert.equal(next.subs.length, 19);
  assert.ok(next.subs.every((s) => s.stream !== "s7"));
  assert.equal(c.requests.filter((r) => r.path.endsWith(":long-poll")).length, 0);
  controller.abort();
  await Promise.allSettled(firsts);
  c.close();
});

test("a cell without the route answers 404 and every tail waits per stream", async () => {
  const c = await cell({ waitStatus: 404 });
  const client = c.client();
  const controller = new AbortController();
  const it = client.stream("solo", { encryptionKey: KEY }).subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
  assert.deepEqual(await it.next(), { done: false, value: { perStream: "/v1/streams/solo/records:long-poll" } });
  assert.equal(c.requests.filter((r) => r.path === "/v1/projects/p1/live:wait").length, 1, "asked once");
  controller.abort();
  c.close();
});

test("a stream refused not_owner waits per stream while the rest stay multiplexed", async () => {
  const c = await cell({
    onWait: async (body) => {
      const far = body.subs.find((s) => s.stream === "far");
      if (far) {
        return [{ id: far.id, status: 421, error: { code: "not_owner", message: "elsewhere", retryable: true } }];
      }
      await pause(20);
      return [{ id: body.subs[0].id, records: [{ near: true }], cursor: "c9", upToDate: true, sealed: false }];
    },
  });
  const client = c.client();
  const controller = new AbortController();
  const far = client.stream("far", { encryptionKey: KEY }).subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
  const near = client.stream("near", { encryptionKey: KEY }).subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
  assert.deepEqual(await far.next(), { done: false, value: { perStream: "/v1/streams/far/records:long-poll" } });
  assert.deepEqual(await near.next(), { done: false, value: { near: true } });
  assert.ok(c.waits.slice(1).every((w) => w.subs.every((s) => s.stream !== "far")), "far left the wait");
  controller.abort();
  c.close();
});

test("a subscription's own refusal throws on its iterator only", async () => {
  const c = await cell({
    onWait: async (body) => {
      const denied = body.subs.find((s) => s.stream === "b/secret");
      if (denied) {
        return [{ id: denied.id, status: 403, error: { code: "prefix_denied", message: "not yours", retryable: false } }];
      }
      await pause(20);
      return [{ id: body.subs[0].id, records: [{ ok: 1 }], cursor: "c2", upToDate: true, sealed: false }];
    },
  });
  const client = c.client();
  const controller = new AbortController();
  const denied = client.stream("b/secret", { encryptionKey: KEY }).subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
  const ok = client.stream("a/ok", { encryptionKey: KEY }).subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
  await assert.rejects(denied.next(), (e) => e instanceof StreamsError && e.status === 403 && e.code === "prefix_denied");
  assert.deepEqual(await ok.next(), { done: false, value: { ok: 1 } });
  controller.abort();
  c.close();
});

test("a consumer's batch comes through the wait, is settled by its pull's settle, and rejoins after", async () => {
  let round = 0;
  const c = await cell({
    onWait: async (body) => {
      round++;
      const g = body.subs.find((s) => s.kind === "group");
      if (round === 1) {
        return [{ id: g.id, backlog: 0, messages: [{ id: "m1", routingKey: "k", attempts: 1, leaseToken: "L1", value: { job: 1 } }] }];
      }
      await pause(20);
      return [];
    },
  });
  const client = c.client();
  const stream = client.stream("jobs", { encryptionKey: KEY });
  const { Consumer } = await import("../dist/index.js");
  const consumer = new Consumer(client["ctx"], stream, "workers", "v1");
  const it = consumer.messages();
  const first = await it.next();
  assert.deepEqual([first.value.id, first.value.value], ["m1", { job: 1 }]);
  assert.deepEqual(
    c.waits[0].subs.map((s) => [s.kind, s.stream, s.group, s.key]),
    [["group", "jobs", "workers", KEY]],
  );
  first.value.ack();
  const second = it.next();
  await pause(60);
  const settle = c.requests.find((r) => r.path === "/v1/streams/jobs/consumers/workers:settle");
  assert.deepEqual(JSON.parse(settle.body).acks, [{ leaseToken: "L1" }]);
  assert.ok(c.waits.length >= 2 && c.waits[1].subs[0].kind === "group", "rejoined after its settlement");
  await it.return();
  await Promise.race([second, pause(100)]);
  c.close();
});

test("liveMux: false waits per stream and never asks for the multiplexed wait", async () => {
  const c = await cell({ onWait: async () => [] });
  const client = c.client({ liveMux: false });
  const controller = new AbortController();
  const it = client.stream("solo", { encryptionKey: KEY }).subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
  assert.deepEqual(await it.next(), { done: false, value: { perStream: "/v1/streams/solo/records:long-poll" } });
  assert.equal(c.waits.length, 0);
  controller.abort();
  c.close();
});

test("a watch waits through the wait from its pinned cursor and yields only invalidations", async () => {
  const seen = [];
  const c = await cell({
    onWait: async (body) => {
      const w = body.subs.find((s) => s.kind === "watch");
      seen.push([w.cursor, w.watch, w.watchKey, w.key, w.stream]);
      if (w.cursor === "now") return [{ id: w.id, event: { invalidated: false, cursor: "j:1" } }];
      await pause(20);
      return [{ id: w.id, event: { invalidated: true, reason: "changed", cursor: "j:2" } }];
    },
  });
  const client = c.client();
  const { Watch } = await import("../dist/index.js");
  const stream = client.stream("cfg", { encryptionKey: KEY });
  const watch = new Watch(client["ctx"], stream, "byId", "00112233445566ff", KEY, "00".repeat(16));
  const controller = new AbortController();
  const it = watch.subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
  const first = await it.next();
  assert.deepEqual(first.value, { invalidated: true, reason: "changed", cursor: "j:2" });
  assert.deepEqual(seen.slice(0, 2), [
    ["now", "byId", "00112233445566ff", KEY, "cfg"],
    ["j:1", "byId", "00112233445566ff", KEY, "cfg"],
  ]);
  assert.equal(c.requests.filter((r) => r.path.includes("/watches/")).length, 0, "no capability wait");
  controller.abort();
  await it.return?.();
  c.close();
});

test("more waiting subscriptions than one request carries are split across requests of at most 256", async () => {
  const c = await cell({
    onWait: async (body, req) => {
      await new Promise((resolve) => {
        req.on("close", resolve);
        setTimeout(resolve, 2000);
      });
      return [];
    },
  });
  const client = c.client();
  const controller = new AbortController();
  const its = Array.from({ length: 300 }, (_, i) =>
    client.stream(`m${i}`, { encryptionKey: KEY }).subscribe({ signal: controller.signal })[Symbol.asyncIterator]());
  const firsts = its.map((it) => it.next());
  const settled = Promise.allSettled(firsts);
  const joined = () => new Set(c.waits.flatMap((w) => w.subs.map((s) => s.stream))).size;
  for (let i = 0; i < 100 && joined() < 300; i++) await pause(100);
  assert.ok(c.waits.length > 0);
  assert.ok(c.waits.every((w) => w.subs.length <= 256), `largest ${Math.max(...c.waits.map((w) => w.subs.length))}`);
  const streams = new Set(c.waits.flatMap((w) => w.subs.map((s) => s.stream)));
  assert.equal(streams.size, 300, "every subscription waits");
  controller.abort();
  const outcomes = await settled;
  assert.ok(outcomes.every((o) => o.status === "fulfilled" && o.value.done === true), "none threw");
  c.close();
});
