import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import { StreamsClient, WrongCellError } from "../dist/index.js";

// The Prisma Compute edge in front of *.prisma.build strips every response
// header named Prisma-* (edge change #176), and the server sends each also
// as Streams-<Name>. This fake server answers as a client behind the edge
// sees the cell: its answers carry only the Streams- names, never a Prisma-
// one. Paging, long-poll subscribe, scan, the consumer version and the
// wrong-cell header all work from them.
const KEY = "A".repeat(43);
const PAGE = 2;

function cell() {
  const records = [];
  const waiters = [];
  const requests = [];
  const answer = (res, status, body, headers = {}) => {
    for (const name of Object.keys(headers)) assert.ok(!name.startsWith("prisma-"), name);
    res.writeHead(status, { "content-type": "application/json", ...headers });
    res.end(body === undefined ? undefined : JSON.stringify(body));
  };
  const page = (res, from, sealed) => {
    const next = Math.min(from + PAGE, records.length);
    const headers = { "streams-next-cursor": `c${next}` };
    if (next === records.length) headers["streams-up-to-date"] = "true";
    if (sealed) headers["streams-sealed"] = "true";
    answer(res, 200, records.slice(from, next), headers);
  };
  const server = createServer(async (req, res) => {
    const url = new URL(req.url, "http://cell");
    const path = url.pathname;
    let body = "";
    for await (const chunk of req) body += chunk;
    requests.push(`${req.method} ${path}${url.search}`);
    const cursor = url.searchParams.get("cursor");
    const from = cursor === null || cursor === "beginning" ? 0 : Number(cursor.slice(1));
    if (path === "/v1/streams/elsewhere/records") {
      res.writeHead(503, { "streams-error-code": "wrong_cell" });
      return res.end();
    }
    if (path === "/v1/streams/orders/records" && req.method === "POST") {
      records.push(JSON.parse(body));
      for (const wake of waiters.splice(0)) wake();
      return answer(res, 200, { cursor: `c${records.length}`, count: 1, duplicate: false, sealed: false });
    }
    if (path === "/v1/streams/orders/records") return page(res, from, false);
    if (path === "/v1/streams/done/records") return page(res, records.length, true);
    if (path === "/v1/streams/orders/records:long-poll") {
      if (from === records.length) {
        await new Promise(resolve => waiters.push(resolve));
      }
      return page(res, from, false);
    }
    if (path === "/v1/streams/orders:scan") {
      const last = from + 1 >= records.length;
      const headers = last ? { "streams-scan-complete": "true" } : { "streams-next-scan-cursor": `c${from + 1}` };
      return answer(res, 200, [{ routingKey: "", value: records[from] }], headers);
    }
    if (path === "/v1/streams/orders/consumers/workers" && req.method === "PUT") {
      return answer(res, 201, { name: "workers" }, { "streams-consumer-version": "v7" });
    }
    if (path === "/v1/streams/orders/consumers/workers" && req.method === "DELETE") {
      const version = req.headers["streams-consumer-version"];
      if (version === "v7") {
        res.writeHead(204);
        return res.end();
      }
      return answer(res, 400, { error: { code: "missing_consumer_version", message: String(version), retryable: false } });
    }
    answer(res, 404, { error: { code: "not_found", message: path, retryable: false } });
  });
  return { server, records, requests, waiters };
}

async function withCell(run) {
  const fake = cell();
  fake.server.listen(0, "127.0.0.1");
  await once(fake.server, "listening");
  const client = new StreamsClient({ url: `http://127.0.0.1:${fake.server.address().port}` });
  try {
    await run({ ...fake, client, orders: client.stream("orders", { encryptionKey: KEY }) });
  } finally {
    for (const wake of fake.waiters.splice(0)) wake();
    fake.server.closeAllConnections();
    fake.server.close();
  }
}

test("read pages through a stream by the Streams-Next-Cursor and stops at Streams-Up-To-Date", async () => {
  await withCell(async ({ orders }) => {
    for (let n = 1; n <= 5; n++) await orders.append({ n });
    const pages = [];
    let from;
    for (let reads = 0; reads < 10; reads++) {
      const page = await orders.read(from === undefined ? undefined : { from });
      pages.push({ records: page.records.map(r => r.n), cursor: page.cursor, upToDate: page.upToDate });
      if (page.upToDate) break;
      from = page.cursor;
    }
    assert.deepEqual(pages, [
      { records: [1, 2], cursor: "c2", upToDate: false },
      { records: [3, 4], cursor: "c4", upToDate: false },
      { records: [5], cursor: "c5", upToDate: true },
    ]);
  });
});

test("subscribe catches up by Streams-Next-Cursor, goes live on Streams-Up-To-Date and wakes on an append", async () => {
  await withCell(async ({ orders, requests }) => {
    for (let n = 1; n <= 3; n++) await orders.append({ n });
    const abort = new AbortController();
    const seen = [];
    const iterator = orders.subscribe({ signal: abort.signal })[Symbol.asyncIterator]();
    setTimeout(() => abort.abort(), 3000).unref();
    for (let i = 0; i < 3; i++) seen.push((await iterator.next()).value.n);
    const live = iterator.next();
    await orders.append({ n: 4 });
    seen.push((await live).value.n);
    abort.abort();
    assert.deepEqual(seen, [1, 2, 3, 4]);
    const reads = requests.filter(r => r.startsWith("GET /v1/streams/orders/records"));
    assert.deepEqual(reads.slice(0, 3).map(r => r.split("?")[0]), [
      "GET /v1/streams/orders/records",
      "GET /v1/streams/orders/records",
      "GET /v1/streams/orders/records:long-poll",
    ]);
    assert.match(reads[2], /cursor=c3/);
  });
});

test("subscribe ends on Streams-Sealed with Streams-Up-To-Date", async () => {
  await withCell(async ({ client }) => {
    const done = client.stream("done", { encryptionKey: KEY });
    const seen = [];
    // Bounded: a client that cannot read the headers re-reads forever.
    for await (const record of done.subscribe({ signal: AbortSignal.timeout(3000) })) seen.push(record);
    assert.deepEqual(seen, []);
    const page = await done.read();
    assert.equal(page.sealed, true);
  });
});

test("scan follows Streams-Next-Scan-Cursor to Streams-Scan-Complete", async () => {
  await withCell(async ({ orders }) => {
    for (let n = 1; n <= 3; n++) await orders.append({ n });
    const seen = [];
    for await (const item of orders.scan()) seen.push(item.value.n);
    assert.deepEqual(seen, [1, 2, 3]);
  });
});

test("a consumer's delete sends the version its create read from Streams-Consumer-Version", async () => {
  await withCell(async ({ orders, requests }) => {
    const workers = await orders.consumer("workers");
    await workers.delete();
    assert.deepEqual(requests.slice(-2), [
      "PUT /v1/streams/orders/consumers/workers",
      "DELETE /v1/streams/orders/consumers/workers",
    ]);
  });
});

test("Streams-Error-Code: wrong_cell is WrongCellError", async () => {
  await withCell(async ({ client }) => {
    const elsewhere = client.stream("elsewhere", { encryptionKey: KEY });
    await assert.rejects(elsewhere.read(), error => error instanceof WrongCellError && error.status === 503);
  });
});
