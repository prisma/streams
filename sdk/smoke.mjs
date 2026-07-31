// End-to-end SDK smoke against a live server (run by CI/scripts, not
// part of the package). Usage: node smoke.mjs http://127.0.0.1:8971
import {
  StreamsClient,
  MemoryProducerStateStore,
  ProducerSequenceReusedError,
} from "./src/index.ts";

const url = process.argv[2] ?? "http://127.0.0.1:8971";
const encryptionKey =
  process.env.STREAMS_KEY ?? "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";
const token = process.env.STREAMS_TOKEN;
let failures = 0;
function check(name, cond, extra = "") {
  if (cond) console.log(`ok   ${name}`);
  else {
    console.log(`FAIL ${name} ${extra}`);
    failures++;
  }
}

const client = new StreamsClient({ url, token });

// Create with watches; idempotent retry.
const orders = await client.createStream("smoke/orders", {
  encryptionKey,
  format: { kind: "json" },
  watches: [{ name: "by-customer", fields: ["/customerId"] }],
});
await client.createStream("smoke/orders", {
  encryptionKey,
  format: { kind: "json" },
  watches: [{ name: "by-customer", fields: ["/customerId"] }],
});
check("create + idempotent retry", true);

// Producer session: appends, batch, duplicate handling, reuse conflict.
const store = new MemoryProducerStateStore();
const producer = orders.producer("checkout", { state: store });
const a1 = await producer.append({ customerId: "c1", n: 0 }, { routingKey: "c1" });
check("producer append", a1.count === 1 && !a1.duplicate);
const a2 = await producer.appendMany(
  [{ customerId: "c1", n: 1 }, { customerId: "c1", n: 2 }],
  { routingKey: "c1" },
);
check("producer appendMany", a2.count === 2);
// Rewind the local state to replay seq 1 with the SAME body: duplicate.
await store.save({ stream: "smoke/orders", producerId: "checkout", routingKey: "c1" }, { epoch: 0, nextSeq: 1 });
const dup = await producer.appendMany(
  [{ customerId: "c1", n: 1 }, { customerId: "c1", n: 2 }],
  { routingKey: "c1" },
);
check("exact retry is duplicate", dup.duplicate === true);
// Same seq, different body: conflict.
await store.save({ stream: "smoke/orders", producerId: "checkout", routingKey: "c1" }, { epoch: 0, nextSeq: 1 });
let reused = false;
try {
  await producer.append({ customerId: "c1", n: 99 }, { routingKey: "c1" });
} catch (e) {
  reused = e instanceof ProducerSequenceReusedError;
}
check("sequence reuse conflicts", reused);

// Read + pagination + cursor state.
const page = await orders.read({ routingKey: "c1" });
check(
  "read returns the key sequence",
  page.records.length === 3 && page.upToDate,
  JSON.stringify(page.records),
);

// Subscribe: catch-up then live (async append wakes it).
const seen = [];
const sub = (async () => {
  for await (const rec of orders.subscribe({ routingKey: "c1" })) {
    seen.push(rec.n);
    if (seen.length >= 4) break;
  }
})();
await new Promise((r) => setTimeout(r, 300));
await orders.append({ customerId: "c1", n: 3 }, { routingKey: "c1" });
await Promise.race([sub, new Promise((r) => setTimeout(r, 8000))]);
check("subscribe catch-up + live wake", JSON.stringify(seen) === "[0,1,2,3]", JSON.stringify(seen));

// Scan: snapshot traversal with routing keys.
await orders.append({ customerId: "c2", n: 0 }, { routingKey: "c2" });
const scanned = [];
for await (const rec of orders.scan()) scanned.push(rec.routingKey);
check("scan sees every record once", scanned.length === 5, JSON.stringify(scanned));

// Consumer: per-key FIFO pull + ack via batch settle.
const workers = await orders.consumer("fulfilment", { maxAttempts: 3 });
const batch = await workers.pull({ max: 10 });
check("pull leases per-key heads", batch.messages.length === 2, `got ${batch.messages.length}`);
for (const m of batch.messages) m.ack();
const settled = await batch.settle();
check("settle acks", settled.acked === 2);

// Watch: derive nothing client-side — use the server's key derivation via
// a wait on the coarse key? The SDK takes a precomputed watchKey hex;
// compute it the documented way is server-side; here we just verify the
// definitions endpoint.
const defs = await orders.watches();
check("watch definitions listed", defs[0]?.name === "by-customer");

// Seal with a final record; subscribe termination; catalog.
await orders.seal({ final: { customerId: "c1", done: true }, routingKey: "c1" });
const meta = await orders.metadata();
check("sealed metadata", meta.sealed === true);
const names = [];
for await (const s of client.listStreams()) names.push(s.name);
check("catalog lists the stream", names.includes("smoke/orders"), JSON.stringify(names));

// Sealed stream refuses appends.
let sealedRefused = false;
try {
  await orders.append({ late: 1 });
} catch (e) {
  sealedRefused = e.code === "sealed" || e.status === 409;
}
check("sealed refuses appends", sealedRefused);

console.log(failures === 0 ? "SMOKE PASS" : `SMOKE FAIL (${failures})`);
process.exit(failures === 0 ? 0 : 1);
