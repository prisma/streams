// End-to-end SDK smoke against a live server (run by CI/scripts, not
// part of the package). Usage: node smoke.mjs http://127.0.0.1:8971
// Imports the PACKAGE (dist), so this same file validates an
// installed tarball; set STREAMS_SDK to override the specifier.
const mod = await import(process.env.STREAMS_SDK ?? "../sdk/dist/index.js");
const { StreamsClient, MemoryProducerStateStore, ProducerSequenceReusedError } = mod;

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

// Watch: the SDK derives the watch key and signs the observation URL
// offline. This only works if the client's derivation matches the
// server's byte for byte, so the round trip below is the real test of
// both — plus the persisted signature verifier.
const defs = await orders.watches();
check("watch definitions listed", defs[0]?.name === "by-customer");

const watch = await orders.watch("by-customer", ["c7"]);
check("watch key derived client-side", /^[0-9a-f]{16}$/.test(watch.key), watch.key);
const pending = watch.wait({ cursor: "now", timeoutMs: 10000 });
await new Promise((r) => setTimeout(r, 300));
await orders.append({ customerId: "c7", n: 0 }, { routingKey: "c7" });
const ev = await Promise.race([
  pending,
  new Promise((r) => setTimeout(() => r({ invalidated: false, reason: "timeout" }), 12000)),
]);
check("signed watch URL observes the derived key", ev.invalidated === true, JSON.stringify(ev));

// The URL is a standalone capability: no key, no token, no SDK.
const bare = await fetch(watch.url({ timeoutMs: 1000 }));
check("watch URL needs no credentials", bare.status === 200, `status ${bare.status}`);
// A tampered signature is refused.
const forged = await fetch(watch.url({ timeoutMs: 1000 }).replace(/sig=\w{4}/, "sig=0000"));
check("forged signature refused", forged.status === 403, `status ${forged.status}`);
// Wrong value count is caught before any request.
let cardinality = false;
try {
  await orders.watch("by-customer", ["a", "b"]);
} catch (e) {
  cardinality = e.code === "invalid_watch_values";
}
check("watch value cardinality checked", cardinality);

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
