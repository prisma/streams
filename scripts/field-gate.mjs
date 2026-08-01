// The cloud gate: the five behaviours that only mean something against
// real infrastructure — a real object store, a real network, real
// concurrency between processes.
//
// SCOPE, precisely: these are REPLAY and coexistence checks, plus one
// genuine auto-split. Nothing here kills or restarts the server, so it
// does not exercise crash recovery at durable boundaries; that is what
// the failpoint tests in the rust suite are for. Read a pass here as
// "the contract holds over the WAN against Tigris", not as "the
// lifecycle survives a crash".
//
//   node scripts/field-gate.mjs <base-url>
//     STREAMS_TOKEN  bearer token the service was deployed with
//     STREAMS_KEY    base64 encryption key (default: the rig key)
//
// Deploy the service with a low split threshold for the split test:
//   SCALE_EVAL_SECS=5 SCALE_RATE_WINDOW_SECS=10 SCALE_HOT_PCT=1
//   SCALE_HOT_EVALS=1 SCALE_COOLDOWN_SECS=5

const base = (process.argv[2] ?? "http://127.0.0.1:8090").replace(/\/$/, "");
const token = process.env.STREAMS_TOKEN;
const key = process.env.STREAMS_KEY ?? "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";
const stamp = process.env.FIELD_STAMP ?? String(process.pid);

let failures = 0;
function check(name, cond, extra = "") {
  console.log(cond ? `ok   ${name}` : `FAIL ${name} ${extra}`);
  if (!cond) failures++;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function headers(extra = {}) {
  const h = { "prisma-encryption-key": key, ...extra };
  if (token) h["authorization"] = `Bearer ${token}`;
  return h;
}
const P = (path, init = {}) => fetch(`${base}/v1/streams/${path}`, init);
const R = (path, init = {}) => fetch(`${base}/v1/stream/${path}`, init);
const json = async (res) => {
  try {
    return JSON.parse(await res.text());
  } catch {
    return null;
  }
};

// --- 1. negative auth -------------------------------------------------
// The product surface is account-authenticated. Anonymous and
// wrong-token requests must be refused before anything else happens.
{
  const name = `gate/auth-${stamp}`;
  const body = JSON.stringify({ format: { kind: "json" } });
  const anon = await P(name, {
    method: "PUT",
    headers: { "prisma-encryption-key": key },
    body,
  });
  const wrong = await P(name, {
    method: "PUT",
    headers: { "prisma-encryption-key": key, authorization: "Bearer not-the-token" },
    body,
  });
  if (token) {
    check("anonymous product request refused", anon.status === 401, `status ${anon.status}`);
    check("wrong bearer token refused", wrong.status === 401, `status ${wrong.status}`);
  } else {
    console.log("skip negative auth: service deployed without a token");
  }
  // The token is not a substitute for the key.
  const created = await P(name, { method: "PUT", headers: headers(), body });
  check("create with token + key", created.status === 201, `status ${created.status}`);
  const noKey = await P(`${name}/records`, {
    method: "POST",
    headers: token ? { authorization: `Bearer ${token}` } : {},
    body: JSON.stringify({ n: 1 }),
  });
  check("token alone cannot append", noKey.status === 400, `status ${noKey.status}`);
}

// --- 2. create replay -------------------------------------------------
// Two identical creates race, exactly as a retrying edge client makes
// them happen. One incarnation, and the initial body lands once.
{
  const name = `gate/replay-${stamp}`;
  // Create-with-initial-body is the raw protocol's create: PUT carries
  // the first record, which is exactly the shape that made the field
  // anomaly (a create that answered 200 over an empty stream).
  const rk = { "stream-encryption-key": key, "content-type": "application/json" };
  if (token) rk["authorization"] = `Bearer ${token}`;
  const body = JSON.stringify({ hello: "world" });
  const P = (p, init) => R(p, init); // this block speaks the raw route
  const [a, b] = await Promise.all([
    P(name, { method: "PUT", headers: rk, body }),
    P(name, { method: "PUT", headers: rk, body }),
  ]);
  const codes = [a.status, b.status].sort();
  check(
    "concurrent identical creates both succeed",
    codes.every((c) => c === 200 || c === 201),
    JSON.stringify(codes),
  );
  const recs = await json(await P(name, { headers: rk }));
  check(
    "the initial record exists exactly once",
    Array.isArray(recs) && recs.length === 1 && recs[0].hello === "world",
    JSON.stringify(recs),
  );
  // A third replay after the fact is still the same incarnation.
  const again = await P(name, { method: "PUT", headers: rk, body });
  check(
    "replay after completion is idempotent",
    again.status === 200 || again.status === 201,
    `status ${again.status}`,
  );
  const recs2 = await json(await P(name, { headers: rk }));
  check("…and appends nothing", Array.isArray(recs2) && recs2.length === 1, JSON.stringify(recs2));
}

// --- 3. seal REPLAY (not a crash test) --------------------------------
// A client that never saw its response retries the seal. This proves
// response-replay idempotence over a real network — it does NOT prove
// crash recovery, which needs failpoints inside the process; those live
// in the rust suite (a_plain_seal_cannot_finish_someone_elses_final,
// a_raw_close_with_content_owes_its_records).
{
  const name = `gate/seal-${stamp}`;
  await P(name, {
    method: "PUT",
    headers: headers(),
    body: JSON.stringify({ format: { kind: "json" } }),
  });
  await P(`${name}/records`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ n: 0 }),
  });
  const seal = JSON.stringify({ final: { done: true }, routingKey: "" });
  const s1 = await P(`${name}:seal`, { method: "POST", headers: headers(), body: seal });
  const s2 = await P(`${name}:seal`, { method: "POST", headers: headers(), body: seal });
  check("seal replay is idempotent", s1.ok && s2.ok, `${s1.status}/${s2.status}`);
  const recs = await json(await P(`${name}/records`, { headers: headers() }));
  const finals = (recs ?? []).filter((r) => r.done === true);
  check("exactly one final record", finals.length === 1, JSON.stringify(recs));
  const meta = await json(await P(name, { headers: headers() }));
  check("collection reports sealed", meta?.sealed === true, JSON.stringify(meta));
  const late = await P(`${name}/records`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ late: 1 }),
  });
  check("sealed collection refuses appends", late.status === 409, `status ${late.status}`);
}

// --- 4. fork REPLAY (not a crash test) --------------------------------
// Forks are a raw-protocol feature: create one, read through the
// boundary, then replay the same create. Again: replay idempotence, not
// crash recovery — see a_crashed_fork_cascade_can_be_resumed and
// a_fork_initialization_is_bound_to_its_source_incarnation for the
// durable-boundary cases.
{
  const src = `gate/fork-src-${stamp}`;
  const fork = `gate/fork-${stamp}`;
  const kh = { "stream-encryption-key": key, "content-type": "application/json" };
  if (token) kh["authorization"] = `Bearer ${token}`;
  await R(src, { method: "PUT", headers: kh, body: JSON.stringify({ i: 0 }) });
  // Fork boundaries are opaque protocol offsets, so take two real ones:
  // one after the first record, one after the third.
  const early = (await R(src, { headers: kh })).headers.get("stream-next-offset");
  for (const i of [1, 2]) {
    await R(src, { method: "POST", headers: kh, body: JSON.stringify({ i }) });
  }
  const at = await R(src, { headers: kh });
  const boundary = at.headers.get("stream-next-offset");
  const forkHeaders = { ...kh, "stream-forked-from": src, "stream-fork-offset": boundary };
  const f1 = await R(fork, { method: "PUT", headers: forkHeaders });
  check("fork created", f1.status === 201 || f1.status === 200, `status ${f1.status}`);
  const f2 = await R(fork, { method: "PUT", headers: forkHeaders });
  check("fork create replay is idempotent", f2.status === 200 || f2.status === 201, `status ${f2.status}`);
  const through = await R(fork, { headers: kh });
  const text = await through.text();
  check(
    "fork reads through the boundary",
    through.ok && text.includes('"i":0') && text.includes('"i":2'),
    text.slice(0, 200),
  );
  // A fork of the same name at a DIFFERENT boundary is a conflict, not
  // a silent re-point.
  const other = await R(fork, {
    method: "PUT",
    headers: { ...kh, "stream-forked-from": src, "stream-fork-offset": early },
  });
  check("conflicting fork refused", other.status === 409, `status ${other.status}`);
}

// --- 5. split coexistence ---------------------------------------------
// Drive a REAL auto-split, then check the two surfaces still agree:
// every product routing key reads back its own sequence, and the raw
// route shows the default key's view and nothing else.
{
  const name = `gate/split-${stamp}`;
  await P(name, {
    method: "PUT",
    headers: headers(),
    body: JSON.stringify({ format: { kind: "json" } }),
  });
  const keys = Array.from({ length: 24 }, (_, i) => `k${i}`);
  const perKey = 12;
  // The payload under test: a fixed, verifiable sequence per key.
  for (let round = 0; round < perKey; round++) {
    await Promise.all(
      keys.map((k) =>
        P(`${name}/records`, {
          method: "POST",
          headers: headers({ "prisma-routing-key": k }),
          body: JSON.stringify({ k, round }),
        }),
      ),
    );
  }
  // Default key gets its own records, through BOTH surfaces.
  await P(`${name}/records`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ via: "product-default" }),
  });
  const rawHeaders = { "stream-encryption-key": key, "content-type": "application/json" };
  if (token) rawHeaders["authorization"] = `Bearer ${token}`;
  await R(name, { method: "POST", headers: rawHeaders, body: JSON.stringify({ via: "raw" }) });

  // Now drive the collection hot until the scaler actually splits it.
  // The load rides SEPARATE keys so the sequences verified below stay
  // exactly as written.
  const t0 = Date.now();
  const hot = Array.from({ length: 32 }, (_, i) => `hot${i}`);
  let segments = 1;
  let rounds = 0;
  while (Date.now() - t0 < 90_000) {
    await Promise.all(
      hot.map((k) =>
        P(`${name}/records`, {
          method: "POST",
          headers: headers({ "prisma-routing-key": k }),
          body: JSON.stringify({ k, r: rounds }),
        }),
      ),
    );
    rounds++;
    if (rounds % 4 === 0) {
      const s = await json(
        await fetch(`${base}/v1/segments/${name}`, {
          headers: token ? { authorization: `Bearer ${token}` } : {},
        }),
      );
      segments = (s?.segments ?? []).filter((x) => x.live !== false).length || 1;
      if (segments > 1) break;
    }
  }
  check(
    "load drove a real split",
    segments > 1,
    `${segments} live segment(s) after ${Math.round((Date.now() - t0) / 1000)}s of load ` +
      `(${rounds * hot.length} records); deploy with SCALE_HOT_PCT=1 SCALE_HOT_EVALS=1 ` +
      `SCALE_EVAL_SECS=5 SCALE_RATE_WINDOW_SECS=10`,
  );
  let intact = 0;
  for (const k of keys) {
    const recs = await json(await P(`${name}/records?routingKey=${k}&limit=100`, { headers: headers() }));
    if (Array.isArray(recs) && recs.length === perKey && recs.every((r) => r.k === k)) intact++;
  }
  check(`every routing key reads back intact across the split`, intact === keys.length, `${intact}/${keys.length}`);

  const rawText = await (await R(name, { headers: rawHeaders })).text();
  const rawHasDefault = rawText.includes("product-default") && rawText.includes('"via":"raw"');
  const rawLeaksKeyed = keys.some((k) => rawText.includes(`"k":"${k}"`));
  check("raw route shows the default key's records", rawHasDefault, rawText.slice(0, 200));
  check("raw route shows ONLY the default key", !rawLeaksKeyed);
}

console.log(failures === 0 ? "FIELD GATE PASS" : `FIELD GATE FAIL (${failures})`);
process.exit(failures === 0 ? 0 : 1);
