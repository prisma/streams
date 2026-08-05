# Running Prisma Streams standalone with Prisma Composer

This guide deploys **Prisma Streams** (the `streams-slate` server in this
repo) as a standalone service on **Prisma Compute**, backed by a
**Prisma Bucket**, together with an app that talks to it — all wired by
**Prisma Composer**. It is written to be executed start-to-finish by a
coding agent with no prior context: every file is complete and verbatim,
every command was run, and every failure mode we hit is in the traps
table at the end.

Verified: 2026-08-05 against `@prisma/composer` 0.6.0 /
`@prisma/composer-prisma-cloud` 0.6.0, streams-slate commit `3b3980e7`
(`prisma-composer dev` bring-up, full append/read round trip, and the
low-latency `deliver: "applied"` subscription).

## What you are building

```
prisma-composer (app.mjs — the wiring)
│
├─ bucket "streams-data"          Prisma Bucket (S3-compatible object storage)
│
├─ compute "streams"              the Prisma Streams server
│    binary: streams-slate        (Rust; WAL + shards + history live in the bucket)
│    exposes port: streams        (kind "streams" — the same contract the
│                                  first-party streams() module exposes)
│
└─ compute "consumer"             your app
     dep: durableStreams()      → hydrates to a client for the streams service,
                                  with the bearer key minted automatically
```

Two API surfaces come out of one server, and this guide uses both:

| Surface | Route | Client | Encrypted with |
|---|---|---|---|
| Raw Durable Streams protocol | `/v1/stream/{name}` | Composer's built-in stream handles (`durableStreams()`), any DS client | the server's default key (`--conformance-default-key`) |
| Prisma product surface | `/v1/streams/{name}` | `@prisma/streams` SDK (this repo, `sdk/`) | a key the client sends per request |

Both address the same underlying streams when the keys line up (the raw
route is the default-key view of the product stream of the same name).
The product surface is the full API: routing keys, consumer groups,
signed cursors, subscriptions — including the low-latency
`deliver: "applied"` mode covered in its own section below.

## Prerequisites

- Node.js ≥ 20 and npm.
- **bun** on PATH — `prisma-composer dev` runs services under bun.
- Rust toolchain (to build `streams-slate` from this repo).
  - For LOCAL dev: a host build is fine.
  - For a REAL Prisma Compute deploy: the fleet is **x86_64** — build
    `x86_64-unknown-linux-musl` with `cargo zigbuild` and verify the ELF
    machine type. An aarch64 binary deploys "successfully" and then
    crash-loops as a silent zombie (ENOEXEC).
- For `prisma-composer deploy` (not needed for `dev`): Prisma platform
  credentials. `dev` is fully local and credential-free (compute
  emulator on :4300, buckets emulator on :4301).

## 1. Project skeleton

Create a fresh directory with exactly these files. Notes that matter:
`"type": "module"` in package.json is REQUIRED (the generated runner is
ESM), and `prisma-composer.config.ts` must exist with exactly these
extensions.

### package.json

```json
{
  "name": "streams-on-composer",
  "private": true,
  "type": "module",
  "dependencies": {
    "@prisma/composer": "^0.6.0",
    "@prisma/composer-prisma-cloud": "^0.6.0",
    "@prisma/streams": "file:./prisma-streams-0.1.0.tgz",
    "arktype": "^2.2.3"
  }
}
```

`prisma-streams-0.1.0.tgz` is the product SDK from this repo:

```bash
cd <streams-repo>/sdk && npm run build && npm pack
# copy sdk/prisma-streams-0.1.0.tgz into the project root
```

### prisma-composer.config.ts

```ts
import { defineConfig } from "@prisma/composer/config";
import { prismaCloud, prismaState } from "@prisma/composer-prisma-cloud/control";
import { nodeBuild } from "@prisma/composer/node/control";

export default defineConfig({
  extensions: [prismaCloud(), nodeBuild()],
  state: prismaState(),
});
```

### app.mjs — the application root

```js
// The application root: one bucket, the Streams server on it, and the
// consumer wired to the server's `streams` port.
import { module } from "@prisma/composer";
import { bucket } from "@prisma/composer-prisma-cloud";
import streamsServer from "./streams-server/service.mjs";
import consumerApp from "./consumer/service.mjs";

export default module("app", {}, ({ provision }) => {
  const store = provision(bucket({ name: "streams-data" }), { id: "store" });
  const streams = provision(streamsServer, {
    id: "streams",
    deps: { store },
  });
  provision(consumerApp, {
    id: "consumer",
    deps: { streams: streams.streams },
  });
  return {};
});
```

### streams-server/service.mjs

```js
// The Prisma Streams SERVER as a Composer compute service: our Rust
// binary behind the same 'streams' contract the first-party module
// exposes, so any durableStreams() consumer wires to it unchanged.
import { compute } from "@prisma/composer-prisma-cloud";
import { bucket } from "@prisma/composer-prisma-cloud";
import { streamsContract } from "@prisma/composer-prisma-cloud/streams";
import node from "@prisma/composer/node";

export function streamsServer() {
  return compute({
    name: "streams",
    deps: { store: bucket() },
    build: node({
      module: new URL("./service.mjs", import.meta.url).href,
      dir: ".",
      entry: "./entry.mjs",
    }),
    expose: { streams: streamsContract({}) },
  });
}
export default streamsServer();
```

### streams-server/entry.mjs

```js
// Boot: hydrate the bucket dep, map it to the server's env, spawn the
// streams-slate binary on the platform port, supervise it.
import { spawn } from "node:child_process";
import { copyFileSync, chmodSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import streamsServer from "./service.mjs";

const svc = streamsServer;
const { store } = svc.load();
const port = svc.port();

const here = dirname(fileURLToPath(import.meta.url));
const shipped = join(here, "streams-slate");
if (!existsSync(shipped)) {
  throw new Error(
    "streams-slate binary missing next to entry.mjs — build it and copy it into streams-server/ before deploying",
  );
}
// Always copy fresh out of the (possibly read-only) bundle dir.
const bin = "/tmp/streams-slate";
copyFileSync(shipped, bin);
chmodSync(bin, 0o755);

// The bearer key the platform minted for this module's `streams` port.
// The runtime re-stashes it under the ADDRESS-FREE name
// COMPOSER_STREAMS_API_KEY during `run()` (ADR-0031) — read that, with
// the raw name as a fallback for direct/manual runs.
const apiKeyRaw =
  process.env["COMPOSER_STREAMS_API_KEY"] ?? process.env["STREAMS_API_KEY"];
let apiKey;
try { apiKey = JSON.parse(apiKeyRaw ?? ""); } catch { apiKey = apiKeyRaw; }
if (typeof apiKey !== "string" || apiKey.length === 0) {
  throw new Error(
    "streams: no bearer key provisioned — wire a durableStreams() consumer to this module's `streams` port",
  );
}

// The stream encryption key for the DEFAULT (raw-protocol) view. The
// Composer stream handles speak the raw Durable Streams protocol and
// carry no encryption header, so the server needs a default key.
// Product-SDK callers send the same key explicitly. Override with a
// secret of your own in production (32 random bytes, base64).
const DEFAULT_KEY =
  process.env["STREAMS_DEFAULT_KEY"] ?? "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";

const env = {
  ...process.env,
  SLATE_S3_ENDPOINT: store.url,
  SLATE_S3_BUCKET: store.bucket,
  SLATE_S3_REGION: "auto",
  SLATE_S3_ACCESS_KEY_ID: store.accessKeyId,
  SLATE_S3_SECRET_ACCESS_KEY: store.secretAccessKey,
  AUTH_TOKEN: apiKey,
  // Commit pipeline posture (validated defaults for object-store WALs).
  WAL_GROUP_COMMIT: "1",
  WAL_FLUSH_GAP_MS: "10",
  FLUSH_INTERVAL_MS: "25",
  // Mandatory with the current SlateDB pin: max_unflushed_bytes must
  // exceed l0_sst_size_bytes (32 MiB) or every shard open fails with
  // "invalid configuration".
  MAX_UNFLUSHED_BYTES: "67108864",
};
const args = [
  "--listen", `0.0.0.0:${port}`,
  "--initial-shards", "4",
  "--path-prefix", "composer",
  "--conformance-default-key", DEFAULT_KEY,
];
console.log(`streams: starting streams-slate on :${port} (bucket ${store.bucket})`);
const child = spawn(bin, args, { env, stdio: "inherit" });
child.on("exit", (code, sig) => {
  console.error(`streams-slate exited (code=${code} sig=${sig})`);
  process.exit(code ?? 1);
});
```

### The server binary

Build `streams-slate` and place it next to the entry:

```bash
# local dev (host build):
cd <streams-repo> && cargo build --release
cp target/release/streams-slate <project>/streams-server/streams-slate
```

```bash
# real Prisma Compute deploy (x86_64-musl — mandatory):
cd <streams-repo> && cargo zigbuild --release --target x86_64-unknown-linux-musl
cp target/x86_64-unknown-linux-musl/release/streams-slate <project>/streams-server/streams-slate
# verify e_machine == 0x3e (x86-64): prints "3e00"
xxd -s 18 -l 2 -p <project>/streams-server/streams-slate
```

### consumer/service.mjs

```js
// The demo app: a tiny HTTP service that talks to Prisma Streams
// through its durableStreams() dependency. The BARE form (no contract)
// hydrates to a StreamsClient, which exposes the transport itself
// (base URL + bearer header) — that lets the entry use the raw stream
// handles AND the @prisma/streams product SDK over the same binding.
import { compute } from "@prisma/composer-prisma-cloud";
import { durableStreams } from "@prisma/composer-prisma-cloud/streams";
import node from "@prisma/composer/node";

export function consumerApp() {
  return compute({
    name: "consumer",
    deps: { streams: durableStreams() },
    build: node({
      module: new URL("./service.mjs", import.meta.url).href,
      dir: ".",
      entry: "./entry.mjs",
    }),
  });
}
export default consumerApp();
```

### consumer/entry.mjs

```js
import { createServer } from "node:http";
import consumerApp from "./service.mjs";
import { StreamsClient as ProductClient } from "@prisma/streams";

const svc = consumerApp;
// Bare durableStreams() hydrates to the Composer StreamsClient: raw
// protocol handles via .stream(name), plus the transport config this
// entry reuses for the product SDK.
const { streams } = svc.load();
const port = svc.port();

// Same key the server uses for the raw default view (entry.mjs of the
// streams module) — both surfaces then address the same stream bytes.
const ENCRYPTION_KEY =
  process.env["STREAMS_DEFAULT_KEY"] ?? "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";

const jobs = streams.stream("jobs"); // raw-protocol handle (ensure-creates)

// Product SDK over the same transport: streams.base is the service
// origin, streams.headers.authorization is "Bearer <apiKey>".
const token = streams.headers.authorization.replace(/^Bearer /, "");
const product = new ProductClient({ url: streams.base, token });
const jobsProduct = product.stream("jobs", { encryptionKey: ENCRYPTION_KEY });

// Low-latency watcher: subscribe with deliver:"applied" so events
// arrive as soon as the server ACCEPTS them — before they are fully
// persisted to object storage. The crash trade-off is documented in
// the guide; for a "latest event" cache it is exactly right.
let latest = null;
let latestSeenAt = 0;
async function watchLatest() {
  for (;;) {
    try {
      // Idempotent create so the product subscribe below never races
      // the stream's existence.
      await streams.create("jobs");
      for await (const ev of jobsProduct.subscribe({
        from: "now",
        deliver: "applied",
      })) {
        latest = ev;
        latestSeenAt = Date.now();
      }
    } catch (e) {
      // Stream may not exist yet, or the server is restarting: retry.
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}
watchLatest();

const server = createServer(async (req, res) => {
  try {
    if (req.method === "POST" && req.url === "/jobs") {
      let body = "";
      for await (const c of req) body += c;
      await jobs.append(JSON.parse(body || "{}"));
      res.writeHead(202).end("queued\n");
    } else if (req.method === "GET" && req.url === "/jobs") {
      const { events } = await jobs.read();
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(events) + "\n");
    } else if (req.method === "GET" && req.url === "/jobs/latest") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ latest, seenAtMs: latestSeenAt }) + "\n");
    } else if (req.url === "/health") {
      res.writeHead(200).end("ok\n");
    } else {
      res.writeHead(404).end();
    }
  } catch (e) {
    res.writeHead(500).end(String(e?.message ?? e) + "\n");
  }
});
server.listen(port, "0.0.0.0", () =>
  console.log(`consumer listening on :${port}`),
);
```

## 2. Run it locally

```bash
npm install
npx prisma-composer dev app.mjs
```

Expected: the compute emulator comes up on :4300 and the buckets
emulator on :4301, a plan of ~21 resources is created (services, the
bucket, minted keys, `COMPOSER_*` config vars), and it ends with:

```
Done: 42 succeeded
[dev] ready:
[dev] consumer  http://localhost:3001
[dev] streams  http://localhost:3000
```

Verify the whole path (these exact commands and outputs are from the
verification run):

```bash
curl -s http://localhost:3001/health
# ok
```

```bash
curl -s -X POST http://localhost:3001/jobs -H 'content-type: application/json' -d '{"task":"resize-image","id":1}'
# queued
```

```bash
curl -s http://localhost:3001/jobs
# [{"id":1,"task":"resize-image"}]
```

```bash
curl -s -X POST http://localhost:3001/jobs -H 'content-type: application/json' -d '{"task":"fresh","id":3}'; sleep 1; curl -s http://localhost:3001/jobs/latest
# queued
# {"latest":{"id":3,"task":"fresh"},"seenAtMs":...}   <- served by the deliver:"applied" subscriber
```

The streams service itself enforces bearer auth (`curl
http://localhost:3000/v1/streams` → 401 without the minted key — that is
correct).

Service logs (dev): `npx prisma-composer log app.mjs`, or read the
emulator's files directly at
`~/.prisma-composer/emulators/compute/logs/<app>/<service>.log`.

## 3. Deploy to Prisma Cloud

Same artifacts, different command:

```bash
npx prisma-composer deploy app.mjs
```

- Requires Prisma platform credentials; `dev` does not.
- The binary MUST be the x86_64-musl build (see above). This is the
  single most common way to ship a broken deploy from an ARM Mac.
- `npx prisma-composer log app.mjs` tails the deployed services;
  `npx prisma-composer destroy app.mjs` tears everything down.

What Composer does for you on deploy: provisions the bucket, mints S3
credentials for it, deploys both compute services, mints the streams
bearer key (because the consumer declares `durableStreams()` against the
streams port), and injects everything as env/config. The server entry
maps the hydrated bucket to `SLATE_S3_*` env and the minted key to
`AUTH_TOKEN` — nothing is hand-configured.

## 4. Low-latency subscriptions: `deliver: "applied"`

Prisma Streams supports a subscription mode where you get events
**before they are fully persisted** to object storage. Use this for low
latency.

By default, every read and subscription is **durable**: a record is
visible only after the server's write-ahead log reaches object storage.
On S3-class storage that durability barrier is a PUT round trip —
typically tens of milliseconds. In **applied** mode the server serves
the live tail at its *applied* watermark instead: a record becomes
visible the moment the server has accepted and ordered it, one storage
round trip earlier. The wake path is also earlier — an applied-mode
long-poll is woken by the write's apply, not by its flush.

```js
// SDK: identical iterator, one option.
for await (const ev of stream.subscribe({ from: "now", deliver: "applied" })) {
  render(ev); // arrives before the record is durable
}
```

`read()` accepts the same option and then reports two extra fields:
`pendingFrom` (index into the page from which records were not yet
durable when served) and `durableCursor` (see below).

### The contract you are opting into

- **Ordering and offsets are unchanged.** Applied mode never reorders;
  it only moves the visibility line forward within the live tail.
- **The crash window.** If the server crashes after serving you a
  record but before its WAL flush, that record is gone — and because
  its producer was never acked either, the producer will retry, and the
  same offsets may be reused with different content. Everything
  downstream must treat not-yet-durable records as provisional.
- **Cursor discipline (the SDK does this for you).** Applied-mode
  responses carry TWO cursors: the session cursor
  (`Prisma-Next-Cursor`) which may point into the provisional suffix,
  and `Prisma-Durable-Cursor`, clamped to the durable frontier. Only
  the durable cursor is safe to persist or resume from after a
  disconnect. The SDK's `subscribe()` tracks both, and on a transport
  error — or on the server's `409 cursor_beyond_tail` refusal of a
  stale session cursor — silently rewinds to the durable cursor and
  continues, re-delivering the un-durable suffix rather than skipping
  or stalling.
- **Everything else stays durable.** Appends still ack only at
  durability. Consumer groups, watches, and the raw `/v1/stream/{name}`
  surface are untouched by this mode. `deliver=applied` is rejected on
  SSE connections and forked streams (typed 400s).

**Use it for:** dashboards, presence, progress feeds, "latest value"
caches (the consumer's `/jobs/latest` above), game/collab fan-out —
anywhere a few lost-and-retried events during a rare crash are
invisible. **Do not use it** to trigger side effects you cannot undo:
for that, durable mode (the default) or consumer groups are the tool.

### Wire detail (non-JS clients)

| Item | Value |
|---|---|
| Request | `GET /v1/streams/{name}/records?deliver=applied` and `.../records:long-poll?...&deliver=applied` |
| Provisional marker | `Prisma-Pending-From: <index>` — records from this array index on were not yet durable when served |
| Resume cursor | `Prisma-Durable-Cursor: <cursor>` — persist THIS, not `Prisma-Next-Cursor` |
| Stale session cursor | `409` with code `cursor_beyond_tail` — resume from the durable cursor |
| Invalid values | `400 invalid_deliver`; `400 deliver_sse_unsupported`; `400 deliver_unsupported_fork` |

## 5. Traps

Every entry below was hit for real while building this guide.

| Symptom | Cause → fix |
|---|---|
| `Cannot use import statement outside a module` from the generated runner | package.json missing `"type": "module"` |
| `No extension "@prisma/composer/node" is configured` | `nodeBuild()` missing from `prisma-composer.config.ts` extensions |
| Build resolves a bogus path for your service dir | `node()`'s `dir` is RELATIVE to `dirname(module)` — use `dir: "."`, never a `file://` URL |
| streams service exits instantly; log says `no bearer key provisioned` | The minted key arrives as the ADDRESS-FREE env `COMPOSER_STREAMS_API_KEY` (re-stashed by the runtime during `run()`); the address-scoped `COMPOSER_<module>_STREAMS_API_KEY` you see in emulator state is not what entries read. Also check something actually declares `durableStreams()` against the port — no consumer, no key. |
| Every shard open fails: `max_unflushed_bytes (16777216) must be greater than l0_sst_size_bytes (33554432)` | The SlateDB pin validates this pair. Set `MAX_UNFLUSHED_BYTES=67108864` (the entry above does). |
| `[emulator] held after 5 consecutive fast exits — send a new deployment to resume` | The service crash-looped; fix the cause, then redeploy (`dev` re-run). Logs: `~/.prisma-composer/emulators/compute/logs/<app>/<service>.log` |
| Deployed service is a zombie: deploy green, every request hangs/resets | aarch64 binary on the x86_64 Compute fleet (ENOEXEC crash-loop). Build `x86_64-unknown-linux-musl` via `cargo zigbuild`; verify with `xxd -s 18 -l 2` = `3e00`. |
| SSE subscriptions don't work through Compute ingress | Known platform limitation (PRO-218). The SDK's `subscribe()` long-polls precisely for this reason — and `deliver: "applied"` rides long-poll, so low latency works behind the ingress. |
| Raw-protocol handles fail with key errors | The Composer stream handles speak the raw protocol with NO encryption header — the server must run with `--conformance-default-key` (the entry above always sets one; override `STREAMS_DEFAULT_KEY` with your own secret in production). |
| Product SDK sees different data than the raw handles | Use the SAME key for the SDK's `encryptionKey` as the server's default key; the raw route is the default-key view of the product stream of the same name. |

## 6. Cleanup

```bash
npx prisma-composer destroy app.mjs
```

Dev-mode state lives under the project's `.prisma-composer/` +
`.alchemy/` directories and `~/.prisma-composer/emulators/`; deleting
them resets local state completely.
