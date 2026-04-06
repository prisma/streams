import { randomBytes } from "node:crypto";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

type ApiName =
  | "bun-file-arraybuffer"
  | "bun-file-bytes"
  | "bun-serve-fetch"
  | "bun-spawn-true"
  | "bun-sqlite-query"
  | "bun-s3-write-delete"
  | "bun-s3-write-file-delete"
  | "bun-hash-xxhash3"
  | "bun-hash-xxhash64"
  | "bun-hash-xxhash32"
  | "bun-hash-wyhash"
  | "bun-sleep"
  | "bun-jsc-heapstats"
  | "bun-jsc-memoryusage";

const API_NAMES: ApiName[] = [
  "bun-file-arraybuffer",
  "bun-file-bytes",
  "bun-serve-fetch",
  "bun-spawn-true",
  "bun-sqlite-query",
  "bun-s3-write-delete",
  "bun-s3-write-file-delete",
  "bun-hash-xxhash3",
  "bun-hash-xxhash64",
  "bun-hash-xxhash32",
  "bun-hash-wyhash",
  "bun-sleep",
  "bun-jsc-heapstats",
  "bun-jsc-memoryusage",
];

function usage(): never {
  process.stderr.write(
    [
      "usage: bun run experiments/bench/bun_api_loop.ts --api <name> [--iterations N] [--bytes N] [--yield-every N]",
      `supported apis: ${API_NAMES.join(", ")}`,
    ].join("\n") + "\n"
  );
  process.exit(1);
}

function argValue(args: string[], flag: string): string | null {
  const idx = args.indexOf(flag);
  if (idx === -1) return null;
  return args[idx + 1] ?? null;
}

function parseIntArg(args: string[], flag: string, fallback: number): number {
  const raw = argValue(args, flag);
  if (raw == null) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || !Number.isInteger(value) || value <= 0) usage();
  return value;
}

function parseApiArg(args: string[]): ApiName {
  const raw = (argValue(args, "--api") ?? "").trim() as ApiName;
  if (!API_NAMES.includes(raw)) usage();
  return raw;
}

async function maybeYield(iteration: number, every: number): Promise<void> {
  if (iteration > 0 && iteration % every === 0) await Bun.sleep(0);
}

function prepareTempFile(byteLength: number): string {
  const dir = mkdtempSync(join(tmpdir(), "bun-api-loop-"));
  const path = join(dir, `payload-${byteLength}.bin`);
  writeFileSync(path, randomBytes(byteLength));
  return path;
}

function requireS3Env(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`missing required env ${name}`);
  }
  return value;
}

async function run(): Promise<void> {
  const args = process.argv.slice(2);
  const api = parseApiArg(args);
  const iterations = parseIntArg(args, "--iterations", 1_000_000);
  const bytes = parseIntArg(args, "--bytes", 256 * 1024);
  const yieldEvery = parseIntArg(args, "--yield-every", 1000);
  let sink = 0n;

  if (api === "bun-file-arraybuffer" || api === "bun-file-bytes") {
    const path = prepareTempFile(bytes);
    const file = Bun.file(path);
    for (let i = 1; i <= iterations; i++) {
      if (api === "bun-file-arraybuffer") {
        const ab = await file.arrayBuffer();
        sink ^= BigInt(new Uint8Array(ab)[0] ?? 0);
      } else {
        const value = await file.bytes();
        sink ^= BigInt(value[0] ?? 0);
      }
      await maybeYield(i, yieldEvery);
    }
  } else if (api === "bun-serve-fetch") {
    const server = Bun.serve({
      port: 0,
      fetch() {
        return new Response("ok");
      },
    });
    try {
      const url = `http://127.0.0.1:${server.port}`;
      for (let i = 1; i <= iterations; i++) {
        const response = await fetch(url);
        const body = new Uint8Array(await response.arrayBuffer());
        sink ^= BigInt(body[0] ?? 0);
        await maybeYield(i, yieldEvery);
      }
    } finally {
      server.stop(true);
    }
  } else if (api === "bun-spawn-true") {
    for (let i = 1; i <= iterations; i++) {
      const proc = Bun.spawn({
        cmd: ["/usr/bin/true"],
        stdout: "ignore",
        stderr: "ignore",
      });
      const exitCode = await proc.exited;
      sink ^= BigInt(exitCode);
      await maybeYield(i, yieldEvery);
    }
  } else if (api === "bun-sqlite-query") {
    const { Database } = await import("bun:sqlite");
    const db = new Database(":memory:");
    db.exec("create table items (id integer primary key autoincrement, payload blob not null)");
    const insert = db.query("insert into items (payload) values (?)");
    const latest = db.query("select length(payload) as len from items order by id desc limit 1");
    const cleanup = db.query("delete from items where id <= ?");
    const payload = randomBytes(bytes);
    try {
      for (let i = 1; i <= iterations; i++) {
        insert.run(payload);
        const row = latest.get() as { len?: number } | null;
        sink ^= BigInt(row?.len ?? 0);
        if (i % 1024 === 0) cleanup.run(i - 512);
        await maybeYield(i, yieldEvery);
      }
    } finally {
      db.close();
    }
  } else if (api === "bun-s3-write-delete" || api === "bun-s3-write-file-delete") {
    const accountId = requireS3Env("DURABLE_STREAMS_R2_ACCOUNT_ID");
    const bucket = requireS3Env("DURABLE_STREAMS_R2_BUCKET");
    const accessKeyId = requireS3Env("DURABLE_STREAMS_R2_ACCESS_KEY_ID");
    const secretAccessKey = requireS3Env("DURABLE_STREAMS_R2_SECRET_ACCESS_KEY");
    const client = new Bun.S3Client({
      bucket,
      accessKeyId,
      secretAccessKey,
      region: "auto",
      endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
    });
    const payload = randomBytes(bytes);
    const tempPath = api === "bun-s3-write-file-delete" ? prepareTempFile(bytes) : null;
    const prefix = `bun-api-loop/${api}/${Date.now()}-${process.pid}`;
    for (let i = 1; i <= iterations; i++) {
      const file = client.file(`${prefix}/${i.toString().padStart(12, "0")}.bin`);
      if (api === "bun-s3-write-file-delete") {
        await file.write(Bun.file(tempPath!));
      } else {
        await file.write(payload);
      }
      const stat = await file.stat();
      sink ^= BigInt(stat.size);
      await file.delete();
      await maybeYield(i, yieldEvery);
    }
  } else if (api === "bun-hash-xxhash3" || api === "bun-hash-xxhash64" || api === "bun-hash-xxhash32" || api === "bun-hash-wyhash") {
    const payload = randomBytes(bytes);
    for (let i = 1; i <= iterations; i++) {
      const value =
        api === "bun-hash-xxhash3"
          ? Bun.hash.xxHash3(payload)
          : api === "bun-hash-xxhash64"
            ? Bun.hash.xxHash64(payload)
            : api === "bun-hash-xxhash32"
              ? Bun.hash.xxHash32(payload)
              : Bun.hash.wyhash(payload);
      sink ^= BigInt(value as number | bigint);
      await maybeYield(i, yieldEvery);
    }
  } else if (api === "bun-sleep") {
    for (let i = 1; i <= iterations; i++) {
      await Bun.sleep(0);
      sink ^= BigInt(i & 1);
      await maybeYield(i, yieldEvery);
    }
  } else if (api === "bun-jsc-heapstats" || api === "bun-jsc-memoryusage") {
    const jsc = await import("bun:jsc");
    for (let i = 1; i <= iterations; i++) {
      const value = api === "bun-jsc-heapstats" ? jsc.heapStats() : jsc.memoryUsage();
      sink ^= BigInt(JSON.stringify(value).length);
      await maybeYield(i, yieldEvery);
    }
  } else {
    usage();
  }

  process.stdout.write(
    `[bun-api-loop] completed api=${api} iterations=${iterations} bytes=${bytes} ts=${new Date().toISOString()} sink=${sink.toString()}\n`
  );
  await new Promise<void>(() => {});
}

if (import.meta.main) {
  run().catch((error) => {
    // eslint-disable-next-line no-console
    console.error(error);
    process.exitCode = 1;
  });
}
