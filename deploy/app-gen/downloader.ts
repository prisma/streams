// Direct S3 read via instance credentials — presigned URLs proved fragile
// (platform Bun-canary fetch broke SigV4 query encoding, 2026-07-20) and
// they expire. Chunked ranges with per-attempt timeouts survive the
// egress path's silent flow kills (2026-07-19 zombie).
import { S3Client } from "bun";
export async function downloadBinary(key: string, dest: string, log: (s: string) => void) {
  const env = process.env;
  // S3_* ONLY: project-scope env merge leaks the servers' SLATE_S3_* (data
  // bucket) vars into this service's snapshot, and that bucket has no bin/.
  const c = new S3Client({
    endpoint: env.S3_ENDPOINT!,
    bucket: env.S3_BUCKET!,
    region: env.S3_REGION ?? "auto",
    accessKeyId: env.S3_ACCESS_KEY_ID!,
    secretAccessKey: env.S3_SECRET_ACCESS_KEY!,
  });
  const f = c.file(key);
  const total = (await f.stat()).size;
  if (!Number.isFinite(total) || total < 1_000_000) throw new Error(`bad size ${total}`);
  log(`binary ${key} size ${total}`);
  const CHUNK = 4 * 1024 * 1024;
  const parts = Math.ceil(total / CHUNK);
  const bufs: Uint8Array[] = new Array(parts);
  let done = 0;
  const one = async (i: number) => {
    const start = i * CHUNK, end = Math.min(total, start + CHUNK);
    for (let attempt = 1; ; attempt++) {
      try {
        const b = new Uint8Array(await Promise.race([
          f.slice(start, end).arrayBuffer(),
          new Promise<never>((_, rej) => setTimeout(() => rej(new Error("chunk timeout")), 45_000)),
        ]) as ArrayBuffer);
        if (b.length !== end - start) throw new Error(`short ${b.length}`);
        bufs[i] = b; done++;
        log(`chunk ${i + 1}/${parts} ok (attempt ${attempt}) [${done}/${parts}]`);
        return;
      } catch (e: any) {
        log(`chunk ${i + 1}/${parts} attempt ${attempt} failed: ${e?.message ?? e}`);
        if (attempt >= 5) throw e;
        await new Promise((r2) => setTimeout(r2, 500 * attempt));
      }
    }
  };
  const CONC = 5;
  for (let base = 0; base < parts; base += CONC)
    await Promise.all(Array.from({ length: Math.min(CONC, parts - base) }, (_, k) => one(base + k)));
  const out = new Uint8Array(total);
  bufs.forEach((b, i) => out.set(b, i * CHUNK));
  await Bun.write(dest, out);
  const hd = new Uint8Array(await Bun.file(dest).slice(0, 20).arrayBuffer());
  const machine = hd[18] | (hd[19] << 8);
  log(`assembled ${Bun.file(dest).size} bytes e_machine=${machine}`);
  if (!(hd[0] === 0x7f && hd[1] === 0x45 && hd[2] === 0x4c && hd[3] === 0x46)) throw new Error("not ELF");
  if (machine !== 0x3e) throw new Error(`machine ${machine} != x86_64`);
}

// Small-object variant for campaign side-files (feeds bundle, token
// map): single GET, retries, and NO minimum-size gate — that gate is
// binary-corruption armor and would refuse a 500 KB JSON document.
export async function downloadFile(key: string, dest: string, log: (s: string) => void) {
  const env = process.env;
  const c = new S3Client({
    endpoint: env.S3_ENDPOINT!,
    bucket: env.S3_BUCKET!,
    region: env.S3_REGION ?? "auto",
    accessKeyId: env.S3_ACCESS_KEY_ID!,
    secretAccessKey: env.S3_SECRET_ACCESS_KEY!,
  });
  for (let attempt = 1; ; attempt++) {
    try {
      const b = new Uint8Array(await Promise.race([
        c.file(key).arrayBuffer(),
        new Promise<never>((_, rej) => setTimeout(() => rej(new Error("timeout")), 45_000)),
      ]) as ArrayBuffer);
      if (b.length === 0) throw new Error("empty object");
      await Bun.write(dest, b);
      log(`file ${key} -> ${dest} (${b.length} bytes)`);
      return;
    } catch (e: any) {
      log(`file ${key} attempt ${attempt} failed: ${e?.message ?? e}`);
      if (attempt >= 5) throw e;
      await new Promise((r) => setTimeout(r, 500 * attempt));
    }
  }
}
