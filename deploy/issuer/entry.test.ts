// The issuer's entry as Compute runs it: one bundled file, its inputs in
// the environment (public-beta issuer triage B2.1-B2.3). Run with
// `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { cleanup, pepperFile, readBundle, scratchDir, verifiedClaims } from "../cell-admin/test-rig";
import { CELL, cellState, customerSecret, type IssuerCell, issuerCell } from "./rig";

const children: ReturnType<typeof Bun.spawn>[] = [];
afterEach(() => {
  for (const c of children.splice(0)) c.kill();
  cleanup();
});

/// The entry built as the Composer build makes it, @prisma/compute left
/// out (KEEP_AWAKE=0 never loads it), into a directory of its own.
async function bundle(): Promise<string> {
  const dir = scratchDir();
  const built = await Bun.build({
    entrypoints: [join(import.meta.dir, "main.ts")],
    target: "bun",
    outdir: dir,
    naming: "issuer.js",
    external: ["@prisma/compute"],
  });
  expect([built.success, built.outputs.length]).toEqual([true, 1]);
  return join(dir, "issuer.js");
}

type Launched =
  | { child: ReturnType<typeof Bun.spawn>; origin: string; out: string[]; reader: ReadableStreamDefaultReader<Uint8Array> }
  | { child: ReturnType<typeof Bun.spawn>; code: number; err: string; out: string[] };

/// Start the bundle with `env` (and nothing else) on a free port; resolves
/// with its origin once it listens, or with its exit code and stderr.
async function launch(file: string, env: Record<string, string>, flags: string[] = []): Promise<Launched> {
  const child = Bun.spawn([process.execPath, ...flags, file], {
    env: { ...env, PORT: "0" },
    cwd: join(file, ".."),
    stdout: "pipe",
    stderr: "pipe",
  });
  children.push(child);
  const out: string[] = [];
  const reader = (child.stdout as ReadableStream<Uint8Array>).getReader();
  const decoder = new TextDecoder();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    out.push(decoder.decode(value));
    const port = /listening on :(\d+)/.exec(out.join(""))?.[1];
    if (port) return { child, origin: `http://127.0.0.1:${port}`, out, reader };
  }
  return { child, code: await child.exited, err: await new Response(child.stderr as ReadableStream).text(), out };
}

async function exchange(origin: string, secret: string): Promise<Response> {
  return fetch(`${origin}/v1/token/streams`, { method: "POST", headers: { authorization: `StreamsCredential ${secret}` } });
}

describe("the bundled entry", () => {
  test("is one file whose only import outside the runtime is the keep-awake guard", async () => {
    const file = await bundle();
    expect(readdirSync(join(file, ".."))).toEqual(["issuer.js"]);
    const text = readFileSync(file, "utf8");
    const specifiers = [...text.matchAll(/(?:from\s*|import\s*\(\s*)"([^"]+)"/g)].map((m) => m[1]);
    const outside = specifiers.filter((s) => !/^(node:|bun(:|$))/.test(s) && !["crypto", "fs", "path", "os"].includes(s));
    expect([...new Set(outside)]).toEqual(["@prisma/compute"]);
  });

  test("boots from its environment away from the repository, serves the exchange, and stops on SIGTERM", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const started = await launch(await bundle(), cell.env);
    if (!("origin" in started)) throw new Error(`the issuer did not start: ${started.err}`);
    expect(started.out.join("")).toContain(`streams-issuer: cell ${CELL}, issuer https://auth.prisma.io, kid streams-eddsa-`);
    const res = await exchange(started.origin, a.secret);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(verifiedClaims(readBundle(cell.state), body.accessToken)?.credential_id).toBe(a.id);
    expect((await fetch(`${started.origin}/healthz`)).status).toBe(200);
    started.child.kill("SIGTERM");
    expect(await started.child.exited).toBe(0);
    let rest = "";
    for (;;) {
      const { done, value } = await started.reader.read();
      if (done) break;
      rest += new TextDecoder().decode(value);
    }
    // The open minute's counts are written at the stop.
    expect(rest).toContain(`"kind":"exchanges"`);
    expect(rest).toContain(`"credential_id":"${a.id}"`);
  });

  test("writes the re-hashes it noted when it stops, so a redeploy loses none", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const v2 = pepperFile(cell.root, 0o600, 2);
    const peppers = `v2:${v2.key.toString("base64")},${readFileSync(cell.pepper, "utf8").trim()}`;
    const started = await launch(await bundle(), { ...cell.env, STREAMS_ISSUER_PEPPER: peppers });
    if (!("origin" in started)) throw new Error(`the issuer did not start: ${started.err}`);
    expect((await exchange(started.origin, a.secret)).status).toBe(200);
    expect(cellState(cell).credentials[a.id].hv).toBe(1);
    started.child.kill("SIGTERM");
    expect(await started.child.exited).toBe(0);
    expect(cellState(cell).credentials[a.id].hv).toBe(2);
  });

  test("pins the keep-awake guard's package exactly, and refuses to boot rather than fetch it when a build leaves it out", async () => {
    const pkg = JSON.parse(readFileSync(join(import.meta.dir, "package.json"), "utf8"));
    const version = pkg.dependencies["@prisma/compute"];
    expect(Object.keys(pkg.dependencies)).toEqual(["@prisma/compute"]);
    expect(version).toMatch(/^\d+\.\d+\.\d+(-[0-9A-Za-z.]+)?$/);
    const lock = readFileSync(join(import.meta.dir, "bun.lock"), "utf8");
    expect(lock).toContain(`"@prisma/compute": "${version}",`);
    expect(lock).toMatch(new RegExp(`"@prisma/compute": \\["@prisma/compute@${version.replaceAll(".", "\\.")}", "", \\{\\}, "sha512-`));
    // A build that left the import out, run as the service runs it (bun
    // --no-install): exit 1 at boot, and nothing fetched from npm.
    const cell = await issuerCell();
    const started = await launch(await bundle(), { ...cell.env, KEEP_AWAKE: "1" }, ["--no-install"]);
    expect(["code" in started && started.code, "err" in started && started.err]).toEqual([
      1, "streams-issuer: KEEP_AWAKE=1 needs @prisma/compute's KeepAwakeGuard in the build (set KEEP_AWAKE=0 to run without it)\n",
    ]);
  });

  test("refuses to boot with exit 1 and a message that quotes ids only", async () => {
    const cell = await issuerCell();
    const file = await bundle();
    const wrongIssuer = await launch(file, { ...cell.env, STREAMS_ISSUER_ID: "https://other.example.com" });
    expect(["code" in wrongIssuer && wrongIssuer.code, "err" in wrongIssuer && wrongIssuer.err]).toEqual([
      1, `streams-issuer: cell ${CELL} names issuer https://auth.prisma.io, not STREAMS_ISSUER_ID https://other.example.com\n`,
    ]);
    const { STREAMS_ISSUER_PEPPER: _, ...noPepper } = cell.env;
    const missing = await launch(file, noPepper);
    expect(["code" in missing && missing.code, "err" in missing && missing.err]).toEqual([
      1, "streams-issuer: STREAMS_ISSUER_PEPPER is required\n",
    ]);
  });
});

describe("the issuer's sources", () => {
  test("read the environment only in the entry, and only the names config.ts lists", () => {
    const dir = import.meta.dir;
    const sources = readdirSync(dir).filter((f) => f.endsWith(".ts") && !f.endsWith(".test.ts") && f !== "rig.ts");
    const uses = sources.flatMap((f) =>
      [...readFileSync(join(dir, f), "utf8").matchAll(/.*process\.env.*/g)].map((m) => `${f}: ${m[0].trim()}`),
    );
    expect(uses).toEqual([
      "main.ts: cfg = readConfig(Object.fromEntries(INPUTS.map((name) => [name, process.env[name]])));",
      "main.ts: for (const name of SECRET_INPUTS) delete process.env[name];",
    ]);
  });

  test("hold Compute's keep-awake guard for the process's life unless KEEP_AWAKE=0 (I10: awake by default)", () => {
    const main = readFileSync(join(import.meta.dir, "main.ts"), "utf8");
    expect(main).toContain("if (!cfg.keepAwake) return;");
    expect(main).toContain("guard = new KeepAwakeGuard();");
    expect(readFileSync(join(import.meta.dir, "config.ts"), "utf8")).toContain('export const KEEP_AWAKE_DEFAULT = "1";');
  });
});
