// The interim issuer's Composer service (public-beta issuer triage B6): an
// app of its own in deploy/composer-issuer, deployed into a Compute project
// of its own (owner answer I1 (a) of 2026-10-12), whose entry runs this
// directory's issuer on the service's input. Run with
// `bun test ./deploy/issuer`.
//
// The binding tests read the app's sources as text, so they run without
// deploy/composer-issuer's packages, as CI's bun step does. The bundled
// entry's tests need those packages and are opt-in:
//
//   (cd deploy/composer-issuer && bun install --frozen-lockfile)
//   ISSUER_COMPOSER_E2E=1 bun test ./deploy/issuer/composer.test.ts
import { afterEach, describe, expect, test } from "bun:test";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { join } from "node:path";
import { cleanup, readBundle, scratchDir, verifiedClaims } from "../cell-admin/test-rig";
import { INPUTS, readConfig, SECRET_INPUTS } from "./config";
import { CELL, customerSecret, exchange, type IssuerCell, issuerCell, launch, restOf, stopLaunched } from "./rig";

afterEach(() => {
  stopLaunched();
  cleanup();
});

const DEPLOY = join(import.meta.dir, "..");
const read = (path: string) => readFileSync(join(DEPLOY, path), "utf8");

/// The platform's own names, which no binding carries: Compute's port, and
/// the keep-awake flag, left at its default (I10: awake).
const PLATFORM_NAMES = ["PORT", "KEEP_AWAKE"];
/// The bucket key ids: no secrets themselves, but each half of a key pair
/// travels as the platform hands the pair over.
const KEY_IDS = ["STREAMS_CONTROL_S3_ACCESS_KEY_ID", "STREAMS_FEEDS_S3_ACCESS_KEY_ID"];

type Binding = { field: string; kind: "envSecret" | "envParam"; name: string };

/// app.ts's input binding, line by line.
function bindings(): Binding[] {
  return [...read("composer-issuer/app.ts").matchAll(/^\s*(\w+): (envSecret|envParam)\("([A-Z0-9_]+)"\),$/gm)].map(
    (m) => ({ field: m[1], kind: m[2] as Binding["kind"], name: m[3] }),
  );
}

/// service.ts's input schema: each field, whether it may be absent, and its type.
function schemaFields(): { field: string; optional: boolean; type: string }[] {
  return [...read("composer-issuer/issuer/service.ts").matchAll(/^\s*"?(\w+)(\??)"?: (secretString\(\)|"string"),$/gm)].map(
    (m) => ({ field: m[1], optional: m[2] === "?", type: m[3] }),
  );
}

const sorted = (names: Iterable<string>) => [...names].sort();
/// A source's lines but its comments.
const code = (path: string) => read(path).split("\n").filter((l) => !/^\s*\/\//.test(l));

describe("the issuer's Composer app", () => {
  test("binds every input config.ts reads, under its own name: the secrets and the key ids as envSecret, every other as envParam", () => {
    const bound = bindings();
    expect(bound.filter((b) => b.field !== b.name)).toEqual([]);
    const secrets = bound.filter((b) => b.kind === "envSecret").map((b) => b.name);
    const params = bound.filter((b) => b.kind === "envParam").map((b) => b.name);
    expect(sorted(secrets)).toEqual(sorted([...SECRET_INPUTS, ...KEY_IDS]));
    expect(sorted(params)).toEqual(sorted(INPUTS.filter((n) => !secrets.includes(n) && !PLATFORM_NAMES.includes(n))));
    expect(new Set(bound.map((b) => b.name)).size).toBe(bound.length);
  });

  test("binds nothing else: no generated value (D3), no Composer port, no other service's origin", () => {
    const app = code("composer-issuer/app.ts").join("\n");
    for (const text of [app, code("composer-issuer/issuer/service.ts").join("\n")]) {
      expect(text.match(/\b(generatedParam|serviceOrigin|paramSource|secretSource|expose)\b/g)).toBeNull();
    }
    // Every envSecret or envParam call is one of the lines read above.
    expect(app.match(/\b(envSecret|envParam)\(/g)?.length).toBe(bindings().length);
  });

  test("declares each bound field in the service's input: a secret as secretString(), a setting as a string that may be absent exactly where config.ts has a default", async () => {
    const cell = await issuerCell();
    const absentOk = (name: string) => {
      const { [name]: _, ...rest } = cell.env;
      try {
        readConfig(rest);
        return true;
      } catch {
        return false;
      }
    };
    const want = bindings().map((b) => ({
      field: b.field,
      optional: b.kind === "envParam" && absentOk(b.name),
      type: b.kind === "envSecret" ? "secretString()" : '"string"',
    }));
    expect(sorted(schemaFields().map((f) => JSON.stringify(f)))).toEqual(sorted(want.map((f) => JSON.stringify(f))));
    // A Composer secret cannot be absent: a confidential OAuth client
    // (Console ask C1) is what the binding assumes.
    expect(absentOk("STREAMS_ISSUER_OAUTH_CLIENT_SECRET")).toBe(true);
    expect(want.find((f) => f.field === "STREAMS_ISSUER_OAUTH_CLIENT_SECRET")?.optional).toBe(false);
  });

  test("is an app of its own, on the cell app's Composer and the issuer's keep-awake guard, built as one bundled file", () => {
    const moduleName = (path: string) => /\bmodule\("([^"]+)"/.exec(read(path))?.[1];
    expect([moduleName("composer-issuer/app.ts"), moduleName("composer/app.ts")]).toEqual(["streams-issuer", "streams-cell"]);
    const pkg = (path: string) => JSON.parse(read(path));
    const app = pkg("composer-issuer/package.json");
    expect(app.dependencies).toEqual(pkg("composer/package.json").dependencies);
    expect(app.dependencies["@prisma/compute"]).toBe(pkg("issuer/package.json").dependencies["@prisma/compute"]);
    // The same lockfile as the cell's but for the workspace's name.
    const lock = (path: string, name: string) => read(path).replace(`"name": "${name}",`, '"name": "<app>",');
    expect(lock("composer-issuer/bun.lock", app.name)).toBe(lock("composer/bun.lock", pkg("composer/package.json").name));
    // The same Composer configuration as the cell's.
    expect(code("composer-issuer/prisma.config.ts")).toEqual(code("composer/prisma.config.ts"));
    expect(read("composer-issuer/issuer/service.ts")).toContain(
      'build: node({ module: import.meta.url, entry: "entry.js" }),',
    );
  });

  test("has an entry that reads the process environment for PORT only, and holds the keep-awake guard it bundles", () => {
    const entry = read("composer-issuer/issuer/entry.ts");
    expect(entry.match(/.*process\.env.*/g)).toEqual([
      "await runIssuer({ ...inputs(), PORT: process.env.PORT }, async () => new KeepAwakeGuard());",
    ]);
    expect(entry).toContain('import { KeepAwakeGuard } from "@prisma/compute";');
  });
});

const E2E = process.env.ISSUER_COMPOSER_E2E === "1";
const maybe = E2E ? test : test.skip;

/// The entry as a release cuts it: bundled into one file of its own.
async function bundle(): Promise<string> {
  if (!existsSync(join(DEPLOY, "composer-issuer", "node_modules"))) {
    throw new Error("deploy/composer-issuer has no node_modules: run `bun install --frozen-lockfile` there first");
  }
  const dir = scratchDir();
  const built = await Bun.build({
    entrypoints: [join(DEPLOY, "composer-issuer", "issuer", "entry.ts")],
    target: "bun",
    format: "esm",
    outdir: dir,
    naming: "entry.js",
  });
  if (!built.success) throw new Error(`bun build failed: ${built.logs.join("\n")}`);
  return join(dir, "entry.js");
}

/// The input document Composer writes for app.ts's binding: a $secret
/// pointer per envSecret, each envParam's value from `settings` as a
/// literal, and an envParam with no value left out.
function inputDocument(settings: Record<string, string | undefined>): string {
  const doc: Record<string, unknown> = {};
  for (const b of bindings()) {
    if (b.kind === "envSecret") doc[b.field] = { $secret: b.name };
    else if (settings[b.name] !== undefined) doc[b.field] = settings[b.name];
  }
  return JSON.stringify(doc);
}

/// The issuer's inputs for `cell` as its Composer deploy gives them: no
/// KEEP_AWAKE (the platform's default holds), and an OAuth client secret.
function composerInputs(cell: IssuerCell, patch: Record<string, string> = {}): Record<string, string> {
  const { KEEP_AWAKE: _, ...inputs } = cell.env;
  return { ...inputs, STREAMS_ISSUER_OAUTH_CLIENT_SECRET: "oauth-client-secret-e2e", ...patch };
}

/// The process environment Compute gives the entry for `env` (the issuer's
/// inputs): the secrets by their platform names and the input document;
/// every setting's name carries another value, which must not be read.
function platformEnv(env: Record<string, string>, drop: string[] = []): Record<string, string> {
  const secrets = new Set(bindings().filter((b) => b.kind === "envSecret").map((b) => b.name));
  const out: Record<string, string> = { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/" };
  for (const b of bindings()) {
    if (!secrets.has(b.name)) out[b.name] = `laced-${b.name}`;
    else if (!drop.includes(b.name)) out[b.name] = env[b.name];
  }
  return { ...out, COMPOSER_INPUT: inputDocument(env) };
}

describe("the issuer's Composer entry, bundled", () => {
  maybe("is one file whose imports are the runtime's own", async () => {
    const file = await bundle();
    expect(readdirSync(join(file, ".."))).toEqual(["entry.js"]);
    const specifiers = [...readFileSync(file, "utf8").matchAll(/(?:from\s*|import\s*\(\s*)"([^"]+)"/g)].map((m) => m[1]);
    const runtime = (s: string) => /^(node:|bun(:|$))/.test(s) || builtinModules.includes(s);
    expect([specifiers.length > 0, [...new Set(specifiers.filter((s) => !runtime(s)))]]).toEqual([true, []]);
  });

  maybe("boots from its service input away from the repository, serves the exchange, and stops on SIGTERM", async () => {
    const cell = await issuerCell();
    const a = await customerSecret(cell, "sp_a");
    const inputs = composerInputs(cell);
    const env = platformEnv(inputs);
    const started = await launch(await bundle(), env);
    if (!("origin" in started)) throw new Error(`the issuer did not start: ${started.err}`);
    expect(started.out.join("")).toContain(`streams-issuer: cell ${CELL}, issuer https://auth.prisma.io, kid streams-eddsa-`);
    const res = await exchange(started.origin, a.secret);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect([verifiedClaims(readBundle(cell.state), body.accessToken)?.credential_id, body.endpoint]).toEqual([
      a.id,
      inputs.STREAMS_CELL_ORIGIN,
    ]);
    expect((await fetch(`${started.origin}/healthz`)).status).toBe(200);
    started.child.kill("SIGTERM");
    expect(await started.child.exited).toBe(0);
    const out = started.out.join("") + (await restOf(started));
    expect(SECRET_INPUTS.filter((n) => out.includes(inputs[n]))).toEqual([]);
  });

  maybe("refuses to boot with exit 1 when a secret's platform variable is unset, naming the variable and no value", async () => {
    const cell = await issuerCell();
    const env = platformEnv(composerInputs(cell), ["STREAMS_ISSUER_PEPPER"]);
    const started = await launch(await bundle(), env);
    expect(["code" in started && started.code, "err" in started && started.err]).toEqual([
      1,
      'streams-issuer: the service\'s input: secret input is not provisioned (env COMPOSER_INPUT → STREAMS_ISSUER_PEPPER): the platform var "STREAMS_ISSUER_PEPPER" is unset or empty.\n',
    ]);
  });

  maybe("refuses to boot with exit 1 on a setting the issuer refuses, as its bare entry does", async () => {
    const cell = await issuerCell();
    const env = platformEnv(composerInputs(cell, { STREAMS_ISSUER_ID: "https://other.example.com" }));
    const started = await launch(await bundle(), env);
    expect(["code" in started && started.code, "err" in started && started.err]).toEqual([
      1,
      `streams-issuer: cell ${CELL} names issuer https://auth.prisma.io, not STREAMS_ISSUER_ID https://other.example.com\n`,
    ]);
  });
});
