// The cell watch's Composer app (public-beta alerting triage B4): an app of
// its own (app.ts) mounting the watch's module (module.ts), whose one
// service (service.ts) is bound to config.ts's inputs by their platform
// names and whose entry (entry.ts) runs the watch on the service's input.
// Run with `bun test ./deploy/cell-watch`.
//
// The binding tests read the app's sources as text, so they run without
// this directory's packages, as CI's bun step does. The bundled entry's
// tests need those packages and are opt-in:
//
//   (cd deploy/cell-watch && bun install --frozen-lockfile)
//   CELL_WATCH_COMPOSER_E2E=1 bun test ./deploy/cell-watch/composer.test.ts
import { afterEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { builtinModules } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { INPUTS, PARAM_INPUTS, SECRET_INPUTS } from "./config";
import { EXCHANGE_PATH, RECORDS_PATH } from "./canary";
import { CANARY_CREDENTIAL, CANARY_TOKEN, FakeCell, FakeIssuer, FakeServer, METRICS_TOKEN, OPERATOR_TOKEN } from "./fake-cell";

const HERE = import.meta.dir;
const read = (file: string) => readFileSync(join(HERE, file), "utf8");
/// A source's lines but its comments.
const code = (file: string) => read(file).split("\n").filter((l) => !/^\s*\/\//.test(l));

type Binding = { field: string; kind: "envSecret" | "envParam"; name: string };

/// module.ts's input binding, line by line.
function bindings(): Binding[] {
  return [...read("module.ts").matchAll(/^\s*(\w+): (envSecret|envParam)\("([A-Z0-9_]+)"\),$/gm)].map((m) => ({
    field: m[1]!,
    kind: m[2] as Binding["kind"],
    name: m[3]!,
  }));
}

/// service.ts's input schema: each field, whether it may be absent, and its type.
function schemaFields(): { field: string; optional: boolean; type: string }[] {
  return [...read("service.ts").matchAll(/^\s*"?(\w+)(\??)"?: (secretString\(\)|"string"),$/gm)].map((m) => ({
    field: m[1]!,
    optional: m[2] === "?",
    type: m[3]!,
  }));
}

const scratch: string[] = [];
const servers: { stop(): void }[] = [];
const children: ReturnType<typeof Bun.spawn>[] = [];
afterEach(() => {
  for (const c of children.splice(0)) c.kill("SIGKILL");
  for (const s of servers.splice(0)) s.stop();
  for (const d of scratch.splice(0)) rmSync(d, { recursive: true, force: true });
});

describe("the cell watch's Composer app", () => {
  test("binds every input config.ts reads, under its own name: the secrets as envSecret, the settings as envParam", () => {
    const bound = bindings();
    expect(bound.filter((b) => b.field !== b.name)).toEqual([]);
    expect(bound.filter((b) => b.kind === "envSecret").map((b) => b.name)).toEqual([...SECRET_INPUTS]);
    expect(bound.filter((b) => b.kind === "envParam").map((b) => b.name)).toEqual([...PARAM_INPUTS]);
    expect(new Set(bound.map((b) => b.name)).size).toBe(INPUTS.length);
  });

  test("binds nothing else: no generated value, no Composer port, no dependency, no other service's origin", () => {
    const app = code("module.ts").join("\n");
    for (const text of [app, code("app.ts").join("\n"), code("service.ts").join("\n")]) {
      expect(text.match(/\b(generatedParam|serviceOrigin|paramSource|secretSource|expose|durableStreams|rpc)\b/g)).toBeNull();
    }
    expect(app.match(/\b(envSecret|envParam)\(/g)?.length).toBe(bindings().length);
    expect(code("service.ts").join("\n")).toContain("deps: {},");
  });

  test("declares each bound field in the service's input, none optional: a secret as secretString(), a setting as a string", () => {
    const want = bindings().map((b) => ({
      field: b.field,
      optional: false,
      type: b.kind === "envSecret" ? "secretString()" : '"string"',
    }));
    expect(schemaFields()).toEqual(want);
  });

  test("is an app of its own whose module mounts in another app with one line, built as one bundled file", () => {
    const moduleName = (file: string) => /\bmodule\("([^"]+)"/.exec(read(file))?.[1];
    expect([moduleName("app.ts"), moduleName("module.ts")]).toEqual(["streams-cell-watch", "cellwatch"]);
    // The app is the module and nothing else; below the root, Composer takes
    // only [A-Za-z0-9] names, and a provision id of 3 or more.
    expect(code("app.ts").filter((l) => l.trim() !== "")).toEqual([
      'import { module } from "@prisma/composer";',
      'import cellWatch from "./module.ts";',
      'export default module("streams-cell-watch", ({ provision }) => {',
      "  provision(cellWatch);",
      "});",
    ]);
    expect(code("module.ts").join("\n")).toContain('id: "watch",');
    expect(code("service.ts").join("\n")).toContain('name: "cellwatch",');
    expect(read("service.ts")).toContain('build: node({ module: import.meta.url, entry: "entry.js" }),');
    expect(read(".gitignore").split("\n")).toEqual([".alchemy/", ".prisma-composer/", "entry.js", "pager-entry.js", ""]);
  });

  test("pins Composer, the CLI and the keep-awake guard exactly, the guard at the issuer's version", () => {
    const pkg = (file: string) => JSON.parse(readFileSync(join(HERE, file), "utf8"));
    const deps = pkg("package.json").dependencies as Record<string, string>;
    expect(deps).toEqual({
      "@prisma/composer": "0.29.1",
      "@prisma/composer-prisma-cloud": "0.29.1",
      "@prisma/compute": "0.1.0-beta.0",
      arktype: "2.2.8",
      prisma: "8.0.0-rc.22",
    });
    expect(deps["@prisma/compute"]).toBe(pkg("../issuer/package.json").dependencies["@prisma/compute"]);
    for (const [name, version] of Object.entries(deps)) {
      expect(read("bun.lock")).toContain(`"${name}": ["${name}@${version}"`);
    }
  });

  test("has an entry that reads nothing from the process environment and holds the keep-awake guard it bundles", () => {
    expect(read("entry.ts")).toContain('import { KeepAwakeGuard } from "@prisma/compute";');
    expect(code("entry.ts").join("\n")).toContain("held.push(new KeepAwakeGuard());");
    // No file of the watch reads its environment; the tests' rigs neither.
    const sources = readdirSync(HERE).filter((f) => f.endsWith(".ts") && !f.endsWith(".test.ts"));
    expect(sources.filter((f) => code(f).some((l) => l.includes("process.env")))).toEqual([]);
  });
});

const E2E = process.env.CELL_WATCH_COMPOSER_E2E === "1";
const maybe = E2E ? test : test.skip;

/// The entry as a release cuts it: bundled into one file of its own.
async function bundle(): Promise<string> {
  if (!existsSync(join(HERE, "node_modules"))) {
    throw new Error("deploy/cell-watch has no node_modules: run `bun install --frozen-lockfile` there first");
  }
  const dir = mkdtempSync(join(tmpdir(), "cell-watch-entry-"));
  scratch.push(dir);
  const built = await Bun.build({
    entrypoints: [join(HERE, "entry.ts")],
    target: "bun",
    format: "esm",
    outdir: dir,
    naming: "entry.js",
  });
  if (!built.success) throw new Error(`bun build failed: ${built.logs.join("\n")}`);
  return join(dir, "entry.js");
}

/// The process environment Composer's runtime leaves the entry: the input
/// document (a $secret pointer per envSecret, each setting a literal), the
/// secrets by their platform names and the port. Each setting's own name
/// carries another value, which must not be read.
function platformEnv(inputs: Record<string, string>, port: number, drop: string[] = []): Record<string, string> {
  const doc: Record<string, unknown> = {};
  const out: Record<string, string> = { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/" };
  for (const b of bindings()) {
    if (b.kind === "envSecret") {
      doc[b.field] = { $secret: b.name };
      if (!drop.includes(b.name)) out[b.name] = inputs[b.name]!;
    } else {
      doc[b.field] = inputs[b.name];
      out[b.name] = `laced-${b.name}`;
    }
  }
  return { ...out, COMPOSER_INPUT: JSON.stringify(doc), COMPOSER_PORT: String(port) };
}

type Started = { child: ReturnType<typeof Bun.spawn>; out: () => string; err: () => string };

/// The bundle in a directory of its own, with `env` and nothing else.
function launch(file: string, env: Record<string, string>): Started {
  const child = Bun.spawn([process.execPath, file], { env, cwd: join(file, ".."), stdout: "pipe", stderr: "pipe" });
  children.push(child);
  const [out, err] = [[] as string[], [] as string[]];
  const pump = async (s: ReadableStream<Uint8Array>, into: string[]) => {
    for await (const chunk of s) into.push(new TextDecoder().decode(chunk));
  };
  void pump(child.stdout as ReadableStream<Uint8Array>, out);
  void pump(child.stderr as ReadableStream<Uint8Array>, err);
  return { child, out: () => out.join(""), err: () => err.join("") };
}

async function until(what: string, cond: () => boolean, ms = 15_000): Promise<void> {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await Bun.sleep(25);
  }
}

function freePort(): number {
  const s = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const port = Number(s.port);
  s.stop(true);
  return port;
}

function world() {
  const cell = new FakeCell();
  const sink = new FakeServer({ "/hook": () => ({ status: 202, body: "" }) });
  const monitor = new FakeServer({ "/beat/heartbeat-secret-e2e": () => ({ status: 200, body: "OK" }) });
  servers.push(cell, sink, monitor);
  const inputs: Record<string, string> = {
    STREAMS_OPERATOR_TOKEN: OPERATOR_TOKEN,
    ALERT_SINK_URL: `${sink.origin}/hook`,
    ALERT_SINK_KEY: "none",
    HEARTBEAT_URL: `${monitor.origin}/beat/heartbeat-secret-e2e`,
    STREAMS_ISSUER_METRICS_TOKEN: "none",
    CANARY_CREDENTIAL: "none",
    STREAMS_CELL_ORIGIN: cell.origin,
    STREAMS_CELL_ID: "cell_sc1",
    ALERT_SINK_KIND: "webhook",
    EXPECTED_GIT: "none",
    NOTIFY_ONLY: "0",
    ISSUER_ORIGIN: "none",
  };
  return { cell, sink, monitor, inputs };
}

describe("the cell watch's Composer entry, bundled", () => {
  maybe("is one file whose imports are the runtime's own", async () => {
    const file = await bundle();
    expect(readdirSync(join(file, ".."))).toEqual(["entry.js"]);
    const specifiers = [...readFileSync(file, "utf8").matchAll(/(?:from\s*|import\s*\(\s*)"([^"]+)"/g)].map((m) => m[1]!);
    const runtime = (s: string) => /^(node:|bun(:|$))/.test(s) || builtinModules.includes(s);
    expect([specifiers.length > 0, [...new Set(specifiers.filter((s) => !runtime(s)))]]).toEqual([true, []]);
  });

  maybe("runs from its service input away from the repository: ok on its port, the cell polled with the bearer, a heartbeat", async () => {
    const w = world();
    const port = freePort();
    const started = launch(await bundle(), platformEnv(w.inputs, port));
    await until("the first heartbeat", () => w.monitor.seen.length === 1);
    expect(started.out()).toBe(
      "cell-watch: cell cell_sc1, sink webhook, notify_only false, issuer false, expected_git false, " +
        `keep_awake unavailable, listening on :${port}\n`,
    );
    const ok = await fetch(`http://127.0.0.1:${port}/`);
    expect([ok.status, await ok.text()]).toEqual([200, "ok"]);
    // The start's bearer check, then the first cycle's GET.
    const data = w.cell.seen.filter((s) => s.path === "/operator/data.json");
    expect(data.map((s) => s.authorization)).toEqual([`Bearer ${OPERATOR_TOKEN}`, `Bearer ${OPERATOR_TOKEN}`]);
    expect([w.sink.seen, started.err()]).toEqual([[], ""]);
  });

  maybe("runs the canary from its secret: the credential to the issuer's exchange, the token to the cell's stream", async () => {
    const w = world();
    const issuer = new FakeIssuer();
    issuer.endpoint = w.cell.origin;
    servers.push(issuer);
    const inputs = {
      ...w.inputs,
      ISSUER_ORIGIN: issuer.origin,
      STREAMS_ISSUER_METRICS_TOKEN: METRICS_TOKEN,
      CANARY_CREDENTIAL,
    };
    const started = launch(await bundle(), platformEnv(inputs, freePort()));
    await until("the first heartbeat", () => w.monitor.seen.length === 1);
    expect(started.out()).toContain("issuer true,");
    expect(issuer.seen.filter((s) => s.path === EXCHANGE_PATH).map((s) => [s.method, s.authorization])).toEqual([
      ["POST", `StreamsCredential ${CANARY_CREDENTIAL}`],
    ]);
    const appends = w.cell.seen.filter((s) => s.path === RECORDS_PATH && s.method === "POST");
    expect(appends.map((s) => s.authorization)).toEqual([`Bearer ${CANARY_TOKEN}`]);
    expect([w.cell.stream?.records.length, w.sink.seen, started.err()]).toEqual([1, [], ""]);
  });

  maybe("refuses to boot with exit 1 when a secret's platform variable is unset, naming the variable and no value", async () => {
    const w = world();
    const started = launch(await bundle(), platformEnv(w.inputs, freePort(), ["HEARTBEAT_URL"]));
    expect([await started.child.exited, started.out()]).toEqual([1, ""]);
    expect(started.err()).toBe(
      "cell-watch: refused: the service's input: secret input is not provisioned (env COMPOSER_INPUT → HEARTBEAT_URL): " +
        'the platform var "HEARTBEAT_URL" is unset or empty.\n',
    );
  });

  maybe("refuses to boot with exit 1 on a setting the watch refuses, naming it and quoting no value", async () => {
    const w = world();
    const env = platformEnv({ ...w.inputs, STREAMS_CELL_ORIGIN: "http://cell-zq7mark.example.com" }, freePort());
    const started = launch(await bundle(), env);
    expect([await started.child.exited, started.out()]).toEqual([1, ""]);
    expect(started.err()).toBe("cell-watch: refused: STREAMS_CELL_ORIGIN must be https (http only to this machine)\n");
    expect(w.cell.seen).toEqual([]);
  });

  maybe("refuses to boot with exit 1 when the cell refuses its bearer, naming STREAMS_OPERATOR_TOKEN and quoting no value", async () => {
    const w = world();
    const wrong = "not-the-bearer-zq7mark";
    const started = launch(await bundle(), platformEnv({ ...w.inputs, STREAMS_OPERATOR_TOKEN: wrong }, freePort()));
    expect([await started.child.exited, started.out()]).toEqual([1, ""]);
    expect(started.err()).toBe(
      "cell-watch: refused: STREAMS_OPERATOR_TOKEN is refused by the cell (/operator/data.json answered 401)\n",
    );
    expect([w.cell.seen.map((s) => s.path), w.monitor.seen]).toEqual([["/operator/data.json"], []]);
  });
});
