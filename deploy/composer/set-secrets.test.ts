// set-secrets (public-beta deploy-q5 triage B5, D10): a stage's secrets
// reach the platform by name, from files only their owner reads, into the
// stage's own scope, with no value ever logged. Offline, against a fake
// Management API (fake-platform.ts). Run with `bun test ./deploy/composer`.
import { afterAll, describe, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakePlatform } from "./fake-platform";

const TOOL = join(import.meta.dir, "set-secrets");
const CELL_NAMES = [
  "STREAMS_OPERATOR_TOKEN",
  "STREAMS_USAGE_STREAM_KEY",
  "STREAMS_CURSOR_KEY",
  "STREAMS_DATA_S3_ACCESS_KEY_ID",
  "STREAMS_DATA_S3_SECRET_ACCESS_KEY",
  "STREAMS_FEEDS_S3_ACCESS_KEY_ID",
  "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY",
];
const ISSUER_NAMES = [
  "STREAMS_ISSUER_SIGNING_KEY",
  "STREAMS_ISSUER_PEPPER",
  "STREAMS_ISSUER_SESSION_KEY",
  "STREAMS_ISSUER_OAUTH_CLIENT_SECRET",
  "STREAMS_ISSUER_METRICS_TOKEN",
  "STREAMS_CONTROL_S3_ACCESS_KEY_ID",
  "STREAMS_CONTROL_S3_SECRET_ACCESS_KEY",
  "STREAMS_FEEDS_S3_ACCESS_KEY_ID",
  "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY",
];
const WATCH_NAMES = [
  "STREAMS_OPERATOR_TOKEN",
  "ALERT_SINK_URL",
  "ALERT_SINK_KEY",
  "HEARTBEAT_URL",
  "STREAMS_ISSUER_METRICS_TOKEN",
  "CANARY_CREDENTIAL",
];

const scratch: string[] = [];
const fakes: FakePlatform[] = [];
afterAll(() => {
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
  for (const f of fakes.splice(0)) f.stop();
});

function rig() {
  const root = mkdtempSync(join(tmpdir(), "set-secrets-"));
  scratch.push(root);
  const api = new FakePlatform();
  fakes.push(api);
  const project = api.addProject("streams-cell", "eu-central-1");
  const tokenFile = join(root, "token");
  writeFileSync(tokenFile, `${api.token}\n`, { mode: 0o600 });
  return { root, api, project, tokenFile };
}

/// DIR/<NAME> for each name, mode 0600, with a trailing newline.
function secretsDir(root: string, names: readonly string[], value = (n: string) => `${n.toLowerCase()}-${randomBytes(6).toString("hex")}`) {
  const dir = join(root, `secrets-${randomBytes(3).toString("hex")}`);
  mkdirSync(dir, { mode: 0o700 });
  const values: Record<string, string> = {};
  for (const n of names) {
    values[n] = value(n);
    writeFileSync(join(dir, n), `${values[n]}\n`, { mode: 0o600 });
  }
  return { dir, values };
}

async function setSecrets(env: Record<string, string>, ...args: string[]) {
  const proc = Bun.spawn([process.execPath, TOOL, ...args], {
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/", ...env },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [out, err, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  return { code, out, err };
}

describe("set-secrets", () => {
  test("creates the cell's seven names in the production class from files only their owner reads, and logs no value", async () => {
    const r = rig();
    const s = secretsDir(r.root, CELL_NAMES);
    const run = await setSecrets({ PRISMA_TOKEN_FILE: r.tokenFile }, "production", "--project", r.project.id, "--from", s.dir,
      "--management-api", r.api.url);
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(run.out.trim().split("\n")).toEqual([
      ...CELL_NAMES.map((n) => `set-secrets: ${n} created in project ${r.project.id}'s production variables`),
      "set-secrets: the cell's 7 secrets are set for production; deploy to use them",
    ]);
    expect(r.api.variables.map((v) => [v.key, v.class, v.branchId, v.projectId])).toEqual(
      CELL_NAMES.map((n) => [n, "production", null, r.project.id]),
    );
    expect(CELL_NAMES.map((n) => r.api.valueOf(r.project.id, "production", null, n))).toEqual(CELL_NAMES.map((n) => s.values[n]));
    expect(Object.values(s.values).filter((v) => run.out.includes(v) || run.err.includes(v))).toEqual([]);
  });

  test("replaces a value the scope holds (409, then PATCH) and leaves every other scope alone", async () => {
    const r = rig();
    const cert = r.api.addBranch(r.project.id, "cert");
    r.api.addVariable({ projectId: r.project.id, branchId: null, class: "production", key: "STREAMS_CURSOR_KEY" }, "old");
    r.api.addVariable({ projectId: r.project.id, branchId: cert.id, class: "preview", key: "STREAMS_CURSOR_KEY" }, "cert-old");
    const s = secretsDir(r.root, CELL_NAMES);
    const run = await setSecrets({ PRISMA_TOKEN_FILE: r.tokenFile }, "production", "--project", r.project.id, "--from", s.dir,
      "--management-api", r.api.url);
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(run.out).toContain(`set-secrets: STREAMS_CURSOR_KEY replaced in project ${r.project.id}'s production variables`);
    expect([
      r.api.valueOf(r.project.id, "production", null, "STREAMS_CURSOR_KEY"),
      r.api.valueOf(r.project.id, "preview", cert.id, "STREAMS_CURSOR_KEY"),
      r.api.variables.filter((v) => v.key === "STREAMS_CURSOR_KEY").length,
    ]).toEqual([s.values.STREAMS_CURSOR_KEY, "cert-old", 2]);
    expect(r.api.requests.filter((q) => q.method === "PATCH").map((q) => q.status)).toEqual([200]);
  });

  test("a named stage sets its Branch's overrides only, and the default stage takes no Branch", async () => {
    const r = rig();
    const cert = r.api.addBranch(r.project.id, "cert");
    const s = secretsDir(r.root, CELL_NAMES);
    const env = { PRISMA_TOKEN_FILE: r.tokenFile };
    const base = ["--project", r.project.id, "--from", s.dir, "--management-api", r.api.url];
    const run = await setSecrets(env, "cert", ...base, "--branch", cert.id);
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(new Set(r.api.variables.map((v) => `${v.class}/${v.branchId}`))).toEqual(new Set([`preview/${cert.id}`]));
    const without = await setSecrets(env, "cert", ...base);
    expect([without.code, without.err.split("\n")[0]]).toEqual([
      2, "set-secrets: the cert stage is a named stage: --branch names its Branch",
    ]);
    const prod = await setSecrets(env, "production", ...base, "--branch", cert.id);
    expect([prod.code, prod.err.split("\n")[0]]).toEqual([
      2, "set-secrets: the production stage is the default stage: it takes no --branch",
    ]);
    expect(r.api.variables.length).toBe(CELL_NAMES.length);
  });

  test("refuses before it writes anything: a file others can read, a missing one, a value that is not space-free", async () => {
    const r = rig();
    const env = { PRISMA_TOKEN_FILE: r.tokenFile };
    const run = (dir: string) =>
      setSecrets(env, "production", "--project", r.project.id, "--from", dir, "--management-api", r.api.url);
    const loose = secretsDir(r.root, CELL_NAMES);
    chmodSync(join(loose.dir, "STREAMS_USAGE_STREAM_KEY"), 0o640);
    expect(await run(loose.dir)).toEqual({
      code: 1, out: "",
      err: `set-secrets: the secret file ${join(loose.dir, "STREAMS_USAGE_STREAM_KEY")} has mode 0640: only its owner may read it (chmod 600)\n`,
    });
    const missing = secretsDir(r.root, CELL_NAMES.slice(0, 6));
    expect(await run(missing.dir)).toEqual({
      code: 1, out: "",
      err: `set-secrets: the secret file ${join(missing.dir, "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY")} does not exist\n`,
    });
    const spaced = secretsDir(r.root, CELL_NAMES, (n) => (n === "STREAMS_CURSOR_KEY" ? "two words" : "v"));
    expect(await run(spaced.dir)).toEqual({
      code: 1, out: "",
      err: `set-secrets: the value of STREAMS_CURSOR_KEY in ${join(spaced.dir, "STREAMS_CURSOR_KEY")} is not space-free: the cell's entry refuses it\n`,
    });
    expect(r.api.requests).toEqual([]);
  });

  test("--blank empties one secret of the scratch stage's Branch only, an empty file is refused, and --blank goes alone", async () => {
    const r = rig();
    const scratch = r.api.addBranch(r.project.id, "scratch");
    const cert = r.api.addBranch(r.project.id, "cert");
    const env = { PRISMA_TOKEN_FILE: r.tokenFile };
    const api = ["--management-api", r.api.url];
    const s = secretsDir(r.root, CELL_NAMES);
    expect((await setSecrets(env, "scratch", "--project", r.project.id, "--branch", scratch.id, "--from", s.dir, ...api)).code).toBe(0);
    const blank = await setSecrets(env, "scratch", "--project", r.project.id, "--branch", scratch.id, "--blank", "STREAMS_CURSOR_KEY", ...api);
    expect(blank).toEqual({
      code: 0, err: "",
      out: `set-secrets: STREAMS_CURSOR_KEY blanked in Branch ${scratch.id} of project ${r.project.id}: the next deploy boots ` +
        "without it (the entry refuses an empty secret); set it again with --from\n",
    });
    expect(CELL_NAMES.map((n) => r.api.valueOf(r.project.id, "preview", scratch.id, n))).toEqual(
      CELL_NAMES.map((n) => (n === "STREAMS_CURSOR_KEY" ? "" : s.values[n])),
    );
    const writes = r.api.requests.length;
    const refused = [
      await setSecrets(env, "cert", "--project", r.project.id, "--branch", cert.id, "--blank", "STREAMS_CURSOR_KEY", ...api),
      await setSecrets(env, "production", "--project", r.project.id, "--blank", "STREAMS_CURSOR_KEY", ...api),
      await setSecrets(env, "scratch", "--project", r.project.id, "--branch", scratch.id, "--blank", "STREAMS_NOPE", ...api),
    ];
    expect(refused).toEqual([
      { code: 1, out: "", err: "set-secrets: --blank empties a secret on the scratch stage only (the missing-secret boot failure); cert is not scratch\n" },
      { code: 1, out: "", err: "set-secrets: --blank empties a secret on the scratch stage only (the missing-secret boot failure); production is not scratch\n" },
      { code: 1, out: "", err: "set-secrets: the cell binds no STREAMS_NOPE: --blank takes one of its envSecret names\n" },
    ]);
    const both = await setSecrets(env, "scratch", "--project", r.project.id, "--branch", scratch.id, "--blank", "STREAMS_CURSOR_KEY",
      "--from", s.dir, ...api);
    const neither = await setSecrets(env, "scratch", "--project", r.project.id, "--branch", scratch.id, ...api);
    expect([both, neither].map((x) => [x.code, x.err.split("\n")[0]])).toEqual([
      [2, "set-secrets: give --from DIR (every secret) or --blank NAME (one, on scratch only)"],
      [2, "set-secrets: give --from DIR (every secret) or --blank NAME (one, on scratch only)"],
    ]);
    const empty = secretsDir(r.root, CELL_NAMES, (n) => (n === "STREAMS_CURSOR_KEY" ? "" : "v"));
    expect(await setSecrets(env, "scratch", "--project", r.project.id, "--branch", scratch.id, "--from", empty.dir, ...api)).toEqual({
      code: 1, out: "", err: `set-secrets: the secret file ${join(empty.dir, "STREAMS_CURSOR_KEY")} is empty\n`,
    });
    expect(r.api.requests.length).toBe(writes);
  });

  test("sets the issuer's nine names from its own app, a multi-line key included", async () => {
    const r = rig();
    const issuer = r.api.addProject("streams-issuer", "eu-central-1");
    const pem = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEI\n-----END PRIVATE KEY-----";
    const s = secretsDir(r.root, ISSUER_NAMES, (n) => (n === "STREAMS_ISSUER_SIGNING_KEY" ? pem : `${n}-v`));
    const run = await setSecrets({ PRISMA_TOKEN_FILE: r.tokenFile }, "production", "--project", issuer.id, "--from", s.dir,
      "--app", "issuer", "--management-api", r.api.url);
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(r.api.variables.map((v) => [v.projectId, v.key])).toEqual(ISSUER_NAMES.map((n) => [issuer.id, n]));
    expect(r.api.valueOf(issuer.id, "production", null, "STREAMS_ISSUER_SIGNING_KEY")).toBe(pem);
    expect(run.out.includes("BEGIN PRIVATE KEY")).toBe(false);
  });

  test("sets the cell watch's six names from its module (--app cell-watch), refuses a URL its boot would refuse, and blanks one on scratch", async () => {
    const r = rig();
    const watch = r.api.addProject("streams-cell-watch", "eu-central-1");
    const scratchBranch = r.api.addBranch(watch.id, "scratch");
    const value = (n: string) =>
      n === "ALERT_SINK_URL" ? "https://pager.example/v2/enqueue" : n === "HEARTBEAT_URL" ? "https://pager.example/heartbeat/t" : `${n}-v`;
    const s = secretsDir(r.root, WATCH_NAMES, value);
    const env = { PRISMA_TOKEN_FILE: r.tokenFile };
    const base = ["--project", watch.id, "--app", "cell-watch", "--management-api", r.api.url];
    const run = await setSecrets(env, "production", ...base, "--from", s.dir);
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(run.out.trim().split("\n").at(-1)).toBe("set-secrets: the cell-watch's 6 secrets are set for production; deploy to use them");
    expect(r.api.variables.map((v) => [v.projectId, v.class, v.key])).toEqual(WATCH_NAMES.map((n) => [watch.id, "production", n]));
    expect(WATCH_NAMES.map((n) => r.api.valueOf(watch.id, "production", null, n))).toEqual(WATCH_NAMES.map(value));
    expect(Object.values(s.values).filter((v) => run.out.includes(v) || run.err.includes(v))).toEqual([]);
    const writes = r.api.requests.length;
    const plain = secretsDir(r.root, WATCH_NAMES, (n) => (n === "HEARTBEAT_URL" ? "http://pager.example/heartbeat/t" : value(n)));
    expect(await setSecrets(env, "production", ...base, "--from", plain.dir)).toEqual({
      code: 1, out: "",
      err: `set-secrets: the value of HEARTBEAT_URL in ${join(plain.dir, "HEARTBEAT_URL")} is not an https URL: the watch's boot refuses it\n`,
    });
    expect(r.api.requests.length).toBe(writes);
    const blank = await setSecrets(env, "scratch", "--project", watch.id, "--branch", scratchBranch.id, "--app", "cell-watch",
      "--blank", "CANARY_CREDENTIAL", "--management-api", r.api.url);
    expect([blank.code, r.api.valueOf(watch.id, "preview", scratchBranch.id, "CANARY_CREDENTIAL")]).toEqual([0, ""]);
    const other = await setSecrets(env, "production", "--project", watch.id, "--app", "watch", "--from", s.dir, "--management-api", r.api.url);
    expect([other.code, other.err.split("\n")[0]]).toEqual([2, "set-secrets: --app watch is cell, issuer or cell-watch"]);
  });

  test("reads the token only from the file PRISMA_TOKEN_FILE names, which only its owner may read", async () => {
    const r = rig();
    const s = secretsDir(r.root, CELL_NAMES);
    const args = ["production", "--project", r.project.id, "--from", s.dir, "--management-api", r.api.url];
    const none = await setSecrets({}, ...args);
    expect([none.code, none.err.split("\n")[0]]).toEqual([
      2, "set-secrets: PRISMA_TOKEN_FILE must name the file holding the Management API token",
    ]);
    chmodSync(r.tokenFile, 0o644);
    const loose = await setSecrets({ PRISMA_TOKEN_FILE: r.tokenFile }, ...args);
    expect(loose).toEqual({
      code: 1, out: "", err: `set-secrets: the token file ${r.tokenFile} has mode 0644: only its owner may read it (chmod 600)\n`,
    });
    const wrong = join(r.root, "wrong-token");
    writeFileSync(wrong, "not-the-token", { mode: 0o600 });
    const refused = await setSecrets({ PRISMA_TOKEN_FILE: wrong }, ...args);
    expect(refused).toEqual({
      code: 1, out: "", err: "set-secrets: POST /environment-variables answered 401 (unauthorized)\n",
    });
    expect(r.api.variables).toEqual([]);
  });
});
