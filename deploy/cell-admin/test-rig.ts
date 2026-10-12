// Shared rig for cell-admin's tests: scratch directories, signing keys
// generated for the test only, the tool run in-process, token checks made
// the way the cell makes them, and the two state stores: a local
// directory, or an s3:// state on an in-process fake S3 (fake-s3.ts).
// The suite runs once per store (`useStore` in a beforeEach); every
// helper below takes the `--state` value and reads whichever store it
// names.
import { createPublicKey, generateKeyPairSync, verify } from "node:crypto";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { main } from "./cell-admin";
import { FakeS3 } from "./fake-s3";
import type { Bundle } from "./feeds";
import type { CellState } from "./state";

const scratch: string[] = [];

export type StoreKind = "dir" | "s3";
export const STORES: readonly StoreKind[] = ["dir", "s3"];
let kind: StoreKind = "dir";
let s3: FakeS3 | undefined;
let buckets = 0;
/// s3:// state URL -> its credentials file and buckets.
const s3States = new Map<string, { creds: string; control: string; feeds: string; cell: string }>();

/// The store the next `initCell` and `freshState` use.
export function useStore(k: StoreKind): void {
  kind = k;
}

export function fakeS3(): FakeS3 {
  s3 ??= new FakeS3();
  return s3;
}

export function cleanup(): void {
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
  s3?.stop();
  s3 = undefined;
  s3States.clear();
  kind = "dir";
}

/// A `--state` for a new cell in `root` on the current store: a directory,
/// or fresh control and feeds buckets with a mode-0600 credentials file.
export function freshState(root: string, cell = "cell_sc1"): string {
  if (kind === "dir") return join(root, "state");
  const fake = fakeS3();
  buckets += 1;
  const [control, feeds] = [`ctl-${buckets}`, `feeds-${buckets}`];
  const target = (bucket: string) => ({
    endpoint: fake.endpoint, bucket, region: "auto", accessKeyId: "test-key", secretAccessKey: "test-secret",
  });
  const creds = join(root, `s3-${buckets}.json`);
  writeFileSync(creds, JSON.stringify({ ...target(control), feeds: target(feeds) }), { mode: 0o600 });
  const url = `s3://${control}/cells/${cell}`;
  s3States.set(url, { creds, control, feeds, cell });
  return url;
}

const s3Of = (where: string) => s3States.get(where);

/// The credentials file of an s3:// state, for tests that pass it by hand.
export const credentialsOf = (where: string) => s3Of(where)?.creds;

/// Where the store keeps the state, as its messages name it.
export function statePathOf(where: string): string {
  const s = s3Of(where);
  return s ? `${where}/cell-state.json` : join(where, "cell-state.json");
}

export function readStateText(where: string): string {
  const s = s3Of(where);
  if (!s) return readFileSync(join(where, "cell-state.json"), "utf8");
  const text = fakeS3().text(s.control, `cells/${s.cell}/cell-state.json`);
  if (text === undefined) throw new Error(`no state at ${where}`);
  return text;
}

/// Replace the state as another writer would (a new ETag on S3).
export function writeStateText(where: string, text: string): void {
  const s = s3Of(where);
  if (!s) writeFileSync(join(where, "cell-state.json"), text);
  else fakeS3().put(s.control, `cells/${s.cell}/cell-state.json`, text);
}

export function stateExists(where: string): boolean {
  const s = s3Of(where);
  return s ? fakeS3().text(s.control, `cells/${s.cell}/cell-state.json`) !== undefined
    : existsSync(join(where, "cell-state.json"));
}

export function readBundleText(where: string): string {
  const s = s3Of(where);
  if (!s) return readFileSync(join(where, "feeds-bundle.json"), "utf8");
  const text = fakeS3().text(s.feeds, `cells/${s.cell}/feeds-bundle.json`);
  if (text === undefined) throw new Error(`no bundle at ${where}`);
  return text;
}

/// The denylist the cell's state names.
export function denylistText(where: string): string {
  const s = s3Of(where);
  if (s) return fakeS3().text(s.control, "denylist.txt") ?? "";
  return readFileSync(readState(where).cell.denylist, "utf8");
}

/// The audit objects the store holds, oldest key first.
export function auditTexts(where: string): string[] {
  const s = s3Of(where);
  if (s) {
    return [...fakeS3().objects.entries()]
      .filter(([k]) => k.startsWith(`${s.control}/audit/`))
      .sort(([a], [b]) => (a < b ? -1 : 1))
      .map(([, o]) => o.body);
  }
  const root = join(where, "audit");
  if (!existsSync(root)) return [];
  return (readdirSync(root, { recursive: true }) as string[])
    .map((rel) => join(root, rel))
    .filter((path) => statSync(path).isFile())
    .sort()
    .map((path) => readFileSync(path, "utf8"));
}

/// Every document the store holds for the cell (a directory's files, the
/// buckets' objects), to check none carries a secret.
export function storeTexts(where: string): string[] {
  const s = s3Of(where);
  if (s) {
    return [...fakeS3().objects.entries()]
      .filter(([k]) => k.startsWith(`${s.control}/`) || k.startsWith(`${s.feeds}/`))
      .map(([, o]) => o.body);
  }
  return readdirSync(where)
    .map((name) => join(where, name))
    .filter((path) => !statSync(path).isDirectory())
    .map((path) => readFileSync(path, "utf8"));
}

export function scratchDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "cell-admin-"));
  scratch.push(dir);
  return dir;
}

/// A fresh private key in `dir`, mode 0600 unless `mode` says otherwise.
export function keyFile(dir: string, kind: "rsa" | "ed25519" = "rsa", mode = 0o600, name = "cell.pem"): string {
  const { privateKey } =
    kind === "rsa"
      ? generateKeyPairSync("rsa", { modulusLength: 2048 })
      : generateKeyPairSync("ed25519");
  const path = join(dir, name);
  writeFileSync(path, privateKey.export({ type: "pkcs8", format: "pem" }), { mode });
  return path;
}

/// The public half of the key in `privatePath`, as `openssl pkey -pubout`
/// writes it.
export function publicKeyFile(privatePath: string, name = "cell.pub"): string {
  const pub = createPublicKey(readFileSync(privatePath)).export({ type: "spki", format: "pem" });
  const path = join(dirname(privatePath), name);
  writeFileSync(path, pub, { mode: 0o644 });
  return path;
}

export interface Run {
  code: number;
  out: string;
  err: string;
}

/// Run cell-admin in-process. An s3:// `--state` this rig made gets its
/// `--s3-credentials` unless the test passes one.
export async function run(...argv: string[]): Promise<Run> {
  const at = argv.indexOf("--state");
  const s = at >= 0 ? s3Of(argv[at + 1]) : undefined;
  if (s && !argv.includes("--s3-credentials")) argv = [...argv, "--s3-credentials", s.creds];
  const out: string[] = [];
  const err: string[] = [];
  const code = await main(argv, { out: (l) => out.push(l), err: (l) => err.push(l) });
  return { code, out: out.join("\n"), err: err.join("\n") };
}

export const readState = (where: string): CellState => JSON.parse(readStateText(where));
export const readBundle = (where: string): Bundle => JSON.parse(readBundleText(where));

export interface Cell {
  root: string;
  state: string;
  key: string;
}

/// A scratch root with a key and an initialised cell on the shipped
/// profiles, on the current store.
export async function initCell(extra: string[] = []): Promise<Cell> {
  const root = scratchDir();
  const key = keyFile(root);
  const state = freshState(root);
  const r = await run(
    "init", "--state", state, "--cell-id", "cell_sc1", "--deployment-project", "proj_deploy",
    "--account", "acct_sink", "--key-file", key, ...extra,
  );
  if (r.code !== 0) throw new Error(`init failed: ${r.err}`);
  return { root, state, key };
}

/// The id of `project`'s customer credential that admit minted.
export function customerCredential(stateDir: string, project: string): string {
  const s = readState(stateDir);
  const found = Object.entries(s.credentials).find(([, c]) => c.project_id === project && c.role === "customer");
  if (!found) throw new Error(`no credential for ${project}`);
  return found[0];
}

export function decodeJwt(token: string): { header: any; claims: any; input: Buffer; sig: Buffer } {
  const [h, c, s] = token.split(".");
  return {
    header: JSON.parse(Buffer.from(h, "base64url").toString()),
    claims: JSON.parse(Buffer.from(c, "base64url").toString()),
    input: Buffer.from(`${h}.${c}`),
    sig: Buffer.from(s, "base64url"),
  };
}

/// Verify `token` against the keys snapshot the way the cell does.
export function verifiedClaims(bundle: Bundle, token: string): any | undefined {
  const { header, claims, input, sig } = decodeJwt(token);
  const key = (bundle.keys.keys as any[]).find((k) => k.kid === header.kid);
  if (!key || key.aud !== "prisma-streams-data" || key.alg !== header.alg) return undefined;
  const ok = verify(header.alg === "RS256" ? "sha256" : null, input, key.pem, sig);
  return ok ? claims : undefined;
}
