// Shared rig for cell-admin's tests: scratch directories, signing keys
// generated for the test only, the tool run in-process, and token checks
// made the way the cell makes them.
import { generateKeyPairSync, verify } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { main } from "./cell-admin";
import type { Bundle } from "./feeds";
import type { CellState } from "./state";

const scratch: string[] = [];

export function cleanup(): void {
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
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

export interface Run {
  code: number;
  out: string;
  err: string;
}

export async function run(...argv: string[]): Promise<Run> {
  const out: string[] = [];
  const err: string[] = [];
  const code = await main(argv, { out: (l) => out.push(l), err: (l) => err.push(l) });
  return { code, out: out.join("\n"), err: err.join("\n") };
}

export const readState = (dir: string): CellState =>
  JSON.parse(readFileSync(join(dir, "cell-state.json"), "utf8"));
export const readBundle = (dir: string): Bundle => JSON.parse(readFileSync(join(dir, "feeds-bundle.json"), "utf8"));

export interface Cell {
  root: string;
  state: string;
  key: string;
}

/// A scratch root with a key and an initialised cell on the shipped profiles.
export async function initCell(extra: string[] = []): Promise<Cell> {
  const root = scratchDir();
  const key = keyFile(root);
  const state = join(root, "state");
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
