// Shared rig for the issuer's tests: a cell initialised by cell-admin on
// the in-process fake S3 (deploy/cell-admin/fake-s3.ts), with an Ed25519
// signing key and a pepper made for the test only, and the environment
// the issuer would be given for it.
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { presentedSecret } from "../cell-admin/secrets";
import type { CellState } from "../cell-admin/state";
import {
  credentialsOf,
  fakeS3,
  freshState,
  keyFile,
  pepperFile,
  readState,
  run,
  scratchDir,
  useStore,
} from "../cell-admin/test-rig";
import type { Env } from "./config";

export interface IssuerCell {
  root: string;
  /// The s3:// state the cell-admin commands take.
  state: string;
  /// The signing key file and the pepper file cell-admin uses.
  key: string;
  pepper: string;
  env: Record<string, string>;
}

export const CELL = "cell_sc1";
export const ISSUER = "https://auth.prisma.io";
export const CELL_ORIGIN = "https://cell-sc1.example.com";

/// The issuer's environment for the s3:// state `state` and key `key`.
function envFor(state: string, key: string, pepper: string): Record<string, string> {
  const creds = JSON.parse(readFileSync(credentialsOf(state)!, "utf8"));
  const s3 = (prefix: string, t: Record<string, string>) => ({
    [`${prefix}_ENDPOINT`]: t.endpoint,
    [`${prefix}_BUCKET`]: t.bucket,
    [`${prefix}_REGION`]: t.region,
    [`${prefix}_ACCESS_KEY_ID`]: t.accessKeyId,
    [`${prefix}_SECRET_ACCESS_KEY`]: t.secretAccessKey,
  });
  return {
    STREAMS_CELL_ID: CELL,
    STREAMS_ISSUER_ID: ISSUER,
    STREAMS_CELL_ORIGIN: CELL_ORIGIN,
    STREAMS_ISSUER_SIGNING_KEY: readFileSync(key, "utf8"),
    STREAMS_ISSUER_PEPPER: readFileSync(pepper, "utf8"),
    STREAMS_ISSUER_SESSION_KEY: randomBytes(32).toString("base64"),
    STREAMS_ISSUER_OAUTH_CLIENT_ID: "streams-beta-test",
    STREAMS_ISSUER_METRICS_TOKEN: randomBytes(24).toString("base64url"),
    ...s3("STREAMS_CONTROL_S3", creds),
    ...s3("STREAMS_FEEDS_S3", creds.feeds),
    KEEP_AWAKE: "0",
  };
}

/// A cell on the fake S3, initialised by cell-admin with an Ed25519 key.
export async function issuerCell(): Promise<IssuerCell> {
  useStore("s3");
  const root = scratchDir();
  const key = keyFile(root, "ed25519", 0o600, "issuer.pem");
  const pepper = pepperFile(root).path;
  const state = freshState(root, CELL);
  const r = await run(
    "init", "--state", state, "--cell-id", CELL, "--deployment-project", "proj_deploy", "--account", "acct_sink",
    "--key-file", key,
  );
  if (r.code !== 0) throw new Error(`init failed: ${r.err}`);
  return { root, state, key, pepper, env: envFor(state, key, pepper) };
}

/// Admit `project` for workspace `ws_<project>` and give it a credential
/// with a secret; returns the credential id and the presented secret.
export async function customerSecret(
  cell: IssuerCell,
  project: string,
  extra: string[] = [],
): Promise<{ id: string; secret: string }> {
  const admitted = await run("admit", "--state", cell.state, "--project", project, "--workspace", `ws_${project}`);
  if (admitted.code !== 0 && !admitted.err.includes("was used before")) throw new Error(`admit: ${admitted.err}`);
  const out = join(cell.root, `secret-${project}-${randomBytes(4).toString("hex")}`);
  const r = await run(
    "credential", "--state", cell.state, "--project", project, "--pepper-file", cell.pepper, "--out", out, ...extra,
  );
  if (r.code !== 0) throw new Error(`credential: ${r.err}`);
  const presented = readFileSync(out, "utf8").trim();
  const id = /^prisma_streams_(strcred_[0-9a-f]{32})\./.exec(presented)![1];
  return { id, secret: presented };
}

export const cellState = (cell: IssuerCell): CellState => readState(cell.state);

/// A presented secret for `id` that is not its secret.
export const wrongSecret = (id: string) => presentedSecret(id, randomBytes(32).toString("base64url"));

export const envWith = (cell: IssuerCell, patch: Record<string, string | undefined>): Env => ({ ...cell.env, ...patch });

export { fakeS3 };
