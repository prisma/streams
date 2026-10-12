// Shared rig for the issuer's tests: a cell initialised by cell-admin on
// the in-process fake S3 (deploy/cell-admin/fake-s3.ts), with an Ed25519
// signing key and a pepper made for the test only, the environment the
// issuer would be given for it, and an issuer booted on it that the
// management tests call as a signed-in browser.
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { presentedSecret } from "../cell-admin/secrets";
import type { CellState } from "../cell-admin/state";
import {
  auditTexts,
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
import { boot } from "./boot";
import { StateCache } from "./cache";
import { type Env, readConfig } from "./config";
import { Issuer } from "./server";
import { csrfToken, newSession, readSession, type Session, sessionCookie } from "./session";

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
/// Where the issuer itself is served: the sign-in's callback is here.
export const ISSUER_ORIGIN = "https://issuer.example.com";

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
    STREAMS_ISSUER_ORIGIN: ISSUER_ORIGIN,
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

/// The management audit objects the control bucket holds, oldest first.
export const managementAudits = (cell: IssuerCell) =>
  auditTexts(cell.state).map((t) => JSON.parse(t)).filter((d) => d.kind === "management");

/// Run cell-admin on `cell`'s state, or throw with its error.
export async function admin(cell: IssuerCell, command: string, ...args: string[]): Promise<string> {
  const r = await run(command, "--state", cell.state, ...args);
  if (r.code !== 0) throw new Error(`${command}: ${r.err}`);
  return r.out;
}

/// A presented secret for `id` that is not its secret.
export const wrongSecret = (id: string) => presentedSecret(id, randomBytes(32).toString("base64url"));

export const envWith = (cell: IssuerCell, patch: Record<string, string | undefined>): Env => ({ ...cell.env, ...patch });

export { fakeS3 };

/// When the management tests' clock starts.
export const T0 = 1_800_000_000_000;

/// An issuer booted on `cell` with a clock the test moves, and the
/// management API's calls made as a browser signed in to a workspace.
export async function managed(cell: IssuerCell, env: Env = cell.env) {
  const clock = { t: T0 };
  const now = () => clock.t;
  const cfg = readConfig(env);
  const b = await boot(cfg);
  const cache = new StateCache(b.store, b, { now, log: () => {} });
  const issuer = new Issuer(cfg, { store: b.store, cache, now, log: () => {} });
  const session = (workspace: string, patch: Partial<Session> = {}): Session => ({
    ...newSession({ user_id: `usr_${workspace}`, email: `owner@${workspace}.example.com`, workspace_id: workspace }, clock.t),
    ...patch,
  });
  /// The Cookie header of a new session signed in to `workspace`.
  const signIn = (workspace: string, patch: Partial<Session> = {}) =>
    sessionCookie(cfg.sessionKey, session(workspace, patch)).split(";")[0];
  // Every call takes 1 ms of the clock, so audit objects (keyed by the
  // millisecond) sort in the order of the calls that wrote them.
  const call = (method: string, path: string, cookie?: string, init: { body?: string; headers?: Record<string, string> } = {}) => {
    clock.t += 1;
    return issuer.fetch(
      new Request(`${ISSUER_ORIGIN}${path}`, {
        method,
        headers: { ...(cookie === undefined ? {} : { cookie }), ...init.headers },
        body: init.body,
      }),
    );
  };
  const get = (path: string, cookie?: string) => call("GET", path, cookie);
  /// A change as the issuer's page asks for it: JSON, with the CSRF value
  /// of the cookie's session.
  const send = (method: string, path: string, cookie: string, body?: unknown) => {
    const sid = readSession(cfg.sessionKey, new Request(`${ISSUER_ORIGIN}${path}`, { headers: { cookie } }), clock.t)?.sid;
    return call(method, path, cookie, {
      body: body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body),
      headers: { "content-type": "application/json", "x-csrf-token": csrfToken(cfg.sessionKey, sid ?? "") },
    });
  };
  return { cfg, cache, clock, issuer, store: b.store, session, signIn, call, get, send };
}
