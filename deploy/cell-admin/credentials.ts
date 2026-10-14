// Credentials the owner uses (public-beta issuer triage B1.8):
// `credential` adds a customer credential to a project and writes its
// secret, once, to a new file only its owner can read; the state keeps the
// secret's peppered hash (secrets.ts), exactly as the issuer stores the
// credentials it creates, so the issuer's exchange serves both. `canary`
// mints a token for the canary's catalog-read credential to prove the cell
// serves a key (the rotation drill's check, design §7.3). `export` writes
// what the platform's Stage 1 imports when it replaces the interim issuer.
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import type { Flags } from "./args";
import { call, cellBase, describeReply } from "./cell-client";
import type { Io } from "./commands";
import { AdminError, notYet } from "./errors";
import { loadChecked, mutate, unpublishedMessage } from "./publish";
import {
  addCredential,
  canaryProbeCredential,
  isCanary,
  placedProjects,
  prefixesOf,
  randomCredentialId,
  scopesOf,
} from "./rules";
import { newSecret, presentedSecret, readPepperFile, secretFields } from "./secrets";
import { type CellState, journal, stageFile, writeDurable } from "./state";
import { openStore, publishNote } from "./stores";
import { cellSigningKey, customerClaims, signJwt } from "./tokens";

/// A canary token lives 5 minutes (design §5.1).
const CANARY_TOKEN_TTL_SECS = 300;
const CANARY_POLL_MS = 2_000;

export async function credential(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const project = f.must("project");
  const out = resolve(f.must("out"));
  const hidden = f.has("hidden");
  if (hidden !== isCanary(project)) {
    throw new AdminError(
      hidden ? "--hidden is for the canary's credentials only" : "the canary's credentials are hidden: pass --hidden",
    );
  }
  if (existsSync(out)) throw new AdminError(`--out ${out} exists: a secret is written once, to a new file`);
  const peppers = readPepperFile(resolve(f.must("pepper-file")));
  const scopes = scopesOf(f.get("scopes"));
  const stream_prefixes = prefixesOf(f.all("prefix"));
  const secret = newSecret();
  const fields = secretFields(peppers, secret);
  // The id is chosen before the commit: the secret file names it. The
  // secret is staged beside `out` and moved there once the commit has
  // landed, published or not: a committed credential's secret is never lost.
  const id = randomCredentialId();
  const staged = stageFile(out, `${presentedSecret(id, secret)}\n`, "the secret");
  await store.exclusive(async () => {
    let why: Error | undefined;
    let state: CellState;
    try {
      ({ state } = await mutate(
        store,
        (s) => {
          const display_name = f.get("name");
          addCredential(s, project, {
            scopes,
            stream_prefixes,
            credential: { id, ...fields, ...(display_name ? { display_name } : {}), ...(hidden ? { hidden } : {}) },
          });
          journal(s, "credential", `${project} credential=${id}${hidden ? " hidden" : ""}`);
        },
        undefined,
        (e) => {
          why = e;
        },
      ));
    } catch (e) {
      staged.drop();
      throw e;
    }
    staged.keep();
    io.out(`credential ${id} for ${project} (${scopes}), hint ${fields.hint}`);
    io.out(`its secret is in ${out} (mode 0600) and nowhere else: hand the file over, never paste it`);
    if (why !== undefined) throw new AdminError(unpublishedMessage(state, why));
    io.out(`bundle: ${store.bundlePath} (feed_version ${state.feed_version}); ${publishNote(store)}`);
  });
}

/// Mint a canary token under `--key-file` (a key the cell's state holds)
/// and send `GET /v1/streams?limit=1` until it answers 200, for at most
/// `--wait-secs` (default 120). A 200 proves the cell serves that key and
/// a grants generation holding the canary credential's current version;
/// anything else after the wait is exit 3 (publish, wait, run again).
/// Each probe mints from the state as it is then: offboarding a suspended
/// project moves the credential's grant_version (its witness, offboard.ts),
/// and a token at the version a drill began with would never answer 200
/// once the cell takes that bundle.
export async function canary(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const base = cellBase(f.must("cell-url"));
  const waitMs = f.count("wait-secs", 120) * 1000;
  let { state } = await loadChecked(store);
  const sk = cellSigningKey(state, f.must("key-file"));
  const deadline = Date.now() + waitMs;
  for (;;) {
    const id = canaryProbeCredential(state);
    const token = signJwt(sk, customerClaims(state, id, CANARY_TOKEN_TTL_SECS, "cell-admin-canary"));
    const reply = await call(base, "GET", "/v1/streams?limit=1", token);
    if (reply.status === 200) {
      io.out(`canary: the cell serves key ${sk.record.kid}: GET /v1/streams?limit=1 answered 200`);
      return;
    }
    if (Date.now() + CANARY_POLL_MS > deadline) {
      throw notYet(
        `the canary answered ${describeReply(reply)} for ${waitMs / 1000} s, not 200: the cell does not serve ` +
          `kid ${sk.record.kid} yet; publish the bundle, wait, run again`,
      );
    }
    await new Promise((r) => setTimeout(r, CANARY_POLL_MS));
    ({ state } = await loadChecked(store));
  }
}

/// The Stage 1 import set (design §16.4): every active customer project
/// (suspended ones included; offboarding ones and the canary not) with
/// its versions and quotas, and its customer credentials that are not
/// omitted, with their grant versions and, for those with a secret, the
/// secret's hash and pepper version. Written to `--out` with mode 0600.
export async function exportCell(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const out = resolve(f.must("out"));
  const { state } = await loadChecked(store);
  const projects = placedProjects(state)
    .filter(([, p]) => p.phase === "active")
    .sort(([a], [b]) => (a < b ? -1 : 1))
    .map(([id, p]) => ({
      project_id: id,
      workspace_id: p.workspace_id,
      status: p.status,
      project_policy_version: p.project_policy_version,
      ownership_version: p.ownership_version,
      quotas: { ...p.quotas },
      credentials: Object.entries(state.credentials)
        .filter(([, c]) => c.project_id === id && c.role === "customer" && !c.omitted)
        .sort(([a], [b]) => (a < b ? -1 : 1))
        .map(([cid, c]) => ({
          credential_id: cid,
          grant_version: c.grant_version,
          status: c.status,
          scopes: c.scopes,
          ...(c.stream_prefixes ? { stream_prefixes: [...c.stream_prefixes] } : {}),
          expires_at: c.expires_at,
          ...(c.secret_hash !== undefined ? { secret_hash: c.secret_hash, hv: c.hv } : {}),
        })),
    }));
  const doc = { format: "streams-cell-admin-export/v1", cell_id: state.cell.cell_id, feed_version: state.feed_version, projects };
  writeDurable(out, `${JSON.stringify(doc, null, 2)}\n`);
  io.out(`export of cell ${state.cell.cell_id} at feed_version ${state.feed_version}: ${projects.length} project(s)`);
  io.out(`written to ${out} (mode 0600); it holds credentials' secret hashes: keep it like the state`);
}
