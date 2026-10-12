// The shared cell's child environment (public-beta deploy-q5 triage B2):
// everything the Composer entry (entry.ts) hands streams-slate, built from
// named sources only. Pure: the entry reads every file and passes the
// texts, and nothing here reads the disk or the process environment.
//
// The layers, in order:
//   1. the profiles, compute-1g.env then shared-cell.env, the later
//      file's value winning, checked by cell-admin's own posture check
//      (cellSizing: enforce, the release posture, BILLING_MODE=required,
//      ROLLUP=1, one shard, KEEP_AWAKE=1, no fleet names and no
//      KEEP_AWAKE_UNTIL_MS), so the cell's shape has one copy;
//   2. the stage's cells file (cells/<stage>.env, space-free KEY=VALUE as
//      the profiles), every key of CHILD_NAMES whose source is the cells
//      file and nothing else: an allowlist, so no retired name can enter;
//   3. the seven secret targets, each from its platform name;
//   4. APP_BINARY_SHA256, the shipped binary's sha256 as the entry
//      computed it (the binary echoes it on /v1/debug/load);
//   5. the runtime pass-through, PASS_THROUGH, copied when present (the
//      binary's store client finds a CA bundle through SSL_CERT_FILE and
//      SSL_CERT_DIR, rustls-native-certs; triage D1).
// Each name has one source: the profiles may set none of the names the
// later layers set, and the cells file none the profiles set. KEEP_AWAKE
// (the profiles', which must be 1) and KEEP_AWAKE_UNTIL_MS (only a scratch
// cell's file may set it) are consumed into `keepAwake` and never reach
// the binary, which reads neither. Nothing else of the process environment
// reaches it: not a campaign wrapper's names, not a laced value of a name
// set here.
import { AdminError } from "../../cell-admin/errors";
import { cellSizing, parseProfiles } from "../../cell-admin/profile";

export type ProfileText = { path: string; text: string };

export type Source = { from: "cells"; required: boolean } | { from: "platform"; name: string };
const cell: Source = { from: "cells", required: true };
const optional: Source = { from: "cells", required: false };
const platform = (name: string): Source => ({ from: "platform", name });

/// Every name the child environment takes from somewhere other than the
/// profiles, by source. One anchored literal, one `NAME:` line per name,
/// closed by a line "};": scripts/effective-config reads the names from it
/// (mjs_block_names; triage D19), so it keeps that shape.
export const CHILD_NAMES: Readonly<Record<string, Source>> = {
  CELL_STAGE: cell,
  CELL_ID: cell,
  PROJECT_ID: cell,
  ACCOUNT_ID: cell,
  STREAMS_AUTH_ISSUER: cell,
  PATH_PREFIX: cell,
  REGION: cell,
  SLATE_S3_ENDPOINT: cell,
  SLATE_S3_BUCKET: cell,
  SLATE_S3_REGION: cell,
  FEEDS_S3_ENDPOINT: cell,
  FEEDS_S3_BUCKET: cell,
  FEEDS_S3_REGION: cell,
  FEEDS_S3_KEY: cell,
  RESOLV_OVERRIDE: optional,
  CPU_LOG_SECS: optional,
  RUST_LOG: optional,
  AUTH_TOKEN: platform("STREAMS_OPERATOR_TOKEN"),
  USAGE_STREAM_KEY: platform("STREAMS_USAGE_STREAM_KEY"),
  STREAMS_CURSOR_KEY: platform("STREAMS_CURSOR_KEY"),
  SLATE_S3_ACCESS_KEY_ID: platform("STREAMS_DATA_S3_ACCESS_KEY_ID"),
  SLATE_S3_SECRET_ACCESS_KEY: platform("STREAMS_DATA_S3_SECRET_ACCESS_KEY"),
  FEEDS_S3_ACCESS_KEY_ID: platform("STREAMS_FEEDS_S3_ACCESS_KEY_ID"),
  FEEDS_S3_SECRET_ACCESS_KEY: platform("STREAMS_FEEDS_S3_SECRET_ACCESS_KEY"),
};

/// The runtime names copied through when the instance has them.
export const PASS_THROUGH = ["PATH", "HOME", "SSL_CERT_FILE", "SSL_CERT_DIR"] as const;
/// The binary's own identity, which the entry computes.
const BINARY_SHA256 = "APP_BINARY_SHA256";
/// The keep-awake guard's switch (the profiles') and its end (a scratch
/// cell's file only).
const KEEP_AWAKE = "KEEP_AWAKE";
const KEEP_AWAKE_UNTIL_MS = "KEEP_AWAKE_UNTIL_MS";
export const STAGES = ["scratch", "cert", "production"] as const;

export interface ChildEnvInput {
  /// compute-1g.env then shared-cell.env, as the release carries them.
  profiles: readonly ProfileText[];
  /// cells/<stage>.env.
  cellEnv: ProfileText;
  /// The stage the deploy named (the service's CELL_STAGE input).
  stage: string;
  /// The process environment: the seven platform names and the runtime's.
  platformEnv: Readonly<Record<string, string | undefined>>;
  binarySha256: string;
  /// The port the binary listens on.
  port: string;
}

export interface ChildEnv {
  env: Record<string, string>;
  /// Hold Compute's keep-awake guard, until `untilMs` when it is set.
  keepAwake: { hold: boolean; untilMs: number | null };
}

export type RefusalKind =
  | "input"
  | "stage"
  | "profile"
  | "posture"
  | "duplicate_target"
  | "cells_file"
  | "profile_key"
  | "unknown_key"
  | "missing"
  | "keep_awake"
  | "not_space_free";

/// Why the binary must not start; `message` names keys and files, never a
/// secret's value.
export interface Refusal {
  refused: RefusalKind;
  message: string;
}

class Refused extends Error {
  constructor(
    readonly refused: RefusalKind,
    message: string,
  ) {
    super(message);
  }
}

/// The child environment, or why there is none.
export function buildChildEnv(input: ChildEnvInput): ChildEnv | Refusal {
  try {
    return build(input);
  } catch (e) {
    if (e instanceof Refused) return { refused: e.refused, message: e.message };
    throw e;
  }
}

function build(input: ChildEnvInput): ChildEnv {
  checkInput(input);
  const profiles = layeredProfiles(input.profiles);
  const cells = cellsFile(input.cellEnv, input.stage, profiles);
  const untilMs = keepAwakeEnd(cells, input.cellEnv.path, input.stage);
  const secrets = platformSecrets(input.platformEnv);
  profiles.delete(KEEP_AWAKE);
  cells.delete(KEEP_AWAKE_UNTIL_MS);
  const env: Record<string, string> = { ...Object.fromEntries(profiles), ...Object.fromEntries(cells), ...secrets };
  env[BINARY_SHA256] = input.binarySha256;
  for (const name of PASS_THROUGH) {
    const value = input.platformEnv[name];
    if (value !== undefined) env[name] = value;
  }
  return { env, keepAwake: { hold: true, untilMs } };
}

function checkInput({ binarySha256, port, stage }: ChildEnvInput): void {
  if (!/^[0-9a-f]{64}$/.test(binarySha256)) {
    throw new Refused("input", `the binary's sha256 ${binarySha256} is not 64 lowercase hex digits`);
  }
  if (!/^[0-9]{1,5}$/.test(port) || Number(port) < 1 || Number(port) > 65535) {
    throw new Refused("input", `PORT ${port} is not a TCP port`);
  }
  if (!(STAGES as readonly string[]).includes(stage)) {
    throw new Refused("stage", `CELL_STAGE ${stage} is not scratch, cert or production`);
  }
}

/// Where a name the profiles may not set comes from, if anywhere.
function sourceOf(name: string): string | undefined {
  const source = CHILD_NAMES[name];
  if (source?.from === "platform") return `the platform's ${source.name}`;
  if (source?.from === "cells" || name === KEEP_AWAKE_UNTIL_MS) return "its cells file";
  if (name === BINARY_SHA256) return "the binary it ships";
  if ((PASS_THROUGH as readonly string[]).includes(name)) return "its runtime";
  return undefined;
}

/// Run one of cell-admin's checks, its refusal as `kind`.
function adminCheck<T>(kind: RefusalKind, check: () => T): T {
  try {
    return check();
  } catch (e) {
    if (e instanceof AdminError) throw new Refused(kind, e.message);
    throw e;
  }
}

function layeredProfiles(files: readonly ProfileText[]): Map<string, string> {
  const env = adminCheck("profile", () => parseProfiles(files));
  adminCheck("posture", () => cellSizing(env));
  for (const name of env.keys()) {
    const source = sourceOf(name);
    if (source) throw new Refused("duplicate_target", `the profiles set ${name}, which the cell takes from ${source}`);
  }
  return env;
}

/// The cells file's keys, checked against the allowlist, the profiles and
/// the stage.
function cellsFile(file: ProfileText, stage: string, profiles: Map<string, string>): Map<string, string> {
  const cells = adminCheck("cells_file", () => parseProfiles([file]));
  const seen = new Set<string>();
  for (const raw of file.text.split("\n")) {
    const line = raw.trim();
    if (line === "" || line.startsWith("#")) continue;
    const key = line.slice(0, line.indexOf("="));
    if (seen.has(key)) throw new Refused("cells_file", `${file.path} sets ${key} twice`);
    seen.add(key);
  }
  for (const key of cells.keys()) {
    if (profiles.has(key)) throw new Refused("profile_key", `${file.path} sets ${key}, which the profiles set`);
    if (CHILD_NAMES[key]?.from !== "cells" && key !== KEEP_AWAKE_UNTIL_MS) {
      throw new Refused("unknown_key", `${file.path} sets ${key}, which is not a cells-file key`);
    }
  }
  for (const [key, value] of cells) if (value === "") cells.delete(key);
  const missing = Object.entries(CHILD_NAMES)
    .filter(([key, source]) => source.from === "cells" && source.required && !cells.has(key))
    .map(([key]) => key);
  if (missing.length) throw new Refused("missing", `${file.path} does not set ${missing.join(", ")}`);
  if (cells.get("CELL_STAGE") !== stage) {
    throw new Refused("stage", `${file.path} names CELL_STAGE ${cells.get("CELL_STAGE")}, not ${stage}`);
  }
  return cells;
}

/// KEEP_AWAKE_UNTIL_MS: a scratch cell's guard may end at that instant;
/// every other stage is kept awake for good (edge #140).
function keepAwakeEnd(cells: Map<string, string>, path: string, stage: string): number | null {
  const raw = cells.get(KEEP_AWAKE_UNTIL_MS);
  if (raw === undefined) return null;
  if (stage !== "scratch") {
    throw new Refused(
      "keep_awake",
      `${path} sets ${KEEP_AWAKE_UNTIL_MS}: only a scratch cell's keep-awake may end; a ${stage} cell is kept awake for good`,
    );
  }
  if (!/^[1-9][0-9]*$/.test(raw) || !Number.isSafeInteger(Number(raw))) {
    throw new Refused("keep_awake", `${path}: ${KEEP_AWAKE_UNTIL_MS}=${raw} is not a time in epoch milliseconds`);
  }
  return Number(raw);
}

/// The seven secret targets from their platform names; a refusal names the
/// platform name, never the value.
function platformSecrets(platformEnv: ChildEnvInput["platformEnv"]): Record<string, string> {
  const out: Record<string, string> = {};
  const missing: string[] = [];
  for (const [target, source] of Object.entries(CHILD_NAMES)) {
    if (source.from !== "platform") continue;
    const value = platformEnv[source.name];
    if (value === undefined || value === "") missing.push(source.name);
    else if (/\s/.test(value)) throw new Refused("not_space_free", `the platform's ${source.name} is not space-free`);
    else out[target] = value;
  }
  if (missing.length) throw new Refused("missing", `the platform provides no ${missing.join(", ")}`);
  return out;
}
