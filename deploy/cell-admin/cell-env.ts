// `init --cell-env FILE` (public-beta deploy-q5 triage B4): the four
// per-cell values a cell's stage file (cells/<stage>.env, which the
// Composer entry reads, deploy/composer/streams/env.ts) takes from the
// state `init` makes, so the cell's identity and the `iss` its tokens must
// carry have one source: CELL_ID, PROJECT_ID, ACCOUNT_ID and
// STREAMS_AUTH_ISSUER. They are set by key, in place, and every other line
// stays as it was; the provisioning script writes the rest of the file.
import { existsSync, readFileSync, statSync } from "node:fs";
import { AdminError } from "./errors";
import { parseProfiles } from "./profile";
import { type CellConfig, stageFile } from "./state";

/// A cells file holds no secret (the entry takes every secret from the
/// platform), so a new one is readable by all; an existing one keeps its
/// mode.
const NEW_FILE_MODE = 0o644;

/// The keys `init` sets, in the order it appends the missing ones.
export const CELL_ENV_KEYS = ["CELL_ID", "PROJECT_ID", "ACCOUNT_ID", "STREAMS_AUTH_ISSUER"] as const;
type Values = Record<(typeof CELL_ENV_KEYS)[number], string>;

const valuesOf = (cell: CellConfig): Values => ({
  CELL_ID: cell.cell_id,
  PROJECT_ID: cell.deployment_project_id,
  ACCOUNT_ID: cell.account_id,
  STREAMS_AUTH_ISSUER: cell.issuer,
});

/// `text` with each of `values` set by key: an existing line replaced
/// where it stands, a missing key appended in CELL_ENV_KEYS' order.
/// Refuses a line that is not space-free KEY=VALUE, and a key set twice
/// (which the entry refuses too, and which no single line could update).
function updated(path: string, text: string, values: Values): string {
  parseProfiles([{ path, text }]);
  for (const key of CELL_ENV_KEYS) {
    const value = values[key];
    if (!/^\S+$/.test(value)) {
      throw new AdminError(`${key} ${JSON.stringify(value)} cannot go in ${path}: a cells file's values are space-free`, 2);
    }
  }
  const seen = new Set<string>();
  const lines = text.split("\n").map((raw) => {
    const line = raw.trim();
    if (line === "" || line.startsWith("#")) return raw;
    const key = line.slice(0, line.indexOf("="));
    if (seen.has(key)) throw new AdminError(`${path} sets ${key} twice`);
    seen.add(key);
    return Object.hasOwn(values, key) ? `${key}=${values[key as keyof Values]}` : raw;
  });
  // A file ending in a newline splits into a last "": appended keys go before it.
  if (lines.at(-1) === "") lines.pop();
  const appended = CELL_ENV_KEYS.filter((key) => !seen.has(key)).map((key) => `${key}=${values[key]}`);
  return `${[...lines, ...appended].join("\n")}\n`;
}

/// The cells file at `path` with the cell's four values, staged beside it
/// (stageFile): `init` keeps it once its state is saved, and drops it
/// otherwise.
export function stageCellEnv(path: string, cell: CellConfig): { keep: () => void; drop: () => void } {
  let text = "";
  let mode = NEW_FILE_MODE;
  if (existsSync(path)) {
    try {
      text = readFileSync(path, "utf8");
      mode = statSync(path).mode & 0o777;
    } catch {
      throw new AdminError(`cannot read --cell-env ${path}`);
    }
  }
  return stageFile(path, updated(path, text, valuesOf(cell)), "the cells file", mode);
}
