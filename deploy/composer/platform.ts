// The Prisma Management API as the shared cell's deploy tools call it
// (public-beta deploy-q5 triage B5): set-secrets, deploy-release.sh and,
// in Python, provision-cell.py. The bearer is read from the file
// PRISMA_TOKEN_FILE names, which only its owner may read; it is never an
// argument. Listings are followed through their pagination. A refusal
// names the call, its status and the platform's error code, never a
// request body (a body may carry a secret's value).
import { lstatSync, readFileSync } from "node:fs";
import { AdminError } from "../cell-admin/errors";

export const MANAGEMENT_API = "https://api.prisma.io/v1";

export interface Answer {
  status: number;
  body: any;
}

/// A file only its owner can read: a secret's value, or the Management
/// token. Its text without one trailing newline.
export function ownerOnlyFile(path: string, what: string): string {
  let mode: number;
  try {
    const st = lstatSync(path);
    if (!st.isFile()) throw new AdminError(`${what} ${path} is not a regular file`);
    mode = st.mode;
  } catch (e) {
    if (e instanceof AdminError) throw e;
    throw new AdminError(`${what} ${path} does not exist`);
  }
  if ((mode & 0o077) !== 0) {
    throw new AdminError(`${what} ${path} has mode 0${(mode & 0o777).toString(8)}: only its owner may read it (chmod 600)`);
  }
  const text = readFileSync(path, "utf8").replace(/\r?\n$/, "");
  if (text === "") throw new AdminError(`${what} ${path} is empty`);
  return text;
}

export class Platform {
  private constructor(
    readonly base: string,
    private readonly token: string,
  ) {}

  /// The API at `base` with the token PRISMA_TOKEN_FILE names.
  static open(base: string, env: Readonly<Record<string, string | undefined>> = process.env): Platform {
    const file = env.PRISMA_TOKEN_FILE;
    if (!file) throw new AdminError("PRISMA_TOKEN_FILE must name the file holding the Management API token", 2);
    return new Platform(base.replace(/\/$/, ""), ownerOnlyFile(file, "the token file"));
  }

  /// The token, for the one child that needs it in its environment (the
  /// pinned prisma CLI); never logged.
  get bearer(): string {
    return this.token;
  }

  /// One call; a status outside 2xx and `allow` is a refusal.
  async call(method: string, path: string, body?: unknown, allow: readonly number[] = []): Promise<Answer> {
    let res: Response;
    try {
      res = await fetch(`${this.base}${path}`, {
        method,
        headers: { authorization: `Bearer ${this.token}`, "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(60_000),
      });
    } catch (e) {
      throw new AdminError(`${method} ${path}: ${(e as Error).message}`);
    }
    const text = await res.text();
    let parsed: unknown = null;
    try {
      parsed = text === "" ? null : JSON.parse(text);
    } catch {
      parsed = null;
    }
    if (res.ok || allow.includes(res.status)) return { status: res.status, body: parsed };
    const code = (parsed as { error?: { code?: string } } | null)?.error?.code ?? "no error code";
    throw new AdminError(`${method} ${path} answered ${res.status} (${code})`);
  }

  /// Every item of a paginated listing.
  async list(path: string): Promise<any[]> {
    const out: unknown[] = [];
    let cursor: string | null = null;
    for (let page = 0; page < 200; page++) {
      const sep = path.includes("?") ? "&" : "?";
      const { body } = await this.call("GET", cursor ? `${path}${sep}cursor=${encodeURIComponent(cursor)}` : path);
      out.push(...(body?.data ?? []));
      cursor = body?.pagination?.hasMore ? body.pagination.nextCursor : null;
      if (!cursor) return out;
    }
    throw new AdminError(`GET ${path}: more than 200 pages`);
  }
}

/// A stage's scope for platform variables: the production class for the
/// default stage, a named stage's Branch override otherwise (D0 finding
/// 10: Composer's preflight lets a branchless preview variable through to
/// every named stage, so a named stage's secrets are Branch overrides).
export type Scope = { project: string; class: "production" } | { project: string; class: "preview"; branch: string };

export function scopeOf(stage: string, project: string, branch: string | undefined): Scope {
  if (stage === "production") {
    if (branch !== undefined) throw new AdminError("the production stage is the default stage: it takes no --branch", 2);
    return { project, class: "production" };
  }
  if (branch === undefined) throw new AdminError(`the ${stage} stage is a named stage: --branch names its Branch`, 2);
  return { project, class: "preview", branch };
}

export const scopeQuery = (s: Scope) =>
  `projectId=${encodeURIComponent(s.project)}&class=${s.class}${s.class === "preview" ? `&branchId=${encodeURIComponent(s.branch)}` : ""}`;

export const describeScope = (s: Scope) =>
  s.class === "production" ? `project ${s.project}'s production variables` : `Branch ${s.branch} of project ${s.project}`;

/// A row of the variables listing that belongs to exactly `s`.
export const inScope = (s: Scope, row: any) =>
  row?.projectId === s.project &&
  row?.class === s.class &&
  (s.class === "preview" ? row?.branchId === s.branch : row?.branchId === null || row?.branchId === undefined);

/// The module name an app's app.ts gives Composer, which names the
/// project `prisma deploy` deploys the app into (D0 finding 4).
export function appModuleName(appTs: string): string {
  const m = appTs.match(/^export default module\("([^"]+)",/m);
  if (!m) throw new AdminError("the app's app.ts names no module");
  return m[1];
}

/// The envSecret names an app's app.ts binds, in its order: the names the
/// platform must hold before Composer deploys it.
export function envSecretNames(appTs: string): string[] {
  const names = [...appTs.matchAll(/^\s*\w+: envSecret\("([A-Z0-9_]+)"\),$/gm)].map((m) => m[1]);
  if (names.length === 0) throw new AdminError("the app binds no envSecret");
  return names;
}
