// A fake Prisma Management API for the deploy tools' offline self-tests
// (set-secrets, deploy-release.sh, provision-cell.py): the calls those
// tools make, answered in the shapes of @prisma/management-api-sdk 1.80.0
// (projects, branches, environment variables, buckets and bucket keys),
// listings paged two items at a time so every caller follows pagination.
// It keeps every request it served; a variable's value is kept apart, as
// the platform keeps it, and never answered.
import { randomBytes } from "node:crypto";

export interface Project {
  id: string;
  name: string;
  defaultRegion: string | null;
  /// The workspace the project is in (answered as workspace.id).
  workspaceId: string;
  /// The id Composer gives a project it creates (its app's name); null on
  /// one made otherwise (provision-cell.py, the Console).
  logicalId: string | null;
  createdAt: string;
}
export interface Branch {
  id: string;
  projectId: string;
  gitName: string;
  role: "production" | "preview";
  /// Composer 0.29.1 finds the default stage's Branch by this flag.
  isDefault: boolean;
}
export interface Variable {
  id: string;
  projectId: string;
  branchId: string | null;
  class: "production" | "preview";
  key: string;
  updatedAt: string;
}
export interface Bucket {
  id: string;
  projectId: string;
  name: string;
}
export interface BucketKey {
  id: string;
  bucketId: string;
  name: string;
  role: "read" | "read_write";
}
export interface Served {
  method: string;
  path: string;
  status: number;
}

const PAGE = 2;

export class FakePlatform {
  readonly token = randomBytes(16).toString("hex");
  projects: Project[] = [];
  branches: Branch[] = [];
  variables: Variable[] = [];
  /// Each variable's value, by its id.
  values = new Map<string, string>();
  buckets: Bucket[] = [];
  keys: BucketKey[] = [];
  requests: Served[] = [];
  /// The next POST of this path is applied but its answer lost (a 502
  /// with no body), as a dropped connection would leave it.
  loseNextAnswer?: string;
  /// The role the platform gives a project's first Branch.
  firstBranchRole: Branch["role"] = "production";
  private seq = 0;
  private server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: (req) => this.serve(req) });

  get url(): string {
    return `http://127.0.0.1:${this.server.port}/v1`;
  }

  stop(): void {
    this.server.stop(true);
  }

  id(prefix: string): string {
    this.seq += 1;
    return `${prefix}_${this.seq}`;
  }

  /// A project in workspace wksp_1 unless `o` says otherwise, created
  /// after every earlier one unless `o.createdAt` says otherwise.
  addProject(name: string, defaultRegion: string | null, o: Partial<Pick<Project, "workspaceId" | "logicalId" | "createdAt">> = {}): Project {
    const id = this.id("proj");
    const p = {
      id, name, defaultRegion, workspaceId: "wksp_1", logicalId: null,
      createdAt: new Date(1_760_000_000_000 + this.seq * 1000).toISOString(), ...o,
    };
    this.projects.push(p);
    return p;
  }

  addBranch(projectId: string, gitName: string): Branch {
    const role = this.branches.some((b) => b.projectId === projectId) ? "preview" : this.firstBranchRole;
    const b: Branch = { id: this.id("br"), projectId, gitName, role, isDefault: role === "production" };
    this.branches.push(b);
    return b;
  }

  /// Set a variable directly, as another tool would have.
  addVariable(v: Omit<Variable, "id" | "updatedAt">, value: string): Variable {
    const row = { ...v, id: this.id("env"), updatedAt: new Date().toISOString() };
    this.variables.push(row);
    this.values.set(row.id, value);
    return row;
  }

  /// The value a scope holds for `key`, or undefined.
  valueOf(projectId: string, cls: string, branchId: string | null, key: string): string | undefined {
    const row = this.variables.find(
      (v) => v.projectId === projectId && v.class === cls && v.branchId === branchId && v.key === key,
    );
    return row ? this.values.get(row.id) : undefined;
  }

  private json(status: number, body: unknown): Response {
    return new Response(body === undefined ? null : JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    });
  }

  private error(status: number, code: string): Response {
    return this.json(status, { error: { code, message: code } });
  }

  /// One page of `items` after `cursor` (an index).
  private page(items: unknown[], url: URL): Response {
    const from = Number(url.searchParams.get("cursor") ?? "0");
    const data = items.slice(from, from + PAGE);
    const more = from + PAGE < items.length;
    return this.json(200, { data, pagination: { nextCursor: more ? String(from + PAGE) : null, hasMore: more } });
  }

  private async serve(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const res = await this.route(req, url);
    this.requests.push({ method: req.method, path: url.pathname + url.search, status: res.status });
    if (req.method === "POST" && this.loseNextAnswer === url.pathname) {
      this.loseNextAnswer = undefined;
      return new Response(null, { status: 502 });
    }
    return res;
  }

  private async route(req: Request, url: URL): Promise<Response> {
    if (req.headers.get("authorization") !== `Bearer ${this.token}`) return this.error(401, "unauthorized");
    const parts = url.pathname.replace(/^\/v1\//, "").split("/");
    const body = req.method === "POST" || req.method === "PATCH" ? await req.json() : undefined;
    const q = (k: string) => url.searchParams.get(k);
    const key = `${req.method} ${parts.map((p, i) => (i % 2 === 1 ? "*" : p)).join("/")}`;
    switch (key) {
      case "GET projects":
        return this.page(this.projects.map((p) => this.project(p)), url);
      case "POST projects": {
        const p = this.addProject(body.name, body.region ?? null, {
          ...(body.workspaceId ? { workspaceId: body.workspaceId } : {}), logicalId: body.logicalId ?? null,
        });
        return this.json(201, { data: this.project(p) });
      }
      case "GET projects/*": {
        const p = this.projects.find((x) => x.id === parts[1]);
        return p ? this.json(200, { data: this.project(p) }) : this.error(404, "not_found");
      }
      case "GET projects/*/branches":
        return this.page(
          this.branches.filter((b) => b.projectId === parts[1] && (q("gitName") === null || b.gitName === q("gitName"))),
          url,
        );
      case "POST projects/*/branches":
        if (!this.projects.some((p) => p.id === parts[1])) return this.error(404, "not_found");
        return this.json(201, { data: this.addBranch(parts[1], body.gitName) });
      case "GET environment-variables":
        return this.page(
          this.variables.filter(
            (v) =>
              (q("projectId") === null || v.projectId === q("projectId")) &&
              (q("class") === null || v.class === q("class")) &&
              (q("key") === null || v.key === q("key")) &&
              (q("branchId") === null || v.branchId === q("branchId")),
          ),
          url,
        );
      case "POST environment-variables": {
        const branchId = body.branchId ?? null;
        if (this.valueOf(body.projectId, body.class, branchId, body.key) !== undefined) return this.error(409, "conflict");
        const row = this.addVariable({ projectId: body.projectId, branchId, class: body.class, key: body.key }, body.value);
        return this.json(201, { data: row });
      }
      case "PATCH environment-variables/*": {
        const row = this.variables.find((v) => v.id === parts[1]);
        if (!row) return this.error(404, "not_found");
        this.values.set(row.id, body.value);
        row.updatedAt = new Date().toISOString();
        return this.json(200, { data: row });
      }
      case "GET buckets":
        return this.page(
          this.buckets.filter((b) => q("projectId") === null || b.projectId === q("projectId")).map((b) => this.bucket(b)),
          url,
        );
      case "POST buckets": {
        const b = { id: this.id("bkt"), projectId: body.projectId, name: body.name };
        this.buckets.push(b);
        return this.json(201, { data: this.bucket(b) });
      }
      case "DELETE buckets/*": {
        const at = this.buckets.findIndex((b) => b.id === parts[1]);
        if (at < 0) return this.error(404, "not_found");
        this.buckets.splice(at, 1);
        this.keys = this.keys.filter((k) => k.bucketId !== parts[1]);
        return new Response(null, { status: 204 });
      }
      case "GET buckets/*/keys":
        return this.page(this.keys.filter((k) => k.bucketId === parts[1]), url);
      case "POST buckets/*/keys": {
        const b = this.buckets.find((x) => x.id === parts[1]);
        if (!b) return this.error(404, "not_found");
        const k: BucketKey = { id: this.id("key"), bucketId: b.id, name: body.name, role: body.role };
        this.keys.push(k);
        return this.json(201, {
          data: {
            ...k, type: "bucketKey", accessKeyId: `AK${k.id}`, secretAccessKey: `SK${k.id}${randomBytes(4).toString("hex")}`,
            endpoint: "https://t3.storage.example", bucketName: `phys-${b.name}`,
          },
        });
      }
      case "DELETE buckets/*/keys/*": {
        const at = this.keys.findIndex((k) => k.bucketId === parts[1] && k.id === parts[3]);
        if (at < 0) return this.error(404, "not_found");
        this.keys.splice(at, 1);
        return new Response(null, { status: 204 });
      }
      default:
        return this.error(404, "no_such_route");
    }
  }

  private project({ workspaceId, ...p }: Project) {
    return { ...p, type: "project", workspace: { id: workspaceId, name: "streams" } };
  }

  private bucket(b: Bucket) {
    return { id: b.id, type: "bucket", name: b.name, project: { id: b.projectId }, branchId: null };
  }
}
