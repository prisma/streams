import { timingSafeEqual } from "node:crypto";
import { Result } from "better-result";

export type AuthConfig =
  | {
      mode: "none";
    }
  | {
      mode: "api-key";
      apiKeyBytes: Buffer;
    }
  | {
      mode: "scoped-api-key";
      keys: ScopedApiKey[];
    };

export type AuthPermission = "read" | "write" | "admin";

export type StreamScope =
  | {
      kind: "all";
      pattern: "*";
    }
  | {
      kind: "exact";
      pattern: string;
      stream: string;
    }
  | {
      kind: "prefix";
      pattern: string;
      prefix: string;
    };

export type ScopedApiKey = {
  keyBytes: Buffer;
  permissions: ReadonlySet<AuthPermission>;
  scopes: StreamScope[];
};

export type AuthConfigError = {
  message: string;
};

const API_KEY_MIN_LENGTH = 10;
const SCOPED_KEYS_ENV = "DS_AUTH_SCOPED_KEYS_JSON";

function hasFlagArg(args: string[], flag: string): boolean {
  return args.includes(flag);
}

function valuesForOption(args: string[], option: string): string[] {
  const values: string[] = [];
  const prefix = `${option}=`;
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === option) {
      const value = args[i + 1];
      if (value != null && !value.startsWith("--")) {
        values.push(value);
        i += 1;
      } else {
        values.push("");
      }
      continue;
    }
    if (arg.startsWith(prefix)) {
      values.push(arg.slice(prefix.length));
    }
  }
  return values;
}

function authError<T>(message: string): Result<T, AuthConfigError> {
  return Result.err({ message });
}

function authConfigError(message: string): Result<AuthConfig, AuthConfigError> {
  return authError(message);
}

function readPermission(value: unknown): Result<AuthPermission, AuthConfigError> {
  if (value === "read" || value === "write" || value === "admin") return Result.ok(value);
  return authError("scoped auth permissions must be read, write, or admin");
}

function expandPermissions(raw: AuthPermission[]): ReadonlySet<AuthPermission> {
  const permissions = new Set<AuthPermission>();
  for (const permission of raw) {
    permissions.add(permission);
    if (permission === "write") permissions.add("read");
    if (permission === "admin") {
      permissions.add("read");
      permissions.add("write");
    }
  }
  return permissions;
}

function hasUnsafeStreamPatternParts(value: string, opts: { allowTrailingSlash?: boolean } = {}): boolean {
  const parts = value.split("/");
  if (opts.allowTrailingSlash && parts[parts.length - 1] === "") parts.pop();
  return parts.some((part) => part === "" || part === "." || part === "..");
}

function parseStreamScopeResult(value: unknown): Result<StreamScope, AuthConfigError> {
  if (typeof value !== "string" || value.trim() === "") {
    return authError("scoped auth streams must be non-empty strings");
  }
  const pattern = value.trim();
  if (pattern === "*") return Result.ok({ kind: "all", pattern });
  if (pattern.includes("\0") || pattern.includes("\\") || pattern.startsWith("/") || pattern.endsWith("/")) {
    return authError(`invalid scoped auth stream pattern: ${pattern}`);
  }
  const starIndex = pattern.indexOf("*");
  if (starIndex === -1) {
    if (hasUnsafeStreamPatternParts(pattern)) return authError(`invalid scoped auth stream pattern: ${pattern}`);
    return Result.ok({ kind: "exact", pattern, stream: pattern });
  }
  if (starIndex !== pattern.length - 1 || pattern.indexOf("*", starIndex + 1) !== -1) {
    return authError(`invalid scoped auth stream pattern: ${pattern}`);
  }
  const prefix = pattern.slice(0, -1);
  if (prefix.length === 0 || !prefix.endsWith("/") || hasUnsafeStreamPatternParts(prefix, { allowTrailingSlash: true })) {
    return authError(`invalid scoped auth stream pattern: ${pattern}`);
  }
  return Result.ok({ kind: "prefix", pattern, prefix });
}

function parseScopedApiKeysResult(env: NodeJS.ProcessEnv): Result<ScopedApiKey[], AuthConfigError> {
  const raw = env[SCOPED_KEYS_ENV];
  if (raw == null || raw.trim() === "") return authError(`${SCOPED_KEYS_ENV} must be a non-empty JSON array`);
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return authError(`${SCOPED_KEYS_ENV} must be valid JSON`);
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    return authError(`${SCOPED_KEYS_ENV} must be a non-empty JSON array`);
  }

  const seenKeys = new Set<string>();
  const keys: ScopedApiKey[] = [];
  for (const [index, item] of parsed.entries()) {
    if (!item || typeof item !== "object" || Array.isArray(item)) {
      return authError(`scoped auth key at index ${index} must be an object`);
    }
    const spec = item as Record<string, unknown>;
    const unknownKeys = Object.keys(spec).filter((key) => key !== "key" && key !== "streams" && key !== "permissions");
    if (unknownKeys.length > 0) {
      return authError(`unknown scoped auth key fields: ${unknownKeys.sort().join(", ")}`);
    }
    if (typeof spec.key !== "string" || spec.key.length < API_KEY_MIN_LENGTH) {
      return authError(`scoped auth key at index ${index} must contain key with at least ${API_KEY_MIN_LENGTH} characters`);
    }
    if (seenKeys.has(spec.key)) return authError("duplicate scoped auth key");
    seenKeys.add(spec.key);

    if (!Array.isArray(spec.permissions) || spec.permissions.length === 0) {
      return authError(`scoped auth key at index ${index} must contain non-empty permissions`);
    }
    const rawPermissions: AuthPermission[] = [];
    for (const permission of spec.permissions) {
      const permissionRes = readPermission(permission);
      if (Result.isError(permissionRes)) return permissionRes;
      rawPermissions.push(permissionRes.value);
    }

    if (!Array.isArray(spec.streams) || spec.streams.length === 0) {
      return authError(`scoped auth key at index ${index} must contain non-empty streams`);
    }
    const scopes: StreamScope[] = [];
    for (const stream of spec.streams) {
      const scopeRes = parseStreamScopeResult(stream);
      if (Result.isError(scopeRes)) return scopeRes;
      scopes.push(scopeRes.value);
    }

    keys.push({
      keyBytes: Buffer.from(spec.key, "utf8"),
      permissions: expandPermissions(rawPermissions),
      scopes,
    });
  }
  return Result.ok(keys);
}

export function parseAuthConfigResult(args: string[], env: NodeJS.ProcessEnv = process.env): Result<AuthConfig, AuthConfigError> {
  const noAuth = hasFlagArg(args, "--no-auth");
  const authStrategyValues = valuesForOption(args, "--auth-strategy");
  const hasAuthStrategy = authStrategyValues.length > 0;

  if (noAuth && hasAuthStrategy) {
    return authConfigError("invalid auth configuration: provide exactly one of --no-auth or --auth-strategy api-key");
  }
  if (!noAuth && !hasAuthStrategy) {
    return authConfigError("missing auth configuration: expected --no-auth or --auth-strategy api-key");
  }
  if (noAuth) {
    return Result.ok({ mode: "none" });
  }
  if (authStrategyValues.length !== 1 || (authStrategyValues[0] !== "api-key" && authStrategyValues[0] !== "scoped-api-key")) {
    return authConfigError("invalid --auth-strategy (expected: api-key or scoped-api-key)");
  }

  if (authStrategyValues[0] === "scoped-api-key") {
    const scopedKeysRes = parseScopedApiKeysResult(env);
    if (Result.isError(scopedKeysRes)) return scopedKeysRes;
    return Result.ok({
      mode: "scoped-api-key",
      keys: scopedKeysRes.value,
    });
  }

  const apiKey = env.API_KEY;
  if (apiKey == null || apiKey.length < API_KEY_MIN_LENGTH) {
    return authConfigError(`API_KEY must be present and contain at least ${API_KEY_MIN_LENGTH} characters`);
  }

  return Result.ok({
    mode: "api-key",
    apiKeyBytes: Buffer.from(apiKey, "utf8"),
  });
}

function unauthorized(): Response {
  return new Response(
    JSON.stringify({
      error: {
        code: "unauthorized",
        message: "unauthorized",
      },
    }),
    {
      status: 401,
      headers: {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store",
        "x-content-type-options": "nosniff",
        "www-authenticate": "Bearer",
      },
    }
  );
}

function forbidden(): Response {
  return new Response(
    JSON.stringify({
      error: {
        code: "forbidden",
        message: "forbidden",
      },
    }),
    {
      status: 403,
      headers: {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store",
        "x-content-type-options": "nosniff",
      },
    }
  );
}

function parseBearerCredential(header: string | null): string | null {
  if (header == null) return null;
  const match = /^Bearer (.+)$/i.exec(header);
  return match?.[1] ?? null;
}

function keyBytesMatch(expected: Buffer, credential: string): boolean {
  const credentialBytes = Buffer.from(credential, "utf8");
  const paddedCredential = Buffer.alloc(expected.length);
  credentialBytes.copy(paddedCredential, 0, 0, Math.min(credentialBytes.length, paddedCredential.length));
  const bytesMatch = timingSafeEqual(expected, paddedCredential);
  return credentialBytes.length === expected.length && bytesMatch;
}

function credentialsMatch(config: Extract<AuthConfig, { mode: "api-key" }>, credential: string): boolean {
  return keyBytesMatch(config.apiKeyBytes, credential);
}

function matchingScopedKey(config: Extract<AuthConfig, { mode: "scoped-api-key" }>, credential: string): ScopedApiKey | null {
  let matched: ScopedApiKey | null = null;
  for (const key of config.keys) {
    if (keyBytesMatch(key.keyBytes, credential)) matched = key;
  }
  return matched;
}

function decodeStreamName(raw: string): string | null {
  try {
    const stream = decodeURIComponent(raw);
    if (stream.includes("\0") || stream.includes("\\")) return null;
    const parts = stream.split("/");
    if (parts.some((part) => part === "" || part === "." || part === "..")) return null;
    return stream;
  } catch {
    return null;
  }
}

function parseTopLevelGitStream(path: string): { stream: string; permission: AuthPermission } | null {
  const infoRefsSuffix = ".git/info/refs";
  if (path.endsWith(infoRefsSuffix)) {
    const stream = decodeStreamName(path.slice(1, -infoRefsSuffix.length));
    return stream ? { stream, permission: "read" } : null;
  }
  const uploadPackSuffix = ".git/git-upload-pack";
  if (path.endsWith(uploadPackSuffix)) {
    const stream = decodeStreamName(path.slice(1, -uploadPackSuffix.length));
    return stream ? { stream, permission: "read" } : null;
  }
  const receivePackSuffix = ".git/git-receive-pack";
  if (path.endsWith(receivePackSuffix)) {
    const stream = decodeStreamName(path.slice(1, -receivePackSuffix.length));
    return stream ? { stream, permission: "write" } : null;
  }
  return null;
}

function permissionForGitProfileRoute(method: string, segments: string[], url: URL): AuthPermission {
  const first = segments[0] ?? "";
  const second = segments[1] ?? "";
  const third = segments[2] ?? "";
  if (first === "smart" && second === "info" && third === "refs") {
    return url.searchParams.get("service") === "git-receive-pack" ? "write" : "read";
  }
  if (first === "smart" && second === "git-receive-pack") return "write";
  if (first === "smart" && second === "git-upload-pack") return "read";
  if (method === "GET" || method === "HEAD") return "read";
  if (first === "maintenance" || first === "import") return "admin";
  return "write";
}

function permissionForWorkspaceFsProfileRoute(method: string): AuthPermission {
  return method === "GET" || method === "HEAD" ? "read" : "write";
}

function permissionForStreamSubresource(args: {
  method: string;
  isSchema: boolean;
  isProfile: boolean;
  isSearch: boolean;
  isAggregate: boolean;
  isDetails: boolean;
  isIndexStatus: boolean;
  isRoutingKeys: boolean;
  pathKeyParam: boolean;
  touchMode: boolean;
}): AuthPermission {
  if (args.isSchema || args.isProfile) return args.method === "GET" || args.method === "HEAD" ? "read" : "admin";
  if (args.isSearch || args.isAggregate || args.isDetails || args.isIndexStatus || args.isRoutingKeys || args.pathKeyParam || args.touchMode) return "read";
  if (args.method === "GET" || args.method === "HEAD") return "read";
  if (args.method === "PUT" || args.method === "DELETE") return "admin";
  return "write";
}

function parseV1StreamAccess(path: string, method: string, url: URL): { stream: string; permission: AuthPermission } | null {
  const streamPrefix = "/v1/stream/";
  if (!path.startsWith(streamPrefix)) return null;
  const rawRest = path.slice(streamPrefix.length).replace(/\/+$/, "");
  if (rawRest.length === 0) return null;
  const segments = rawRest.split("/");
  let isSchema = false;
  let isProfile = false;
  let isSearch = false;
  let isAggregate = false;
  let isDetails = false;
  let isIndexStatus = false;
  let isRoutingKeys = false;
  let pathKeyParam = false;
  let touchMode = false;
  let vfsSegments: string[] | null = null;
  let vfsNamespace: "_vfs" | "_git" | null = null;

  if (segments[segments.length - 1] === "_schema") {
    isSchema = true;
    segments.pop();
  } else if (segments[segments.length - 1] === "_profile") {
    isProfile = true;
    segments.pop();
  } else if (segments[segments.length - 1] === "_search") {
    isSearch = true;
    segments.pop();
  } else if (segments[segments.length - 1] === "_aggregate") {
    isAggregate = true;
    segments.pop();
  } else if (segments[segments.length - 1] === "_details") {
    isDetails = true;
    segments.pop();
  } else if (segments[segments.length - 1] === "_index_status") {
    isIndexStatus = true;
    segments.pop();
  } else if (segments[segments.length - 1] === "_routing_keys") {
    isRoutingKeys = true;
    segments.pop();
  } else {
    const vfsIndex = Math.max(segments.lastIndexOf("_vfs"), segments.lastIndexOf("_git"));
    if (vfsIndex >= 0) {
      vfsNamespace = segments[vfsIndex] === "_git" ? "_git" : "_vfs";
      vfsSegments = segments.slice(vfsIndex + 1);
      segments.splice(vfsIndex);
    }
  }

  if (!vfsSegments) {
    if (
      segments.length >= 3 &&
      segments[segments.length - 3] === "touch" &&
      segments[segments.length - 2] === "templates" &&
      segments[segments.length - 1] === "activate"
    ) {
      touchMode = true;
      segments.splice(segments.length - 3, 3);
    } else if (segments.length >= 2 && segments[segments.length - 2] === "touch" && segments[segments.length - 1] === "meta") {
      touchMode = true;
      segments.splice(segments.length - 2, 2);
    } else if (segments.length >= 2 && segments[segments.length - 2] === "touch" && segments[segments.length - 1] === "wait") {
      touchMode = true;
      segments.splice(segments.length - 2, 2);
    } else if (segments.length >= 2 && segments[segments.length - 2] === "pk") {
      pathKeyParam = true;
      segments.splice(segments.length - 2, 2);
    }
  }

  const stream = decodeStreamName(segments.join("/"));
  if (!stream) return null;
  if (vfsSegments) {
    return {
      stream,
      permission: vfsNamespace === "_git" ? permissionForGitProfileRoute(method, vfsSegments, url) : permissionForWorkspaceFsProfileRoute(method),
    };
  }
  return {
    stream,
    permission: permissionForStreamSubresource({
      method,
      isSchema,
      isProfile,
      isSearch,
      isAggregate,
      isDetails,
      isIndexStatus,
      isRoutingKeys,
      pathKeyParam,
      touchMode,
    }),
  };
}

function requiredScopedAccess(request: Request): { kind: "authenticated" } | { kind: "server-admin" } | { kind: "stream"; stream: string; permission: AuthPermission } {
  const url = new URL(request.url, "http://localhost");
  const method = request.method.toUpperCase();
  const path = url.pathname;

  if (path === "/health") return { kind: "authenticated" };

  const topLevelGit = parseTopLevelGitStream(path);
  if (topLevelGit) {
    return {
      kind: "stream",
      stream: topLevelGit.stream,
      permission: topLevelGit.permission === "read" && url.searchParams.get("service") === "git-receive-pack" ? "write" : topLevelGit.permission,
    };
  }

  const streamAccess = parseV1StreamAccess(path, method, url);
  if (streamAccess) return { kind: "stream", ...streamAccess };

  return { kind: "server-admin" };
}

function scopeMatchesStream(scope: StreamScope, stream: string): boolean {
  if (scope.kind === "all") return true;
  if (scope.kind === "exact") return stream === scope.stream;
  return stream.startsWith(scope.prefix);
}

function scopedKeyHasPermission(key: ScopedApiKey, permission: AuthPermission): boolean {
  return key.permissions.has(permission);
}

function scopedKeyCanAccessStream(key: ScopedApiKey, stream: string, permission: AuthPermission): boolean {
  return scopedKeyHasPermission(key, permission) && key.scopes.some((scope) => scopeMatchesStream(scope, stream));
}

function scopedKeyCanAccessServer(key: ScopedApiKey): boolean {
  return scopedKeyHasPermission(key, "admin") && key.scopes.some((scope) => scope.kind === "all");
}

function authorizeScopedRequest(key: ScopedApiKey, request: Request): Response | null {
  const access = requiredScopedAccess(request);
  if (access.kind === "authenticated") return null;
  if (access.kind === "server-admin") return scopedKeyCanAccessServer(key) ? null : forbidden();
  return scopedKeyCanAccessStream(key, access.stream, access.permission) ? null : forbidden();
}

export function authenticateRequest(config: AuthConfig, request: Request): Response | null {
  if (config.mode === "none") return null;
  const credential = parseBearerCredential(request.headers.get("authorization"));
  if (credential == null) {
    return unauthorized();
  }
  if (config.mode === "api-key") {
    return credentialsMatch(config, credential) ? null : unauthorized();
  }
  const scopedKey = matchingScopedKey(config, credential);
  if (!scopedKey) return unauthorized();
  return authorizeScopedRequest(scopedKey, request);
}

export function withAuth(config: AuthConfig, fetch: (request: Request) => Promise<Response>): (request: Request) => Promise<Response> {
  if (config.mode === "none") return fetch;
  return async (request: Request): Promise<Response> => authenticateRequest(config, request) ?? fetch(request);
}
