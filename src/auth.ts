import { timingSafeEqual } from "node:crypto";
import { Result } from "better-result";

export type AuthConfig =
  | {
      mode: "none";
    }
	  | {
	      mode: "api-key";
	      apiKeyBytes: Buffer;
	    };

export type AuthConfigError = {
  message: string;
};

const API_KEY_MIN_LENGTH = 10;

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

function authConfigError(message: string): Result<AuthConfig, AuthConfigError> {
  return Result.err({ message });
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
  if (authStrategyValues.length !== 1 || authStrategyValues[0] !== "api-key") {
    return authConfigError("invalid --auth-strategy (expected: api-key)");
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

function parseBearerCredential(header: string | null): string | null {
  if (header == null) return null;
  const match = /^Bearer (.+)$/i.exec(header);
  return match?.[1] ?? null;
}

function credentialsMatch(config: Extract<AuthConfig, { mode: "api-key" }>, credential: string): boolean {
  const credentialBytes = Buffer.from(credential, "utf8");
  const paddedCredential = Buffer.alloc(config.apiKeyBytes.length);
  credentialBytes.copy(paddedCredential, 0, 0, Math.min(credentialBytes.length, paddedCredential.length));
  const bytesMatch = timingSafeEqual(config.apiKeyBytes, paddedCredential);
  return credentialBytes.length === config.apiKeyBytes.length && bytesMatch;
}

export function authenticateRequest(config: AuthConfig, request: Request): Response | null {
  if (config.mode === "none") return null;
  const credential = parseBearerCredential(request.headers.get("authorization"));
  if (credential == null || !credentialsMatch(config, credential)) {
    return unauthorized();
  }
  return null;
}

export function withAuth(config: AuthConfig, fetch: (request: Request) => Promise<Response>): (request: Request) => Promise<Response> {
  if (config.mode === "none") return fetch;
  return async (request: Request): Promise<Response> => authenticateRequest(config, request) ?? fetch(request);
}
