// The mock identity provider (public-beta issuer triage B4.5): "Sign in
// with Prisma"'s three endpoints for `prisma dev` and the end-to-end
// test. Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomBytes } from "node:crypto";
import { MockIdp } from "./mock-idp";

const idps: MockIdp[] = [];
afterEach(() => {
  for (const idp of idps.splice(0)) idp.stop();
});

const REDIRECT = "https://issuer.example.com/auth/callback";

function mock(opts: ConstructorParameters<typeof MockIdp>[0] = {}): MockIdp {
  const idp = new MockIdp({ clientId: "streams-beta-dev", ...opts });
  idps.push(idp);
  return idp;
}

const verifierAndChallenge = () => {
  const verifier = randomBytes(32).toString("base64url");
  return { verifier, challenge: createHash("sha256").update(verifier).digest("base64url") };
};

/// Ask `idp` to authorize as a browser would; the redirect it answers.
async function authorize(idp: MockIdp, params: Record<string, string>): Promise<Response> {
  const url = new URL(`${idp.url}/authorize`);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  return fetch(url, { redirect: "manual" });
}

const authorizeParams = (challenge: string, extra: Record<string, string> = {}) => ({
  response_type: "code",
  client_id: "streams-beta-dev",
  redirect_uri: REDIRECT,
  scope: "workspace:admin",
  state: "st_1",
  code_challenge: challenge,
  code_challenge_method: "S256",
  ...extra,
});

const token = (idp: MockIdp, form: Record<string, string>, headers: Record<string, string> = {}) =>
  fetch(`${idp.url}/token`, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded", ...headers },
    body: new URLSearchParams(form),
  });

const me = (idp: MockIdp, accessToken: string) => fetch(`${idp.url}/v1/me`, { headers: { authorization: `Bearer ${accessToken}` } });

const codeOf = (res: Response) => new URL(res.headers.get("location")!).searchParams.get("code")!;

describe("the mock identity provider", () => {
  test("approves a PKCE authorization as its principal: a code for the challenge's verifier, once, then /v1/me", async () => {
    const idp = mock();
    const { verifier, challenge } = verifierAndChallenge();
    const res = await authorize(idp, authorizeParams(challenge));
    expect(res.status).toBe(302);
    const back = new URL(res.headers.get("location")!);
    expect([back.origin + back.pathname, back.searchParams.get("state"), [...back.searchParams.keys()].sort()]).toEqual([
      REDIRECT, "st_1", ["code", "state"],
    ]);
    const form = {
      grant_type: "authorization_code",
      code: codeOf(res),
      redirect_uri: REDIRECT,
      client_id: "streams-beta-dev",
      code_verifier: verifier,
    };
    const issued = await token(idp, form);
    const body = await issued.json();
    expect([issued.status, Object.keys(body).sort(), body.token_type, body.expires_in, body.scope]).toEqual([
      200, ["access_token", "expires_in", "scope", "token_type"], "Bearer", 3600, "workspace:admin",
    ]);
    expect(await (await me(idp, body.access_token)).json()).toEqual({
      data: {
        user: { id: "usr_dev", email: "dev@example.com", name: "Dev User" },
        workspace: { id: "ws_dev", name: "Dev Workspace" },
        credential: { type: "oauth", id: null, name: null },
      },
    });
    // A code is good once.
    const again = await token(idp, form);
    expect([again.status, await again.json()]).toEqual([400, { error: "invalid_grant", error_description: "unknown, used or expired code" }]);
    expect(idp.calls).toEqual({ authorize: 1, token: 2, me: 1 });
    expect(idp.scopesAsked).toEqual(["workspace:admin"]);
  });

  test("refuses a wrong verifier, another redirect_uri or client, a code past 60 s, and a client without its secret", async () => {
    let t = 1_000_000;
    const idp = mock({ clientSecret: "s3cret", now: () => t });
    const basic = { authorization: `Basic ${Buffer.from("streams-beta-dev:s3cret").toString("base64")}` };
    const flow = async () => {
      const pair = verifierAndChallenge();
      const code = codeOf(await authorize(idp, authorizeParams(pair.challenge)));
      return {
        grant_type: "authorization_code", code, redirect_uri: REDIRECT, client_id: "streams-beta-dev", code_verifier: pair.verifier,
      };
    };
    const refused = async (res: Response) => [res.status, (await res.json()).error];
    expect(await refused(await token(idp, { ...(await flow()), code_verifier: verifierAndChallenge().verifier }, basic))).toEqual([400, "invalid_grant"]);
    expect(await refused(await token(idp, { ...(await flow()), redirect_uri: "https://evil.example.com/cb" }, basic))).toEqual([400, "invalid_grant"]);
    expect(await refused(await token(idp, { ...(await flow()), client_id: "other" }, basic))).toEqual([401, "invalid_client"]);
    expect(await refused(await token(idp, await flow()))).toEqual([401, "invalid_client"]);
    const late = await flow();
    t += 60_001;
    expect(await refused(await token(idp, late, basic))).toEqual([400, "invalid_grant"]);
    expect((await token(idp, await flow(), basic)).status).toBe(200);
    // An authorization it cannot redirect is answered 400 in place.
    for (const p of [{ client_id: "other" }, { redirect_uri: "" }] as Record<string, string>[]) {
      expect((await authorize(idp, authorizeParams("c".repeat(43), p))).status).toBe(400);
    }
    // One without S256 PKCE is redirected back refused.
    const plain = await authorize(idp, authorizeParams("c".repeat(43), { code_challenge_method: "plain" }));
    expect(new URL(plain.headers.get("location")!).searchParams.get("error")).toBe("invalid_request");
  });

  test("redirects a declined authorization with access_denied, gives a refresh token only for offline_access, and answers /v1/me 401 without its token", async () => {
    const idp = mock();
    idp.decision = "deny";
    const denied = new URL((await authorize(idp, authorizeParams("c".repeat(43)))).headers.get("location")!);
    expect([denied.searchParams.get("error"), denied.searchParams.get("state"), denied.searchParams.get("code")]).toEqual([
      "access_denied", "st_1", null,
    ]);
    idp.decision = "approve";
    const { verifier, challenge } = verifierAndChallenge();
    const res = await authorize(idp, authorizeParams(challenge, { scope: "workspace:admin offline_access" }));
    const issued = await (await token(idp, {
      grant_type: "authorization_code", code: codeOf(res), redirect_uri: REDIRECT, client_id: "streams-beta-dev", code_verifier: verifier,
    })).json();
    expect(typeof issued.refresh_token).toBe("string");
    expect((await me(idp, "not-a-token")).status).toBe(401);
    expect((await fetch(`${idp.url}/v1/me`)).status).toBe(401);
    // The principal is the one set when the token was issued.
    idp.principal = { ...idp.principal, workspace: null };
    expect((await (await me(idp, issued.access_token)).json()).data.workspace).toEqual({ id: "ws_dev", name: "Dev Workspace" });
    idp.meFails = true;
    expect((await me(idp, issued.access_token)).status).toBe(500);
  });
});
