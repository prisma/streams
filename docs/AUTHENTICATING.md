# Authenticating to the Prisma Streams public beta

This page is for customers of the public beta. It says how your app turns
its project credential into access tokens, how long a token lives and how
it is refreshed, which headers every request needs, what each refusal
means and whether to retry it, and how revocation and rotation take
effect. Each section names the file where an engineer checks it. Limits
and the server's other answers are in [BETA-REFERENCE.md](BETA-REFERENCE.md).

## Credentials and tokens

A beta project has up to 10 live **credentials**. Each one is a durable
secret of the form

```
prisma_streams_<credential id>.<secret>
```

(`strcred_` and 32 hex characters, a dot, then 43 base64url characters).
The issuer's page shows the whole value once, when you create the
project or the credential, or rotate it. It never shows it again: the
beta stores only a keyed hash of the secret and its last 6 characters,
which the page shows as a hint.

Your app does not send the credential to the Streams server. It
**exchanges** it at the beta's token issuer for a short-lived **access
token**, and sends that token to the server as a bearer token. Keep the
credential on a server, in an environment variable or a secret manager:
never in a browser bundle, a mobile app, a log line or a repository. The
issuer's answers carry no CORS header, so a page on another origin cannot
use it.

Source: `deploy/issuer/exchange.ts`, `deploy/cell-admin/secrets.ts`.

## With the SDK

```ts
import { StreamsClient } from "@prisma/streams";

const client = await StreamsClient.connect({
  credential: process.env.PRISMA_STREAMS_CREDENTIAL!,
  issuerUrl: "https://<the issuer's origin, as its page shows it>",
});
```

`connect` makes the first exchange, sends every request to the
`endpoint` the issuer names, exchanges again before the token expires and
once after a `401`, and follows a new `endpoint` if an exchange names
one. `issuerUrl` must be `https:` (or a loopback host for local
development): the SDK refuses any other before it sends the credential
(`CredentialExchangeError` `insecure_issuer`). The SDK's README,
"Authenticating", has the details and `prismaStreamsCredential` for
building a client yourself.

## The exchange on the wire

```
POST {issuer}/v1/token/streams
Authorization: StreamsCredential prisma_streams_<credential id>.<secret>
```

No body is needed. An optional JSON object of at most 4,096 bytes may
narrow the token: `requestedScopes` (a non-empty array; the token carries
the credential's scopes among them) and `ttlSecs` (a whole number from
300 to the issuer's lifetime). Any other member is refused.

A success is `200`:

```json
{
  "accessToken": "<a JWT>",
  "tokenType": "Bearer",
  "expiresIn": 3540,
  "projectId": "sp_...",
  "cellId": "...",
  "endpoint": "https://<the cell's origin>"
}
```

Send every request to `endpoint` with `Authorization: Bearer
<accessToken>`. Treat `accessToken` as opaque: its claims are the
server's business, and their shape may change.

Source: `deploy/issuer/exchange.ts`; edge change #149.

## Token lifetime and refresh

A token lives **60 minutes** by default. It is issued backdated by
60 seconds to absorb clock skew, so `expiresIn` is 3,540 seconds. A
`ttlSecs` in the request shortens it, down to 300 seconds; nothing
lengthens it past the issuer's lifetime, and the server refuses any token
whose lifetime exceeds 24 hours.

Refresh before `expiresIn` runs out (the SDK refreshes at 80% of it) and
once after the server answers `401`. Do not exchange per request: a
credential may exchange 2 times a second, with a burst of 60, and the
issuer refuses more with `429` (below).

Source: `deploy/issuer/config.ts` (`TOKEN_TTL_DEFAULT_SECS`),
`deploy/cell-admin/tokens.ts` (`customerClaims`), `src/auth.rs`
(`check_times`), `deploy/issuer/limits.ts`.

## Headers on every stream request

| Header | Carries |
|---|---|
| `Authorization: Bearer <accessToken>` | the token, on every request |
| `Streams-Encryption-Key` | the stream's key, on every request that reads or writes a stream |

Send the stream key as `Streams-Encryption-Key`, never as
`Prisma-Encryption-Key`: the Prisma Compute edge in front of the beta's
cells removes every request header whose name starts with `Prisma-`, so
the server never sees it and answers `400 missing_key` (edge change #173).
The SDK sends the `Streams-` names. [BETA-REFERENCE.md](BETA-REFERENCE.md),
"Request headers", lists the others.

## Errors and retries

Every refusal is JSON, `{"error": {"code": "...", "message": "..."}}`.

**From the exchange** (`POST /v1/token/streams`):

| Status | `code` | Meaning | Retry |
|---|---|---|---|
| 400 | `invalid_request` | the body is not one the exchange takes, or the credential grants none of `requestedScopes` | no: fix the request |
| 401 | `invalid_credential` | the header is malformed, or the credential is unknown or its secret wrong | no: the same secret cannot succeed. Copy the credential again, or make a new one |
| 403 | `credential_not_active` | the credential is revoked or past its expiry | no: use another credential |
| 403 | `project_not_active` | the project is suspended, or being deleted | no, until the beta's operator resumes it |
| 429 | `rate_limited` | too many exchanges for this credential, for this issuer, or failed ones for this credential id | yes, after `Retry-After` seconds |
| 503 | `issuer_unavailable` | the issuer cannot mint right now (it cannot see the cell's current state, or a key change is in progress) | yes, after `Retry-After` (5 s) |

The SDK replays a `429` and a `503` on `Retry-After`, and a transport
failure after 1, 2 and 4 seconds, within four attempts; any other refusal
rejects at once with `CredentialExchangeError`. It refuses some inputs
before any request: `invalid_credential_format` (the credential is not
`prisma_streams_<id>.<secret>` in printable characters without spaces:
a line break pasted into it, say) and `insecure_issuer` (above).

**From the server**, for a token:

| Status | `code` | Meaning | Retry |
|---|---|---|---|
| 401 | e.g. `expired`, `kid_unknown`, `grant_version_mismatch` | the token is expired, malformed, signed under a key the cell no longer holds, or minted before the credential's last change | exchange again once, then retry (the SDK does) |
| 403 | `credential_not_active` | the credential was revoked | no |
| 403 | `project_not_active` | the project is suspended or being deleted | no |
| 403 | `missing_scope`, `prefix_denied` | the credential does not grant this operation, or this stream | no |
| 421 | `wrong_cell` | this cell does not serve the project | exchange again and use the new `endpoint` (the credential is fine) |
| 503 | `policy_stale`, `grants_stale`, `keys_stale` | the cell cannot confirm the credential's current state | yes, after `Retry-After` (1 s) |

Source: `deploy/issuer/exchange.ts`, `sdk/src/index.ts`
(`prismaStreamsCredential`), `src/auth.rs` (`AuthError::kind`),
`src/auth/refusal.rs`; WIRE-MATRIX §0 and §5.

## Revocation and rotation

**Revoke** a credential on the issuer's page. Within seconds the issuer
exchanges it no more, and within about a minute the server refuses every
token already minted from it (`403 credential_not_active`): revocation
does not wait for tokens to expire. A revoked credential cannot be
restored.

**Rotate** gives a credential a new secret at once, with no overlap: the
old secret exchanges no more, and the server refuses its tokens within
about a minute (`401`, which makes the SDK exchange again, and an app
still holding the old secret then gets `401 invalid_credential`). To
replace a secret without downtime, create a second credential, deploy it,
then revoke the first. A project may create or rotate 20 credentials a
day.

**Deleting the project** revokes all of its credentials at once and then
deletes every stream; it cannot be undone. A project the beta's operator
suspends is refused (`403 project_not_active`) until it is resumed.

If a credential leaks, revoke it first and replace it second.

Source: `deploy/issuer/credentials.ts`, `deploy/issuer/projects.ts`,
`src/auth.rs` (`verify_customer`: the credential's status and
`grant_version` are checked on every request).
