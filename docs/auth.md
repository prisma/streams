# Prisma Streams Authentication And Authorization

Status: **implemented**.

This document describes the supported authentication model for the production
Prisma Streams server. It does not apply to local streams.

## Scope

Authentication is a startup-selected behavior for the full server only:

- `@prisma/streams-server`
- `src/server.ts`
- deployment entrypoints that start the full server, including Prisma Compute

The local development server remains a loopback-oriented integration tool and
does not participate in this auth contract.

## Startup Configuration

Every full server startup must choose exactly one auth mode:

- `--no-auth`
- `--auth-strategy api-key`

Providing neither option is a startup error. Providing both options is a startup
error.

`--auth-strategy` supports exactly one value:

- `api-key`

Any other value is a startup error.

When `--auth-strategy api-key` is selected, the `API_KEY` environment variable
must be present and contain at least 10 characters. Missing, empty, or too-short
values are startup errors. The configured key is the complete credential; there
is no key id, key list, hashing layer, or remote lookup in the server.

The server does not trim or otherwise normalize `API_KEY`. Leading or trailing
spaces are part of the configured key. The minimum length check uses the
JavaScript string length of the configured value.

`--no-auth` disables built-in request authentication for deployments that rely
on an external trusted boundary. It is an explicit opt-out so production
entrypoints cannot accidentally start with unauthenticated defaults.

## Request Authentication

When `--auth-strategy api-key` is selected, every HTTP request to every endpoint
must be authenticated before endpoint-specific handling runs.

This includes:

- durable stream protocol endpoints under `/v1/*`
- live and touch endpoints
- metrics, memory, health, debug, and operational endpoints
- `OPTIONS` and other methods if they reach the server
- unknown routes

There are no unauthenticated probe, health, or metadata endpoints in this mode.
Browser CORS preflight requests are not exempt from authentication; deployments
that need unauthenticated preflight handling should terminate that behavior in a
trusted proxy or gateway before requests reach Streams.

Clients authenticate with the `Authorization` header:

```http
Authorization: Bearer <API_KEY>
```

The server accepts the auth scheme case-insensitively, but the credential value
must match `API_KEY` exactly. The credential is every character after the
`Bearer ` prefix and is not trimmed or otherwise normalized. The header is
malformed when it is missing, uses a scheme other than `Bearer`, omits the
credential, or otherwise cannot be parsed as one bearer credential.

Malformed, missing, or incorrect credentials receive `401 Unauthorized` and do
not invoke endpoint-specific handlers. The response includes:

```http
WWW-Authenticate: Bearer
```

The response body should be a small generic error payload and must not reveal
whether the key was missing, malformed, or incorrect.

Credential comparison should avoid timing leaks where the runtime provides a
reasonable constant-time comparison primitive.

## Authorization

The initial production auth model is authentication-only. A valid API key has
access to all server endpoints and all streams served by that process.

Per-stream authorization, multiple keys, key rotation, scoped keys, user
identity, and audit attribution are not part of this contract.

## Operational Notes

Use TLS outside the Streams server when requests cross a network boundary.

Avoid logging the configured `API_KEY` or request `Authorization` header. Existing
profile redaction treats `authorization` as sensitive, and server auth code must
preserve that posture.

`--no-auth` should be used only when another trusted component enforces the
intended access policy, such as a reverse proxy, API gateway, VPN boundary, or
local-only deployment wrapper.
