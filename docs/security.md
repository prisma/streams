# Prisma Streams Security Policy

## Supported Versions

Security fixes are currently provided on the active main branch only. There are no long-lived release branches yet.

## Reporting A Vulnerability

Please do not open a public GitHub issue for security vulnerabilities.

Use GitHub's private vulnerability reporting flow for this repository if it is enabled. If that is not available, contact the maintainers privately before disclosing the issue publicly.

When reporting a vulnerability, include:

- The affected commit, branch, or version
- Your deployment mode: full server or local development server
- Reproduction steps or a minimal proof of concept
- Any relevant logs, traces, or configuration details

## Current Security Posture

The full Prisma Streams server requires an explicit startup auth mode:

- `--auth-strategy api-key` enables built-in API key authentication for every
  request
- `--no-auth` disables built-in authentication for deployments that rely on a
  trusted external boundary

That has concrete deployment consequences:

- Prefer `--auth-strategy api-key` when the server receives network traffic
  directly.
- Use `--no-auth` only behind a trusted reverse proxy, API gateway, VPN
  boundary, or local-only deployment wrapper.
- Terminate TLS outside the server.
- Treat the local development server as a loopback-only tool for trusted local workflows such as `npx prisma dev`.

The local development server is intentionally optimized for local integration,
not hostile-network deployment, and does not participate in the full-server auth
contract.

More detail is documented in [auth.md](./auth.md).
