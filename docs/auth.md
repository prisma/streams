# Prisma Streams Authentication And Authorization

Status: **not implemented**.

Prisma Streams currently does not enforce authentication or authorization in either server mode.

That means:

- the full server must be deployed behind a trusted reverse proxy, API gateway, VPN boundary, or other authenticated network perimeter
- TLS should be terminated outside the server
- the local development server should be treated as a loopback-only tool for trusted local workflows such as `npx prisma dev`

Unsupported deployment model:

- exposing Prisma Streams directly to the public internet without an external auth layer

The previous DSAT-based design notes are not active behavior and should not be read as implemented functionality.
