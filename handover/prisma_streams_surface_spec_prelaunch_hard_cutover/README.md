# Prisma Streams Product-Surface Specification

This package contains the implementation specification for simplifying Prisma Streams through eight implementation workstreams while preserving the Durable Streams protocol.

## Pre-launch clean switch

These specifications require a destructive pre-GA cutover. Implement the final schema, routes, codecs, and storage layout directly in a fresh bucket or `PATH_PREFIX`. Do not implement migration readers, legacy request aliases, dual writes, deprecation windows, N/N-1 rolling compatibility, or any other update path from the current experimental code or data. The only preserved external contract is the pinned Durable Streams protocol on the singular standards route.

## Critical standards decision

The raw protocol route remains a fully conforming Durable Stream URL:

```text
/v1/stream/{name}
```

It represents the collection's default routing-key sequence and retains the complete pinned Durable Streams contract. It exists for standards conformance, not to preserve the current experimental Prisma API.

The Prisma product route is the only Prisma product API:

```text
/v1/streams/{name}
```

It represents the automatically partitioned, per-routing-key collection and uses distinct key and scan cursors. It never claims that all routing keys form one globally ordered Durable Stream.

## Documents

1. [Overview and cross-stage invariants](00-OVERVIEW.md)
2. [Stage 1 — Remove profiles](01-REMOVE-PROFILES.md)
3. [Stage 2 — Consumer groups and watches](02-CONSUMERS-AND-WATCHES.md)
4. [Stage 3 — Unified routing, automatic scaling, and compact postings](03-UNIFIED-ROUTING-SCALING-POSTINGS.md)
5. [Stage 4 — `append` and `appendMany`](04-APPEND-AND-APPEND-MANY.md)
6. [Stage 5 — Producer sessions](05-PRODUCER-SESSIONS.md)
7. [Stage 6 — `read`, `subscribe`, and `scan`](06-READ-SUBSCRIBE-SCAN.md)
8. [Stage 7 — Typed creation document](07-TYPED-CREATION-DOCUMENT.md)
9. [Stage 8 — Naming, lifecycle, and routes](08-NAMING-LIFECYCLE-ROUTES.md)
10. [Conformance matrix and hard-cutover plan](09-CONFORMANCE-MATRIX-AND-HARD-CUTOVER.md)

A consolidated edition is available as `PRISMA_STREAMS_PRODUCT_SURFACE_SPEC.md`.
