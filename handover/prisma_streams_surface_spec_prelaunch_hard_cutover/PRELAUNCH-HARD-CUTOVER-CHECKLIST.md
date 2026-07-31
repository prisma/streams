# Pre-launch Hard-Cutover Checklist

This checklist is normative for the eight-stage implementation.

## Required

- [ ] Final server, SDK, descriptor, cursor, routing, postings, consumer, watch, and lifecycle implementations are complete.
- [ ] `/v1/stream/{name}` passes the pinned official Durable Streams server and client conformance suites.
- [ ] `/v1/streams/{name}` passes Prisma product conformance.
- [ ] A fresh bucket or `PATH_PREFIX` is used.
- [ ] Current development/staging/campaign data is deleted rather than migrated.
- [ ] Old profile, routing, covering-index, queue, touch, cursor, and descriptor formats are absent from the binary.
- [ ] Old Prisma product routes, headers, SDK methods, aliases, and docs are absent.
- [ ] No intermediate stage is deployed as a supported environment.
- [ ] Server and SDK switch together after all final gates pass.

## Forbidden

- [ ] Legacy descriptor/storage decoders.
- [ ] Route, header, request, or SDK translation layers.
- [ ] Dual-read, dual-write, shadow-write, or mixed-layout serving.
- [ ] Cutover offsets, migration markers, backfills, data-copy/import jobs, or converters.
- [ ] Feature flags that preserve old product semantics.
- [ ] N/N-1 rolling compatibility or mixed-version ownership transfer.
- [ ] Deprecation windows, sunset headers, legacy-usage telemetry, or alias handlers.

## Rollback

Before new-format writes, rollback is source-control rollback. After new-format writes, rollback requires deleting the fresh namespace and redeploying a selected binary. Never point an old binary at the new namespace or the new binary at the old namespace.
