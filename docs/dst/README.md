# Prisma Streams DST expansion package

This package contains a complete handoff specification for extending Prisma Streams Deterministic Simulation Testing based on the failure modes uncovered across the SlateDB implementation, cost campaigns, Routing v3, product-surface work, protocol conformance, and repeated lifecycle audits.

## Files

- `DST-EXPANSION-SPEC.md` — normative architecture, invariants, fault model, replay, liveness, cost budgets, CI, and release requirements.
- `SCENARIO-CATALOG.md` — detailed catalogue of focused and whole-system scenarios, with status and exact failure class.
- `IMPLEMENTATION-PLAN.md` — ordered workstreams and acceptance criteria.
- `scenario-example.yaml` — example serialized scenario.

## Baseline

The source baseline reviewed for this specification is:

```text
681ea0fe73ca49c74fc10a61846b9dbf7195d443
```

The catalogue intentionally includes failure classes found in later review rounds as required permanent regressions, even when the reviewed baseline does not yet contain the corresponding fix.

## Immediate priority

Before building the full simulator, add the catalogue entries marked **L1-now**, especially:

1. same-group duplicate and close responses under failed `db.write`;
2. state-dependent producer and Stream-Seq errors behind durability barriers;
3. fence behavior under failed/current/prior durability barriers;
4. no time-based fence cleanup while stale requests remain queued;
5. concurrent final attempts with semantically distinct producer/content metadata;
6. create replay with initial-content write failure;
7. fork child deletion between child stamp and source-reference installation;
8. merge phase-B publication under sealing.

Then consolidate all ad hoc gates into the semantic failpoint registry before adding further lifecycle tests.
