# SDK review remediation evidence

Baseline: `a7e2070f3b4346b3e54d552069ff91c56e900130`, Node `v24.8.0`.
The focused tests use injected HTTP responses and explicit barriers; they
do not certify the live Node/Bun/Deno server smoke legs. The package CI
now executes these regressions after its lockfile installation and build.

## R19 — token refresh generation ownership

Implemented request-bound token generations, shared refresh acquisition,
generation-guarded completion, and recoverable synchronous/provider errors.
A stale 401 cannot invalidate a completed or pending successor generation;
provider acquisition cannot overlap within a generation.

Before the implementation, `node --test sdk/scripts/token-refresh.test.mjs`
failed all 3 tests: four cached 401s made 5 provider calls instead of 2,
refresh waiters received inconsistent outcomes, and synchronous failure
called the provider twice. Afterward the same command passes all 3 tests,
including a delayed old-generation 401 after refresh completion.

Verification: `npm run typecheck --prefix sdk`,
`npm run build --prefix sdk`, and `npm test --prefix sdk`.
Commit: see the commit introducing this R19 section and `git log --grep=R19`.
