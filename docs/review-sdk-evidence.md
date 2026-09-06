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
Commit: `6a79dfb`.

## R20 — one producer mutation queue

Removed the duplicate send queue. Append, batch append, final seal and epoch
bump now share `_chain`; automatic reclaim remains inside the held send.
The caller receives its original rejection while the queue tail drains and
releases its routing-key entry. The README defines exclusive instance scope
ownership and uncertain fetch/save outcomes without claiming cross-process
locking.

Before the fix, `node --test sdk/scripts/producer-ordering.test.mjs` failed
the barrier ordering case: epoch became 1 before the held append completed.
The fetch/save failure recovery cases already passed and remain controls.
After the fix all 3 cases pass, including captured append/batch/final-seal
headers `0/0`, `1/0`, `1/1` and final state `{epoch:1,nextSeq:2}`.

Verification: SDK typecheck, build, and complete `npm test --prefix sdk`.
Commit: see `git log --grep=R20`.
