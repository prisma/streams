# SDK review remediation evidence

Baseline: `a7e2070f3b4346b3e54d552069ff91c56e900130`, Node `v24.8.0`.
Initial red/green R19/R20 runs used global TypeScript 5.0.4; after the
sandbox DNS-restricted npm attempt failed, the authorized network install
completed `npm ci --prefix sdk --cache /tmp/prisma-streams-sdk-npm-cache`.
Subsequent gates use lockfile-pinned TypeScript 5.9.3.
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
After the fix all 4 cases pass, including captured append/batch/final-seal
headers `0/0`, `1/0`, `1/1` and final state `{epoch:1,nextSeq:2}`.

Verification: SDK typecheck, build, and complete `npm test --prefix sdk`.
An additional controlled fenced response proves automatic reclaim runs
before a queued bump and ends at epoch 6 after reclaiming server epoch 4.
Commit: `29daf44`.

## R21 — consumer cleanup owns recorded decisions

The iterator settles in `finally` on exhaustion, break, explicit return and
throw. Settlement is memoized for each batch; no decision means no request.
`return()` and the new optional pull/iterator signal cancel parked pulls.
An explicit throw aggregates handler and cleanup failures. A `closed`
outcome also retains failure when JavaScript preserves the loop body's
original exception; the README describes this language-level distinction.

Before implementation, the focused break/handler-throw/retry-extend tests
all failed with zero settlement requests. After implementation
`node --test sdk/scripts/consumer-cleanup.test.mjs` passes 7 cases, covering
all three exit forms, unseen/undecided leases, idempotence, dual failures,
and aborting a parked pull without a timer. Tests prove submitted intentions
and failure propagation; they do not claim a live server redelivery result.

Verification: pinned SDK typecheck/build and all package tests.
Commit: see `git log --grep=R21`.
