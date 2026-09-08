# Rust quality policy adoption

The repository owner adopted [the normative policy](../RUST-QUALITY.md) on
8 September 2026. The proposal's structural standards, focused lint profile,
non-growing legacy migration, and invariant-specific verification are retained.
Tool success does not grant source, performance, cryptographic or deployment
acceptance for PR #19, which remains draft.

The exact legacy source is `5bdaf9684197ff84bd544fd0fcd69520001ea196`.
Platform inventories are captured from that commit with the adopted lint profile
and the explicitly recorded scoped-lint overlay, never from the modified code.
The macOS inventory records 3,549 warning occurrences; 3,538 remain after the
initial ownership and safety-comment fixes. Denied diagnostics are never allowed.
The Linux inventory is being captured and reviewed separately because cfg paths
and compiler diagnostics differ by target. Its gate fails closed until recorded.

Implementation includes exact tool pins, one local/CI entry point, parsed Rust
source rules, occurrence-sensitive diagnostic ratchets, private-owner compiler
fixtures, generated decoder/admission cases, actual handoff-state Loom tests,
Miri-compatible buffer tests, saved fuzz seeds and diff-scoped mutation checks.
The pinned repository-local Cursor skill is unchanged upstream content. The
Cursor path points at the canonical `.agents` copy.

Runtime changes release planning windows and test lock guards before suspension,
give the RSS sampler its actual admission/shard handles, and surface failed
allocator-worker joins. Existing unsafe calls now state their pointer/layout
preconditions. No read algorithm or retirement transition has been rewritten.
Dependency decisions are in [dependencies.md](dependencies.md).

Initial verification has passed real privacy and typed-lint negative fixtures,
generated properties, bounded Loom exploration, three compatible Miri buffer
tests, saved decoder seeds and a 121-second fuzz run (26,513,694 inputs).
Mutation testing exposed missing nonzero-prefix extension cases; those cases
were added. Final mutation, common-gate and CI results will be recorded when the
adoption validation completes. Incomplete runs are not counted as passes.

Miri does not cover the allocator/system FFI paths. The initial macOS async Miri
fixture stopped at unsupported `kqueue` setup before assertions; directly polling
the I/O-free buffer checks then passed. Fuzz throughput is instrumentation data,
not a server performance measurement. Raw logs, binaries and generated corpus
remain local under the existing evidence-upload hold.
