# Repository coding standard

Follow [docs/RUST-QUALITY.md](docs/RUST-QUALITY.md), the adopted normative Rust
policy. Its legacy inventories are a migration ceiling, not permission to add
warnings, unowned effects, or complexity. Keep proof-bearing fields private and
review the whole canonical owner rather than adding pass-through abstractions.

Use the exact root toolchain. Run `scripts/quality.sh` and the existing test and
protocol gates appropriate to the change. `scripts/gate.sh` is the full local
commit gate. Select invariant verification using
`scripts/quality/verification_plan.py` against the actual PR target merge base.
Prune obsolete warning allowances with `scripts/quality/gate.py --prune`; never
regenerate or grow an adoption baseline during ordinary work.

The repository-local `thermo-nuclear-code-quality-review` skill is pinned at
`.agents/skills/thermo-nuclear-code-quality-review/SKILL.md`. Invoke it explicitly
for structural reviews; `.cursor/skills` points at the same canonical file.

Retain PR #19's draft status and its existing performance, cryptographic,
merge/deployment and raw-evidence upload holds until their separate acceptance
requirements are met. Local quality results do not lift those holds.

Read optimisations have one permanent production path. Keep experiments in
isolated source revisions rather than adding runtime on/off switches. The
current decisions are in [docs/read-experiments/final-disposition.md](docs/read-experiments/final-disposition.md).
