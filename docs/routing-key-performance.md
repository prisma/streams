# Prisma Streams Routing-Key Performance

Status: **informational status note**. Dedicated routing-key perf tooling is not
currently maintained in this repo.

Current implementation status:

- Per-block bloom filters + segment footer index are implemented.
- Tier‑2 routing-key index runs (L0 + compaction to higher levels) are implemented.

For details, see:

- `tiered-index.md`
- `overview.md` (repository overview and benchmark entry points)

If you need custom routing-key workload benchmarks, use `experiments/bench/synth.ts`
or build a dedicated driver for your query pattern.
