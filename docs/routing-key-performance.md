# Prisma Streams Routing-Key Performance

Status: **informational status note**. Dedicated routing-key perf tooling is not
currently maintained in this repo.

Current implementation status:

- Per-block bloom filters + segment footer index are implemented.
- Tier‑2 routing-key index runs (L0 + compaction to higher levels) are implemented.
- Keyed reads now populate the local segment cache on first touch and scan that
  local file, rather than issuing remote block-range reads against uploaded
  segments.
- Once cached locally, keyed reads use `Bun.mmap()` and a single forward pass
  over block headers / matching blocks instead of reopening the cached segment
  file for many tiny footer and block reads.
- If mmap is unavailable, keyed reads fall back to a single full-file byte
  buffer for the segment scan, not repeated small local file reads.
- When a routing-key tiered index candidate set exists, keyed reads no longer
  walk the full indexed sealed prefix segment-by-segment. They build a sealed
  read plan up front and scan only:
  - the candidate segments from the indexed uploaded prefix
  - the uncovered uploaded tail after `indexed_through`
- Timestamp seeks with `key=...` use the same sealed read plan, so routing-key
  cursor seeks also scale with candidate-set size instead of total sealed
  history.

That means routing-key reads should scale with the size of the candidate set and
the uncovered tail, not with the total number of sealed uploaded segments in the
stream.

For details, see:

- `tiered-index.md`
- `overview.md` (repository overview and benchmark entry points)
- `segment-performance.md`

If you need custom routing-key workload benchmarks, use `experiments/bench/synth.ts`
or build a dedicated driver for your query pattern.
