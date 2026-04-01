# Mmap Companion Read-Path Comparison

Date: 2026-04-01

## Scope

This note compares the pre-mmap bundled companion read path against the new local immutable `.cix` cache plus `Bun.mmap()` read path for the broad no-hit browse query:

- `q=type:pushevent owner:prisma*`
- `size=1`
- `sort=["offset:desc"]`
- `track_total_hits=false`

The focus is the sealed-segment `.fts` candidate path, especially:

- `indexed_segment_time_ms`
- `fts_section_get_ms`
- `fts_decode_ms`
- `fts_clause_estimate_ms`

## Baseline

Environment:

- temp worktree rooted at commit `7b52a3f`
- current published-prefix search semantics and FTS subphase instrumentation patched in
- old read path retained:
  - remote TOC range GET
  - remote `.fts` section range GET
  - in-memory TOC / section caches
  - no local immutable `.cix` cache
  - no `Bun.mmap()`

Comparable sample:

- elapsed: first 5-minute ingest sample
- logical size: `206,923,556`
- uploaded segments: `12`
- pending rows: `13,929`
- pending bytes: `5,594,406`
- server RSS: `1,250,152 KB`

Browse result:

- `took_ms = 10,692`
- `indexed_segments = 10`
- `indexed_segment_time_ms = 10,486`
- `fts_section_get_ms = 5,388`
- `fts_decode_ms = 52`
- `fts_clause_estimate_ms = 2`
- `scanned_segments = 0`
- `scanned_tail_docs = 0`
- `exact_candidate_time_ms = 205`

Interpretation:

- most browse time was sealed-segment `.fts` work
- most of that `.fts` time was section fetch, not decode or clause estimation

## Mmap Read Path

Environment:

- current working tree
- full remote `.cix` download on first read
- local immutable cache under `${DS_ROOT}/cache/companions`
- `Bun.mmap()` over cached `.cix`
- zero-copy section slicing from mapped bytes
- zero-copy `.fts` metadata views

First post-change attempt was not representative because ingest had not yet cut any segments by the first 5-minute sample. A second warm-cache rerun reached a comparable published prefix quickly, so the comparison below uses a live sample from that rerun once uploaded segments were available.

Comparable sample:

- elapsed: live sample during ingest after published prefix became available
- logical size: `187,965,801`
- uploaded segments: `10`
- pending rows: `8,441`
- pending bytes: `3,414,224`
- server RSS: `747,776 KB`

Browse result:

- `took_ms = 2,129`
- `indexed_segments = 8`
- `indexed_segment_time_ms = 1,893`
- `fts_section_get_ms = 1,628`
- `fts_decode_ms = 18`
- `fts_clause_estimate_ms = 2`
- `scanned_segments = 0`
- `scanned_tail_docs = 0`
- `exact_candidate_time_ms = 234`

## Improvement

Approximate change versus baseline:

- browse total: `10,692 ms -> 2,129 ms` (`-80%`)
- indexed segment time: `10,486 ms -> 1,893 ms` (`-82%`)
- `.fts` section get time: `5,388 ms -> 1,628 ms` (`-70%`)
- `.fts` decode time: `52 ms -> 18 ms` (`-65%`)
- server RSS at comparable ingest size: `1,250,152 KB -> 747,776 KB` (`-40%`)

## Conclusion

The mmap-backed local companion cache materially improved the sealed-segment `.fts` browse path.

The dominant browse cost is still `.fts` section access, but it is much smaller than before. Decode and clause-estimation time remain minor, which confirms the main win is removing repeated remote companion fetch overhead and extra in-memory copies rather than optimizing parser CPU alone.

The remaining path to sub-100ms or single-digit-ms browse latency is not more container work. It is a different browse architecture, likely a stream-level prefix synopsis so no-hit prefix queries can avoid per-segment `.fts` probing altogether.
