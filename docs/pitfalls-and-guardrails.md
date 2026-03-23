# Prisma Streams Pitfalls And Guardrails

This document lists non-negotiable guardrails to avoid deadlocks/wedges, memory blowups, and multi-minute stalls.

The failures you are avoiding are the classic “system gets overwhelmed and never recovers” incidents.

---

## 1) Never do slow work in the request path

Forbidden in HTTP handlers:
- building segments
- uploading to R2
- scanning large WAL ranges
- reading entire large segment objects into memory

HTTP handlers may:
- validate input
- enqueue work
- do small indexed SQLite lookups
- wait on long-poll condition variables with timeouts

---

## 2) SQLite in Bun is synchronous: design around it

The native driver is fast but synchronous. Treat large queries as “slow I/O”.

Rules:
- prepared statements only
- use iterators for large result sets (never `.all()` on unbounded queries)
- batch writes with group commit
- cap the amount of work per tick for segmenter/uploader

If request tail latency matters, isolate heavy SQLite work in background loops (or Workers).

---

## 3) Bounded queues everywhere

Define and enforce limits:
- global append queue length
- per-stream append backlog (optional but recommended)
- segment build queue length
- upload queue length
- inflight uploads

Overload behavior must be explicit:
- block with timeout OR return 429/503
- never silently buffer until OOM

Expose queue depths via metrics.

---

## 4) Lock/ordering rules (even without mutexes)

Even in JS, you create “logical locks” when you serialize operations per stream.

Rules:
- per stream, ensure only one of:
  - segment build in progress
  - manifest update in progress
is active at a time (use DB flags)
- do not allow upload completion callbacks to synchronously invoke more work recursively (avoid call stack blowups)

---

## 5) Manifest upload is the commit point

Never advance `uploaded_through` until:
- segment bytes are uploaded successfully
- and the manifest generation that references them is uploaded successfully

If segment upload succeeds but manifest upload fails:
- segment must be treated as invisible
- uploader must retry manifest upload

This prevents readers from observing missing data in R2.

---

## 6) Stream isolation: one bad stream must not stall the node

If a stream repeatedly fails upload or segment build:
- quarantine it (cooldown)
- keep servicing other streams

Avoid global locks/queues that allow one stream to monopolize:
- candidate selection
- upload slots

---

## 7) Avoid “million stream” footguns

- do not create an in-memory object per stream
- do not keep per-stream open file descriptors
- keep caches bounded and keyed by active streams only
- listing `/v1/streams` must page from SQLite; do not load all streams into memory

---

## 8) Instrument stall points

You should be able to prove where time is spent:
- SQLite transaction duration histogram
- segment build duration histogram (per stage)
- upload duration (segment vs manifest)
- WAL lag bytes gauge (per stream and global)

If a “stall” happens, you must be able to say:
- is the writer loop blocked?
- is the segmenter behind?
- is the uploader behind?
- is R2 failing?
- is SQLite locked/busy?

---

## 9) Timeouts and retries are mandatory

For every external I/O:
- explicit timeout
- retry with exponential backoff + jitter
- maximum retry cap + quarantine

Never use infinite timeouts.

---

## 10) Disk pressure management

- segment temp directory must have cleanup on startup
- local cache has max size; enforce eviction
- monitor free disk; on low disk:
  - reduce segment build concurrency
  - or reject appends gracefully

---

## 11) Deterministic tests for wedge prevention

Add tests that:
- simulate slow R2 PUT for manifest
- simulate slow SQLite queries
- verify queues eventually drain and throughput recovers
- verify other streams continue to make progress
