# Index Builder Memory Investigation

Started: 2026-03-31 13:47:27 +07

## Scope

This report tracks:

- recovery after stopping the ingester
- code-level investigation of index and companion builders
- reproduction after restarting the server and rerunning the `all` ingester
- final conclusions about likely memory-retention causes and remediation

## Environment

- repo: `/Users/sorenschmidt/code/streams`
- server root: `/tmp/lib/prisma-streams`
- server url: `http://127.0.0.1:8787`
- primary stream: `gharchive-demo-all`
- server pid: `53208`

## Investigation Notes

### Code-level findings so far

- Exact and routing runs still only build on full `indexL0SpanSegments` windows. For a stream with 87 uploaded segments and a span of 16, exact indexes stop at 80 by design. That means "all indexes fully catch up" is not currently a valid expectation for a trailing partial span.
- The original L0 builders for routing and exact indexes walked `iterateBlocksResult()`, which fully decoded each block into `decoded.records` and copied every routing key and payload into fresh `Uint8Array` instances before the builders touched them.
- The original compaction path accumulated postings in `Map<bigint, Set<number>>`, which is substantially heavier than necessary because the input runs cover disjoint segment ranges. That inflated object count and retention pressure during compaction.
- The original exact-index builder also resolved all exact-search fields per record before selecting the current field, multiplying transient allocations by the number of configured exact indexes.
- The local segment and disk-cache read paths were cloning full files with `new Uint8Array(readFileSync(...))`, doubling the bytes held for every locally read segment or cached run payload.
- The memory guard only forced `Bun.gc()` when a request or append hit the overload path. If the process went idle while still over the limit, nothing proactively nudged recovery.

### Fixes applied in the repo

- Added a streaming `iterateBlockRecordsResult()` path in `src/segment/format.ts` and switched routing-index builds, exact-index builds, and bundled companion builds to consume records directly instead of materializing full decoded record arrays.
- Changed compaction assembly in `src/index/indexer.ts` and `src/index/secondary_indexer.ts` from `Set`-backed postings to array-backed postings with a final sort/dedupe pass.
- Changed exact-index extraction to resolve only the current configured field instead of building a map for all exact fields on every record.
- Removed unnecessary full-buffer copies from local segment reads and disk-cache reads so the builders now reuse the `Buffer` returned by `readFileSync()`.
- Changed `src/memory.ts` so the periodic sampler forces GC when the process remains over the limit, which is required for idle self-recovery tests.

### Verification

- `bun test test/segment_format.test.ts test/index_compaction.test.ts test/exact_index_backfill.test.ts test/memory_guard.test.ts`
- `bun run typecheck`
- `bun run check:result-policy`
- `bun test`

### Internet research comparison

- Bun is not V8-based here; it runs on JavaScriptCore. Bun’s own docs say the runtime is "powered by JavaScriptCore" and describe separate JavaScript and native heaps, with `Bun.gc(true)` for JS GC and `MIMALLOC_SHOW_STATS=1` for native-heap reporting. That means high RSS can persist even when the JS heap is not obviously leaking.
- WebKit’s JavaScriptCore GC notes explain that some reclamation is lazy and that dead-object destructors may run later via `IncrementalSweeper`, not immediately when objects become unreachable. This matches the observed pattern where the server can go idle but still hold a large footprint for a long time.
- A current Bun issue about repeated large file reads (`oven-sh/bun#15020`) reports memory staying high after idle and dropping materially only after explicit GC. Our workload is similar: repeated segment reads, decompression, and large transient string/JSON allocations.
- V8 guidance still helped as a pattern catalogue. The main tricky patterns I checked for in this repo were:
  - closure-captured objects kept alive by shared lexical environments
  - listener/timer accumulation without cleanup
  - unbounded caches in `Map`/`Set`
  - weak-reference misuse
- I did not find strong evidence for classic long-lived closure/listener leaks in the Streams code. The bounded caches (`LruCache`, `IndexRunCache`) are small, and the timer/listener sites I checked are stable and cleaned up. The strongest match remains transient allocation churn in the index/companion builders combined with JSC/Bun retaining or lazily reclaiming those pages.

### Baseline interpretation before restart

- Stopping the ingester does not stop the original server from holding a ~50 GiB physical footprint. By 14:03 local time, the server had been idle from the ingest side for about 15 minutes and still reported a 50.1-50.4 GiB physical footprint with dominant `WebKit Malloc`, `JS VM Gigacage`, and a very large swapped `IOAccelerator` region.
- The server is not deadlocked. `/health` remains responsive, the stream is stable, bundled companions remain fully caught up, and there is no uploaded-segment backlog.
- The baseline failure mode is therefore: forward progress can stop on the ingest side while the process remains alive and responsive, but memory does not automatically return to a healthy range. That is the behavior the patched restart needs to improve.

## Timeline


### 2026-03-31 13:47:57 +07 | recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.812370
HTTP_CODE=200

[index_status]
{"stream":"gharchive-demo-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":87,"uploaded_count":87},"manifest":{"generation":226,"uploaded_generation":226,"last_uploaded_at":"2026-03-31T06:45:19.621Z","last_uploaded_etag":"W/\"9fed16e3585add487a370b9412d0c2ec"},"routing_key_index":{"configured":false,"indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T06:35:48.710Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:25.094Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:53.079Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:04.138Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:29.742Z"},{"name":"isBot","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:27.212Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:03.373Z"},{"name":"public","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:04.188Z"},{"name":"refType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:43.439Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:18.833Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:40.979Z"}],"bundled_companions":{"object_count":87,"fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"}]}
TIME_TOTAL=0.002813
HTTP_CODE=200

[ps]
53208 61973  96.8 23.4 15691776 580110240 01:40:05 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 587 total, 5 running, 582 sleeping, 5748 threads 
2026/03/31 13:47:59
Load Avg: 4.03, 4.67, 4.74 
CPU usage: 7.94% user, 11.47% sys, 80.58% idle 
SharedLibs: 910M resident, 154M data, 587M linkedit.
MemRegions: 862138 total, 15G resident, 69M private, 4338M shared.
PhysMem: 63G used (4614M wired, 34G compressor), 96M unused.
VM: 288T vsize, 5702M framework vsize, 204663(0) swapins, 1260300(0) swapouts.
Networks: packets: 62260691/106G in, 22085783/43G out.
Disks: 9026106/134G read, 16228043/341G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
53208  bun     0.0  01:52:19 26/1 66     48G 48G N/A   N/A   running

[stream]
stream              next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-demo-all  3600000      3595563         3595563           87                      4436          1827313        4436      1827313    0                    1774939513396   1774939503838      

[segments]
total_segments  uploaded_segments
--------------  -----------------
87              87               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
87               

[routing_index]
indexed_through
---------------
80             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
80                         80                         10               
```

### 2026-03-31 13:52:59 +07 | recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.482821
HTTP_CODE=200

[index_status]
{"stream":"gharchive-demo-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":87,"uploaded_count":87},"manifest":{"generation":226,"uploaded_generation":226,"last_uploaded_at":"2026-03-31T06:45:19.621Z","last_uploaded_etag":"W/\"9fed16e3585add487a370b9412d0c2ec"},"routing_key_index":{"configured":false,"indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T06:35:48.710Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:25.094Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:53.079Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:04.138Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:29.742Z"},{"name":"isBot","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:27.212Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:03.373Z"},{"name":"public","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:04.188Z"},{"name":"refType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:43.439Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:18.833Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:40.979Z"}],"bundled_companions":{"object_count":87,"fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"}]}
TIME_TOTAL=0.005640
HTTP_CODE=200

[ps]
53208 61973  80.8 22.3 14976128 580110240 01:45:07 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 506 total, 3 running, 503 sleeping, 5581 threads 
2026/03/31 13:53:00
Load Avg: 4.09, 4.50, 4.65 
CPU usage: 6.56% user, 11.11% sys, 82.31% idle 
SharedLibs: 911M resident, 154M data, 587M linkedit.
MemRegions: 853255 total, 11G resident, 96M private, 4325M shared.
PhysMem: 63G used (4575M wired, 35G compressor), 88M unused.
VM: 257T vsize, 5702M framework vsize, 213650(0) swapins, 1397568(0) swapouts.
Networks: packets: 63606216/108G in, 22250389/43G out.
Disks: 9115702/135G read, 16314067/345G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
53208  bun     0.0  01:58:49 26/1 66     48G 48G N/A   N/A   running

[stream]
stream              next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-demo-all  3600000      3595563         3595563           87                      4436          1827313        4436      1827313    0                    1774939513396   1774939503838      

[segments]
total_segments  uploaded_segments
--------------  -----------------
87              87               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
87               

[routing_index]
indexed_through
---------------
80             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
80                         80                         10               
```

### 2026-03-31 13:58:01 +07 | recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.910476
HTTP_CODE=200

[index_status]
{"stream":"gharchive-demo-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":87,"uploaded_count":87},"manifest":{"generation":226,"uploaded_generation":226,"last_uploaded_at":"2026-03-31T06:45:19.621Z","last_uploaded_etag":"W/\"9fed16e3585add487a370b9412d0c2ec"},"routing_key_index":{"configured":false,"indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T06:35:48.710Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:25.094Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:53.079Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:04.138Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:29.742Z"},{"name":"isBot","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:27.212Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:03.373Z"},{"name":"public","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:04.188Z"},{"name":"refType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:43.439Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:18.833Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:40.979Z"}],"bundled_companions":{"object_count":87,"fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"}]}
TIME_TOTAL=0.002432
HTTP_CODE=200

[ps]
53208 61973 127.9 23.2 15565360 583911344 01:50:08 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 540 total, 4 running, 536 sleeping, 5596 threads 
2026/03/31 13:58:02
Load Avg: 5.43, 4.92, 4.78 
CPU usage: 11.51% user, 9.90% sys, 78.57% idle 
SharedLibs: 915M resident, 156M data, 587M linkedit.
MemRegions: 855047 total, 15G resident, 71M private, 4128M shared.
PhysMem: 63G used (4613M wired, 34G compressor), 88M unused.
VM: 270T vsize, 5702M framework vsize, 218221(0) swapins, 1482664(0) swapouts.
Networks: packets: 64940564/110G in, 22346555/43G out.
Disks: 9270979/138G read, 16463671/349G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
53208  bun     0.0  02:05:32 26/1 66     52G 52G N/A   N/A   running

[stream]
stream              next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-demo-all  3600000      3595563         3595563           87                      4436          1827313        4436      1827313    0                    1774939513396   1774939503838      

[segments]
total_segments  uploaded_segments
--------------  -----------------
87              87               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
87               

[routing_index]
indexed_through
---------------
80             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
80                         80                         10               
```

### 2026-03-31 14:03:02 +07 | recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=1.089141
HTTP_CODE=200

[index_status]
{"stream":"gharchive-demo-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":87,"uploaded_count":87},"manifest":{"generation":226,"uploaded_generation":226,"last_uploaded_at":"2026-03-31T06:45:19.621Z","last_uploaded_etag":"W/\"9fed16e3585add487a370b9412d0c2ec"},"routing_key_index":{"configured":false,"indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T06:35:48.710Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:25.094Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:53.079Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:04.138Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:29.742Z"},{"name":"isBot","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:27.212Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:03.373Z"},{"name":"public","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:04.188Z"},{"name":"refType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:43.439Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:18.833Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:40.979Z"}],"bundled_companions":{"object_count":87,"fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"}]}
TIME_TOTAL=0.002329
HTTP_CODE=200

[ps]
53208 61973  99.4 20.4 13683152 588367808 01:55:11 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 573 total, 6 running, 567 sleeping, 5698 threads 
2026/03/31 14:03:05
Load Avg: 5.12, 4.91, 4.81 
CPU usage: 9.83% user, 17.45% sys, 72.70% idle 
SharedLibs: 906M resident, 154M data, 586M linkedit.
MemRegions: 856768 total, 14G resident, 89M private, 4395M shared.
PhysMem: 63G used (4893M wired, 34G compressor), 93M unused.
VM: 283T vsize, 5702M framework vsize, 220612(0) swapins, 1580976(0) swapouts.
Networks: packets: 66289269/112G in, 22493882/43G out.
Disks: 9352243/140G read, 16520643/351G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
53208  bun     0.0  02:12:21 26/4 66     53G 53G N/A   N/A   running

[stream]
stream              next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-demo-all  3600000      3595563         3595563           87                      4436          1827313        4436      1827313    0                    1774939513396   1774939503838      

[segments]
total_segments  uploaded_segments
--------------  -----------------
87              87               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
87               

[routing_index]
indexed_through
---------------
80             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
80                         80                         10               
```

### 2026-03-31 14:08:05 +07 | recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.706158
HTTP_CODE=200

[index_status]
{"stream":"gharchive-demo-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":87,"uploaded_count":87},"manifest":{"generation":226,"uploaded_generation":226,"last_uploaded_at":"2026-03-31T06:45:19.621Z","last_uploaded_etag":"W/\"9fed16e3585add487a370b9412d0c2ec"},"routing_key_index":{"configured":false,"indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T06:35:48.710Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:25.094Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:53.079Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:04.138Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:29.742Z"},{"name":"isBot","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:27.212Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:03.373Z"},{"name":"public","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:04.188Z"},{"name":"refType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:43.439Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:18.833Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:40.979Z"}],"bundled_companions":{"object_count":87,"fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"}]}
TIME_TOTAL=0.002358
HTTP_CODE=200

[ps]
53208 61973 119.3 22.3 14964336 588367808 02:00:12 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 579 total, 6 running, 573 sleeping, 5685 threads 
2026/03/31 14:08:06
Load Avg: 5.10, 5.05, 4.88 
CPU usage: 15.8% user, 9.58% sys, 75.32% idle 
SharedLibs: 907M resident, 154M data, 586M linkedit.
MemRegions: 857108 total, 12G resident, 67M private, 4319M shared.
PhysMem: 63G used (4308M wired, 34G compressor), 108M unused.
VM: 285T vsize, 5702M framework vsize, 221235(0) swapins, 1675900(0) swapouts.
Networks: packets: 67582915/114G in, 22576095/43G out.
Disks: 9594784/143G read, 16566388/353G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
53208  bun     0.0  02:18:47 26/2 66     51G 51G N/A   N/A   running

[stream]
stream              next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-demo-all  3600000      3595563         3595563           87                      4436          1827313        4436      1827313    0                    1774939513396   1774939503838      

[segments]
total_segments  uploaded_segments
--------------  -----------------
87              87               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
87               

[routing_index]
indexed_through
---------------
80             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
80                         80                         10               
```

### 2026-03-31 14:13:06 +07 | recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=3.096582
HTTP_CODE=200

[index_status]
{"stream":"gharchive-demo-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":87,"uploaded_count":87},"manifest":{"generation":226,"uploaded_generation":226,"last_uploaded_at":"2026-03-31T06:45:19.621Z","last_uploaded_etag":"W/\"9fed16e3585add487a370b9412d0c2ec"},"routing_key_index":{"configured":false,"indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T06:35:48.710Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:25.094Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:53.079Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:04.138Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:29.742Z"},{"name":"isBot","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:27.212Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:03.373Z"},{"name":"public","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:04.188Z"},{"name":"refType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:43.439Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:18.833Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:40.979Z"}],"bundled_companions":{"object_count":87,"fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"}]}
TIME_TOTAL=0.007816
HTTP_CODE=200

[ps]
53208 61973  99.0 22.0 14741504 592693200 02:05:16 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 562 total, 6 running, 1 stuck, 555 sleeping, 5677 threads 
2026/03/31 14:13:10
Load Avg: 4.50, 4.78, 4.82 
CPU usage: 16.57% user, 17.12% sys, 66.29% idle 
SharedLibs: 907M resident, 154M data, 586M linkedit.
MemRegions: 857426 total, 9721M resident, 84M private, 3899M shared.
PhysMem: 63G used (5122M wired, 34G compressor), 154M unused.
VM: 279T vsize, 5702M framework vsize, 222210(0) swapins, 1777984(0) swapouts.
Networks: packets: 68926472/115G in, 22670875/43G out.
Disks: 9723815/146G read, 16675103/355G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
53208  bun     0.0  02:25:41 26/1 66     50G 50G N/A   N/A   running

[stream]
stream              next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-demo-all  3600000      3595563         3595563           87                      4436          1827313        4436      1827313    0                    1774939513396   1774939503838      

[segments]
total_segments  uploaded_segments
--------------  -----------------
87              87               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
87               

[routing_index]
indexed_through
---------------
80             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
80                         80                         10               
```

### 2026-03-31 14:18:10 +07 | recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.469587
HTTP_CODE=200

[index_status]
{"stream":"gharchive-demo-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":87,"uploaded_count":87},"manifest":{"generation":226,"uploaded_generation":226,"last_uploaded_at":"2026-03-31T06:45:19.621Z","last_uploaded_etag":"W/\"9fed16e3585add487a370b9412d0c2ec"},"routing_key_index":{"configured":false,"indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T06:35:48.710Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:25.094Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:53.079Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:04.138Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:36:29.742Z"},{"name":"isBot","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:27.212Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:03.373Z"},{"name":"public","kind":"bool","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:39:04.188Z"},{"name":"refType","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:38:43.439Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:18.833Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":80,"active_run_count":5,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T06:37:40.979Z"}],"bundled_companions":{"object_count":87,"fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":87,"stale_segment_count":0,"object_count":87,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T05:08:15.801Z"}]}
TIME_TOTAL=0.001558
HTTP_CODE=200

[ps]
53208 61973 103.2 22.8 15303344 601081824 02:10:18 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 576 total, 4 running, 572 sleeping, 5664 threads 
2026/03/31 14:18:12
Load Avg: 3.93, 5.27, 5.14 
CPU usage: 14.8% user, 6.54% sys, 79.37% idle 
SharedLibs: 910M resident, 155M data, 587M linkedit.
MemRegions: 859042 total, 9155M resident, 104M private, 2544M shared.
PhysMem: 63G used (3154M wired, 34G compressor), 100M unused.
VM: 284T vsize, 5702M framework vsize, 229054(0) swapins, 1892260(0) swapouts.
Networks: packets: 70258007/117G in, 22799655/43G out.
Disks: 9788651/147G read, 16797441/358G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
53208  bun     0.0  02:32:34 26/5 66     51G 51G N/A   N/A   running

[stream]
stream              next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-demo-all  3600000      3595563         3595563           87                      4436          1827313        4436      1827313    0                    1774939513396   1774939503838      

[segments]
total_segments  uploaded_segments
--------------  -----------------
87              87               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
87               

[routing_index]
indexed_through
---------------
80             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
80                         80                         10               
```

### 2026-03-31 14:18:18 +07 | baseline-idle-recovery-conclusion

```text
- Baseline window length: 30 minutes (13:47:57 -> 14:18:10 local time)
- Result: the original server did not self-recover its memory footprint after ingest stopped.
- Memory stayed in the same rough band the whole time:
  - top MEM: ~48G -> 53G -> 50G -> 51G
  - vmmap physical footprint snapshots: ~50.1G to ~50.4G
- Search coverage was already fully caught up the whole time:
  - uploaded segments: 87/87
  - bundled companions: 87/87
  - search families stale segments: 0
- Exact/routing runs stayed at 80 indexed segments for the full window because the trailing 7 segments do not form another 16-segment span.
- Operational conclusion: on the unpatched server, stopping ingest is not sufficient to bring process memory back down to a healthy range without a restart.
```

### 2026-03-31 14:18:39 +07 | patched-phase-start

```text
server_log=experiments/reports/patched-server.log
ingester_log=experiments/reports/patched-ingester.log
base_url=http://127.0.0.1:8787
stream=gharchive-memtest-all
sample_interval_seconds=300
ingest_samples=6
recovery_samples=6
```

### 2026-03-31 14:18:39 +07 | patched-server-started

```text
pid=44765
```

### 2026-03-31 14:18:41 +07 | patched-server-before-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.000531
HTTP_CODE=200

[index_status]
{"error":{"code":"not_found","message":"not_found"}}
TIME_TOTAL=0.000685
HTTP_CODE=404

[ps]
44765 44755  28.2  0.3 224192 485321680 00:02 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 592 total, 4 running, 588 sleeping, 5762 threads 
2026/03/31 14:18:41
Load Avg: 3.77, 5.10, 5.09 
CPU usage: 11.12% user, 5.90% sys, 82.97% idle 
SharedLibs: 911M resident, 155M data, 587M linkedit.
MemRegions: 860273 total, 8642M resident, 92M private, 2957M shared.
PhysMem: 63G used (3423M wired, 35G compressor), 155M unused.
VM: 291T vsize, 5702M framework vsize, 229526(0) swapins, 1892260(0) swapouts.
Networks: packets: 70413707/117G in, 22817868/43G out.
Disks: 9797603/147G read, 16802995/358G written.

PID    COMMAND %CPU TIME     #TH #PORTS MEM  MEM  RPRVT VSIZE STATE   
44765  bun     0.0  00:00.64 34  70     111M 111M N/A   N/A   sleeping

[stream]


[segments]
total_segments  uploaded_segments
--------------  -----------------
0                                

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
0                

[routing_index]


[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
                                                      0                
```

### 2026-03-31 14:18:42 +07 | patched-ingester-started

```text
pid=44916
stream=gharchive-memtest-all
```

### 2026-03-31 14:23:42 +07 | patched-server-during-all-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.405984
HTTP_CODE=200

[index_status]
{"stream":"gharchive-memtest-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":15,"uploaded_count":15},"manifest":{"generation":30,"uploaded_generation":30,"last_uploaded_at":"2026-03-31T07:23:39.616Z","last_uploaded_etag":"W/\"f0386e83a302fb4f9fb780d240b9cebb","last_uploaded_size_bytes":"8991"},"routing_key_index":{"configured":false,"indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:18:43.065Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.069Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.069Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.068Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.068Z"},{"name":"isBot","kind":"bool","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.070Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.069Z"},{"name":"public","kind":"bool","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.070Z"},{"name":"refType","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.070Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.069Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:18:43.069Z"}],"bundled_companions":{"object_count":14,"bytes_at_rest":"65594052","fully_indexed_uploaded_segments":false},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":14,"contiguous_covered_segment_count":14,"lag_segments":1,"lag_ms":"29334","bytes_at_rest":"5247132","object_count":14,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T07:18:43.078Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":14,"contiguous_covered_segment_count":14,"lag_segments":1,"lag_ms":"29334","bytes_at_rest":"17099357","object_count":14,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T07:18:43.078Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":14,"contiguous_covered_segment_count":14,"lag_segments":1,"lag_ms":"29334","bytes_at_rest":"43244087","object_count":14,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T07:18:43.078Z"}]}
TIME_TOTAL=0.144904
HTTP_CODE=200

[ps]
44765 44755 140.3 30.9 20754912 506279936 05:03 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 583 total, 5 running, 578 sleeping, 5705 threads 
2026/03/31 14:23:43
Load Avg: 2.99, 3.38, 4.25 
CPU usage: 12.22% user, 7.19% sys, 80.58% idle 
SharedLibs: 911M resident, 155M data, 587M linkedit.
MemRegions: 871287 total, 11G resident, 175M private, 3434M shared.
PhysMem: 44G used (3521M wired, 5848M compressor), 19G unused.
VM: 288T vsize, 5702M framework vsize, 340498(0) swapins, 1989925(0) swapouts.
Networks: packets: 71703761/119G in, 23040745/44G out.
Disks: 9847590/149G read, 16984267/363G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM   MEM   RPRVT VSIZE STATE  
44765  bun     0.0  04:26.17 30/1 67     4980M 4980M N/A   N/A   running

[stream]
stream                 next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
---------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-memtest-all  639000       623857          623857            15                      15142         6113085        15142     6113085    0                    1774941823381   1774941816518      

[segments]
total_segments  uploaded_segments
--------------  -----------------
15              15               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
14               

[routing_index]
indexed_through
---------------
0              

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          0                          10               
```

### 2026-03-31 14:28:43 +07 | patched-server-during-all-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.001338
HTTP_CODE=200

[index_status]
{"stream":"gharchive-memtest-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:28:38.554Z","last_uploaded_etag":"W/\"74c6d3cde2553b10758bc53a6071b772","last_uploaded_size_bytes":"15931"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:24:04.464Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:25:26.976Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:24:40.076Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:24:12.446Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:24:27.988Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:25:55.595Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:25:18.251Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:25:45.249Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:25:34.973Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:24:56.654Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"284677","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:25:09.785Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127767405","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"11623","bytes_at_rest":"10149732","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:18:43.078Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"11623","bytes_at_rest":"33211764","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:18:43.078Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"11623","bytes_at_rest":"84399196","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:18:43.078Z"}]}
TIME_TOTAL=0.022954
HTTP_CODE=200

[ps]
44765 44755  22.8 40.6 27232304 512050128 10:04 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 582 total, 4 running, 578 sleeping, 5722 threads 
2026/03/31 14:28:44
Load Avg: 3.79, 3.56, 4.07 
CPU usage: 10.89% user, 6.52% sys, 82.57% idle 
SharedLibs: 912M resident, 155M data, 587M linkedit.
MemRegions: 872397 total, 14G resident, 217M private, 3438M shared.
PhysMem: 51G used (3229M wired, 5445M compressor), 12G unused.
VM: 288T vsize, 5702M framework vsize, 340762(0) swapins, 1989925(0) swapouts.
Networks: packets: 72961459/121G in, 23331757/44G out.
Disks: 9850318/149G read, 17131693/365G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM   MEM   RPRVT VSIZE STATE  
44765  bun     0.0  09:06.46 29/1 66     7750M 7750M N/A   N/A   running

[stream]
stream                 next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
---------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-memtest-all  1151000      1122206         1122206           27                      28793         11656848       28793     11656848   0                    1774942120506   1774942109364      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 14:33:44 +07 | patched-server-during-all-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.012232
HTTP_CODE=200

[index_status]
{"stream":"gharchive-memtest-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":36,"uploaded_count":36},"manifest":{"generation":89,"uploaded_generation":89,"last_uploaded_at":"2026-03-31T07:33:26.881Z","last_uploaded_etag":"W/\"a6bd98b59566e7e42af825439b7f3f26","last_uploaded_size_bytes":"21784"},"routing_key_index":{"configured":false,"indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"72","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:30:48.108Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"404","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:32:07.546Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"1740426","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:31:24.955Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"524","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:30:56.215Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"14801596","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:31:12.670Z"},{"name":"isBot","kind":"bool","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"192","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:32:36.303Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"83616","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:32:00.748Z"},{"name":"public","kind":"bool","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"172","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:32:24.706Z"},{"name":"refType","kind":"keyword","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"192","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:32:13.553Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"2063314","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:31:39.459Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":32,"lag_segments":4,"lag_ms":"183674","bytes_at_rest":"1151536","object_count":2,"active_run_count":2,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:31:52.381Z"}],"bundled_companions":{"object_count":36,"bytes_at_rest":"170312670","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":36,"contiguous_covered_segment_count":36,"lag_segments":0,"lag_ms":"25671","bytes_at_rest":"13475092","object_count":36,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:18:43.078Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":36,"contiguous_covered_segment_count":36,"lag_segments":0,"lag_ms":"25671","bytes_at_rest":"44301366","object_count":36,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:18:43.078Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":36,"contiguous_covered_segment_count":36,"lag_segments":0,"lag_ms":"25671","bytes_at_rest":"112527258","object_count":36,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:18:43.078Z"}]}
TIME_TOTAL=0.025792
HTTP_CODE=200

[ps]
44765 44755  90.1 44.0 29520672 514441664 15:05 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 583 total, 4 running, 579 sleeping, 5726 threads 
2026/03/31 14:33:45
Load Avg: 3.92, 3.94, 4.09 
CPU usage: 13.40% user, 8.67% sys, 77.92% idle 
SharedLibs: 913M resident, 155M data, 587M linkedit.
MemRegions: 873344 total, 17G resident, 267M private, 3606M shared.
PhysMem: 54G used (3228M wired, 4929M compressor), 9311M unused.
VM: 288T vsize, 5702M framework vsize, 343887(0) swapins, 1989925(0) swapouts.
Networks: packets: 74231727/123G in, 23592891/44G out.
Disks: 9859036/150G read, 17269114/368G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM   MEM   RPRVT VSIZE STATE  
44765  bun     0.0  13:38.17 29/1 66     9636M 9636M N/A   N/A   running

[stream]
stream                 next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
---------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-memtest-all  1537000      1495436         1495436           36                      41563         16799213       41563     16799213   1                    1774942425139   1774942398468      

[segments]
total_segments  uploaded_segments
--------------  -----------------
36              36               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
36               

[routing_index]
indexed_through
---------------
32             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
32                         32                         10               
```
### 2026-03-31 14:33:00 +07 | patched-run-invalidated-by-darwin-guard-bug

```text
- This run includes the builder-side memory fixes, but it was started before the Darwin memory-guard probe was corrected.
- The macOS guard was effectively disabled because it sampled `top -stats mem`, which omits the PID column required by the parser.
- Result: this run remains useful for measuring builder transient memory, but it is not a valid recovery/backpressure experiment.
- Action: terminate this run and rerun from a fresh server binary with the fixed Darwin guard.
```
### 2026-03-31 14:35:30 +07 | patched-phase-2-start

```text
server_log=experiments/reports/patched2-server.log
ingester_log=experiments/reports/patched2-ingester.log
base_url=http://127.0.0.1:8787
stream=gharchive-guardfix-all
sample_interval_seconds=300
ingest_samples=6
recovery_samples=6
notes=Includes builder memory reductions plus fixed Darwin memory-guard physical-footprint probe.
```

### 2026-03-31 14:36:05 +07 | patched-phase-start

```text
server_log=experiments/reports/patched2-server.log
ingester_log=experiments/reports/patched2-ingester.log
base_url=http://127.0.0.1:8787
stream=gharchive-guardfix-all
sample_interval_seconds=300
ingest_samples=6
recovery_samples=6
```

### 2026-03-31 14:36:05 +07 | patched-server-started

```text
pid=54375
```

### 2026-03-31 14:36:06 +07 | patched-server-before-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.000515
HTTP_CODE=200

[index_status]
{"error":{"code":"not_found","message":"not_found"}}
TIME_TOTAL=0.000654
HTTP_CODE=404

[ps]
54375 54367 114.7  2.2 1468992 486524528 00:01 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 591 total, 5 running, 586 sleeping, 5692 threads 
2026/03/31 14:36:07
Load Avg: 4.63, 4.15, 4.15 
CPU usage: 17.37% user, 6.59% sys, 76.2% idle 
SharedLibs: 913M resident, 155M data, 587M linkedit.
MemRegions: 871425 total, 9906M resident, 281M private, 3758M shared.
PhysMem: 28G used (3188M wired, 4781M compressor), 35G unused.
VM: 291T vsize, 5702M framework vsize, 344007(0) swapins, 1989925(0) swapouts.
Networks: packets: 74729736/124G in, 23680407/44G out.
Disks: 9860096/150G read, 17337507/369G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM   MEM   RPRVT VSIZE STATE  
54375  bun     0.0  00:01.90 33/1 69     2137M 2137M N/A   N/A   running

[stream]


[segments]
total_segments  uploaded_segments
--------------  -----------------
0                                

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
0                

[routing_index]


[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
                                                      0                
```

### 2026-03-31 14:36:07 +07 | patched-ingester-started

```text
pid=54539
stream=gharchive-guardfix-all
```

### 2026-03-31 14:41:07 +07 | patched-server-during-all-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=2.054321
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":18,"uploaded_count":18},"manifest":{"generation":39,"uploaded_generation":39,"last_uploaded_at":"2026-03-31T07:41:03.322Z","last_uploaded_etag":"W/\"752097407ab4f0ee8fec90b87936a86d","last_uploaded_size_bytes":"11369"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":2,"lag_ms":"72937","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":0,"lag_segments":18,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:36:09.643Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":2,"lag_ms":"72937","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":2,"lag_ms":"72937","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":2,"lag_ms":"72937","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":0,"lag_segments":18,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:36:09.644Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":0,"lag_segments":18,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:36:09.643Z"},{"name":"public","kind":"bool","indexed_segment_count":0,"lag_segments":18,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:36:09.644Z"},{"name":"refType","kind":"keyword","indexed_segment_count":0,"lag_segments":18,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:36:09.644Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":2,"lag_ms":"72937","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":0,"lag_segments":18,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:36:09.643Z"}],"bundled_companions":{"object_count":17,"bytes_at_rest":"79879274","fully_indexed_uploaded_segments":false},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":17,"contiguous_covered_segment_count":17,"lag_segments":1,"lag_ms":"46171","bytes_at_rest":"6381688","object_count":17,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":17,"contiguous_covered_segment_count":17,"lag_segments":1,"lag_ms":"46171","bytes_at_rest":"20808616","object_count":17,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":17,"contiguous_covered_segment_count":17,"lag_segments":1,"lag_ms":"46171","bytes_at_rest":"52684730","object_count":17,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.240422
HTTP_CODE=200

[ps]
54375 54367 154.9 32.8 22029952 506459472 05:04 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 604 total, 4 running, 1 stuck, 599 sleeping, 5814 threads 
2026/03/31 14:41:10
Load Avg: 4.81, 4.13, 4.09 
CPU usage: 17.10% user, 8.61% sys, 74.27% idle 
SharedLibs: 915M resident, 155M data, 587M linkedit.
MemRegions: 887563 total, 11G resident, 335M private, 3603M shared.
PhysMem: 48G used (3494M wired, 4685M compressor), 16G unused.
VM: 298T vsize, 5702M framework vsize, 345893(0) swapins, 1989925(0) swapouts.
Networks: packets: 75985496/126G in, 23984037/45G out.
Disks: 9865984/150G read, 17554323/373G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM   MEM   RPRVT VSIZE STATE  
54375  bun     0.0  04:56.56 28/1 65     3257M 3257M N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  765000       748605          748605            18                      16394         6599140        16394     6599140    0                    1774942870477   1774942857283      

[segments]
total_segments  uploaded_segments
--------------  -----------------
18              18               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
17               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          16                         10               
```

### 2026-03-31 14:46:10 +07 | patched-server-during-all-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.184957
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.033677
HTTP_CODE=200

[ps]
54375 54367  82.8 37.7 25281440 510061744 10:05 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 602 total, 4 running, 1 stuck, 597 sleeping, 5768 threads 
2026/03/31 14:46:11
Load Avg: 3.26, 3.75, 3.93 
CPU usage: 12.5% user, 6.55% sys, 81.39% idle 
SharedLibs: 915M resident, 155M data, 587M linkedit.
MemRegions: 876913 total, 13G resident, 335M private, 3607M shared.
PhysMem: 51G used (3199M wired, 4668M compressor), 12G unused.
VM: 296T vsize, 5702M framework vsize, 347042(0) swapins, 1989925(0) swapouts.
Networks: packets: 77236130/128G in, 24215835/45G out.
Disks: 9867037/150G read, 17687692/375G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM   MEM   RPRVT VSIZE STATE  
54375  bun     0.0  09:29.98 29/1 66     4837M 4837M N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 14:51:11 +07 | patched-server-during-all-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.030958
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.016317
HTTP_CODE=200

[ps]
54375 54367 117.4 40.1 26901248 513206352 15:06 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 599 total, 4 running, 595 sleeping, 5754 threads 
2026/03/31 14:51:12
Load Avg: 3.29, 3.64, 3.84 
CPU usage: 8.74% user, 9.75% sys, 81.49% idle 
SharedLibs: 916M resident, 155M data, 587M linkedit.
MemRegions: 888737 total, 15G resident, 334M private, 3719M shared.
PhysMem: 52G used (3193M wired, 4407M compressor), 11G unused.
VM: 296T vsize, 5702M framework vsize, 369037(0) swapins, 2011272(0) swapouts.
Networks: packets: 78328523/129G in, 24284376/45G out.
Disks: 9874449/150G read, 17756050/376G written.

PID    COMMAND %CPU TIME     #TH #PORTS MEM   MEM   RPRVT VSIZE STATE   
54375  bun     0.0  13:28.38 28  65     6411M 6411M N/A   N/A   sleeping

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 14:56:12 +07 | patched-server-during-all-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.000483
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.017688
HTTP_CODE=200

[ps]
54375 54367  86.2 42.5 28510960 515304064 20:07 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 602 total, 6 running, 596 sleeping, 5782 threads 
2026/03/31 14:56:13
Load Avg: 3.62, 3.74, 3.81 
CPU usage: 11.51% user, 7.9% sys, 81.39% idle 
SharedLibs: 916M resident, 155M data, 587M linkedit.
MemRegions: 877929 total, 16G resident, 341M private, 3726M shared.
PhysMem: 54G used (3193M wired, 4395M compressor), 9402M unused.
VM: 296T vsize, 5702M framework vsize, 369333(0) swapins, 2011272(0) swapouts.
Networks: packets: 79422352/131G in, 24370603/45G out.
Disks: 9876807/150G read, 17803417/376G written.

PID    COMMAND %CPU TIME     #TH #PORTS MEM   MEM   RPRVT VSIZE STATE   
54375  bun     0.0  17:29.74 29  66     7976M 7976M N/A   N/A   sleeping

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:01:13 +07 | patched-server-during-all-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.003895
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.018511
HTTP_CODE=200

[ps]
54375 54367 102.1 44.9 30155680 517401216 25:08 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 593 total, 5 running, 1 stuck, 587 sleeping, 5756 threads 
2026/03/31 15:01:14
Load Avg: 4.46, 4.07, 3.92 
CPU usage: 17.63% user, 8.27% sys, 74.8% idle 
SharedLibs: 917M resident, 155M data, 587M linkedit.
MemRegions: 866154 total, 18G resident, 331M private, 3881M shared.
PhysMem: 56G used (4464M wired, 4385M compressor), 7485M unused.
VM: 291T vsize, 5702M framework vsize, 369739(0) swapins, 2011272(0) swapouts.
Networks: packets: 80537777/132G in, 24438964/45G out.
Disks: 9879500/150G read, 17854975/377G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM   MEM   RPRVT VSIZE STATE  
54375  bun     0.0  21:34.63 29/4 66     9583M 9583M N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:06:14 +07 | patched-server-during-all-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.126367
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.030477
HTTP_CODE=200

[ps]
54375 54367 173.3 48.7 32690256 519661648 30:09 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 588 total, 6 running, 582 sleeping, 5715 threads 
2026/03/31 15:06:15
Load Avg: 3.19, 3.51, 3.70 
CPU usage: 10.8% user, 7.19% sys, 82.72% idle 
SharedLibs: 918M resident, 155M data, 587M linkedit.
MemRegions: 889738 total, 20G resident, 396M private, 6868M shared.
PhysMem: 61G used (4752M wired, 3835M compressor), 1879M unused.
VM: 292T vsize, 5702M framework vsize, 373961(0) swapins, 2011272(0) swapouts.
Networks: packets: 82053081/134G in, 24614439/45G out.
Disks: 9896230/150G read, 17882676/377G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
54375  bun     0.0  26:41.66 28/1 65     12G 12G N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:06:15 +07 | patched-ingester-finished-before-stop

```text
pid=54539
```

### 2026-03-31 15:06:15 +07 | patched-server-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.022201
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.019274
HTTP_CODE=200

[ps]
54375 54367 136.0 48.7 32699984 521758800 30:10 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 588 total, 5 running, 583 sleeping, 5715 threads 
2026/03/31 15:06:16
Load Avg: 3.25, 3.52, 3.70 
CPU usage: 10.78% user, 7.26% sys, 81.94% idle 
SharedLibs: 918M resident, 155M data, 587M linkedit.
MemRegions: 889765 total, 20G resident, 396M private, 6866M shared.
PhysMem: 61G used (4638M wired, 3835M compressor), 1866M unused.
VM: 292T vsize, 5702M framework vsize, 373961(0) swapins, 2011272(0) swapouts.
Networks: packets: 82059968/134G in, 24614751/45G out.
Disks: 9896232/150G read, 17882752/377G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
54375  bun     0.0  26:42.83 28/1 65     12G 12G N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:11:16 +07 | patched-server-recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.462205
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.026915
HTTP_CODE=200

[ps]
54375 54367 121.4 52.3 35118704 523855952 35:12 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 594 total, 5 running, 589 sleeping, 5704 threads 
2026/03/31 15:11:18
Load Avg: 4.15, 3.97, 3.85 
CPU usage: 15.63% user, 7.79% sys, 76.56% idle 
SharedLibs: 916M resident, 155M data, 587M linkedit.
MemRegions: 890672 total, 23G resident, 410M private, 6723M shared.
PhysMem: 63G used (4956M wired, 3674M compressor), 144M unused.
VM: 294T vsize, 5702M framework vsize, 374625(0) swapins, 2011272(0) swapouts.
Networks: packets: 83653816/137G in, 24719486/45G out.
Disks: 9933842/151G read, 17899543/377G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
54375  bun     0.0  32:06.18 27/1 64     14G 14G N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:16:18 +07 | patched-server-recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=2.115434
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.061324
HTTP_CODE=200

[ps]
54375 54367 122.7 49.7 33347472 525953104 40:15 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 599 total, 4 running, 595 sleeping, 5735 threads 
2026/03/31 15:16:21
Load Avg: 3.17, 3.75, 3.82 
CPU usage: 8.98% user, 7.80% sys, 83.21% idle 
SharedLibs: 916M resident, 155M data, 587M linkedit.
MemRegions: 891633 total, 23G resident, 417M private, 5914M shared.
PhysMem: 63G used (5016M wired, 6856M compressor), 161M unused.
VM: 296T vsize, 5702M framework vsize, 375844(0) swapins, 2011272(0) swapouts.
Networks: packets: 85222920/139G in, 24861406/45G out.
Disks: 9944206/151G read, 17916321/378G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
54375  bun     0.0  37:22.41 28/1 65     17G 17G N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:21:21 +07 | patched-server-recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.267426
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.020706
HTTP_CODE=200

[ps]
54375 54367 118.9 41.3 27706816 530147424 45:16 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 600 total, 6 running, 1 stuck, 593 sleeping, 5762 threads 
2026/03/31 15:21:22
Load Avg: 4.68, 3.96, 3.87 
CPU usage: 9.40% user, 7.64% sys, 82.95% idle 
SharedLibs: 916M resident, 155M data, 587M linkedit.
MemRegions: 892281 total, 20G resident, 350M private, 5612M shared.
PhysMem: 63G used (5808M wired, 14G compressor), 331M unused.
VM: 296T vsize, 5702M framework vsize, 376255(0) swapins, 2011272(0) swapouts.
Networks: packets: 86806789/141G in, 24960586/45G out.
Disks: 9969881/152G read, 17941138/378G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
54375  bun     0.0  42:43.29 28/1 65     22G 22G N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:26:22 +07 | patched-server-recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.974878
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.022122
HTTP_CODE=200

[ps]
54375 54367  71.0 30.7 20628192 534341744 50:18 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 599 total, 3 running, 596 sleeping, 5769 threads 
2026/03/31 15:26:24
Load Avg: 3.67, 4.04, 3.96 
CPU usage: 9.18% user, 8.25% sys, 82.56% idle 
SharedLibs: 908M resident, 154M data, 587M linkedit.
MemRegions: 892851 total, 13G resident, 176M private, 4997M shared.
PhysMem: 63G used (5104M wired, 23G compressor), 368M unused.
VM: 296T vsize, 5702M framework vsize, 376577(0) swapins, 2011272(0) swapouts.
Networks: packets: 88393083/143G in, 25125120/45G out.
Disks: 10167800/156G read, 17969961/378G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
54375  bun     0.0  48:04.20 28/1 65     25G 25G N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:31:24 +07 | patched-server-recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=0.535312
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.022797
HTTP_CODE=200

[ps]
54375 54367 133.3 27.9 18736464 539289728 55:20 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 603 total, 4 running, 599 sleeping, 5826 threads 
2026/03/31 15:31:26
Load Avg: 6.16, 4.94, 4.37 
CPU usage: 12.85% user, 9.36% sys, 77.77% idle 
SharedLibs: 908M resident, 154M data, 587M linkedit.
MemRegions: 893798 total, 10G resident, 105M private, 4467M shared.
PhysMem: 63G used (4673M wired, 29G compressor), 72M unused.
VM: 298T vsize, 5702M framework vsize, 376951(0) swapins, 2011272(0) swapouts.
Networks: packets: 89947456/145G in, 25223702/45G out.
Disks: 10205004/156G read, 17996474/379G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
54375  bun     0.0  53:29.24 28/1 65     29G 29G N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:36:26 +07 | patched-server-recovery-after-ingest-stop

```text
[health]
{"ok":true}
TIME_TOTAL=1.485847
HTTP_CODE=200

[index_status]
{"stream":"gharchive-guardfix-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":27,"uploaded_count":27},"manifest":{"generation":62,"uploaded_generation":62,"last_uploaded_at":"2026-03-31T07:43:46.746Z","last_uploaded_etag":"W/\"0d8603cf344385104e08941c3ef02db1","last_uploaded_size_bytes":"15924"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:40:06.867Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:27.031Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:42.433Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:13.227Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:28.931Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:55.867Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:18.300Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:45.916Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:33.650Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:40:56.246Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":11,"lag_ms":"226521","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T07:41:09.710Z"}],"bundled_companions":{"object_count":27,"bytes_at_rest":"127753256","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"10149495","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"33201016","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":27,"contiguous_covered_segment_count":27,"lag_segments":0,"lag_ms":"4165","bytes_at_rest":"84396005","object_count":27,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T07:36:16.801Z"}]}
TIME_TOTAL=0.108064
HTTP_CODE=200

[ps]
54375 54367 119.0 24.8 16636896 539911296 01:00:22 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 533 total, 5 running, 528 sleeping, 5659 threads 
2026/03/31 15:36:28
Load Avg: 5.90, 5.79, 4.94 
CPU usage: 14.9% user, 10.42% sys, 75.47% idle 
SharedLibs: 908M resident, 154M data, 587M linkedit.
MemRegions: 877202 total, 12G resident, 81M private, 4466M shared.
PhysMem: 63G used (4482M wired, 31G compressor), 118M unused.
VM: 269T vsize, 5702M framework vsize, 377782(0) swapins, 2011272(0) swapouts.
Networks: packets: 91360542/147G in, 25324837/45G out.
Disks: 10308454/158G read, 18054065/380G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM MEM RPRVT VSIZE STATE  
54375  bun     0.0  59:05.44 28/1 65     33G 33G N/A   N/A   running

[stream]
stream                  next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
----------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-guardfix-all  1147000      1122206         1122206           27                      24793         10045910       24793     10045910   0                    1774943022929   1774943019164      

[segments]
total_segments  uploaded_segments
--------------  -----------------
27              27               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
27               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         10               
```

### 2026-03-31 15:36:36 +07 | patched-server-stopped

```text
pid=54375
signal=TERM
```

### 2026-03-31 15:45:00 +07 | bun-execfilesync-top-micro-repro

```text
Goal:
Check whether the Darwin memory probe itself (`execFileSync("/usr/bin/top", ...)`) is sufficient to create the large idle-memory growth seen after ingest stops.

Method:
1. Idle Bun control for 60s with periodic forced GC, no child-process probe.
2. Bun variant for 60s with the same periodic forced GC plus `execFileSync("/usr/bin/top", ["-l","1","-pid", String(process.pid), "-stats", "pid,mem"])` once per second.
3. Sample both with `ps` RSS and `/usr/bin/top -stats pid,mem`.

Results:
- Idle control:
  - `ps` RSS stabilized around 21.3-22.9 MB
  - `top MEM` stabilized around 6.5 MB
  - in-process `process.memoryUsage().rss` stabilized around 21.9-22.9 MB
- `execFileSync(top)` variant:
  - `ps` RSS stabilized around 29.4 MB
  - `top MEM` stabilized around 8.7-9.0 MB
  - in-process `process.memoryUsage().rss` stabilized around 27.7-30.2 MB

Conclusion:
The Darwin probe is not free, but the isolated overhead is on the order of ~7-8 MB RSS, not tens of GB. It is therefore not a sufficient explanation for the production-scale recovery failure by itself.
```

### 2026-03-31 15:46:00 +07 | patched-phase-2-final-conclusion

```text
Phase-2 setup:
- server restarted with the builder-side memory fixes and the corrected Darwin memory-guard probe
- stream: `gharchive-guardfix-all`
- ingest workload: `bun run demo:gharchive all`
- observation cadence: every 5 minutes during ingest, then every 5 minutes for 30 minutes after ingest stopped

Observed outcome:
- active ingest no longer reproduced the earlier 50+ GB immediate blow-up
- the run reached 27 uploaded segments, with bundled companions fully caught up
- routing and exact indexes remained at 16 uploaded segments because the next L0 boundary is 32 segments
- after ingest stopped, the server stayed responsive on `/health` and `/_index_status`, but made zero further progress for 30 minutes
- after the full 30-minute recovery window, the stream was still:
  - 27 uploaded segments
  - 27 companion objects
  - routing indexed through 16
  - all exact indexes indexed through 16
  - pending WAL still present
- at the final recovery sample, Activity Monitor / `top` still showed the Bun process in the ~33 GB memory range
- the server log's internal RSS samples were lower and eventually drifted down into the mid/high teens, but that did not translate into practical physical-footprint recovery

Why this is not active index-building work:
- routing builds return immediately when `uploadedCount < indexedThrough + span`; with 27 uploaded and 16 indexed, the next buildable span would be 32, so routing cannot be building at all
- exact secondary index builds have the same guard and also return immediately in this state
- bundled companions were already fully caught up and there were no pending segment uploads
- append requests are memory-gated before `req.arrayBuffer()`, so rejected append traffic does not explain large request-body accumulation in our handler path

Interpretation:
The remaining failure mode is not well explained by an app-level "one index builder keeps one 16 MB segment alive forever" leak. The builder-side fixes removed several real sources of transient JS/native pressure, and they materially improved the ingest-phase profile. But the post-ingest recovery failure points to Bun/macOS native allocator retention, delayed reclamation, or another runtime-level footprint issue after heavy segment decode / JSON parse / zstd work.

Strongest current theory:
- primary problem: Bun / JavaScriptCore / native allocator footprint retention under this workload on macOS
- not the primary problem: continued routing/exact/companion building after ingest stops
- not the primary problem: the Darwin `top` probe itself

Most useful next diagnostic tool:
Add a purpose-built runtime-memory sampler that records, once per interval:
- `process.memoryUsage()`
- `bun:jsc.heapStats()`
- per-phase labels (append, segment cut, upload, routing L0, exact L0, companion build)
- optional one-shot `vmmap -summary` snapshots when physical footprint crosses thresholds

That would let us separate JavaScript heap growth from non-JS/native retention without relying on heavyweight ad hoc sampling during an incident.
```
