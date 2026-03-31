
### 2026-03-31 16:04:29 +07 | memory-sampler-experiment-start

```text
server_log=/Users/sorenschmidt/code/streams/experiments/reports/sampler-indexed-server.log
ingester_log=/Users/sorenschmidt/code/streams/experiments/reports/sampler-indexed-ingester.log
sampler_path=/Users/sorenschmidt/code/streams/experiments/reports/sampler-indexed.jsonl
stream=gharchive-sampler-indexed-all
base_url=http://127.0.0.1:8787
db_path=/tmp/lib/prisma-streams-sampler-indexed/wal.sqlite
sample_interval_seconds=300
ingest_samples=6
recovery_samples=6
sampler_interval_ms=1000
gharchive_noindex=0
```

### 2026-03-31 16:04:29 +07 | memory-sampler-server-started

```text
pid=81879
```

### 2026-03-31 16:04:30 +07 | server-before-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.000701
HTTP_CODE=200

[index_status]
{"error":{"code":"not_found","message":"not_found"}}
TIME_TOTAL=0.000674
HTTP_CODE=404

[ps]
81879 81868   9.6  0.2 151056 485316656 00:01 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 561 total, 4 running, 557 sleeping, 5679 threads 
2026/03/31 16:04:31
Load Avg: 6.07, 5.20, 4.43 
CPU usage: 15.16% user, 9.79% sys, 75.4% idle 
SharedLibs: 918M resident, 157M data, 588M linkedit.
MemRegions: 939160 total, 14G resident, 405M private, 7127M shared.
PhysMem: 36G used (5401M wired, 3861M compressor), 27G unused.
VM: 280T vsize, 5702M framework vsize, 380840(0) swapins, 2011272(0) swapouts.
Networks: packets: 91621939/147G in, 25552737/45G out.
Disks: 10349677/159G read, 18331974/385G written.

PID    COMMAND %CPU TIME     #TH #PORTS MEM  MEM  RPRVT VSIZE STATE   
81879  bun     0.0  00:00.45 32  68     109M 109M N/A   N/A   sleeping

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

### 2026-03-31 16:04:31 +07 | memory-sampler-ingester-started

```text
pid=82051
stream=gharchive-sampler-indexed-all
args=all --url http://127.0.0.1:8787 --stream-prefix gharchive-sampler-indexed --debug-progress --debug-progress-interval-ms 5000
```

### 2026-03-31 16:09:31 +07 | server-during-ingest

```text
[health]
{"ok":true}
TIME_TOTAL=0.051716
HTTP_CODE=200

[index_status]
{"stream":"gharchive-sampler-indexed-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":7,"uploaded_count":7},"manifest":{"generation":15,"uploaded_generation":15,"last_uploaded_at":"2026-03-31T09:09:30.577Z","last_uploaded_etag":"W/\"2541a61ac3f79cc9919c872475527f94","last_uploaded_size_bytes":"7091"},"routing_key_index":{"configured":false,"indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.366Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.366Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"isBot","kind":"bool","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.368Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"public","kind":"bool","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.368Z"},{"name":"refType","kind":"keyword","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":0,"lag_segments":7,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"}],"bundled_companions":{"object_count":7,"bytes_at_rest":"32296743","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":7,"contiguous_covered_segment_count":7,"lag_segments":0,"lag_ms":"4851","bytes_at_rest":"2614680","object_count":7,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.368Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":7,"contiguous_covered_segment_count":7,"lag_segments":0,"lag_ms":"4851","bytes_at_rest":"8466776","object_count":7,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.368Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":7,"contiguous_covered_segment_count":7,"lag_segments":0,"lag_ms":"4851","bytes_at_rest":"21213495","object_count":7,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.368Z"}]}
TIME_TOTAL=0.003927
HTTP_CODE=200

[ps]
81879 81868  41.0  3.9 2628320 488307952 05:02 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 575 total, 7 running, 568 sleeping, 5842 threads 
2026/03/31 16:09:32
Load Avg: 6.67, 5.66, 4.86 
CPU usage: 16.53% user, 8.85% sys, 74.61% idle 
SharedLibs: 921M resident, 158M data, 592M linkedit.
MemRegions: 957529 total, 16G resident, 456M private, 12G shared.
PhysMem: 46G used (5552M wired, 3716M compressor), 18G unused.
VM: 293T vsize, 5702M framework vsize, 386853(0) swapins, 2011272(0) swapouts.
Networks: packets: 91704388/148G in, 25626625/45G out.
Disks: 10379199/161G read, 18507433/388G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM  MEM  RPRVT VSIZE STATE  
81879  bun     0.0  02:04.76 27/1 62     247M 247M N/A   N/A   running

[stream]
stream                         next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-----------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-sampler-indexed-all  299000       291145          291145            7                       7854          3182171        7854      3182171    0                    1774948171443   1774948162613      

[segments]
total_segments  uploaded_segments
--------------  -----------------
7               7                

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
7                

[routing_index]
indexed_through
---------------
0              

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          0                          10               
```

### 2026-03-31 16:14:32 +07 | server-during-ingest

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.000631
HTTP_CODE=200

[index_status]
{"stream":"gharchive-sampler-indexed-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":14,"uploaded_count":14},"manifest":{"generation":29,"uploaded_generation":29,"last_uploaded_at":"2026-03-31T09:14:21.047Z","last_uploaded_etag":"W/\"22c2f825d3d730e4ea59cbbac2dcf057","last_uploaded_size_bytes":"8987"},"routing_key_index":{"configured":false,"indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.366Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.366Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"isBot","kind":"bool","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.368Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"public","kind":"bool","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.368Z"},{"name":"refType","kind":"keyword","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":0,"lag_segments":14,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:04:32.367Z"}],"bundled_companions":{"object_count":14,"bytes_at_rest":"65585137","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":14,"contiguous_covered_segment_count":14,"lag_segments":0,"lag_ms":"18608","bytes_at_rest":"5246960","object_count":14,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.368Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":14,"contiguous_covered_segment_count":14,"lag_segments":0,"lag_ms":"18608","bytes_at_rest":"17090690","object_count":14,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.368Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":14,"contiguous_covered_segment_count":14,"lag_segments":0,"lag_ms":"18608","bytes_at_rest":"43243899","object_count":14,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.368Z"}]}
TIME_TOTAL=0.005944
HTTP_CODE=200

[ps]
81879 81868  37.2  6.0 4059088 489692336 10:03 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 577 total, 7 running, 570 sleeping, 5906 threads 
2026/03/31 16:14:33
Load Avg: 5.49, 5.43, 4.98 
CPU usage: 18.67% user, 8.39% sys, 72.92% idle 
SharedLibs: 921M resident, 158M data, 592M linkedit.
MemRegions: 964687 total, 17G resident, 462M private, 12G shared.
PhysMem: 48G used (3765M wired, 3702M compressor), 15G unused.
VM: 294T vsize, 5702M framework vsize, 387026(0) swapins, 2011272(0) swapouts.
Networks: packets: 91855012/148G in, 25711186/46G out.
Disks: 10384026/161G read, 18613662/390G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM  MEM  RPRVT VSIZE STATE  
81879  bun     0.0  04:29.01 26/1 61     287M 287M N/A   N/A   running

[stream]
stream                         next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-----------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-sampler-indexed-all  599000       582236          582236            14                      16763         6739328        16763     6739328    0                    1774948473200   1774948453520      

[segments]
total_segments  uploaded_segments
--------------  -----------------
14              14               

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

### 2026-03-31 16:17:59 +07 | memory-sampler-experiment-start

```text
server_log=/Users/sorenschmidt/code/streams/experiments/reports/sampler-noindex-server.log
ingester_log=/Users/sorenschmidt/code/streams/experiments/reports/sampler-noindex-ingester.log
sampler_path=/Users/sorenschmidt/code/streams/experiments/reports/sampler-noindex.jsonl
stream=gharchive-sampler-noindex-all
base_url=http://127.0.0.1:8788
db_path=/tmp/lib/prisma-streams-sampler-noindex/wal.sqlite
sample_interval_seconds=300
ingest_samples=3
recovery_samples=3
sampler_interval_ms=1000
gharchive_noindex=1
```

### 2026-03-31 16:17:59 +07 | memory-sampler-server-started

```text
pid=94452
```

### 2026-03-31 16:18:00 +07 | server-before-ingest

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.000540
HTTP_CODE=200

[index_status]
{"error":{"code":"not_found","message":"not_found"}}
TIME_TOTAL=0.000919
HTTP_CODE=404

[ps]
94452 94441   4.3  0.2 149152 485125840 00:01 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 577 total, 6 running, 571 sleeping, 5810 threads 
2026/03/31 16:18:01
Load Avg: 4.57, 4.81, 4.79 
CPU usage: 16.59% user, 9.36% sys, 74.4% idle 
SharedLibs: 921M resident, 158M data, 592M linkedit.
MemRegions: 985367 total, 16G resident, 466M private, 12G shared.
PhysMem: 49G used (3217M wired, 3676M compressor), 14G unused.
VM: 294T vsize, 5702M framework vsize, 387445(0) swapins, 2011272(0) swapouts.
Networks: packets: 92005452/148G in, 25774470/46G out.
Disks: 10409518/162G read, 18661112/391G written.

PID    COMMAND %CPU TIME     #TH #PORTS MEM  MEM  RPRVT VSIZE STATE   
94452  bun     0.0  00:00.40 27  63     108M 108M N/A   N/A   sleeping

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

### 2026-03-31 16:18:01 +07 | memory-sampler-ingester-started

```text
pid=94642
stream=gharchive-sampler-noindex-all
args=all --url http://127.0.0.1:8788 --stream-prefix gharchive-sampler-noindex --debug-progress --debug-progress-interval-ms 5000 --noindex
```

### 2026-03-31 09:18:36 UTC | memory-sampler-experiment-start

```text
server_log=/Users/sorenschmidt/code/streams/experiments/reports/sampler-linux-server.log
ingester_log=/Users/sorenschmidt/code/streams/experiments/reports/sampler-linux-ingester.log
sampler_path=/Users/sorenschmidt/code/streams/experiments/reports/sampler-linux.jsonl
stream=gharchive-sampler-linux-all
base_url=http://127.0.0.1:8787
db_path=/tmp/lib/prisma-streams-sampler-linux/wal.sqlite
sample_interval_seconds=300
ingest_samples=3
recovery_samples=3
sampler_interval_ms=1000
gharchive_noindex=0
```

### 2026-03-31 09:18:36 UTC | memory-sampler-server-started

```text
pid=503
```

### 2026-03-31 09:18:37 UTC | server-before-ingest

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.000228
HTTP_CODE=200

[index_status]
{"error":{"code":"not_found","message":"not_found"}}
TIME_TOTAL=0.000698
HTTP_CODE=404

[ps]
    503     492 45.1  0.8 143064 74465700    00:01 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
top - 09:18:37 up 13 min,  0 users,  load average: 0.46, 0.16, 0.05
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us,  0.0 sy,  0.0 ni,100.0 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :  16028.2 total,  13104.7 free,   2476.7 used,    923.8 buff/cache     
MiB Swap:  17052.2 total,  17052.2 free,      0.0 used.  13551.5 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    503 root      25   5   71.0g 142984  44332 D  20.0   0.9   0:00.50 bun

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

### 2026-03-31 09:18:37 UTC | memory-sampler-ingester-started

```text
pid=564
stream=gharchive-sampler-linux-all
args=all --url http://127.0.0.1:8787 --stream-prefix gharchive-sampler-linux --debug-progress --debug-progress-interval-ms 5000
```

### 2026-03-31 16:19:33 +07 | server-during-ingest

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.140803
HTTP_CODE=200

[index_status]
{"stream":"gharchive-sampler-indexed-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":17,"uploaded_count":17},"manifest":{"generation":46,"uploaded_generation":46,"last_uploaded_at":"2026-03-31T09:19:18.647Z","last_uploaded_etag":"W/\"5e89d33b136d42042a7f45e15a47961c","last_uploaded_size_bytes":"13243"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:15:48.886Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"202","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:32.614Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"859224","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:17:05.874Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"262","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:15:59.752Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:16:36.323Z"},{"name":"isBot","kind":"bool","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:19:17.685Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"40042","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:20.143Z"},{"name":"public","kind":"bool","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"86","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:19:00.314Z"},{"name":"refType","kind":"keyword","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"96","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:43.433Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"1036196","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:17:45.283Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":16,"lag_segments":1,"lag_ms":"121146","bytes_at_rest":"567514","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:07.440Z"}],"bundled_companions":{"object_count":17,"bytes_at_rest":"79878712","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":17,"contiguous_covered_segment_count":17,"lag_segments":0,"lag_ms":"4599","bytes_at_rest":"6382045","object_count":17,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.368Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":17,"contiguous_covered_segment_count":17,"lag_segments":0,"lag_ms":"4599","bytes_at_rest":"20806860","object_count":17,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.368Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":17,"contiguous_covered_segment_count":17,"lag_segments":0,"lag_ms":"4599","bytes_at_rest":"52685448","object_count":17,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:04:32.368Z"}]}
TIME_TOTAL=1.667576
HTTP_CODE=200

[ps]
81879 81868  91.4  7.5 5017056 490806832 15:06 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 582 total, 5 running, 577 sleeping, 5875 threads 
2026/03/31 16:19:36
Load Avg: 4.84, 4.88, 4.82 
CPU usage: 39.23% user, 5.44% sys, 55.32% idle 
SharedLibs: 921M resident, 158M data, 592M linkedit.
MemRegions: 993449 total, 17G resident, 470M private, 15G shared.
PhysMem: 54G used (3036M wired, 3671M compressor), 9699M unused.
VM: 296T vsize, 5702M framework vsize, 387496(0) swapins, 2011272(0) swapouts.
Networks: packets: 92229306/149G in, 25843340/46G out.
Disks: 10410407/162G read, 18765185/393G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM  MEM  RPRVT VSIZE STATE  
81879  bun     0.0  08:23.45 26/1 62     772M 772M N/A   N/A   running

[stream]
stream                         next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-----------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-sampler-indexed-all  709000       707047          707047            17                      1952          790484         1952      790484     0                    1774948659179   1774948655569      

[segments]
total_segments  uploaded_segments
--------------  -----------------
17              17               

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
16                         16                         10               
```

### 2026-03-31 16:23:01 +07 | server-during-ingest

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.001243
HTTP_CODE=200

[index_status]
{"stream":"gharchive-sampler-noindex-all","profile":"generic","desired_index_plan_generation":0,"segments":{"total_count":22,"uploaded_count":22},"manifest":{"generation":24,"uploaded_generation":24,"last_uploaded_at":"2026-03-31T09:22:15.323Z","last_uploaded_etag":"W/\"2d0edea12b7c5416a1c025010b89e9dc","last_uploaded_size_bytes":"1451"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":6,"lag_ms":"81697","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:21:03.638Z"},"exact_indexes":[],"bundled_companions":{"object_count":0,"bytes_at_rest":"0","fully_indexed_uploaded_segments":false},"search_families":[]}
TIME_TOTAL=0.013133
HTTP_CODE=200

[ps]
94452 94441   4.6  0.6 407968 485797936 05:02 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 583 total, 4 running, 579 sleeping, 5847 threads 
2026/03/31 16:23:02
Load Avg: 5.30, 4.87, 4.82 
CPU usage: 12.32% user, 8.48% sys, 79.18% idle 
SharedLibs: 922M resident, 158M data, 593M linkedit.
MemRegions: 891705 total, 10G resident, 489M private, 15G shared.
PhysMem: 40G used (3385M wired, 3521M compressor), 23G unused.
VM: 295T vsize, 5702M framework vsize, 388180(0) swapins, 2011272(0) swapouts.
Networks: packets: 92403446/149G in, 25955163/46G out.
Disks: 10447974/163G read, 18935249/397G written.

PID    COMMAND %CPU TIME     #TH #PORTS MEM  MEM  RPRVT VSIZE STATE   
94452  bun     0.0  00:29.42 25  61     182M 182M N/A   N/A   sleeping

[stream]
stream                         next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-----------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-sampler-noindex-all  948000       914667          914667            22                      33332         13494012       33332     13494012   0                    1774948941437   1774948933437      

[segments]
total_segments  uploaded_segments
--------------  -----------------
22              22               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
0                

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
                                                      0                
```

### 2026-03-31 09:23:37 UTC | server-during-ingest

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.108053
HTTP_CODE=200

[index_status]
{"stream":"gharchive-sampler-linux-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":15,"uploaded_count":15},"manifest":{"generation":31,"uploaded_generation":31,"last_uploaded_at":"2026-03-31T09:23:32.612Z","last_uploaded_etag":"W/\"72a226be6cae48c004a74c89a36d0f1d","last_uploaded_size_bytes":"9240"},"routing_key_index":{"configured":false,"indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:18:38.236Z"},"exact_indexes":[{"name":"action","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.259Z"},{"name":"actorLogin","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.250Z"},{"name":"eventType","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.244Z"},{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.247Z"},{"name":"isBot","kind":"bool","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.266Z"},{"name":"orgLogin","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.257Z"},{"name":"public","kind":"bool","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.264Z"},{"name":"refType","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.262Z"},{"name":"repoName","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.252Z"},{"name":"repoOwner","kind":"keyword","indexed_segment_count":0,"lag_segments":15,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T09:18:38.254Z"}],"bundled_companions":{"object_count":15,"bytes_at_rest":"70363390","fully_indexed_uploaded_segments":true},"search_families":[{"family":"col","fields":["commitCount","eventTime","isBot","payloadBytes","payloadKb","public"],"plan_generation":1,"covered_segment_count":15,"contiguous_covered_segment_count":15,"lag_segments":0,"lag_ms":"14249","bytes_at_rest":"5628577","object_count":15,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:18:38.284Z"},{"family":"fts","fields":["action","actorLogin","body","eventType","ghArchiveId","message","orgLogin","refType","repoName","repoOwner","title"],"plan_generation":1,"covered_segment_count":15,"contiguous_covered_segment_count":15,"lag_segments":0,"lag_ms":"14249","bytes_at_rest":"18323117","object_count":15,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:18:38.284Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":15,"contiguous_covered_segment_count":15,"lag_segments":0,"lag_ms":"14249","bytes_at_rest":"46407881","object_count":15,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T09:18:38.284Z"}]}
TIME_TOTAL=0.008441
HTTP_CODE=200

[ps]
    503     492 87.2  9.7 1594764 77716388   05:01 bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
top - 09:23:37 up 18 min,  0 users,  load average: 1.33, 0.79, 0.35
Tasks:   1 total,   1 running,   0 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.4 us,  0.4 sy,  3.1 ni, 96.0 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :  16028.2 total,  11495.8 free,   4107.6 used,    902.2 buff/cache     
MiB Swap:  17052.2 total,  17052.2 free,      0.0 used.  11920.6 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    503 root      25   5   74.1g   1.5g  20120 R  90.0   9.7   4:23.20 bun

[stream]
stream                       next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
---------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-sampler-linux-all  652000       623857          623857            15                      28142         11357096       28142     11357096   0                    1774949017489   1774949003488      

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
15               

[routing_index]
indexed_through
---------------
0              

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          0                          10               
```
