
### 2026-03-31 13:30:16 UTC | memory-sampler-experiment-start

```text
server_log=/tmp/gharchive-heavy-combo-linux/server.log
ingester_log=/tmp/gharchive-heavy-combo-linux/ingester.log
sampler_path=/tmp/gharchive-heavy-combo-linux/sampler.jsonl
stream=gharchive-heavy-combo-linux-all
base_url=http://127.0.0.1:8873
db_path=/tmp/lib/prisma-streams-heavy-combo-linux/wal.sqlite
sample_interval_seconds=60
ingest_samples=6
recovery_samples=2
sampler_interval_ms=1000
gharchive_noindex=0
gharchive_onlyindex=exact:ghArchiveId,fts:message,agg:events
target_uploaded_segments=16
max_ingest_seconds=1200
```

### 2026-03-31 13:30:16 UTC | memory-sampler-server-started

```text
pid=504
```

### 2026-03-31 13:30:17 UTC | server-before-ingest

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.000341
HTTP_CODE=200

[index_status]
{"error":{"code":"not_found","message":"not_found"}}
TIME_TOTAL=0.000816
HTTP_CODE=404

[ps]
    504     492 56.6  0.8 139564 74292900    00:01 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
top - 13:30:17 up  4:25,  0 users,  load average: 0.17, 0.08, 0.07
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us,  0.0 sy,  0.0 ni, 99.5 id,  0.5 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :  16028.2 total,  13059.6 free,   2493.8 used,    952.8 buff/cache     
MiB Swap:  17052.2 total,  17052.2 free,      0.0 used.  13534.5 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    504 root      25   5   70.9g 140192  44332 D  20.0   0.9   0:00.64 bun

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

### 2026-03-31 13:30:17 UTC | memory-sampler-ingester-started

```text
pid=562
stream=gharchive-heavy-combo-linux-all
args=all --url http://127.0.0.1:8873 --stream-prefix gharchive-heavy-combo-linux --debug-progress --debug-progress-interval-ms 5000 --onlyindex exact:ghArchiveId,fts:message,agg:events
```

### 2026-03-31 13:31:17 UTC | server-during-ingest

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.002859
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-linux-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":5,"uploaded_count":5},"manifest":{"generation":11,"uploaded_generation":11,"last_uploaded_at":"2026-03-31T13:31:16.808Z","last_uploaded_etag":"W/\"e178a5b144915f42f7686e5221c88022","last_uploaded_size_bytes":"2907"},"routing_key_index":{"configured":false,"indexed_segment_count":0,"lag_segments":5,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.886Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":5,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T13:30:18.893Z"}],"bundled_companions":{"object_count":5,"bytes_at_rest":"4293741","fully_indexed_uploaded_segments":true},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":5,"contiguous_covered_segment_count":5,"lag_segments":0,"lag_ms":"6671","bytes_at_rest":"3989256","object_count":5,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":5,"contiguous_covered_segment_count":5,"lag_segments":0,"lag_ms":"6671","bytes_at_rest":"303450","object_count":5,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"}]}
TIME_TOTAL=0.011237
HTTP_CODE=200

[ps]
    504     492 50.7  2.2 372952 74456860    01:01 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
top - 13:31:18 up  4:26,  0 users,  load average: 0.51, 0.21, 0.11
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us,  0.5 sy,  0.0 ni, 99.5 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :  16028.2 total,  12476.8 free,   3013.0 used,   1016.5 buff/cache     
MiB Swap:  17052.2 total,  17052.2 free,      0.0 used.  13015.3 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    504 root      25   5   71.0g 372964  47368 S  20.0   2.3   0:31.12 bun

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-linux-all  240000       207874          207874            5                       32125         12914132       32125     12914132   0                    1774963876129   1774963870145      

[segments]
total_segments  uploaded_segments
--------------  -----------------
5               5                

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
5                

[routing_index]
indexed_through
---------------
0              

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          0                          1                
```

### 2026-03-31 13:32:18 UTC | server-during-ingest

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.000403
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-linux-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":11,"uploaded_count":11},"manifest":{"generation":22,"uploaded_generation":22,"last_uploaded_at":"2026-03-31T13:32:14.565Z","last_uploaded_etag":"W/\"a07f960501127300d1fdad1fe81c6176","last_uploaded_size_bytes":"4157"},"routing_key_index":{"configured":false,"indexed_segment_count":0,"lag_segments":11,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.886Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":11,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T13:30:18.893Z"}],"bundled_companions":{"object_count":10,"bytes_at_rest":"8622029","fully_indexed_uploaded_segments":false},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":10,"contiguous_covered_segment_count":10,"lag_segments":1,"lag_ms":"18404","bytes_at_rest":"8011522","object_count":10,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T13:30:18.949Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":10,"contiguous_covered_segment_count":10,"lag_segments":1,"lag_ms":"18404","bytes_at_rest":"608437","object_count":10,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T13:30:18.949Z"}]}
TIME_TOTAL=0.008953
HTTP_CODE=200

[ps]
    504     492 54.6  2.5 423532 74456860    02:01 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
top - 13:32:18 up  4:27,  0 users,  load average: 0.98, 0.43, 0.19
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us,  0.5 sy,  4.1 ni, 95.0 id,  0.5 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :  16028.2 total,  12603.4 free,   3137.6 used,    765.5 buff/cache     
MiB Swap:  17052.2 total,  17052.2 free,      0.0 used.  12890.7 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    504 root      25   5   71.0g 413996  29480 S  70.0   2.5   1:06.64 bun

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-linux-all  493000       457558          457558            11                      35441         14326522       35441     14326522   0                    1774963938387   1774963932175      

[segments]
total_segments  uploaded_segments
--------------  -----------------
11              11               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
10               

[routing_index]
indexed_through
---------------
0              

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          0                          1                
```

### 2026-03-31 13:33:18 UTC | server-during-ingest

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=3.386851
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-linux-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":16,"uploaded_count":16},"manifest":{"generation":33,"uploaded_generation":33,"last_uploaded_at":"2026-03-31T13:33:14.269Z","last_uploaded_etag":"W/\"d5d2c58a7b48eba962b1c11677049aca","last_uploaded_size_bytes":"5659"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":0,"lag_ms":"23890","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:33:12.228Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":16,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T13:30:18.893Z"}],"bundled_companions":{"object_count":16,"bytes_at_rest":"13913817","fully_indexed_uploaded_segments":true},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"23890","bytes_at_rest":"12947274","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"23890","bytes_at_rest":"963225","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"}]}
TIME_TOTAL=4.000720
HTTP_CODE=200

[ps]
    504     492 61.6  5.1 837372 74743180    03:09 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
top - 13:33:26 up  4:28,  0 users,  load average: 1.02, 0.57, 0.26
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.5 us,  0.5 sy,  3.6 ni, 94.6 id,  0.9 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :  16028.2 total,  12360.2 free,   3353.5 used,    792.8 buff/cache     
MiB Swap:  17052.2 total,  17052.2 free,      0.0 used.  12674.8 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    504 root      25   5   71.3g 829012  23288 S  54.5   5.1   1:56.84 bun

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-linux-all  695000       665393          665393            16                      29606         11916381       29606     11916381   0                    1774964005752   1774963981954      

[segments]
total_segments  uploaded_segments
--------------  -----------------
16              16               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
16               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          0                          1                
```

### 2026-03-31 13:33:26 UTC | memory-sampler-stop-condition-met

```text
reason=uploaded_segment_target
uploaded_segments=16
target_uploaded_segments=16
elapsed_seconds=180
```

### 2026-03-31 13:33:26 UTC | memory-sampler-ingester-stopped

```text
pid=562
signal=TERM
```

### 2026-03-31 13:33:26 UTC | server-after-ingest-stop

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.000336
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-linux-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":16,"uploaded_count":16},"manifest":{"generation":33,"uploaded_generation":33,"last_uploaded_at":"2026-03-31T13:33:14.269Z","last_uploaded_etag":"W/\"d5d2c58a7b48eba962b1c11677049aca","last_uploaded_size_bytes":"5659"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:33:12.228Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":16,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T13:30:18.893Z"}],"bundled_companions":{"object_count":16,"bytes_at_rest":"13913817","fully_indexed_uploaded_segments":true},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"12947274","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"963225","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"}]}
TIME_TOTAL=0.007601
HTTP_CODE=200

[ps]
    504     492 61.6  5.0 829240 74743180    03:09 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
top - 13:33:26 up  4:28,  0 users,  load average: 1.02, 0.57, 0.26
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us,  0.0 sy,  0.5 ni, 99.5 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :  16028.2 total,  12388.3 free,   3325.2 used,    792.9 buff/cache     
MiB Swap:  17052.2 total,  17052.2 free,      0.0 used.  12703.1 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    504 root      25   5   71.3g 819828  23316 S  10.0   5.0   1:56.87 bun

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-linux-all  695000       665393          665393            16                      29606         11916381       29606     11916381   0                    1774964005752   1774963981954      

[segments]
total_segments  uploaded_segments
--------------  -----------------
16              16               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
16               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          0                          1                
```

### 2026-03-31 13:34:26 UTC | server-recovery-after-ingest-stop

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.014532
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-linux-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":16,"uploaded_count":16},"manifest":{"generation":34,"uploaded_generation":34,"last_uploaded_at":"2026-03-31T13:33:26.632Z","last_uploaded_etag":"W/\"ca9b44b78c499c7b13696e3f25715477","last_uploaded_size_bytes":"5891"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:33:12.228Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"stale_configuration":false,"updated_at":"2026-03-31T13:33:26.393Z"}],"bundled_companions":{"object_count":16,"bytes_at_rest":"13913817","fully_indexed_uploaded_segments":true},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"12947274","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"963225","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"}]}
TIME_TOTAL=0.014722
HTTP_CODE=200

[ps]
    504     492 50.3  3.4 559752 74743180    04:09 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
top - 13:34:26 up  4:29,  0 users,  load average: 0.37, 0.46, 0.24
Tasks:   1 total,   1 running,   0 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us,  0.0 sy,  0.0 ni,100.0 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :  16028.2 total,  12950.2 free,   2871.7 used,    684.8 buff/cache     
MiB Swap:  17052.2 total,  17052.2 free,      0.0 used.  13156.6 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    504 root      25   5   71.3g 559772  20284 R  80.0   3.4   2:05.81 bun

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-linux-all  695000       665393          665393            16                      29606         11916381       29606     11916381   0                    1774964005752   1774963981954      

[segments]
total_segments  uploaded_segments
--------------  -----------------
16              16               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
16               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         1                
```

### 2026-03-31 13:35:26 UTC | server-recovery-after-ingest-stop

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.000462
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-linux-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":16,"uploaded_count":16},"manifest":{"generation":34,"uploaded_generation":34,"last_uploaded_at":"2026-03-31T13:33:26.632Z","last_uploaded_etag":"W/\"ca9b44b78c499c7b13696e3f25715477","last_uploaded_size_bytes":"5891"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:33:12.228Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"stale_configuration":false,"updated_at":"2026-03-31T13:33:26.393Z"}],"bundled_companions":{"object_count":16,"bytes_at_rest":"13913817","fully_indexed_uploaded_segments":true},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"12947274","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"24318","bytes_at_rest":"963225","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:30:18.949Z"}]}
TIME_TOTAL=0.009133
HTTP_CODE=200

[ps]
    504     492 43.2  3.2 533368 74743244    05:10 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
top - 13:35:26 up  4:30,  0 users,  load average: 0.13, 0.37, 0.22
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us,  0.0 sy,  4.3 ni, 95.7 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :  16028.2 total,  12932.7 free,   2830.2 used,    744.1 buff/cache     
MiB Swap:  17052.2 total,  17052.2 free,      0.0 used.  13198.0 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    504 root      25   5   71.3g 533376  18088 S  10.0   3.2   2:14.29 bun

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-linux-all  695000       665393          665393            16                      29606         11916381       29606     11916381   0                    1774964005752   1774963981954      

[segments]
total_segments  uploaded_segments
--------------  -----------------
16              16               

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
16               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         1                
```

### 2026-03-31 13:35:27 UTC | memory-sampler-server-stopped

```text
pid=504
signal=TERM
```

### 2026-03-31 13:35:27 UTC | memory-sampler-summary

```text
{
  "files": [
    "/tmp/gharchive-heavy-combo-linux/sampler.jsonl",
    "/tmp/gharchive-heavy-combo-linux/sampler.segmenter-worker-1.jsonl",
    "/tmp/gharchive-heavy-combo-linux/sampler.segmenter-worker-2.jsonl",
    "/tmp/gharchive-heavy-combo-linux/sampler.segmenter-worker-3.jsonl",
    "/tmp/gharchive-heavy-combo-linux/sampler.segmenter-worker-4.jsonl"
  ],
  "first_ts": "2026-03-31T13:30:16.873Z",
  "last_ts": "2026-03-31T13:35:26.190Z",
  "sample_count": 3016,
  "peak_process_rss_bytes": 881344512,
  "peak_process_heap_used_bytes": 201920979,
  "peak_jsc_heap_size_bytes": 202355796,
  "peak_jsc_current_bytes": 877326336,
  "last_process_rss_bytes": 543322112,
  "last_process_heap_used_bytes": 2311182,
  "last_jsc_heap_size_bytes": 2311182,
  "last_jsc_current_bytes": 543322112,
  "last_primary_phase": null,
  "last_reason": "interval",
  "by_scope": [
    {
      "scope": "main",
      "sample_count": 1748,
      "peak_rss_bytes": 881344512,
      "peak_heap_used_bytes": 201920979,
      "peak_jsc_heap_size_bytes": 202355796,
      "peak_jsc_current_bytes": 877326336
    },
    {
      "scope": "segmenter-worker-2",
      "sample_count": 327,
      "peak_rss_bytes": 874233856,
      "peak_heap_used_bytes": 4625294,
      "peak_jsc_heap_size_bytes": 4648676,
      "peak_jsc_current_bytes": 874233856
    },
    {
      "scope": "segmenter-worker-3",
      "sample_count": 313,
      "peak_rss_bytes": 874233856,
      "peak_heap_used_bytes": 4272346,
      "peak_jsc_heap_size_bytes": 4328455,
      "peak_jsc_current_bytes": 874233856
    },
    {
      "scope": "segmenter-worker-1",
      "sample_count": 317,
      "peak_rss_bytes": 862126080,
      "peak_heap_used_bytes": 4082736,
      "peak_jsc_heap_size_bytes": 4162523,
      "peak_jsc_current_bytes": 862126080
    },
    {
      "scope": "segmenter-worker-4",
      "sample_count": 311,
      "peak_rss_bytes": 862126080,
      "peak_heap_used_bytes": 4025437,
      "peak_jsc_heap_size_bytes": 4081577,
      "peak_jsc_current_bytes": 862126080
    }
  ],
  "by_primary_phase": [
    {
      "phase": "append",
      "sample_count": 709,
      "peak_rss_bytes": 881344512,
      "peak_jsc_heap_size_bytes": 201948501,
      "peak_jsc_current_bytes": 877326336
    },
    {
      "phase": "idle",
      "sample_count": 1980,
      "peak_rss_bytes": 874233856,
      "peak_jsc_heap_size_bytes": 164734623,
      "peak_jsc_current_bytes": 874233856
    },
    {
      "phase": "exact_l0",
      "sample_count": 3,
      "peak_rss_bytes": 845324288,
      "peak_jsc_heap_size_bytes": 141479037,
      "peak_jsc_current_bytes": 844713984
    },
    {
      "phase": "companion",
      "sample_count": 175,
      "peak_rss_bytes": 802230272,
      "peak_jsc_heap_size_bytes": 202355796,
      "peak_jsc_current_bytes": 802230272
    },
    {
      "phase": "upload",
      "sample_count": 127,
      "peak_rss_bytes": 442392576,
      "peak_jsc_heap_size_bytes": 70688750,
      "peak_jsc_current_bytes": 443047936
    },
    {
      "phase": "cut",
      "sample_count": 21,
      "peak_rss_bytes": 437387264,
      "peak_jsc_heap_size_bytes": 3130748,
      "peak_jsc_current_bytes": 438042624
    },
    {
      "phase": "routing_l0",
      "sample_count": 1,
      "peak_rss_bytes": 333066240,
      "peak_jsc_heap_size_bytes": 23672175,
      "peak_jsc_current_bytes": 364371968
    }
  ],
  "top_rss_samples": [
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.jsonl",
      "ts": "2026-03-31T13:33:25.324Z",
      "scope": "main",
      "primary_phase": "append",
      "reason": "phase_enter",
      "process_rss_bytes": 881344512,
      "process_heap_used_bytes": 163020593,
      "jsc_heap_size_bytes": 163020947,
      "jsc_current_bytes": 877326336
    },
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.jsonl",
      "ts": "2026-03-31T13:33:25.462Z",
      "scope": "main",
      "primary_phase": "append",
      "reason": "interval",
      "process_rss_bytes": 878145536,
      "process_heap_used_bytes": 163020593,
      "jsc_heap_size_bytes": 163428337,
      "jsc_current_bytes": 874233856
    },
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.jsonl",
      "ts": "2026-03-31T13:33:25.605Z",
      "scope": "main",
      "primary_phase": null,
      "reason": "phase_exit",
      "process_rss_bytes": 874233856,
      "process_heap_used_bytes": 163020593,
      "jsc_heap_size_bytes": 163433296,
      "jsc_current_bytes": 862126080
    },
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.segmenter-worker-2.jsonl",
      "ts": "2026-03-31T13:33:25.621Z",
      "scope": "segmenter-worker-2",
      "primary_phase": null,
      "reason": "interval",
      "process_rss_bytes": 874233856,
      "process_heap_used_bytes": 1954301,
      "jsc_heap_size_bytes": 1954301,
      "jsc_current_bytes": 874233856
    },
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.segmenter-worker-3.jsonl",
      "ts": "2026-03-31T13:33:25.627Z",
      "scope": "segmenter-worker-3",
      "primary_phase": null,
      "reason": "interval",
      "process_rss_bytes": 874233856,
      "process_heap_used_bytes": 1952075,
      "jsc_heap_size_bytes": 1952075,
      "jsc_current_bytes": 874233856
    },
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.segmenter-worker-1.jsonl",
      "ts": "2026-03-31T13:33:25.640Z",
      "scope": "segmenter-worker-1",
      "primary_phase": null,
      "reason": "interval",
      "process_rss_bytes": 862126080,
      "process_heap_used_bytes": 2032572,
      "jsc_heap_size_bytes": 2032572,
      "jsc_current_bytes": 862126080
    },
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.segmenter-worker-4.jsonl",
      "ts": "2026-03-31T13:33:25.636Z",
      "scope": "segmenter-worker-4",
      "primary_phase": null,
      "reason": "interval",
      "process_rss_bytes": 862126080,
      "process_heap_used_bytes": 1767594,
      "jsc_heap_size_bytes": 1767594,
      "jsc_current_bytes": 862126080
    },
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.jsonl",
      "ts": "2026-03-31T13:33:25.752Z",
      "scope": "main",
      "primary_phase": "append",
      "reason": "phase_enter",
      "process_rss_bytes": 861880320,
      "process_heap_used_bytes": 163668467,
      "jsc_heap_size_bytes": 163668821,
      "jsc_current_bytes": 853323776
    },
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.jsonl",
      "ts": "2026-03-31T13:33:25.933Z",
      "scope": "main",
      "primary_phase": null,
      "reason": "phase_exit",
      "process_rss_bytes": 854634496,
      "process_heap_used_bytes": 163668467,
      "jsc_heap_size_bytes": 164081225,
      "jsc_current_bytes": 854466560
    },
    {
      "path": "/tmp/gharchive-heavy-combo-linux/sampler.jsonl",
      "ts": "2026-03-31T13:33:21.772Z",
      "scope": "main",
      "primary_phase": "exact_l0",
      "reason": "interval",
      "process_rss_bytes": 845324288,
      "process_heap_used_bytes": 135181405,
      "jsc_heap_size_bytes": 141479037,
      "jsc_current_bytes": 844713984
    }
  ]
}
```
