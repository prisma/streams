
### 2026-03-31 20:24:35 +07 | memory-sampler-experiment-start

```text
server_log=/tmp/gharchive-heavy-combo-macos/server.log
ingester_log=/tmp/gharchive-heavy-combo-macos/ingester.log
sampler_path=/tmp/gharchive-heavy-combo-macos/sampler.jsonl
stream=gharchive-heavy-combo-macos-all
base_url=http://127.0.0.1:8872
db_path=/tmp/lib/prisma-streams-heavy-combo-macos/wal.sqlite
sample_interval_seconds=60
ingest_samples=6
recovery_samples=2
sampler_interval_ms=1000
gharchive_noindex=0
gharchive_onlyindex=exact:ghArchiveId,fts:message,agg:events
target_uploaded_segments=16
max_ingest_seconds=1200
```

### 2026-03-31 20:24:35 +07 | memory-sampler-server-started

```text
pid=97978
```

### 2026-03-31 20:24:36 +07 | server-before-ingest

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.000813
HTTP_CODE=200

[index_status]
{"error":{"code":"not_found","message":"not_found"}}
TIME_TOTAL=0.000962
HTTP_CODE=404

[ps]
97978 97964   4.4  0.2 154112 484931776 00:01 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 682 total, 5 running, 677 sleeping, 6265 threads 
2026/03/31 20:24:37
Load Avg: 6.30, 4.95, 4.63 
CPU usage: 8.43% user, 7.83% sys, 83.72% idle 
SharedLibs: 924M resident, 163M data, 591M linkedit.
MemRegions: 968396 total, 21G resident, 685M private, 14G shared.
PhysMem: 61G used (5078M wired, 911M compressor), 2595M unused.
VM: 343T vsize, 5702M framework vsize, 484727(0) swapins, 2039146(0) swapouts.
Networks: packets: 97041366/162G in, 28682124/57G out.
Disks: 11276093/180G read, 22215657/484G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM  MEM  RPRVT VSIZE STATE  
97978  bun     0.0  00:00.42 33/1 69     113M 113M N/A   N/A   running

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

### 2026-03-31 20:24:37 +07 | memory-sampler-ingester-started

```text
pid=98049
stream=gharchive-heavy-combo-macos-all
args=all --url http://127.0.0.1:8872 --stream-prefix gharchive-heavy-combo-macos --debug-progress --debug-progress-interval-ms 5000 --onlyindex exact:ghArchiveId,fts:message,agg:events
```

### 2026-03-31 20:25:37 +07 | server-during-ingest

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.000523
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-macos-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":4,"uploaded_count":4},"manifest":{"generation":8,"uploaded_generation":8,"last_uploaded_at":"2026-03-31T13:25:34.145Z","last_uploaded_etag":"W/\"c7f03e0916b6e25e63766a8527c941f1","last_uploaded_size_bytes":"2424"},"routing_key_index":{"configured":false,"indexed_segment_count":0,"lag_segments":4,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.749Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":4,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T13:24:38.750Z"}],"bundled_companions":{"object_count":4,"bytes_at_rest":"3423979","fully_indexed_uploaded_segments":true},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":4,"contiguous_covered_segment_count":4,"lag_segments":0,"lag_ms":"6459","bytes_at_rest":"3185201","object_count":4,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.753Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":4,"contiguous_covered_segment_count":4,"lag_segments":0,"lag_ms":"6459","bytes_at_rest":"237950","object_count":4,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.753Z"}]}
TIME_TOTAL=0.016657
HTTP_CODE=200

[ps]
97978 97964  20.6  0.8 526320 485544880 01:02 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 684 total, 3 running, 681 sleeping, 6281 threads 
2026/03/31 20:25:38
Load Avg: 5.10, 4.81, 4.59 
CPU usage: 11.22% user, 9.21% sys, 79.55% idle 
SharedLibs: 924M resident, 163M data, 591M linkedit.
MemRegions: 969490 total, 21G resident, 686M private, 14G shared.
PhysMem: 61G used (5580M wired, 911M compressor), 2085M unused.
VM: 344T vsize, 5702M framework vsize, 484727(0) swapins, 2039146(0) swapouts.
Networks: packets: 97076468/162G in, 28701491/57G out.
Disks: 11276398/180G read, 22229587/485G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM  MEM  RPRVT VSIZE STATE  
97978  bun     0.0  00:20.16 31/1 67     146M 146M N/A   N/A   running

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-macos-all  188000       166230          166230            4                       21769         8791350        21769     8791350    0                    1774963538732   1774963531792      

[segments]
total_segments  uploaded_segments
--------------  -----------------
4               4                

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
4                

[routing_index]
indexed_through
---------------
0              

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          0                          1                
```

### 2026-03-31 20:26:38 +07 | server-during-ingest

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.013391
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-macos-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":11,"uploaded_count":11},"manifest":{"generation":21,"uploaded_generation":21,"last_uploaded_at":"2026-03-31T13:26:34.565Z","last_uploaded_etag":"W/\"5202ccde664ee74c221cf11d2fba8b4c","last_uploaded_size_bytes":"4137"},"routing_key_index":{"configured":false,"indexed_segment_count":0,"lag_segments":11,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.749Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":0,"lag_segments":11,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":false,"stale_configuration":false,"updated_at":"2026-03-31T13:24:38.750Z"}],"bundled_companions":{"object_count":10,"bytes_at_rest":"8622022","fully_indexed_uploaded_segments":false},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":10,"contiguous_covered_segment_count":10,"lag_segments":1,"lag_ms":"10824","bytes_at_rest":"8011521","object_count":10,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T13:24:38.753Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":10,"contiguous_covered_segment_count":10,"lag_segments":1,"lag_ms":"10824","bytes_at_rest":"608431","object_count":10,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T13:24:38.753Z"}]}
TIME_TOTAL=0.018307
HTTP_CODE=200

[ps]
97978 97964  50.2  1.0 652960 485798928 02:03 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 689 total, 8 running, 1 stuck, 680 sleeping, 6312 threads 
2026/03/31 20:26:39
Load Avg: 4.28, 4.65, 4.55 
CPU usage: 15.32% user, 8.3% sys, 76.63% idle 
SharedLibs: 925M resident, 163M data, 591M linkedit.
MemRegions: 981623 total, 21G resident, 699M private, 15G shared.
PhysMem: 62G used (6345M wired, 907M compressor), 1275M unused.
VM: 348T vsize, 5702M framework vsize, 484803(0) swapins, 2039146(0) swapouts.
Networks: packets: 97129764/163G in, 28730771/57G out.
Disks: 11278251/180G read, 22249695/485G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM  MEM  RPRVT VSIZE STATE  
97978  bun     0.0  00:52.03 30/1 66     166M 166M N/A   N/A   running

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-macos-all  479000       457558          416004            11                      21441         8679916        62995     25457189   0                    1774963598734   1774963597776      

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

### 2026-03-31 20:27:39 +07 | server-during-ingest

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=1.249266
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-macos-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":16,"uploaded_count":16},"manifest":{"generation":33,"uploaded_generation":33,"last_uploaded_at":"2026-03-31T13:27:31.154Z","last_uploaded_etag":"W/\"b2a66cfebfc361d538a24f29e6cc3c17","last_uploaded_size_bytes":"5659"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":0,"lag_ms":"18595","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:27:29.960Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":0,"lag_ms":"18595","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"stale_configuration":false,"updated_at":"2026-03-31T13:27:41.504Z"}],"bundled_companions":{"object_count":15,"bytes_at_rest":"13041159","fully_indexed_uploaded_segments":false},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":15,"contiguous_covered_segment_count":15,"lag_segments":1,"lag_ms":"29005","bytes_at_rest":"12127775","object_count":15,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T13:24:38.753Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":15,"contiguous_covered_segment_count":15,"lag_segments":1,"lag_ms":"29005","bytes_at_rest":"910274","object_count":15,"stale_segment_count":1,"fully_indexed_uploaded_segments":false,"updated_at":"2026-03-31T13:24:38.753Z"}]}
TIME_TOTAL=0.299279
HTTP_CODE=200

[ps]
97978 97964 100.7  1.9 1287584 486382368 03:06 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 684 total, 5 running, 679 sleeping, 6295 threads 
2026/03/31 20:27:42
Load Avg: 4.31, 4.61, 4.54 
CPU usage: 12.67% user, 7.98% sys, 79.34% idle 
SharedLibs: 925M resident, 163M data, 591M linkedit.
MemRegions: 969730 total, 22G resident, 692M private, 14G shared.
PhysMem: 62G used (5077M wired, 907M compressor), 931M unused.
VM: 344T vsize, 5702M framework vsize, 484803(0) swapins, 2039146(0) swapouts.
Networks: packets: 97197589/163G in, 28762037/57G out.
Disks: 11279014/180G read, 22267877/486G written.

PID    COMMAND %CPU TIME     #TH  #PORTS MEM  MEM  RPRVT VSIZE STATE  
97978  bun     0.0  01:33.92 31/1 67     341M 341M N/A   N/A   running

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-macos-all  689000       665393          665393            16                      23606         9533178        23606     9533178    0                    1774963661902   1774963642971      

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
15               

[routing_index]
indexed_through
---------------
16             

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
16                         16                         1                
```

### 2026-03-31 20:27:42 +07 | memory-sampler-stop-condition-met

```text
reason=uploaded_segment_target
uploaded_segments=16
target_uploaded_segments=16
elapsed_seconds=180
```

### 2026-03-31 20:27:42 +07 | memory-sampler-ingester-stopped

```text
pid=98049
signal=TERM
```

### 2026-03-31 20:27:42 +07 | server-after-ingest-stop

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.178177
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-macos-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":16,"uploaded_count":16},"manifest":{"generation":34,"uploaded_generation":34,"last_uploaded_at":"2026-03-31T13:27:42.747Z","last_uploaded_etag":"W/\"544469275615a488b0b395192708f888","last_uploaded_size_bytes":"5994"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:27:29.960Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"stale_configuration":false,"updated_at":"2026-03-31T13:27:41.504Z"}],"bundled_companions":{"object_count":16,"bytes_at_rest":"13913814","fully_indexed_uploaded_segments":true},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"12947278","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.753Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"963218","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.753Z"}]}
TIME_TOTAL=0.006845
HTTP_CODE=200

[ps]
97978 97964  88.2  1.9 1287584 486382368 03:07 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 682 total, 4 running, 678 sleeping, 6280 threads 
2026/03/31 20:27:43
Load Avg: 4.20, 4.59, 4.53 
CPU usage: 9.22% user, 8.24% sys, 82.53% idle 
SharedLibs: 925M resident, 163M data, 591M linkedit.
MemRegions: 969004 total, 22G resident, 690M private, 14G shared.
PhysMem: 62G used (5047M wired, 907M compressor), 1075M unused.
VM: 343T vsize, 5702M framework vsize, 484803(0) swapins, 2039146(0) swapouts.
Networks: packets: 97197647/163G in, 28762096/57G out.
Disks: 11279014/180G read, 22267901/486G written.

PID    COMMAND %CPU TIME     #TH #PORTS MEM  MEM  RPRVT VSIZE STATE   
97978  bun     0.0  01:34.38 31  67     341M 341M N/A   N/A   sleeping

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-macos-all  690000       665393          665393            16                      24606         9937740        24606     9937740    0                    1774963662486   1774963642971      

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

### 2026-03-31 20:28:43 +07 | server-recovery-after-ingest-stop

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.000771
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-macos-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":16,"uploaded_count":16},"manifest":{"generation":34,"uploaded_generation":34,"last_uploaded_at":"2026-03-31T13:27:42.747Z","last_uploaded_etag":"W/\"544469275615a488b0b395192708f888","last_uploaded_size_bytes":"5994"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:27:29.960Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"stale_configuration":false,"updated_at":"2026-03-31T13:27:41.504Z"}],"bundled_companions":{"object_count":16,"bytes_at_rest":"13913814","fully_indexed_uploaded_segments":true},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"12947278","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.753Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"963218","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.753Z"}]}
TIME_TOTAL=0.006795
HTTP_CODE=200

[ps]
97978 97964  17.2  1.9 1287408 486381248 04:08 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 692 total, 4 running, 688 sleeping, 6375 threads 
2026/03/31 20:28:44
Load Avg: 3.64, 4.33, 4.44 
CPU usage: 9.42% user, 7.79% sys, 82.77% idle 
SharedLibs: 925M resident, 163M data, 591M linkedit.
MemRegions: 969593 total, 22G resident, 709M private, 14G shared.
PhysMem: 62G used (5047M wired, 905M compressor), 953M unused.
VM: 347T vsize, 5702M framework vsize, 484835(0) swapins, 2039146(0) swapouts.
Networks: packets: 97201108/163G in, 28765304/57G out.
Disks: 11279283/180G read, 22273330/486G written.

PID    COMMAND %CPU TIME     #TH #PORTS MEM  MEM  RPRVT VSIZE STATE   
97978  bun     0.0  01:44.03 29  65     312M 312M N/A   N/A   sleeping

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-macos-all  690000       665393          665393            16                      24606         9937740        24606     9937740    0                    1774963662486   1774963642971      

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

### 2026-03-31 20:29:44 +07 | server-recovery-after-ingest-stop

```text
[platform]
os=Darwin

[health]
{"ok":true}
TIME_TOTAL=0.000856
HTTP_CODE=200

[index_status]
{"stream":"gharchive-heavy-combo-macos-all","profile":"generic","desired_index_plan_generation":1,"segments":{"total_count":16,"uploaded_count":16},"manifest":{"generation":34,"uploaded_generation":34,"last_uploaded_at":"2026-03-31T13:27:42.747Z","last_uploaded_etag":"W/\"544469275615a488b0b395192708f888","last_uploaded_size_bytes":"5994"},"routing_key_index":{"configured":false,"indexed_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"36","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:27:29.960Z"},"exact_indexes":[{"name":"ghArchiveId","kind":"keyword","indexed_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"7407668","object_count":1,"active_run_count":1,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"stale_configuration":false,"updated_at":"2026-03-31T13:27:41.504Z"}],"bundled_companions":{"object_count":16,"bytes_at_rest":"13913814","fully_indexed_uploaded_segments":true},"search_families":[{"family":"fts","fields":["message"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"12947278","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.753Z"},{"family":"agg","fields":["events"],"plan_generation":1,"covered_segment_count":16,"contiguous_covered_segment_count":16,"lag_segments":0,"lag_ms":"20014","bytes_at_rest":"963218","object_count":16,"stale_segment_count":0,"fully_indexed_uploaded_segments":true,"updated_at":"2026-03-31T13:24:38.753Z"}]}
TIME_TOTAL=0.007080
HTTP_CODE=200

[ps]
97978 97964  18.9  1.9 1287408 486381248 05:09 /usr/local/bin/bun run src/server.ts --object-store r2 --auto-tune=4096

[top]
Processes: 681 total, 6 running, 1 stuck, 674 sleeping, 6325 threads 
2026/03/31 20:29:45
Load Avg: 4.22, 4.31, 4.42 
CPU usage: 14.68% user, 11.13% sys, 74.17% idle 
SharedLibs: 925M resident, 163M data, 591M linkedit.
MemRegions: 969245 total, 22G resident, 695M private, 14G shared.
PhysMem: 62G used (5499M wired, 893M compressor), 1361M unused.
VM: 343T vsize, 5702M framework vsize, 484874(0) swapins, 2039146(0) swapouts.
Networks: packets: 97204115/163G in, 28768163/57G out.
Disks: 11284370/180G read, 22292950/486G written.

PID    COMMAND %CPU TIME     #TH #PORTS MEM  MEM  RPRVT VSIZE STATE   
97978  bun     0.0  01:53.44 29  65     311M 311M N/A   N/A   sleeping

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-heavy-combo-macos-all  690000       665393          665393            16                      24606         9937740        24606     9937740    0                    1774963662486   1774963642971      

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

### 2026-03-31 20:29:46 +07 | memory-sampler-server-stopped

```text
pid=97978
signal=TERM
```

### 2026-03-31 20:29:46 +07 | memory-sampler-summary

```text
{
  "files": [
    "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
    "/tmp/gharchive-heavy-combo-macos/sampler.segmenter-worker-1.jsonl",
    "/tmp/gharchive-heavy-combo-macos/sampler.segmenter-worker-2.jsonl",
    "/tmp/gharchive-heavy-combo-macos/sampler.segmenter-worker-3.jsonl",
    "/tmp/gharchive-heavy-combo-macos/sampler.segmenter-worker-4.jsonl"
  ],
  "first_ts": "2026-03-31T13:24:35.741Z",
  "last_ts": "2026-03-31T13:29:45.163Z",
  "sample_count": 3018,
  "peak_process_rss_bytes": 1318486016,
  "peak_process_heap_used_bytes": 296281234,
  "peak_jsc_heap_size_bytes": 258305408,
  "peak_jsc_current_bytes": 1318486016,
  "last_process_rss_bytes": 1318305792,
  "last_process_heap_used_bytes": 2697828,
  "last_jsc_heap_size_bytes": 2697828,
  "last_jsc_current_bytes": 1318305792,
  "last_primary_phase": null,
  "last_reason": "interval",
  "by_scope": [
    {
      "scope": "main",
      "sample_count": 1746,
      "peak_rss_bytes": 1318486016,
      "peak_heap_used_bytes": 296281234,
      "peak_jsc_heap_size_bytes": 258305408,
      "peak_jsc_current_bytes": 1318486016
    },
    {
      "scope": "segmenter-worker-1",
      "sample_count": 310,
      "peak_rss_bytes": 1318486016,
      "peak_heap_used_bytes": 3792331,
      "peak_jsc_heap_size_bytes": 3792331,
      "peak_jsc_current_bytes": 1318486016
    },
    {
      "scope": "segmenter-worker-2",
      "sample_count": 310,
      "peak_rss_bytes": 1318486016,
      "peak_heap_used_bytes": 3952282,
      "peak_jsc_heap_size_bytes": 3952282,
      "peak_jsc_current_bytes": 1318486016
    },
    {
      "scope": "segmenter-worker-3",
      "sample_count": 322,
      "peak_rss_bytes": 1318486016,
      "peak_heap_used_bytes": 9599971,
      "peak_jsc_heap_size_bytes": 9659527,
      "peak_jsc_current_bytes": 1318486016
    },
    {
      "scope": "segmenter-worker-4",
      "sample_count": 330,
      "peak_rss_bytes": 1318486016,
      "peak_heap_used_bytes": 10725526,
      "peak_jsc_heap_size_bytes": 10686279,
      "peak_jsc_current_bytes": 1318486016
    }
  ],
  "by_primary_phase": [
    {
      "phase": "idle",
      "sample_count": 2027,
      "peak_rss_bytes": 1318486016,
      "peak_jsc_heap_size_bytes": 121446515,
      "peak_jsc_current_bytes": 1318486016
    },
    {
      "phase": "append",
      "sample_count": 704,
      "peak_rss_bytes": 1318486016,
      "peak_jsc_heap_size_bytes": 216807420,
      "peak_jsc_current_bytes": 1318486016
    },
    {
      "phase": "companion",
      "sample_count": 164,
      "peak_rss_bytes": 1313357824,
      "peak_jsc_heap_size_bytes": 258305408,
      "peak_jsc_current_bytes": 1313357824
    },
    {
      "phase": "exact_l0",
      "sample_count": 1,
      "peak_rss_bytes": 746160128,
      "peak_jsc_heap_size_bytes": 12717270,
      "peak_jsc_current_bytes": 746192896
    },
    {
      "phase": "upload",
      "sample_count": 104,
      "peak_rss_bytes": 718553088,
      "peak_jsc_heap_size_bytes": 72334725,
      "peak_jsc_current_bytes": 718553088
    },
    {
      "phase": "routing_l0",
      "sample_count": 1,
      "peak_rss_bytes": 718553088,
      "peak_jsc_heap_size_bytes": 63956081,
      "peak_jsc_current_bytes": 746160128
    },
    {
      "phase": "cut",
      "sample_count": 17,
      "peak_rss_bytes": 718487552,
      "peak_jsc_heap_size_bytes": 8783967,
      "peak_jsc_current_bytes": 718487552
    }
  ],
  "top_rss_samples": [
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:41.067Z",
      "scope": "main",
      "primary_phase": "append",
      "reason": "phase_enter",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 109600112,
      "jsc_heap_size_bytes": 116532988,
      "jsc_current_bytes": 1318486016
    },
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:41.217Z",
      "scope": "main",
      "primary_phase": null,
      "reason": "phase_exit",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 117907941,
      "jsc_heap_size_bytes": 118223722,
      "jsc_current_bytes": 1318486016
    },
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:41.359Z",
      "scope": "main",
      "primary_phase": null,
      "reason": "interval",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 117924550,
      "jsc_heap_size_bytes": 118244064,
      "jsc_current_bytes": 1318486016
    },
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:41.627Z",
      "scope": "main",
      "primary_phase": "append",
      "reason": "phase_enter",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 118306728,
      "jsc_heap_size_bytes": 118622756,
      "jsc_current_bytes": 1318486016
    },
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:41.769Z",
      "scope": "main",
      "primary_phase": null,
      "reason": "phase_exit",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 119285801,
      "jsc_heap_size_bytes": 119601582,
      "jsc_current_bytes": 1318486016
    },
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:41.902Z",
      "scope": "main",
      "primary_phase": "append",
      "reason": "phase_enter",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 119291099,
      "jsc_heap_size_bytes": 119607127,
      "jsc_current_bytes": 1318486016
    },
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:42.043Z",
      "scope": "main",
      "primary_phase": null,
      "reason": "phase_exit",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 120261899,
      "jsc_heap_size_bytes": 120577680,
      "jsc_current_bytes": 1318486016
    },
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:42.359Z",
      "scope": "main",
      "primary_phase": null,
      "reason": "interval",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 120267594,
      "jsc_heap_size_bytes": 120582874,
      "jsc_current_bytes": 1318486016
    },
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:42.486Z",
      "scope": "main",
      "primary_phase": "append",
      "reason": "phase_enter",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 120267594,
      "jsc_heap_size_bytes": 120587715,
      "jsc_current_bytes": 1318486016
    },
    {
      "path": "/tmp/gharchive-heavy-combo-macos/sampler.jsonl",
      "ts": "2026-03-31T13:27:42.619Z",
      "scope": "main",
      "primary_phase": null,
      "reason": "phase_exit",
      "process_rss_bytes": 1318486016,
      "process_heap_used_bytes": 120267594,
      "jsc_heap_size_bytes": 120999419,
      "jsc_current_bytes": 1318486016
    }
  ]
}
```
