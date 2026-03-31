
### 2026-03-31 13:42:29 UTC | server-before-ingest

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.000558
HTTP_CODE=200

[index_status]
{"error":{"code":"not_found","message":"not_found"}}
TIME_TOTAL=0.001721
HTTP_CODE=404

[ps]
1843117 1842524 37.2  2.6 102444 74129384    00:01 bun run src/server.ts --object-store r2 --auto-tune=2048

[top]
top - 13:42:29 up 76 days,  6:45,  3 users,  load average: 0.11, 0.12, 0.04
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  9.1 us,  9.1 sy,  0.0 ni, 77.3 id,  4.5 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :   3814.4 total,    209.8 free,   1547.3 used,   2338.1 buff/cache     
MiB Swap:      0.0 total,      0.0 free,      0.0 used.   2267.1 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
1843117 root      20   0   70.7g 104620  48384 S  30.0   2.7   0:00.46 bun

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

### 2026-03-31 13:42:29 UTC | ingester-started

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.000456
HTTP_CODE=200

[index_status]
{"error":{"code":"not_found","message":"not_found"}}
TIME_TOTAL=0.000389
HTTP_CODE=404

[ps]
1843117 1842524 33.3  2.6 104620 74129384    00:01 bun run src/server.ts --object-store r2 --auto-tune=2048

[top]
top - 13:42:29 up 76 days,  6:45,  3 users,  load average: 0.11, 0.12, 0.04
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s): 14.3 us,  4.8 sy,  0.0 ni, 76.2 id,  4.8 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :   3814.4 total,    188.6 free,   1565.6 used,   2341.1 buff/cache     
MiB Swap:      0.0 total,      0.0 free,      0.0 used.   2248.8 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
1843117 root      20   0   70.7g 105100  49024 S   0.0   2.7   0:00.48 bun

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-remote-linux-live-all  0            -1              -1                0                       0             0              0         0          0                    1774964549720   1774964549720      

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

### 2026-03-31 13:42:29 UTC | server-live-sample

```text
[platform]
os=Linux

[health]
{"ok":true}
TIME_TOTAL=0.000525
HTTP_CODE=200

[index_status]
{"stream":"gharchive-remote-linux-live-all","profile":"generic","desired_index_plan_generation":0,"segments":{"total_count":0,"uploaded_count":0},"manifest":{"generation":0,"uploaded_generation":0,"last_uploaded_at":null,"last_uploaded_etag":null,"last_uploaded_size_bytes":null},"routing_key_index":{"configured":false,"indexed_segment_count":0,"lag_segments":0,"lag_ms":null,"bytes_at_rest":"0","object_count":0,"active_run_count":0,"retired_run_count":0,"fully_indexed_uploaded_segments":true,"updated_at":null},"exact_indexes":[],"bundled_companions":{"object_count":0,"bytes_at_rest":"0","fully_indexed_uploaded_segments":true},"search_families":[]}
TIME_TOTAL=0.003201
HTTP_CODE=200

[ps]
1843117 1842524 29.6  2.7 105476 74129384    00:01 bun run src/server.ts --object-store r2 --auto-tune=2048

[top]
top - 13:42:30 up 76 days,  6:45,  3 users,  load average: 0.11, 0.12, 0.04
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us,  0.0 sy,  0.0 ni,100.0 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :   3814.4 total,    189.4 free,   1564.7 used,   2341.2 buff/cache     
MiB Swap:      0.0 total,      0.0 free,      0.0 used.   2249.8 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
1843117 root      20   0   70.7g 105216  49280 S  10.0   2.7   0:00.50 bun

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-remote-linux-live-all  0            -1              -1                0                       0             0              0         0          0                    1774964549720   1774964549720      

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

### 2026-03-31 13:42:30 UTC | metrics-snapshot

```text
[metrics]
{"uptime_ms":1707,"series":11}
[metrics_stream]
[]```

### 2026-03-31 13:47:31 UTC | server-live-sample

```text
[platform]
os=Linux

[health]
curl: (28) Operation timed out after 20008 milliseconds with 0 bytes received

TIME_TOTAL=20.013823
HTTP_CODE=000

[index_status]
curl: (28) Operation timed out after 20005 milliseconds with 0 bytes received

TIME_TOTAL=20.017180
HTTP_CODE=000

[ps]
1843117 1842524 41.4 68.8 2688484 77249364   06:31 bun run src/server.ts --object-store r2 --auto-tune=2048

[top]
top - 13:49:00 up 76 days,  6:51,  3 users,  load average: 7.72, 5.39, 2.48
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us, 88.2 sy,  0.0 ni,  0.0 id, 11.8 wa,  0.0 hi,  0.0 si,  0.0 st 
MiB Mem :   3814.4 total,    126.1 free,   3796.1 used,     39.3 buff/cache     
MiB Swap:      0.0 total,      0.0 free,      0.0 used.     18.3 avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
1843117 root      20   0   73.7g   2.6g   3072 D  36.4  68.8   2:42.32 bun

[stream]
stream                           next_offset  sealed_through  uploaded_through  uploaded_segment_count  pending_rows  pending_bytes  wal_rows  wal_bytes  segment_in_progress  last_append_ms  last_segment_cut_ms
-------------------------------  -----------  --------------  ----------------  ----------------------  ------------  -------------  --------  ---------  -------------------  --------------  -------------------
gharchive-remote-linux-live-all  68000        41601           41601             1                       26398         10672397       26398     10672397   0                    1774964586537   1774964571290      

[segments]
total_segments  uploaded_segments
--------------  -----------------
1               1                

[pending_segments]
pending_segments
----------------
0               

[companions]
companion_objects
-----------------
1                

[routing_index]
indexed_through
---------------
0              

[exact_indexes]
min_exact_indexed_through  max_exact_indexed_through  exact_index_count
-------------------------  -------------------------  -----------------
0                          0                          10               
```

### 2026-03-31 13:49:13 UTC | metrics-snapshot

```text
[metrics]
curl: (28) Operation timed out after 20015 milliseconds with 0 bytes received

[metrics_stream]
curl: (28) Operation timed out after 20007 milliseconds with 0 bytes received
```
