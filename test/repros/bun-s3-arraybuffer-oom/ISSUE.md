# Bun.S3File.arrayBuffer() retains RSS and reaches OOM in a 1 GiB Linux container despite forced GC

I’m seeing large RSS growth from repeated `Bun.S3File.arrayBuffer()` calls in a minimal Linux container repro.

## Environment

- Bun: `1.3.11`
- Platform: Linux
- Arch: `arm64`
- Container memory limit: `1 GiB`
- Backend: MinIO, using `Bun.S3Client`

## Minimal repro

Files:

- `repro.ts`
- `run.sh`

One-command run:

```bash
cd test/repros/bun-s3-arraybuffer-oom
bash run.sh
```

This starts MinIO, seeds one object, then runs a Bun process in a `1 GiB` container that repeatedly does:

```ts
const bytes = new Uint8Array(await file.arrayBuffer());
Bun.gc(true);
```

## Observed behavior

The repro prints structured output like:

```text
REPRO_START {...}
REPRO_BASELINE {...}
REPRO_PROGRESS {...}
REPRO_PROGRESS {...}
REPRO_THRESHOLD_REACHED {...}
```

On Bun `1.3.11`, it reliably crosses the configured `900 MiB` RSS threshold inside the `1 GiB` container.

Example output:

```text
REPRO_START {"bun_version":"1.3.11","platform":"linux","arch":"arm64","payload_mib":8,"target_rss_mib":900,"cgroup_memory_limit_bytes":1073741824,...}
REPRO_BASELINE {"rss_bytes":65523712,...}
REPRO_PROGRESS {"iteration":88,"rss_bytes":877965312,...}
REPRO_THRESHOLD_REACHED {"iteration":95,"rss_bytes":945467392,"target_rss_bytes":943718400,...}
```

The repro forces `Bun.gc(true)` after every iteration, but RSS still climbs.

The printed fields include:

- `rss_bytes`
- `heap_total_bytes`
- `heap_used_bytes`
- `external_bytes`
- `array_buffers_bytes`
- `cgroup_memory_limit_bytes`
- `cgroup_memory_current_bytes`

In my runs, RSS grows far beyond what the JS-managed memory suggests.

## Actual OOM verification

If I continue beyond the threshold:

```bash
PAYLOAD_MIB=8 TARGET_RSS_MIB=900 MAX_ITERATIONS=4096 REPORT_EVERY=4 EXIT_ON_THRESHOLD=0 bash run.sh
```

the same Bun `1.3.11` repro reaches an actual OOM kill in the `1 GiB` container:

```text
DOCKER_EXIT=137
```

The last printed iterations before the kill showed RSS still climbing past the container limit boundary, for example:

```text
REPRO_THRESHOLD_REACHED {"iteration":100,"rss_bytes":1011945472,...}
REPRO_THRESHOLD_REACHED {"iteration":103,"rss_bytes":1037668352,...}
REPRO_PROGRESS {"iteration":108,"rss_bytes":1044312064,...}
DOCKER_EXIT=137
```

## Expected behavior

Repeated `Bun.S3File.arrayBuffer()` calls on the same object should not cause sustained RSS growth toward OOM after forced GC. RSS should remain bounded or return close to baseline after the temporary buffers are no longer referenced.

## Why this matters

This pattern maps directly to real application code that does:

```ts
return new Uint8Array(await body.arrayBuffer());
```

In a long-lived Bun server, this appears to accumulate anonymous RSS even when:

- JS heap remains much smaller
- buffers are not intentionally retained
- GC is forced repeatedly

## Related issues

- `#20487` Memory leak when downloading file (both @google-cloud/storage and Bun's S3)
- `#28741` Blob/ArrayBuffer memory not reclaimed by GC after dereferencing
- `#12941` Memory Leak with Blob/ArrayBuffer and Bun's Garbage Collection
- `#15020` Memory is not getting freed after reading multiple files (Node:fs and Bun.File)

This repro is narrower than those reports because it isolates the built-in Bun S3 API directly:

- `Bun.S3Client`
- `Bun.S3File.arrayBuffer()`

## Additional note

I checked Bun’s public type surface and did not find an explicit cleanup/finalizer API such as `close()`, `destroy()`, `dispose()`, or `finalize()` on `S3File` or `S3Client` for this code path.
