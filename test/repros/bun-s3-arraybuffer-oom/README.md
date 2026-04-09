# Bun S3 `arrayBuffer()` OOM Repro

This is a minimal repro for a Bun process that grows RSS until it approaches or reaches OOM inside a `1 GiB` Linux container when repeatedly calling `Bun.S3File.arrayBuffer()` and forcing GC between iterations.

## What it demonstrates

- Repeated `await file.arrayBuffer()` on a `Bun.S3File`
- `new Uint8Array(...)` around the returned buffer
- Forced `Bun.gc(true)` after every iteration
- RSS still climbs until it crosses a high threshold in a `1 GiB` container
- On Bun `1.3.11`, the same repro also reaches an actual OOM kill when `EXIT_ON_THRESHOLD=0`

This matches the production pattern used by [`R2ObjectStore.get()`](/Users/sorenschmidt/code/streams/src/objectstore/r2.ts), which does:

```ts
return new Uint8Array(await body.arrayBuffer());
```

## Prerequisites

- Docker
- Network access to pull:
  - `oven/bun:1.3.11`
  - `minio/minio:latest`
  - `minio/mc:latest`

## One-command run

```bash
cd test/repros/bun-s3-arraybuffer-oom
bash run.sh
```

## Expected result

The script should print structured lines like:

```text
REPRO_START {...}
REPRO_BASELINE {...}
REPRO_PROGRESS {...}
REPRO_PROGRESS {...}
REPRO_THRESHOLD_REACHED {...}
```

The important line is:

```text
REPRO_THRESHOLD_REACHED {"iteration":...,"rss_bytes":...,"target_rss_bytes":943718400,...}
```

That means the Bun process crossed the `900 MiB` RSS threshold while running inside a `1 GiB` Linux container.

On Bun `1.3.11`, a second verification run with `EXIT_ON_THRESHOLD=0` produced:

```text
DOCKER_EXIT=137
```

which means the Bun process was OOM-killed by the container.

## Tunables

- `PAYLOAD_MIB` default `8`
- `TARGET_RSS_MIB` default `900`
- `MAX_ITERATIONS` default `256`
- `REPORT_EVERY` default `8`
- `EXIT_ON_THRESHOLD` default `1`

Example:

```bash
PAYLOAD_MIB=8 TARGET_RSS_MIB=900 MAX_ITERATIONS=256 REPORT_EVERY=8 bash run.sh
```

To continue beyond the threshold and confirm an actual OOM kill:

```bash
PAYLOAD_MIB=8 TARGET_RSS_MIB=900 MAX_ITERATIONS=4096 REPORT_EVERY=4 EXIT_ON_THRESHOLD=0 bash run.sh
```

## Relevant output fields

- `bun_version`
- `platform`
- `arch`
- `payload_bytes`
- `target_rss_bytes`
- `cgroup_memory_limit_bytes`
- `cgroup_memory_current_bytes`
- `rss_bytes`
- `heap_total_bytes`
- `heap_used_bytes`
- `external_bytes`
- `array_buffers_bytes`

## Why this is useful

It keeps the repro focused on one Bun-native API family:

- `Bun.S3Client`
- `Bun.S3File.arrayBuffer()`

It avoids the rest of the application and still reproduces the pathological RSS growth pattern in a constrained Linux container.
