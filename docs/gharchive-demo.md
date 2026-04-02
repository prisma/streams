# GH Archive Demo

This demo loads public [GH Archive](https://www.gharchive.org/) events into a
Prisma Streams server that is already running on the standard full-server port:

- `http://127.0.0.1:8787`

The script is self-contained:

- it creates one new stream per selected index
- installs the `generic` profile
- installs a minimal schema for exactly that one index on each stream
- downloads the requested GH Archive time range
- appends each normalized batch to the target streams sequentially over raw HTTP
- pauses on `429` or `503` using `Retry-After` and retries only the current
  stream operation
- applies a `20s` request timeout to ingester HTTP calls and retries the current
  operation on timeout
- prints a final summary when all requested hours have been appended to every target stream

## Why GH Archive

GH Archive is a good showcase dataset for Streams because it is already an
append-oriented JSON event corpus with:

- high ingest volume
- keyword dimensions like event type, actor, repo, and org
- text content from issue titles, PR bodies, comments, and commit messages
- natural time-series aggregation over event rates and payload sizes

## Command

```bash
bun run demo:gharchive day
```

Supported time ranges:

- `day`
- `week`
- `month`
- `year`
- `all`

Optional flags:

- `--url <base-url>`
  Default: `http://127.0.0.1:8787`
- `--stream-prefix <prefix>`
  Default: `gharchive-demo`
- `--batch-max-bytes <bytes>`
  Default: `8388608` for `day|week|month|year`, `2097152` for `all`
- `--batch-max-records <count>`
  Default: `1000` for `day|week|month|year`, `250` for `all`
- `--noindex`
  Installs only the JSON schema and skips the search schema entirely. Useful for
  raw-ingest control runs. This mode creates a single stream.
- `--onlyindex <selector>`
  Limits the demo to one or more single-index streams instead of the full set.
  Each selected index gets its own stream with only that selector plus the
  required `eventTime` timestamp field. Supported selectors:
  `exact:eventType`, `exact:ghArchiveId`, `exact:actorLogin`, `exact:repoName`,
  `exact:repoOwner`, `exact:orgLogin`, `exact:action`, `exact:refType`,
  `exact:public`, `exact:isBot`, `col:eventTime`, `col:public`, `col:isBot`,
  `col:commitCount`, `col:payloadBytes`, `col:payloadKb`, `fts:eventType`,
  `fts:repoOwner`, `fts:action`, `fts:refType`, `fts:title`, `fts:message`,
  `fts:body`, `agg:events`.
  Repeat the flag or pass a comma-separated list to create several single-index
  streams in one run.

Example:

```bash
bun run demo:gharchive week --stream-prefix gharchive-lab
```

Isolate one bundled-search field or exact index:

```bash
bun run demo:gharchive all --stream-prefix gharchive-lab --onlyindex fts:message
```

Fan out the same ingest workload into several isolated streams:

```bash
bun run demo:gharchive all \
  --stream-prefix gharchive-lab \
  --onlyindex exact:ghArchiveId \
  --onlyindex fts:message,agg:events
```

That creates streams like:

- `gharchive-lab-all-exact-ghArchiveId`
- `gharchive-lab-all-fts-message`
- `gharchive-lab-all-agg-events`

The range suffix is part of every stream name so you can keep multiple demo
sets side by side:

- `gharchive-demo-day`
- `gharchive-demo-day-exact-eventType`
- `gharchive-demo-day-fts-message`
- `gharchive-demo-day-agg-events`

The script recreates every selected target stream on each run.

For the `all` range, the demo intentionally uses smaller append batches by
default so the workload does not amplify the server's append-path JSON
materialization cost on low-memory hosts. On 1–2 GiB auto-tuned servers, the
runtime also clamps segmenting, upload, and bundled-companion backfill to
single-lane settings and shrinks segment size / target rows so the demo can
keep ingesting while `.agg` companions are being built.

## Archive Availability

GH Archive occasionally has missing hourly archives or publication gaps. The
demo handles that directly:

- an hourly archive returning `404` is skipped
- each skipped hour is logged to stderr with the hour and archive URL
- skipped hours are counted in the final summary as `missing`
- successfully ingested hours are counted as `downloaded`
- the run only fails if the entire requested range has no available hours

This means a `day` run can still succeed even if a few of the newest public
hours have not been published yet.

## Completion Semantics

The demo completes once every available GH Archive hour in the requested range
has been normalized and appended to every target stream:

- there is no `_details` polling
- there is no readiness wait for uploads, companions, or exact indexes
- each batch is sent to the target streams sequentially
- if any stream operation responds with `429` or `503`, the demo pauses using
  `Retry-After` and retries that same operation before doing anything else
- if an ingester HTTP request times out after `20s`, the demo pauses briefly and
  retries that same operation before doing anything else

This keeps the demo ingest loop intentionally simple and makes backpressure
behavior explicit.

## Installed Profile And Schema

The demo uses the built-in `generic` profile. GH Archive is a heterogeneous
event stream, so the right fit is:

- `generic` profile for plain durable JSON storage
- user-managed schema for the normalized GH Archive event envelope
- per-stream `search` and `search.rollups` configuration for exactly one index
  family selector at a time

The normalized event shape includes:

- `ghArchiveId`
- `eventTime`
- `eventType`
- `public`
- `isBot`
- `actorLogin`
- `repoName`
- `repoOwner`
- `orgLogin`
- `action`
- `refType`
- `title`
- `message`
- `body`
- `archiveHour`
- `commitCount`
- `payloadBytes`
- `payloadKb`

Every demo stream also derives its routing key from `repoName` using the schema
registry `routingKey` field. When `repoName` is absent, the append proceeds
without a routing key for that record.

## Search Fields

The default run creates one stream per shipped selector:

- keyword exact/prefix:
  - `eventType`
  - `repoOwner`
  - `action`
  - `refType`
- keyword exact:
  - `ghArchiveId`
  - `actorLogin`
  - `repoName`
  - `orgLogin`
- typed column:
  - `eventTime`
  - `public`
  - `isBot`
  - `commitCount`
  - `payloadBytes`
  - `payloadKb`
- text:
  - `title`
  - `message`
  - `body`

Each one of those selectors is installed on its own stream alongside the shared
`eventTime` timestamp field. For example:

- `gharchive-demo-day-exact-actorLogin` carries only the `actorLogin` exact
  index
- `gharchive-demo-day-col-payloadBytes` carries only the `payloadBytes` column
  index
- `gharchive-demo-day-fts-message` carries only the `message` FTS index
- `gharchive-demo-day-agg-events` carries only the `events` rollup

Useful aliases:

- `type` -> `eventType`
- `repo` -> `repoName`
- `owner` -> `repoOwner`
- `actor` -> `actorLogin`
- `org` -> `orgLogin`
- `id` -> `ghArchiveId`

## Rollups For Studio

The `agg:events` stream installs a rollup named `events` so Studio can
demonstrate the aggregate UI.

Rollup configuration:

- `timestampField`: `eventTime`
- dimensions:
  - `eventType`
  - `repoOwner`
  - `public`
  - `isBot`
- intervals:
  - `1m`
  - `5m`
  - `15m`
  - `1h`
  - `6h`
  - `1d`
  - `7d`
- measures:
  - `events` as `count`
  - `payloadBytes` as `summary`
  - `commitCount` as `summary`

That gives Studio enough structure to show:

- event-rate charts
- grouped breakdowns by event type or repo owner
- payload size summaries
- commit-count summaries

## Query Examples

Chronological filtered browsing:

```json
POST /v1/stream/gharchive-demo-day/_search
{
  "q": "type:pushevent owner:prisma*",
  "size": 100,
  "sort": ["offset:desc"]
}
```

Free-text search:

```json
POST /v1/stream/gharchive-demo-day/_search
{
  "q": "title:\"release\" OR body:\"aggregation examples\" OR message:\"PushEvent\"",
  "size": 50,
  "sort": ["offset:desc"]
}
```

Aggregates:

```json
POST /v1/stream/gharchive-demo-day/_aggregate
{
  "rollup": "events",
  "from": "2026-03-29T00:00:00.000Z",
  "to": "2026-03-30T00:00:00.000Z",
  "interval": "1h",
  "group_by": ["eventType"]
}
```

## Summary Output

The final summary is emitted immediately after the last batch has been appended
to every target stream. It includes:

- requested hours
- downloaded hours
- missing hours
- target stream count
- target stream names
- normalized row count
- source bytes
- normalized bytes
- average ingest rate in MiB/s
- download throughput in MiB/s
- normalize throughput in MiB/s
- append acknowledgement throughput in MiB/s
- append backoff count
- append backoff wait time in milliseconds

`append_backoff_*` includes both explicit server backoff (`429`/`503` with
`Retry-After`) and append retries caused by ingester-side request timeouts.

`avg_ingest_mib_per_s` is computed from normalized appended payload bytes over
total wall-clock ingest time.

`download_mib_per_s` is computed from raw GH Archive source bytes over the time
spent waiting on archive fetches and gzip-decoded body reads.

`normalize_mib_per_s` is computed from normalized appended payload bytes over
the time spent parsing GH Archive JSON, mapping it into the demo envelope, and
serializing the normalized records.

`append_ack_mib_per_s` is computed from normalized appended payload bytes over
the time spent waiting on append responses from the stream server, excluding any
explicit `Retry-After` sleep time.

## Notes

- `all` is very large and will take a long time.
- The demo downloads public GH Archive hourly gzip files directly from the
  network.
- The stream server must already be running before the script starts.
