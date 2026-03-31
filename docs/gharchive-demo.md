# GH Archive Demo

This demo loads public [GH Archive](https://www.gharchive.org/) events into a
Prisma Streams server that is already running on the standard full-server port:

- `http://127.0.0.1:8787`

The script is self-contained:

- it creates a new stream
- installs the `generic` profile
- installs a schema with search fields and rollups
- downloads the requested GH Archive time range
- appends normalized events through `@durable-streams/client`
- waits for durable upload and bundled search readiness
- prints a final summary only when the demo is complete

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
  Default: `8388608`
- `--batch-max-records <count>`
  Default: `1000`
- `--ready-timeout-ms <ms>`
  Default: `1800000`
- `--append-retry-timeout-ms <ms>`
  Default: `900000`
- `--noindex`
  Installs only the JSON schema and skips the search schema entirely. Useful for
  raw-ingest control runs.
- `--onlyindex <selector>`
  Installs only one search path plus the required `eventTime` timestamp field.
  Useful for memory and builder-isolation experiments. Supported selectors:
  `exact:eventType`, `exact:ghArchiveId`, `exact:actorLogin`, `exact:repoName`,
  `exact:repoOwner`, `exact:orgLogin`, `exact:action`, `exact:refType`,
  `exact:public`, `exact:isBot`, `col:eventTime`, `col:public`, `col:isBot`,
  `col:commitCount`, `col:payloadBytes`, `col:payloadKb`, `fts:eventType`,
  `fts:repoOwner`, `fts:action`, `fts:refType`, `fts:title`, `fts:message`,
  `fts:body`, `agg:events`.
  Repeat the flag or pass a comma-separated list to combine several selectors
  into one minimal schema.
- `--debug-progress`
  Logs readiness progress while the script waits for uploaded data and bundled
  search companions to catch up. Off by default so the demo stays quiet unless
  you ask for progress output.
- `--debug-progress-interval-ms <ms>`
  Default: `5000`

Example:

```bash
bun run demo:gharchive week --stream-prefix gharchive-lab
```

Isolate one bundled-search field or exact index:

```bash
bun run demo:gharchive all --stream-prefix gharchive-lab --onlyindex fts:message
```

Combine several heavy selectors into one experiment:

```bash
bun run demo:gharchive all \
  --stream-prefix gharchive-lab \
  --onlyindex exact:ghArchiveId \
  --onlyindex fts:message,agg:events
```

That creates:

- `gharchive-lab-week`

The range suffix is part of the stream name so you can keep multiple demo
streams side by side:

- `gharchive-demo-day`
- `gharchive-demo-week`
- `gharchive-demo-month`

The script recreates the selected stream name on each run.

## Archive Availability

GH Archive occasionally has missing hourly archives or publication gaps. The
demo handles that directly:

- an hourly archive returning `404` is skipped
- skipped hours are counted in the final summary as `missing`
- successfully ingested hours are counted as `downloaded`
- the run only fails if the entire requested range has no available hours

This means a `day` run can still succeed even if a few of the newest public
hours have not been published yet.

## Completion Semantics

The demo completes once the stream is ready for user-facing search and
aggregation:

- uploaded segments are durably published
- bundled companions are built for all uploaded segments
- bundled search families (`.col`, `.fts`, `.agg`) are ready

The internal exact secondary-index family is still reported in the final
summary, but it does not gate demo completion. Exact runs are an internal
accelerator and currently build in fixed spans, so a trailing partial uploaded
span can remain uncovered even when the stream is already fully usable through
the bundled search path.

If you want to watch the wait phase, run with:

```bash
bun run demo:gharchive day --debug-progress
```

## Installed Profile And Schema

The demo uses the built-in `generic` profile. GH Archive is a heterogeneous
event stream, so the right fit is:

- `generic` profile for plain durable JSON storage
- user-managed schema for the normalized GH Archive event envelope
- schema-owned `search` and `search.rollups` for query and aggregate support

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

## Search Fields

The installed schema enables all shipped search families:

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

The demo now full-text indexes the larger `message` and `body` fields again,
not just `title`. That gives the demo a stronger end-to-end exercise of the
bundled `.fts` path over real issue bodies, review comments, and commit
messages.

Useful aliases:

- `type` -> `eventType`
- `repo` -> `repoName`
- `owner` -> `repoOwner`
- `actor` -> `actorLogin`
- `org` -> `orgLogin`
- `id` -> `ghArchiveId`

## Rollups For Studio

The demo also installs a rollup named `events` so Studio can demonstrate the
aggregate UI.

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
  "sort": ["offset:desc"],
  "track_total_hits": false
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

## Readiness

The script waits until:

- uploaded data is published
- bundled companions are ready for uploaded segments
- search families are ready for uploaded segments

When it finishes, the terminal summary includes:

- requested hours
- downloaded hours
- missing hours
- normalized row count
- source bytes
- normalized bytes
- logical stream size
- average ingest rate in MiB/s
- download throughput in MiB/s
- normalize throughput in MiB/s
- append acknowledgement throughput in MiB/s
- time to search-ready after the final append

Only then does it print the final summary.

Append hardening:

- stream creation and appends go through the official
  `@durable-streams/client`
- transient `429` and `503` responses are retried automatically with client
  backoff
- the server includes `Retry-After` on overload and temporary unavailability,
  so the client honors server-guided retry delays instead of busy retrying
- `--append-retry-timeout-ms` caps how long one append batch may keep retrying
  before the demo aborts

This is important for large GH Archive loads, because the server correctly
returns overload instead of buffering unboundedly when the ingest queue is
full.

The final summary includes:

- `raw_source_bytes`
- `normalized_bytes`
- `stream_total_size_bytes`
- `avg_ingest_mib_per_s`
- `download_mib_per_s`
- `normalize_mib_per_s`
- `append_ack_mib_per_s`
- `time_to_search_ready_ms`
- readiness state for uploads and indexing

Current readiness rule:

- `uploaded=true`
- `bundled=true`
- `search=true`
- exact secondary indexes may still be catching up in the background

`avg_ingest_mib_per_s` is computed from normalized appended payload bytes over
total wall-clock ingest time.

`download_mib_per_s` is computed from raw GH Archive source bytes over the time
spent waiting on archive fetches and gzip-decoded body reads.

`normalize_mib_per_s` is computed from normalized appended payload bytes over
the time spent parsing GH Archive JSON, mapping it into the demo envelope, and
serializing the normalized records.

`append_ack_mib_per_s` is computed from normalized appended payload bytes over
the time spent waiting for append acknowledgements from the stream server.

`time_to_search_ready_ms` measures the delay between the final successful append
acknowledgement and the point where uploaded segments plus bundled search
companions are ready according to `/_details`.

## Notes

- `all` is very large and will take a long time.
- The demo downloads public GH Archive hourly gzip files directly from the
  network.
- The stream server must already be running before the script starts.
