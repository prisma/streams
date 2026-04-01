# Companion Storage Layout Architecture

Status: implemented

This document defines the current bundled companion storage layout that backs
`/_search`, `?filter=...`, `/_aggregate`, and metrics-profile aggregate
serving.

The supported on-disk format is now a binary, plan-relative companion
container named `PSCIX2`. Prisma Streams does not retain support for the older
JSON-TOC companion format.

## Goals

The `PSCIX2` cut changes the storage layout for the same reason the query
runtime was changed earlier: the persistent representation now matches the
query algorithm instead of materializing large JSON-shaped object graphs for
every family.

The current design goals are:

1. make the bundled `.cix` container cheap to inspect without decoding every
   family
2. make section payloads plan-relative so per-segment companions do not repeat
   field and rollup names
3. keep hot search families in binary layouts instead of JSON/base64 payloads
4. expose runtime section views and iterators instead of rebuilding old
   `Record<string, ...>` structures
5. preserve one supported format and one supported runtime path

## Container

Each sealed historical segment may have one bundled companion object:

- raw segment: `streams/<hash>/segments/<segment>.bin`
- bundled companion: `streams/<hash>/segments/<segment>-<id>.cix`

The `.cix` payload now starts with a fixed binary header:

- magic: `PSCIX2`
- major version: `2`
- flags
- section count
- plan generation
- segment index
- `stream_hash16`
- reserved bytes

The header is followed by a fixed-width section table. Each section entry
stores:

- section kind (`col`, `fts`, `agg`, `mblk`)
- family codec version
- compression mode
- flags
- payload offset
- payload length
- section directory length
- logical payload length

There is no JSON TOC, no TOC-length stabilization loop, and no legacy decoder.
Query reads parse the header and section table, then decode only the requested
family payload.

## Plan-Relative Layout

Bundled companion sections are tied to the desired companion plan generation
for the stream.

The desired plan persists in `search_companion_plans.plan_json` and now
includes:

- enabled family bits
- stable field ordinals
- stable rollup ordinals
- stable interval ordinals per rollup
- stable measure ordinals per rollup

The container stores ordinals, not repeated field or rollup names. At read
time, the runtime resolves ordinals through the current plan row and rejects
companions whose generation does not match the desired plan generation.

This keeps the companion bytes smaller and ensures the runtime works through
one schema-owned companion plan instead of per-segment duplicated metadata.

## Section Families

### `.col2`

The `.col` family now uses a binary `col2` payload inside `PSCIX2`.

Per field it stores:

- field ordinal
- field kind
- presence docset (`ALL`, bitset, or sparse doc-id list)
- value stream
- min/max values
- optional page index

Current runtime behavior:

- typed equality, range, and existence use `ColSectionView`
- page indexes let range checks skip groups of values without decoding the full
  field stream
- there is no JSON/base64 wrapper around column values

### `.fts2`

The `.fts` family now uses a binary `fts2` payload inside `PSCIX2`.

Per field it stores:

- field ordinal
- field capability flags
- existence docset
- restart-string term dictionary
- document-frequency array
- postings-offset array
- postings payload

Posting lists are stored in blocks instead of nested JSON arrays. The runtime
uses:

- `FtsSectionView`
- `FtsFieldView`
- `PostingIterator`

Queries therefore load only the targeted field, dictionary range, and posting
blocks needed for the current clause.

Read-time planning also treats prefix-capable keyword fields as exact-term FTS
fields. That means a query like `type:pushevent owner:prisma*` can still use
`.fts` doc-id pruning for both clauses even while the internal exact secondary
family is still catching up.

### `.agg2`

The `.agg` family now uses a binary `agg2` payload inside `PSCIX2`.

It stores:

- a rollup directory keyed by rollup ordinal
- an interval directory keyed by interval ordinal
- one interval-local payload per `(rollup, interval)`

Each interval payload stores:

- sorted window starts
- window-to-group offsets
- dimension dictionaries and ordinal columns
- measure columns
- histogram offsets and histogram data blobs

This layout supports `count`, `summary`, and `summary_parts` measures without
materializing a whole section-wide JSON object graph.

### `.mblk2`

The metrics-profile fallback family now uses a binary `mblk2` section inside
`PSCIX2`.

The current implementation stores:

- record count
- min and max covered time bounds
- the canonical metrics block records payload

The query runtime loads this section only for metrics-profile aggregate paths.
It is still a plan-relative binary section inside the bundled companion
container rather than a separate JSON/zstd object.

## Runtime Read Model

The runtime no longer caches decoded bundled sections broadly.

It now caches:

- a tiny byte-budgeted cache of parsed `PSCIX2` section tables
- a byte-budgeted cache of hot non-aggregate section payloads
- local primary-timestamp bounds persisted in `search_segment_companions`
  rows for published segments

On demand it decodes only the requested family:

- `getColSegmentCompanion()`
- `getFtsSegmentCompanion()`
- `getAggSegmentCompanion()`
- `getMetricsBlockSegmentCompanion()`

The returned objects are family views, not old JSON-shaped decoded bundles.
Examples:

- `ColSectionView.getField(...)`
- `FtsSectionView.getField(...)`
- `AggSectionView.getInterval(...)`

This is the key runtime cutover. The bundle is the storage container, but the
fetch and decode units are now the section table plus the requested family
payload. FTS- or column-only reads do not fetch aggregate bytes unless they
explicitly need them.

For `_search`, the runtime orders positive FTS clauses by estimated
selectivity and evaluates later clauses against the current candidate doc-id
set. This keeps prefix and exact keyword pruning in the binary FTS layer
instead of falling back to record JSON decode for obviously non-matching docs.

For aligned aggregate queries whose rollup uses the stream's primary timestamp
field, the reader now checks the persisted per-segment time bounds first and
skips non-overlapping published segments locally. That avoids remote `.cix`
fetches for segments that cannot contribute to the requested window.

## Build Model

`SearchCompanionManager` still owns per-segment bundled companion builds, but
it no longer builds one shared all-family in-memory structure.

For one sealed segment it now:

1. loads the segment bytes once
2. parses each record once
3. extracts the union of required search fields once per record
4. fans those parsed/raw values into the enabled family builders
5. encodes each family directly into its binary section payload
6. wraps the section payloads into one `PSCIX2` `.cix`

This reduces the previous multi-family fan-out problem where one decoded record
fed several unrelated long-lived structures at the same time, and it avoids
reparsing the same segment once per family or once per FTS field.

## Compression Policy

The current companion families do not use outer section-wide zstd compression
for hot search payloads.

That is intentional:

- hot search fields need direct section access
- the main win comes from removing JSON/base64 and storing compact binary
  structures
- query reads should not inflate unrelated families just to answer one clause

Raw segment `.bin` files and manifests still use their own segment/manifest
compression paths. This document only covers bundled companion storage.

## Compatibility

The supported bundled companion format is `PSCIX2` only.

Prisma Streams does not preserve:

- `PSCIX1`
- the JSON TOC format
- old decode paths that inflated every family together

Bootstrap, query reads, and background rebuild all use the same binary
container and family codecs.
