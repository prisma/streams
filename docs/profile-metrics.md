# Metrics Profile

The `metrics` profile is the built-in profile for canonical metric interval
streams.

It means:

- stream content type must be `application/json`
- JSON appends are normalized into the canonical metrics interval envelope
- the profile auto-installs its schema registry, `search` fields, and default
  `search.rollups`
- the canonical routing key is `seriesKey`
- the profile enables the `.mblk` metrics-block family in addition to the
  generic search families

The server uses this profile automatically for `__stream_metrics__`, but the
internal system stream is intentionally leaner than user-created metrics
streams:

- it keeps the `metrics` profile for canonical normalization
- it installs only the canonical schema version
- it does **not** install schema `routingKey`
- it does **not** install `search` fields or `search.rollups`
- it therefore does **not** build routing, lexicon, exact, `.col`, `.fts`,
  `.agg`, or `.mblk` background indexes for the internal stream

This avoids self-indexing feedback loops where operational metrics about the
node create additional heavy background work on the same node. If you need a
searchable or aggregatable metrics stream, create a normal user-managed stream
with the `metrics` profile.

See [metrics.md](./metrics.md) for the full shipped architecture, record shape,
query model, and current limitations.
