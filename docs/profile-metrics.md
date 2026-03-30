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

The server uses this profile automatically for `__stream_metrics__`.

See [metrics.md](./metrics.md) for the full shipped architecture, record shape,
query model, and current limitations.
