# Changelog

## Upcoming

- Add stream profiles with built-in `generic` and `state-protocol` support,
  including a simplified `profile`-based `/_profile` API for live/touch setup.
- Rename state-protocol touch processing metrics and runtime state to
  profile-aligned `processor` / `processed_through` terminology.
- Add an `evlog` profile that normalizes JSON writes into canonical wide events
  with pre-append redaction and `requestId`/`traceId` routing-key defaults.
