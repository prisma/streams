# Features

## Stream Profiles

- Every stream has a profile. If no profile is declared when the stream is
  created, it is treated as the built-in `generic` profile and profile metadata
  is managed via `/_profile`.
- `state-protocol` is implemented as a real profile, so live/touch streams are
  configured through `/_profile` instead of the schema registry.
- `evlog` is implemented as a real profile, so request-log streams normalize
  JSON writes into canonical wide events with profile-owned redaction and
  routing-key defaults.
- The public profile API uses a single `profile` field in requests and
  responses.
- State-protocol touch processing uses profile-aligned `processor` and
  `processed_through` naming across runtime metadata, metrics, and packaging.
