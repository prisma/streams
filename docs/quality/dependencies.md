# Dependency dispositions at adoption

The full graph, including target-specific, development and build dependencies,
is checked by pinned `cargo-deny`. Root package metadata now identifies the
existing Apache-2.0 repository license; no license text was changed.

The TLS stack is rustls-only. `openssl` and `native-tls` are explicitly banned
across the graph, and the configuration regression rejects removal of either
ban. On 9 September 2026, cargo-deny 0.20.2 accepted the locked workspace and
a temporary rustls 0.23 graph, and rejected separate temporary openssl 0.10
and native-tls 0.2 graphs with the `banned` diagnostic naming each package.
The workspace lock contained neither banned package nor `openssl-sys`.

- Removed the unused direct `tokio-stream` dependency. `rg` found no source use;
  `cargo-machete` independently identified it. Transitive dependencies may still
  use this package.
- Updated the locked transitive `chacha20` from yanked 0.10.1 to 0.10.2 within
  `object_store`'s existing dependency requirements. This is a dependency patch,
  not a claim of cryptographic acceptance or performance equivalence.
- On 15 October 2026, updated the locked transitive `rustls` from 0.23.45 to
  0.23.46 for the vulnerability advisories RUSTSEC-2026-0346, -0347, -0348 and
  -0349, and, in the same lockfile change, `lru` from 0.18.0 to 0.18.5
  (RUSTSEC-2026-0253) and `event-listener` from 5.4.1 to 5.4.2
  (RUSTSEC-2026-0221). The last two are `unsound` advisories that reach the
  graph only through SlateDB, so `cargo-deny`'s default `unsound` scope
  (workspace crates' direct dependencies) does not report them; the gate's
  configuration is unchanged. Every update stays within the dependents'
  existing requirements, and no manifest changed. The separate lockfiles of
  `bench/probe` and `bench/awsbench` are not part of this workspace and were
  not changed.
- On 21 October 2026, updated the locked transitive `rustls-webpki` from
  0.103.15 to 0.103.16 (RUSTSEC-2026-0382), and `webpki-roots` and
  `webpki-root-certs` from 1.0.8 to 1.0.10 (RUSTSEC-2026-0381 and -0383), so
  certificate paths through the distrusted emSign intermediates are rejected
  and the bundled roots no longer include `emSign ECC Root CA - G3`. Each
  update stays within the dependents' existing requirements and no manifest
  changed. `deny.toml`'s two version-pinned `CDLA-Permissive-2.0` license
  exceptions move from 1.0.8 to 1.0.10 with them (same crates, same license);
  nothing else in the gate's configuration changed. The separate lockfiles of
  `bench/probe` and `bench/awsbench` were not changed.
- `bincode` 1.3.3, through pinned SlateDB → foyer-common 0.22.3:
  [RUSTSEC-2025-0141](https://rustsec.org/advisories/RUSTSEC-2025-0141) reports
  discontinued maintenance and no safe upgrade. Retain this exact inherited
  storage-format dependency while tracking an upstream migration. Changing the
  serialization format requires its own compatibility review.
- `paste` 1.0.15, through pinned SlateDB → foyer-memory 0.22.3:
  [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436) reports
  discontinued maintenance and no safe upgrade. Retain this build-time macro
  dependency while tracking upstream replacement.
- `unsafe-libyaml` 0.2.11, through pinned SlateDB → figment 0.10.19 (SlateDB
  enables its `yaml` feature unconditionally) → serde_yaml 0.9.34:
  [RUSTSEC-2024-0449](https://rustsec.org/advisories/RUSTSEC-2024-0449) reports
  an archived repository and no safe upgrade. Streams constructs SlateDB
  `Settings` only through `Settings::default()`; SlateDB itself never calls its
  `from_file`, `from_env` or `load` providers, so the YAML parser is linked but
  unreached. Remove the exception when SlateDB drops or gates the provider.
- `zstd` 0.13.3 / `zstd-safe` 7.2.4 / `zstd-sys`, through Streams itself, pinned
  SlateDB and foyer (owner decision of 2026-10-26: a time-boxed ignore until
  2026-12-31, while slatedb/slatedb#2193 and foyer-rs/foyer#1382 bump zstd to
  0.14): RUSTSEC-2026-0391 (an abandoned stream leaks into the next on a reused
  `with_context` encoder; Streams compresses with `bulk::compress`, a fresh
  context per call, SlateDB with bulk / `decode_all`, foyer with plain
  `Encoder::new` / `Decoder::new`), RUSTSEC-2026-0398 (`Seekable::decompress`;
  the seekable feature is not enabled), RUSTSEC-2026-0403 (`stream::write::Decoder`
  only; Streams decodes with `read::Decoder`, `bulk::Decompressor` and
  `decode_all`) and RUSTSEC-2026-0410 (wasm32 only). Remove the four at the pin
  bump that takes the upstream PRs, and no later than 2026-12-31.
  These seven named advisory exceptions do not exempt a new vulnerability
  advisory.
- `webpki-roots` and `webpki-root-certs`, each exactly 1.0.8, contain certificate
  data under CDLA-Permissive-2.0. `deny.toml` permits that license only for these
  exact packages; their notices remain required. There is no blanket allowance
  for this license on unrelated code.
- Duplicate major/minor families remain review signals. The graph includes
  older authentication/client dependencies and newer SlateDB/object-store
  dependencies, plus platform-specific crates. Forcing them to one version is
  not part of adopting the quality policy. `cargo-deny` prints the inclusion
  graphs on every run; it does not suppress duplicates.

The owner for these adoption dispositions is the repository maintainer via this
policy migration. Revisit the three maintenance exceptions whenever SlateDB/foyer
is updated; unknown advisories, Git sources and registries fail the gate. The
source gate independently requires every Git lock entry to match the exact
SlateDB revision, since cargo-deny's source allowlist alone pins only the
repository and the kind of Git specifier.

The test-only `libfuzzer-sys` 0.4.13 additionally requires NCSA for its LLVM
runtime; this exact package has a named license exception. The fuzz package's
machete exclusions name only dependencies imported by the actual production
`crypto`/`tenant` source files outside its package directory. Compiler checks
prove those imports are required; the exclusions do not apply to service code.

The private-owner invariant harness has the same narrow machete treatment for
dependencies imported by its `#[path]` production modules and their real tests.
It excludes SlateDB/server startup dependencies so compatible Miri assertions
run on Linux; it does not replace those dependencies in the shipped server.
