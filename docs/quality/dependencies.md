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
- `bincode` 1.3.3, through pinned SlateDB → foyer-common 0.22.3:
  [RUSTSEC-2025-0141](https://rustsec.org/advisories/RUSTSEC-2025-0141) reports
  discontinued maintenance and no safe upgrade. Retain this exact inherited
  storage-format dependency while tracking an upstream migration. Changing the
  serialization format requires its own compatibility review.
- `paste` 1.0.15, through pinned SlateDB → foyer-memory 0.22.3:
  [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436) reports
  discontinued maintenance and no safe upgrade. Retain this build-time macro
  dependency while tracking upstream replacement. These two named advisory
  exceptions do not exempt a new vulnerability advisory.
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
policy migration. Revisit the two maintenance exceptions whenever SlateDB/foyer
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
