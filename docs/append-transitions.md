# Append application phases

The raw Durable Streams adapter and product adapter independently decode requests and render `AppendOutcome` or `AppendFailure`. Neither adapter reconstructs a successful application result from response headers or JSON. The owner requires a committed offset range and explicit duplicate/closed/producer metadata.

`AppendService::prepare` binds the stream incarnation and verifies its key before mutation body collection. `execute_prepared` verifies that preparation matches the command, preserves the initial incarnation across bounded topology retries, and calls the following phases:

1. `close::prepare_close` validates any trusted final-record claim and renews only an exact owed-final operation. Synthetic producer identities make ordinary raw final-close retries recoverable after durable commit.
2. `content::parse_content` validates media type, entries and permanent record/ingest ceilings. Producer errors remain deferred until duplicate detection. `close::install_intent` runs only after deterministic validation, so an impossible final record cannot strand a collection in Sealing.
3. `admission::admit_usage` resolves one shared counters object. `route::resolve_segment` refreshes topology only under the already-authorized project and epoch. The per-segment admission slot stays alive through submission and lifecycle completion.
4. The owner builds one typed `AppendReq`. Entries retain `Bytes` ownership; binary content uses reference-counted `Bytes` clones rather than payload copying. JSON retains the existing per-record framing. The predecessor vector moves into the request, and content-type validation borrows its input. The body accounting guard transfers at the queue representation boundary.
5. `submit::submit` resolves the actual owner, applies maintenance/wedge admission and awaits the durable reply. Timeout remains an explicitly ambiguous failure. An unavailable or foreign owner cannot become a fabricated success.
6. Successful writes touch TTL. The lifecycle owner decides raw-close claim release, final marking and terminal publication from the durable verdict; product final seals use the same fenced primitives through their coordinator. Only definitive refusals release owed debt. Finally each API renders the typed verdict.

Every extracted append application file is below 1,000 lines and every function below 200, enforced by the fixed-baseline architecture gate. The refactor introduces no serialization of internal outcomes. This is a source-level copy/ownership analysis, not an allocation or latency benchmark.

Verification after phase extraction: 50 append/seal scenarios and two typed-boundary tests passed in the debug test binary. The seal set includes successful and non-closing duplicates, raw final-record crash boundaries, definitive versus ambiguous refusals, producer gaps, lease takeover, recreated incarnations and cross-project fencing. Final release/conformance results are recorded in [the resolution ledger](review-resolution.md).
