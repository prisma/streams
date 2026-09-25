# serve_h1 has no hyper timer: the 30 s head deadline is silently off

Read-only plan, 2026-09-22, against the `slate` working tree (http.rs at
3373 lines, `serve_h1` at `src/http.rs:1275-1342`). Every number below was
measured with the repo's own gate tooling on in-memory copies of the
proposed edits; nothing in the repository was written.

## 0. Verdict on the claim

**Confirmed, first-hand, and slightly understated.**

- `serve_h1` builds `hyper::server::conn::http1::Builder::new()` and sets only
  `max_buf_size` (`src/http.rs:1319-1323`). No `.timer(..)`, no
  `.header_read_timeout(..)`.
- hyper 1.10.1 (`Cargo.lock:1316-1319`) constructs the builder with
  `timer: Time::Empty` and
  `h1_header_read_timeout: Dur::Default(Some(30 s))`
  (`~/.cargo/registry/src/*/hyper-1.10.1/src/server/conn/http1.rs:243,249`).
- `serve_connection` runs `self.timer.check(self.h1_header_read_timeout,
  "header_read_timeout")` (`http1.rs:477-482`). `Time::check` with
  `Dur::Default(Some(_))` and `Time::Empty` logs `warn!` and returns `None`
  (`src/common/time.rs:70-85`), so `conn.set_http1_header_read_timeout` is
  never called and the connection state keeps `h1_header_read_timeout: None`
  (`src/proto/h1/conn.rs:62`).
- The `warn!` is a compile-time no-op here: hyper's `warn!` macro expands only
  under its `tracing` feature (`src/trace.rs:109-115`) and the lock shows hyper
  built without it (`Cargo.lock:1320-1335`, no `tracing` dependency). The loss
  is therefore invisible even at `RUST_LOG=warn`.
- Not a #269 regression: `axum::serve` 0.8.9 also never installs a timer
  (`axum-0.8.9/src/serve/mod.rs:389-393` builds
  `hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())`, whose
  `new` uses `http1::Builder::new()` untouched, `hyper-util-0.1.20/src/server/
  conn/auto/mod.rs:90-101`). The fleet has **never** had a head deadline; every
  soak/rc certification ran without one. That matters for section 6.

What the review did not say, and what the tests must cover (from hyper source):

1. **The deadline is also the idle keep-alive deadline.** After every completed
   response `State::idle()` sets `notify_read` when a header timeout is
   configured (`conn.rs:1127-1133`), and the next `poll_read_head` re-arms a
   fresh deadline whenever `h1_header_read_timeout_running` is false
   (`conn.rs:218-235`; it is cleared after every parsed head, `conn.rs:273-277`).
   So a keep-alive connection that sends no next request within T seconds of
   its last response is closed by the server.
2. **It never runs while a request is in flight.** `poll_read_head` is reached
   only from the dispatcher when `conn.can_read_head()` (`dispatch.rs:216-221`),
   which requires `Reading::Init` (`conn.rs:175-185`), and only after
   `dispatch.poll_ready` (`dispatch.rs:292-303`), which for the server role is
   `Pending` while a request future is in flight (`dispatch.rs:628-632`). Body
   reads, long-polls (≤ 25 s), SSE sessions and slow response bodies are never
   under this timer. Parked SSE connections also hold no timer future: it is
   set to `None` once the head parses (`conn.rs:276`), so the #269 44 KB/parked
   figure is unaffected.
3. **Expiry is a plain close, no 408.** The timeout surfaces as
   `Err(Error::new_header_timeout())` (`conn.rs:263-264`); the server
   dispatcher's `recv_msg(Err)` propagates it (`dispatch.rs:326-334`,
   `:615-616`), the connection future resolves `Err(e)` with `e.is_timeout()`
   (`src/error.rs:315-318`), `serve_h1` discards it (`let _ =`, `http.rs:1323`)
   and the socket is dropped: FIN when hyper has consumed every byte, RST on
   macOS when request bytes are still queued (the `hreq` fixture already
   tolerates that, `src/dst/tests/fixture_requests.rs:33-41`).
4. **The timer type needs no new Cargo feature.** `hyper_util::rt::TokioTimer`
   is exported under hyper-util's `tokio` feature (`hyper-util-0.1.20/src/rt/
   mod.rs:8-12`), already enabled (`Cargo.toml:16`, and `TokioIo` from the same
   module is in use at `http.rs:1318`). `Builder::timer` needs hyper
   `server`+`http1`, both on (`Cargo.toml:15`). No `Cargo.lock`, `deny.toml`,
   `docs/quality/dependencies.md` or cargo-machete change.
5. **Clients that hold pools against this server** (idle timeouts): peer relay
   4 s (`src/peer.rs:164-181`, "under the platform's ~5 s VM-suspend socket
   kill"); pilot 4 s (`src/bin/pilot/client.rs:53-62`); store client
   `pool_idle_secs` 4 s (`src/config/model.rs:344`, `src/bootstrap.rs:46`);
   bench tools 120 s (`src/bin/bench.rs:89`, `src/bin/livebench.rs:119`);
   edgesim reqwest default 90 s (`src/bin/edgesim.rs:199`). The platform edge's
   upstream pool idle is not documented anywhere in the repo.

## 1. Mechanism (evidence table)

| Fact | Where |
| --- | --- |
| builder without timer | `src/http.rs:1319-1320` |
| default 30 s + empty timer | hyper `server/conn/http1.rs:243,249` |
| default dropped when no timer | hyper `common/time.rs:72-78` (`Dur::Default` + `Time::Empty` → `None`) |
| configured-without-timer panics | hyper `common/time.rs:79-80` (relevant to the fix: timer and duration must be set together) |
| arm on first head wait, re-arm after each response | hyper `proto/h1/conn.rs:218-235`, `1127-1133` |
| cleared once a head parses | hyper `proto/h1/conn.rs:273-277` |
| gated on nothing in flight | hyper `proto/h1/dispatch.rs:216-221, 292-303, 628-632` |
| expiry path, no response written | hyper `proto/h1/conn.rs:254-267`, `dispatch.rs:326-334` |
| fd wedge the deadline bounds | `src/http.rs:3025-3029` (L3a EMFILE), `raise_nofile` `3030-3056` |
| rigs serve through the same loop | `src/dst/tests/fixture_http.rs:540-546`, `src/dst/tests/read_peer_compatibility.rs:196-205` |
| production call site | `src/bootstrap.rs:901-903` |

## 2. Designs

### A (recommended): the deadline is HTTP configuration; `serve_h1` takes `&HttpConfig`

- `HttpConfig` gains `h1_header_timeout: Duration` (default 30 s, env
  `SSE_H1_HEADER_TIMEOUT_MS`, `> 0` only: never disabled, never zero).
- New file `src/http/serve.rs` owns `h1_builder(&HttpConfig) -> http1::Builder`
  (timer + deadline + buffer set together) and the connection-lifetime tests.
- `serve_h1(listener, app, http: &HttpConfig, tasks)` builds the builder once
  and clones it per accepted connection. The function shrinks.
- `bootstrap::run` passes `&config.http`; both rigs pass their own loaded
  config instead of the `64 * 1024` literal (which duplicated
  `HttpConfig::default().h1_max_buf`).

Measured against the gate tooling (`scripts/quality/source_rules.py` on
in-memory edits, syntax binary `target/debug/streams-quality-syntax`):

| File / contract | Before | After | Ceiling / rule |
| --- | --- | --- | --- |
| `src/http.rs` lines | 3373 | **3372** | merge-base 3373 (legacy 3382+2 is higher; `min` picks 3373) |
| `serve_h1` `#[expect(disallowed_methods, let_underscore_must_use)]` | scope 68 / nested 1 / facts 76 | **66 / 1 / 73** | reason text unchanged, identity kept, no metric grows |
| `src/bootstrap.rs` lines | 924 | 923 | < 1000 |
| `run` six `#[expect]`s | scope 812 / facts 1000 | 811 / 999 | see the trap below |
| `src/http/serve.rs` | — | ~185 (incl. tests) | new, < 1000 |
| `src/config/{model,load,summary,tests}.rs` | 495/305/109/596 | +5/+3/+1/+4 | < 1000, no `#[expect]` on touched fns (`load.rs` has one only on `overlay_scaler`, `:230`) |
| `src/dst/tests/fixture_http.rs` | 792 | 792 | argument swap only |
| `src/dst/tests/read_peer_compatibility.rs` | 279 | 280 | one `let` |

**The `run` trap, measured.** `bootstrap::run` carries `#[expect(clippy::
expect_used)]` and `#[expect(clippy::unwrap_used)]` (`src/bootstrap.rs:128-135`).
Under those two lints the ratchet fingerprints every `call-site` under the
function *including its argument text* (`source_rules.py:109-113, 176-180`:
`site = qualified\0value`, and `value` carries the call's tokens). Any change to
the `crate::http::serve_h1(...)` call, even `max_buf` → `&config.http`, yields
`accepted exception grew without a new decision: ... expect_site:ordinary-call:
crate::run:9e353ed82e85779a 0 -> 1` (and the same under `unwrap_site`). I
reproduced that failure on the in-memory edit. There is no argument shape that
avoids it: the composition root must hand the posture over, and a `From<usize>
for Posture` or a `let max_buf = &config.http;` to keep the call bytes identical
would be a lint-dodging wrapper, which the policy forbids.

Sanctioned route taken: re-decide the two reason texts (a changed reason is a
new identity, `source_rules.py:204-208`), making them more precise than today
(they now state the site multiplicity they cover), and keep the required
three-part `owner; invariant; alternative` shape. With those two edits
`exception_growth` returns `[]` for all six `run` contracts (measured), and the
four untouched contracts shrink (811/999).

```rust
// src/bootstrap.rs:128-135 (only the two reason strings change)
#[expect(
    clippy::expect_used,
    reason = "run; covers exactly one site, the maintenance-worker spawn: the runtime's task supervisor is fresh at boot, so it accepts that worker; a fallible spawn would leave the process serving without maintenance"
)]
#[expect(
    clippy::unwrap_used,
    reason = "run; covers exactly four sites, the shared-cache lock and the three auth file paths: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"
)]
```

(Site counts verified: one `.expect(` at `run`+579 and four `.unwrap()` at
`run`+471, +617, +620, +623.)

### B (smallest diff): constant only, tests under tokio paused time

Keep `serve_h1(listener, app, max_buf: usize, tasks)` byte-identical for every
caller; `src/http/serve.rs` holds `pub(crate) const H1_HEADER_READ_TIMEOUT:
Duration = 30 s` and `h1_builder(max_buf)`; tests run
`#[tokio::test(start_paused = true)]` (current_thread) and let auto-advance
jump the 30 s deadline in virtual time. Nothing else changes: no `run` edit, no
config, no rig edits, no `review-mechanisms.json`, no inventory regen.

Why it works (verified in source): tokio 1.52.3 paused park does a
zero-timeout I/O poll and then advances the clock to the next timer unless the
time driver itself was unparked (`tokio-1.52.3/src/runtime/time/mod.rs:259-279`);
hyper polls the parser *before* the timer in `poll_read_head`
(`conn.rs:237` vs `:254-267`), so a request that arrived in the same park is
served even if the deadline fired in the same tick.

Why I do not recommend it: the tests depend on two external implementation
details (tokio auto-advance with real loopback sockets, which tokio's docs warn
about and which no test in this repo currently does, and hyper's poll order),
there is no runtime knob for the staging soak against an edge whose upstream
pool idle is unknown, and the h1 posture stays half constant / half config.
It is the right fallback if the reviewer refuses any edit to `run`.

### C (owner-first): move the connection layer out first

Commit 1, pure verbatim move: `serve_h1` (+ optionally `raise_nofile`,
`NOFILE_*`, `open_fds`, `spawn_runtime_watchdog`, `RUNTIME_*`) into
`src/http/serve.rs`, with `mod serve; pub(crate) use serve::serve_h1;` in
http.rs (−66 to −150 lines of http.rs headroom). Commit 2, the fix as in A but
with room to spare.

Ledger cost of the move alone (measured against `owners.json` /
`source-allowances.json`): the `effect` row `crate::serve_h1 /
tokio::task::JoinSet::spawn` (`docs/quality/owners.json:2043-2050`) moves to
`path: src/http/serve.rs`; the legacy `macro-dsl crate::serve_h1 tokio::select`
allowance (`source-allowances.json`) goes stale → `gate.py --prune`, and a new
`owners.json` macro-dsl row is needed at the new path; the same for
`spawn_runtime_watchdog`'s `tokio::select` and four `global` rows for the
statics if they move; the moved `#[expect(clippy::disallowed_methods, ...)]`
needs its effect owner registered at the new path or `source_rules.py:251-257`
fails. The move does **not** remove the `run` re-decision (the call still
changes). Recommendation: not as a rider on this fix; worth its own commit the
next time http.rs needs headroom.

## 3. Red tests (design A), near-complete

File: `src/http/serve.rs`, `#[cfg(test)] mod tests`. Explicit imports (no
`use super::*`, so no `unresolved-glob` row), no `tokio::spawn` (the
`TaskSupervisor` owns the server task, as the rigs do), no `tokio::select!`/
`join!` (macro-dsl rows). The scanner run on this exact module reports **zero**
inventory identities and zero exception contracts; `architecture-gate.py`
reports no reverse edges (`super::super::serve_h1` is deliberate:
`crate::http::serve_h1` would count as a `crate::http` reverse dependency for a
file outside `transport_and_composition_files`).

```rust
#[cfg(test)]
mod tests {
    use crate::config::HttpConfig;
    use crate::tasks::{Policy, TaskResult, TaskSupervisor};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// The rig deadline: short enough to observe, long enough that a
    /// loaded runner's scheduling gap never masquerades as idleness.
    const DEADLINE: Duration = Duration::from_secs(1);
    /// Every wait is bounded well past the deadline; a test that waits
    /// this long fails by assertion, it never hangs.
    const BOUND: Duration = Duration::from_secs(4);

    /// The production serve loop over a two-route app, on `deadline`.
    async fn serve(deadline: Duration) -> (std::net::SocketAddr, TaskSupervisor) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new()
            .route("/fast", axum::routing::get(|| async {}))
            .route(
                "/slow",
                axum::routing::get(move || async move {
                    tokio::time::sleep(deadline * 5 / 2).await;
                }),
            );
        let tasks = TaskSupervisor::new();
        let serve_tasks = tasks.clone();
        let http = HttpConfig {
            h1_header_timeout: deadline,
            ..HttpConfig::default()
        };
        tasks
            .spawn("h1-posture-rig", Policy::Critical, move |_cancel| async move {
                super::super::serve_h1(listener, app, &http, serve_tasks)
                    .await
                    .ok();
                TaskResult::Done
            })
            .unwrap();
        (addr, tasks)
    }

    /// Reads until the server closes. EOF and a reset both mean closed
    /// (a close with unread request bytes queued resets on macOS).
    async fn closed_within(sock: &mut TcpStream, bound: Duration) -> Result<(), String> {
        let mut buf = [0u8; 64];
        match tokio::time::timeout(bound, sock.read(&mut buf)).await {
            Ok(Ok(0)) => Ok(()),
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => Ok(()),
            Ok(Ok(n)) => Err(format!("server wrote {n} bytes instead of closing")),
            Ok(Err(e)) => Err(format!("read: {e}")),
            Err(_) => Err(format!("still open after {bound:?}")),
        }
    }

    /// Reads one response head (the rig's handlers answer without a body).
    async fn response_head(sock: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        while !buf.ends_with(b"\r\n\r\n") {
            let n = tokio::time::timeout(BOUND, sock.read(&mut byte))
                .await
                .expect("response head within the bound")
                .unwrap();
            assert_ne!(n, 0, "closed before the response head completed");
            buf.push(byte[0]);
        }
        String::from_utf8(buf).unwrap()
    }

    /// A connection that never completes a request head is closed at the
    /// deadline: a descriptor and a task are never held for ever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn headless_connection_is_closed_at_the_deadline() {
        let (addr, tasks) = serve(DEADLINE).await;
        let started = tokio::time::Instant::now();
        let mut silent = TcpStream::connect(addr).await.unwrap();
        let mut partial = TcpStream::connect(addr).await.unwrap();
        partial
            .write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n")
            .await
            .unwrap();
        assert_eq!(closed_within(&mut silent, BOUND).await, Ok(()), "silent socket");
        assert_eq!(closed_within(&mut partial, BOUND).await, Ok(()), "partial head");
        let held = started.elapsed();
        assert!(held >= DEADLINE / 2, "closed on sight, not at the deadline: {held:?}");
        tasks.shutdown(Duration::from_secs(5)).await;
    }

    /// The deadline covers only the wait for a request head: a response
    /// that takes longer than the deadline to produce still arrives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_flight_response_outlives_the_deadline() {
        let (addr, tasks) = serve(DEADLINE).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /slow HTTP/1.1\r\nhost: rig\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let head = response_head(&mut sock).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        assert_eq!(closed_within(&mut sock, BOUND).await, Ok(()), "connection: close");
        tasks.shutdown(Duration::from_secs(5)).await;
    }

    /// Between requests the same deadline is the idle keep-alive bound: a
    /// request inside it is served on the same connection, a connection
    /// idle past it is closed by the server.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_keep_alive_is_served_inside_and_closed_past_the_deadline() {
        let (addr, tasks) = serve(DEADLINE).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        for _ in 0..2 {
            sock.write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n\r\n")
                .await
                .unwrap();
            let head = response_head(&mut sock).await;
            assert!(head.starts_with("HTTP/1.1 200"), "{head}");
            tokio::time::sleep(DEADLINE / 10).await;
        }
        assert_eq!(closed_within(&mut sock, BOUND).await, Ok(()), "idle keep-alive");
        tasks.shutdown(Duration::from_secs(5)).await;
    }
}
```

Config pins, in `src/config/tests.rs`:

```rust
// default_values_are_pinned (after :143)
assert_eq!(c.http.h1_header_timeout, std::time::Duration::from_secs(30));
// env_overlay_applies_with_legacy_parse_semantics: add to the list
("SSE_H1_HEADER_TIMEOUT_MS", "0"),        // filtered -> default, never disabled
// and the assertions
assert_eq!(c.http.h1_header_timeout, std::time::Duration::from_secs(30));
let c = load_with(&[("SSE_H1_HEADER_TIMEOUT_MS", "2500")]);
assert_eq!(c.http.h1_header_timeout, std::time::Duration::from_millis(2500));
```

### Exact red behaviour (on the tree with the plumbing but without the two builder calls)

Produce the red state at the intermediate step: commit the plumbing and the
tests with `h1_builder` setting only `max_buf_size` (exactly today's posture),
run `cargo test --lib http::serve::`, record the output in the fix commit's
message, then add the two calls. Push only the finished state (Søren works
directly on `slate`; a red push is a red CI).

- `headless_connection_is_closed_at_the_deadline` fails after 4.0 s:

  ```text
  assertion `left == right` failed: silent socket
    left: Err("still open after 4s")
   right: Ok(())
  ```

- `idle_keep_alive_is_served_inside_and_closed_past_the_deadline` serves both
  requests (the control half passes), then fails after ~4.2 s:

  ```text
  assertion `left == right` failed: idle keep-alive
    left: Err("still open after 4s")
   right: Ok(())
  ```

- `in_flight_response_outlives_the_deadline` passes in both states (control:
  the response arrives after 2.5 × deadline; with the fix it proves the timer
  is not running in flight).
- The config pins fail red on the missing field (compile) if the field is
  omitted; with the field they pass in both states.

Green run: ≈ 1.0 + 2.5 + 1.2 s of wall time for the three connection tests.
Mutation value: the `serve_h1 → Ok(())` body mutant is killed by
`TcpStream::connect(addr).unwrap()` (listener dropped → ECONNREFUSED); a mutant
that drops `.header_read_timeout(..)` is killed by the two red tests; a mutant
that drops `.timer(..)` alone panics inside every `serve_connection`
(hyper `time.rs:80`) and is killed by all three. `h1_builder` has no
`Default`-able return (hyper `Builder` implements no `Default`), so
cargo-mutants generates no body-replacement mutant for it; that is reported as
"no mutant", not as a pass.

If the reviewer wants the tests under the DST contract table instead:
`src/dst/tests/http_connection.rs` (same bodies, `http_rig_build` is
unnecessary — the two-route app is the rig), a `#[path]` entry in
`src/dst/dst_tests.rs`, a row in `src/dst/tests/README.md` ("Connection
lifetime | `http_connection`"), `scripts/test-inventory.py --write`, and the
mutation-owner filter for `src/http/serve.rs` extended with
`dst_tests::http_connection::`. I prefer the module tests: the connection layer
has no data-plane oracle, and the DST inventory/mechanism ledgers stay untouched.

## 4. Code change, per file (design A)

### `src/http/serve.rs` (new, ~60 production lines + the tests above)

```rust
//! The h1 connection posture: what every accepted socket is served with.
//!
//! One builder, cloned per connection, so production and every rig serve
//! with the same bounded read buffer and the same request-head deadline
//! (`HttpConfig::h1_max_buf`, `HttpConfig::h1_header_timeout`).

use crate::config::HttpConfig;

/// The hyper builder every connection is served with; cloned per accept
/// (an `Arc` bump and a parser-config copy).
///
/// hyper 1.x documents a 30 s `header_read_timeout` default and drops it
/// silently unless the builder is also given a timer: `Time::check`
/// answers `None` for a defaulted duration with no timer, and its `warn!`
/// is compiled out without hyper's `tracing` feature (off in this build).
/// Without the deadline a socket that never sends a request head — a
/// suspended VM's corpse, a half-open NAT flow, a crashed client — holds
/// a descriptor and a task for ever: the L3a EMFILE wedge `raise_nofile`
/// widened but could not bound.
///
/// hyper arms the deadline when a connection first waits for a head and
/// again after every response, so it is also the idle keep-alive bound.
/// It never runs while a head is parsed, a body is read or a response is
/// written: long-polls (≤ 25 s) and SSE sessions are untouched. The
/// timer and the deadline are set together: a configured deadline
/// without a timer panics inside `serve_connection`, on every connection.
///
/// `max_buf` bounds each READ chunk, not request body size (#269:
/// axum::serve's default hyper posture measured ~53 KB resident per
/// parked connection; the bounded buffer holds the same fleet at ~44 KB,
/// the floor now dominated by task/future/slab overhead).
pub(crate) fn h1_builder(http: &HttpConfig) -> hyper::server::conn::http1::Builder {
    let mut b = hyper::server::conn::http1::Builder::new();
    b.timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(http.h1_header_timeout)
        .max_buf_size(http.h1_max_buf);
    b
}
```

### `src/http.rs` (3373 → 3372; `serve_h1` 68/76 → 66/73, reason unchanged)

Replace lines 1275-1342 with (only the marked lines differ):

```rust
/// #269: the one h1 serve loop — production and every test rig serve
/// through THIS function, so the suite exercises the real connection
/// path; what each connection is served with is `serve::h1_builder`.   // doc: 6 lines -> 3
#[expect(
    clippy::disallowed_methods,
    clippy::let_underscore_must_use,
    reason = "serve_h1; each accepted connection is served by a task the listener's own JoinSet owns and joins at shutdown, and nodelay and connection errors are routine client behaviour; a supervised task per connection and handled results would restate what the JoinSet already owns"
)]
pub(crate) async fn serve_h1(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    http: &crate::config::HttpConfig,                                     // was: max_buf: usize
    tasks: crate::tasks::TaskSupervisor,
) -> std::io::Result<()> {
    let svc = hyper_util::service::TowerToHyperService::new(app);
    let h1 = serve::h1_builder(http);                                     // new
    let limits = raise_nofile();
    ...unchanged through `let mut conns = tokio::task::JoinSet::new();`...
                Ok((sock, _peer)) => {
                    let svc = svc.clone();
                    let h1 = h1.clone();                                  // new
                    conns.spawn(async move {
                        let _ = sock.set_nodelay(true);
                        let io = hyper_util::rt::TokioIo::new(sock);
                        // Errors here are routine client behaviour (resets,
                        // half-closed keep-alives, head deadlines), not
                        // server faults.                                  // comment: 2 -> 3 lines
                        let _ = h1.serve_connection(io, svc).await;       // replaces 3 lines
                    });
                }
    ...unchanged to the end...
```

And next to the read adapter (`http.rs:3362-3364`), one line:

```rust
mod serve;
#[path = "http/read.rs"]
mod read_adapter;
```

`mod serve;` resolves to `src/http/serve.rs` without `#[path]`, so it adds no
`by-path-module` inventory identity (unlike `read_adapter`). The moved #269
measurement prose lives in `h1_builder`'s doc; the `serve_h1` doc stays
three lines so the function's scope shrinks. Doc lines count: the scanner
spans an item from its first attribute, doc comments included
(`tools/quality-syntax/src/scan.rs:129`, `node.span()`).

### `src/bootstrap.rs` (924 → 923; `run` 812/1000 → 811/999; two reasons re-decided)

```rust
    // #269 / head deadline: the h1 posture is the HTTP config's.
    let served = crate::http::serve_h1(listener, app, &config.http, tasks.clone()).await;
```

replacing `bootstrap.rs:901-903`, plus the two reason strings in section 2.
`config` is a local of `run` and outlives the inline `.await`, so the borrow
is fine; `run` is the composition root, which is where the posture belongs.

### `src/config/model.rs` (+5)

```rust
    /// SSE_H1_MAX_BUF, default 64 KiB — h1 connection buffer ceiling.
    pub h1_max_buf: usize,
    /// SSE_H1_HEADER_TIMEOUT_MS, default 30_000 — the request-head and idle
    /// keep-alive deadline (`http::serve::h1_builder`); 0/unparseable =
    /// default. Never disabled: hyper without it holds a headless socket
    /// for ever.
    pub h1_header_timeout: std::time::Duration,
// Default:
            h1_header_timeout: std::time::Duration::from_secs(30),
```

### `src/config/load.rs` (+3, in `overlay_http`, `:158-170`)

```rust
        if let Some(v) = env_parse::<u64>(env, "SSE_H1_HEADER_TIMEOUT_MS").filter(|v| *v > 0) {
            self.http.h1_header_timeout = std::time::Duration::from_millis(v);
        }
```

### `src/config/summary.rs` (+1, `"http"` object, `:52-57`)

```rust
                "h1_header_timeout_ms": u64::try_from(self.http.h1_header_timeout.as_millis()).unwrap_or(u64::MAX),
```

(the same spelling the summary already uses for `gc_interval`, `:43`; the
summary is an explicit projection, `summary.rs:1-7`, so the key must be added
by hand.)

### `src/config/tests.rs` (+4): section 3.

### `src/dst/tests/fixture_http.rs` (0 net; inside `http_rig_build`, `:544`)

`rig_config` (`Arc<ServerConfig>`, last used at `:487-488`) moves into the
serve closure:

```rust
            crate::http::serve_h1(listener, app, &rig_config.http, serve_tasks)
```

This also removes a duplicated default: the rig now serves with the config it
assembled (the fixture's own stated rule, `:409-414`).

### `src/dst/tests/read_peer_compatibility.rs` (+1; `:195-205`)

```rust
    let tasks = owner.tasks.clone();
    let config = owner.state.config.clone();
    let _server = owner.tasks.spawn(
        "legacy-scan-contract",
        crate::tasks::Policy::Critical,
        move |_cancel| async move {
            crate::http::serve_h1(listener, app, &config.http, tasks)
```

### The constant, its value, its rationale

`HttpConfig::default().h1_header_timeout = 30 s` (`SSE_H1_HEADER_TIMEOUT_MS`
overrides, `> 0` only).

- It is hyper's documented default (`http1.rs:350`): the value every hyper
  server that installs a timer already runs, and the one this server was
  supposed to have had. No novel number to defend.
- It clears every client pool idle that talks to this server by ≥ 7×
  (4 s: peers, pilot, store client) and the platform's ~5 s VM-suspend socket
  kill by 6×, so a client we control never reuses a socket the server may be
  closing at the same instant.
- It bounds a corpse socket (suspended peer VM, NAT drop, crashed client) to
  30 s of descriptor + task, instead of for ever. It is hygiene, not a DoS
  defence: holding the ~1.5k-fd L3a wall would still need only ~50 headless
  connects/s, below the edge's own per-origin wall.
- It is generous for any real head: product heads are < 8 KiB (JWT bearer
  ≤ 2 KiB); 30 s is two orders above a bad mobile RTT.
- Shorter (5-10 s) would be a better fd bound but risks origin-less edge 502s
  on stale upstream reuse if the platform edge pools connections to instances
  longer than that; its pool idle is not documented in the repo. Longer
  (60-75 s) has no evidence behind it and only delays release. 30 s is the
  largest value that still bounds corpses meaningfully; the env knob exists so
  the staging soak can lower it if the edge's pool proves shorter, without a
  rebuild.

## 5. Ledger and doc rows

| Ledger | Change | Why |
| --- | --- | --- |
| `scripts/quality/mutation_owners.py` | add `owner('http_serve', 'src/http/serve.rs', 'http::serve::'),` after the `http_read` row (`:82`) | `src/http` is a `BUFFER_PREFIXES` critical prefix (`verification_plan.py:29-31`); an unregistered critical file fails the plan. Optional: assert it in `test_mutation_owners.py::test_reviewed_moved_owners_are_registered`. |
| `docs/refactor/review-mechanisms.json` `fixture_changes[name=http_rig_build]` | recompute `after_sha256` (currently `feabf4c8…`, matches the working tree); append to `reason`: "The rig serves the production h1 loop with its own loaded HttpConfig (buffer ceiling and request-head deadline) instead of a literal buffer size; no assertion or constructor value changed." Keep `finding` R10 and `before_commit` | `scripts/review-evidence.py --check` runs in CI (`ci.yml:52`) and compares the current helper body hash (`review-evidence.py:90-99`, `:128-132`). One-liner: `python3 -c "import importlib.util as u;s=u.spec_from_file_location('i','scripts/test-inventory.py');m=u.module_from_spec(s);s.loader.exec_module(m);print([f['function_sha256'] for f in m.functions(open('src/dst/tests/fixture_http.rs').read(),include_helpers=True) if f['name']=='http_rig_build'])"` |
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write` then `--check` | `o2c_new_sender_clips_legacy_pages_across_owner_upgrade_and_rollback`'s body hash changes (`read_peer_compatibility.rs:200`). It is not a pinned mechanism test (not in `review-mechanisms.json`). The full-suite floor is the inventory count − 1 (`ci.yml:97`), unchanged. |
| `docs/quality/owners.json` | none | The new file's scan yields no `global`, `macro-dsl`, `effect` or `unresolved-glob` identity; `mod serve;` is not a `by-path-module`; the `serve_h1` effect row stays at `src/http.rs`/`crate::serve_h1`. |
| `docs/quality/source-allowances.json` | none (no allowance goes stale, so no `--prune`) | `serve_h1`'s `tokio::select`, http.rs statics and `run`'s macros all remain. Exception identities with `reason =` are never inventoried (`source_rules.py:263-264`). |
| `docs/refactor/architecture-policy.json` | none | new file is not a hard owner; no `crate::http`/`crate::product` reverse edge (tests use `super::super::serve_h1`); `bootstrap.rs` is a listed composition file. |
| `Cargo.toml` / `Cargo.lock` / `deny.toml` | none | no new crate or feature. |
| `docs/refactor/WIRE-MATRIX.md` §0 | add a bullet after "Router: … h1 only via `serve_h1`" | wire lifetime changed |
| `bench/WORKLOAD-CERT-PLAN.md` "#269 CLOSED" (`:495-503`) | optional one-line addendum naming the deadline | the paragraph documents the h1 posture |

WIRE-MATRIX §0 bullet:

> - **Connection lifetime** (`serve_h1`, `src/http.rs`; posture `src/http/serve.rs::h1_builder`): HTTP/1.1, keep-alive on, 64 KiB read buffer (`SSE_H1_MAX_BUF`), and a 30 s request-head deadline (`SSE_H1_HEADER_TIMEOUT_MS`, hyper `header_read_timeout` on a Tokio timer). The deadline runs from accept until the first request head and again after every response, so a keep-alive connection idle for 30 s after its last response is closed by the server (plain close, no 408). It never runs while a request is in flight: long-polls (≤ 25 s), SSE sessions and body uploads are unaffected. Before this row the deadline was silently absent: hyper 1.x drops its default without a timer.

Config knob inventory: `docs/` has no env-var table for `SSE_H1_MAX_BUF`
(only `bench/WORKLOAD-CERT-PLAN.md:497` and `bench/sse-probes/sse-1per.sh:23`),
so the model.rs doc comment and the redacted summary are the knob's record.

Gate legs the plan will select (`verification_plan.py:72-105`): `src/http`
and `src/bootstrap` → `miri: true` (fixed retained-buffer list,
`nightly.sh:8-10`; the socket tests are not in it), `src/bootstrap` →
`loom: true` (fixed models), `mutants: true` for owners `http`, `http_serve`,
`bootstrap`. `run`'s body-replacement mutant is killed by
`bootstrap/tests.rs:54` (`run(validated).await.unwrap_err()`). Run the CI plan
locally before pushing (memory trap), and `scripts/quality.sh` +
`scripts/gate.sh`.

## 6. What could go wrong

- **Platform edge upstream pools.** If the edge keeps idle upstream sockets to
  an instance longer than 30 s and reuses one at the instant the server closes
  it, the edge reports an origin-less 502 (no `prisma-streams-origin`; the
  CAPACITY-R27 classification, `docs/CAPACITY-R27.md:168-176`). Idle
  detection makes the window one RTT wide, but it is not zero. Canary: count
  origin-less 502s and `/v1/debug/load` descriptor headroom on staging before
  the rc; if 502s appear, raise `SSE_H1_HEADER_TIMEOUT_MS` (no rebuild). This
  is the reason the knob exists and the reason the value is 30 s, not 5 s.
- **Bench tools with 120 s pools** (`bench.rs`, `livebench.rs`, edgesim 90 s):
  a pooled socket idle > 30 s is now server-closed; reqwest drops closed idle
  sockets at checkout and retries idempotent GETs, but a POST that races the
  close fails once. Follow-up (separate): align bench `pool_idle_timeout` to
  ≤ 20 s, as the pilot already does.
- **Conformance suite** (`@durable-streams/server-conformance-tests@0.3.6`,
  Node/undici, 4 s keep-alive idle; long-polls ≤ 25 s in flight): unaffected.
  Product SDK (undici/fetch): unaffected. Browsers: reconnect on a closed idle
  socket; standard server behaviour (nginx 75 s, Node 5 s).
- **Fleet skew during rollout**: old binaries keep idle sockets for ever, new
  ones close at 30 s; the peer client on both sides drops idle sockets at 4 s
  (`peer.rs:176`), so no cross-version interaction. Wire request/response
  formats, persisted formats, cursors and tokens: untouched.
- **DST rigs now serve with a 30 s deadline.** No rig test holds a headless or
  idle raw socket for 30 s; SSE and long-poll tests are in flight (timer off).
  Paused-time tests (`start_paused`) never serve through `serve_h1` today
  (`persistence_faults.rs`, `producer_handoff.rs`, `runtime_open_gate.rs`,
  `sse_delivery.rs` paused tests use no rig). Hazard for the future: under
  auto-advance a 30 s deadline elapses instantly while the runtime is idle, so
  a paused-time test that parks a raw keep-alive socket would see it closed.
- **Memory**: an idle keep-alive connection now holds one boxed `Sleep`
  (`conn.rs:930-931`, ~150 B); parked SSE/long-poll connections hold none.
  #269's 44 KB/parked figure stands.
- **Silence on expiry**: hyper's own `warn!` is compiled out and `serve_h1`
  discards the error, so deadline closes are not counted. A counter would cost
  ~6 syntax facts in the ratcheted `serve_h1` (measured headroom: 3) and a
  `global` owners row; not in this change. If the staging soak needs it, it is
  its own line-budget exercise.
- **The `run` re-decision** is a reviewed change to two exception reasons for a
  call-argument change; the reasons become more precise (site counts) and no
  panic site is added (1 expect, 4 unwraps before and after). A reviewer who
  rejects touching `run` gets design B.
- **Body slow-loris is out of scope**: hyper has no body read timeout; the
  append path already bounds the initial body with its own 408
  `append_timeout` (WIRE-MATRIX §1.1). Reads carry no body.
- **hyper upgrade**: `Time::check` semantics (default dropped without a timer)
  are what this fix relies on being fixed by us, not hyper; a future hyper that
  auto-installs a timer changes nothing here (we set both explicitly).

## 7. Commit sequence

1. `serve.rs` + config field/overlay/summary/tests + `serve_h1(&HttpConfig)` +
   `run` call and reason re-decision + rig call sites + mutation owner row +
   `test-inventory.json --write` + `review-mechanisms.json` after-hash; with
   `h1_builder` setting only `max_buf_size` → run
   `cargo test --lib http::serve::` and record the two red failures verbatim.
2. Add `.timer(TokioTimer::new()).header_read_timeout(http.h1_header_timeout)`
   → green; WIRE-MATRIX row; commit with the red evidence in the message.
   Push both together after `scripts/gate.sh` and the CI plan pass locally.

## Skeptic corrections

Read-only check, 2026-09-22, against the same `slate` working tree. Every
ratchet number below was re-measured with the repo's own scanner
(`target/debug/streams-quality-syntax` via `scripts/quality/common.syntax` +
`source_rules.exception_contracts/exception_growth`) on in-memory copies of
the plan's exact edits; the repository was not written.

### Verdict: sound, with one gate-blocking omission (C1) and three accuracy fixes

What I confirmed first-hand (no correction needed):

- hyper 1.10.1 (`Cargo.lock:1316-1335`, no `tracing` dep): `Builder::new()` sets
  `timer: Time::Empty` + `h1_header_read_timeout: Dur::Default(Some(30s))`
  (`server/conn/http1.rs:243,249`); `Time::check` returns `None` for
  `Dur::Default` + `Time::Empty` and **panics** for `Dur::Configured` +
  `Time::Empty` (`common/time.rs:70-85`); `warn!` is a no-op without the
  `tracing` feature (`trace.rs:109-115`). `serve_h1` (`src/http.rs:1319-1320`)
  sets only `max_buf_size`. axum 0.8.9 / hyper-util 0.1.20 `auto::Builder::new`
  never call `.timer(..)` either. Claim confirmed.
- `hyper_util::rt::TokioTimer` is exported under the already-enabled `tokio`
  feature (`hyper-util/src/rt/mod.rs:8-12`; `Cargo.toml:16`); `TokioTimer::new()`
  exists (`rt/tokio.rs:304-306`); `http1::Builder` is `#[derive(Clone)]`
  (`http1.rs:70`) and `serve_connection(&self, ..)` (`:451`), so build-once /
  clone-per-accept compiles. No Cargo/deny/machete change. Confirmed.
- Ratchet numbers, reproduced exactly: `src/http.rs` 3373 → 3372;
  `serve_h1` 68/1/76 → 66/1/73 (reason unchanged, no metric grows);
  `bootstrap.rs` 924 → 923; `run` 812/1000 → 811/999; the call-only edit
  (no reason change) fails with
  `expect_site:ordinary-call:crate::run:9e353ed82e85779a 0 -> 1` and the same
  under `unwrap_site`; with the two re-decided reasons `exception_growth`
  is `[]`; both new reasons match the three-part regex
  (`"[^";]+;[^";]+;[^";]+"`). The scanner reports zero inventory identities
  and zero exception contracts for the assembled `src/http/serve.rs`
  (production block + test module from section 3/4). Confirmed.
- Callers: exactly three (`bootstrap.rs:903`, `fixture_http.rs:544`,
  `read_peer_compatibility.rs:200`); all covered by the plan. `HttpConfig` has
  no struct-literal constructions outside `model.rs` (only `..Default`), so the
  new field breaks no producer. `owner.state.config` is `Arc<ServerConfig>`
  (`http.rs:163`), `rig_config` is `Arc<ServerConfig>` and unused after
  `fixture_http.rs:488`, so both rig edits borrow-check. Confirmed.
- No pinned mechanism test or support helper lives in `fixture_http.rs` /
  `read_peer_compatibility.rs` except the `http_rig_build` fixture pin
  (`review-mechanisms.json`, current `after_sha256 feabf4c8…` matches the
  tree — recomputed by me). `mod serve;` resolves to `src/http/serve.rs`
  because `lib.rs:39` declares `mod http;` plainly (the `#[path]` style at
  `http.rs:3362-3373` is legacy, not required). `src/http/serve.rs` is not a
  hard owner and not in `transport_and_composition_files`; `super::super::
  serve_h1` matches neither `reverse_edges` root pattern for `(http|product)`
  (`architecture-gate.py:33-51`), so the plan's import choice is necessary and
  sufficient. `src/http` is a `BUFFER_PREFIXES` critical prefix
  (`verification_plan.py:29-31`) and `resolve_sources` fails on an
  unregistered path (`mutation_owners.py:216-225, 273-276`), so the
  `http_serve` owner row is required, as the plan says. Confirmed.
- No DST test parks a raw idle socket or holds an in-flight-free keep-alive
  for ≥ 30 s of real time (`hreq` sends `connection: close`,
  `fixture_requests.rs:19`; the only `from_secs(30..60)` in `src/dst/tests`
  are request/wait bounds); no `start_paused` test builds an HTTP rig
  (`persistence_faults.rs`, `sse_delivery.rs` paused tests use no rig).
  Confirmed.

### C1 (gate-blocking, missed): `http_rig_build` is ratcheted and the rig edit grows it

`src/dst/tests/fixture_http.rs:357-364`: `http_rig_build` carries
`#[expect(clippy::too_many_lines, ..)]` and
`#[expect(clippy::let_underscore_must_use, ..)]`. Both are merge-base
contracts on `scope_lines / nested_items / syntax_facts`. The plan's table
calls the fixture edit "argument swap only (0 net)" and never measures those
two contracts. Measured on the plan's exact edit
(`64 * 1024` → `&rig_config.http` at `:544`):

```text
accepted exception grew without a new decision: ('src/dst/tests/fixture_http.rs', 'crate::http_rig_build', 'function', 'expect (clippy :: too_many_lines , reason = "HTTP rig builder; …")'): syntax_facts 303 -> 304
accepted exception grew without a new decision: ('src/dst/tests/fixture_http.rs', 'crate::http_rig_build', 'function', 'expect (clippy :: let_underscore_must_use , reason = "http_rig_build; …")'): syntax_facts 303 -> 304
```

`64 * 1024` is two literals (zero facts); `&rig_config.http` is one `path`
fact. Both contracts fail `scripts/quality/gate.py` / `scripts/gate.sh` and
CI's rust-quality job.

**Fix (measured, no reason change needed): pay for the fact inside
`http_rig_build` by deleting a duplicated default of exactly the class the
plan is already removing.** `fixture_http.rs:446`:

```rust
    let livefeed = crate::sse::service::LiveFeedService::from_config(&rig_config.sse);
    livefeed.set_heartbeat_ms(15_000);          // <- delete this line
```

`LiveFeedService::from_config` already stores `cfg.heartbeat_ms`
(`src/sse/service.rs:43`); the rig's config is `ServerConfig::load(.., &MapEnvironment::empty())`
(`fixture_http.rs:337-349`) so `rig_config.sse.heartbeat_ms` is the knob
default `15_000` (`src/config/model.rs:411`). The line restates a value the
constructor just applied — the same "rig serves with the config it
assembled" rule the plan invokes for `64 * 1024`. `set_heartbeat_ms` keeps
its other callers (`sse/service.rs:133`, `dst/tests/livefeed_ownership.rs:385`),
so nothing goes dead. Measured with both fixture edits together:

| `http_rig_build` contract | Before | After |
| --- | --- | --- |
| `scope_lines / nested_items / syntax_facts` (both `#[expect]`s) | 201 / 1 / 303 | **200 / 1 / 301** |
| `exception_growth` | — | `[]` |
| `fixture_http.rs` lines | 792 | **791** |

Update section 2's table row for `fixture_http.rs` (792 → 791, two edits),
and extend the `review-mechanisms.json` `http_rig_build` reason append with:
"…instead of a literal buffer size, and no longer restates the default SSE
heartbeat that `LiveFeedService::from_config` already applied; no assertion
or constructor value changed." The `after_sha256` must be recomputed after
BOTH edits (the plan's one-liner does that).

`read_peer_compatibility.rs`'s `o2c_new_sender_clips_legacy_pages_across_owner_upgrade_and_rollback`
carries no `#[expect]` (checked `:157-158`), so the `+1 let` there is free;
measured `exception_growth` `[]`.

### C2 (accuracy): cargo-mutants does generate a mutant for `h1_builder`, and it is *unviable*, not absent

Section 3 says "hyper `Builder` implements no `Default`, so cargo-mutants
generates no body-replacement mutant … reported as 'no mutant'". Wrong on
the mechanism: cargo-mutants 27.1.0 (`quality-tools.toml:10`) falls back to
`Default::default()` for any unrecognised return type
(`cargo-mutants-27.1.0/src/fnvalue.rs:165-169, 257`), so
`replace h1_builder -> Builder with Default::default()` IS listed by
`--list --json` (the driver counts it, `mutation_driver.py:140-150`), then
fails to build and is reported **unviable**. That is harmless for the gate:
`ExitCode` has `Success/Usage/FoundProblems/Timeout/BaselineFailed/…` and no
unviable code (`src/exit_code.rs:17-36`), so `subprocess.run(.., check=True)`
passes. Record the disposition honestly: "`h1_builder`: 1 unviable,
0 viable mutants; the timer/deadline posture is proven by the three socket
tests, not by mutation". Also note that the "drops `.timer(..)`" / "drops
`.header_read_timeout(..)`" mutants in section 3 are hand-reasoned —
cargo-mutants does not delete method calls from a chain — so do not present
them as executed mutation evidence.

### C3 (precision, asked for by the item): what actually keeps the timer off during an SSE/long response

Section 0 item 2 attributes the in-flight protection to `dispatch.poll_ready`
being `Pending` while "a request future is in flight" (`dispatch.rs:628-632`).
That is true only while the *handler* future is running. For an SSE session
or any streaming response, the handler future completes the moment it
returns the `Response`; the body then streams for minutes with
`in_flight == None`, so `poll_ready` is `Ready`. What keeps the head timer
off then is the connection state machine:

- after a head with a zero-length body parses, `reading = Reading::KeepAlive`
  (`proto/h1/conn.rs:301-305`; `Reading::Body(..)` for a request with a body,
  `:319-326`);
- `can_read_head()` requires `Reading::Init` (`conn.rs:174-185`), so the
  dispatcher's `poll_read` takes the `poll_read_keep_alive` branch
  (`dispatch.rs:216-289`), which never touches the header timer;
- `Reading::Init` returns only via `State::idle()`, reached from
  `try_keep_alive` when **both** sides are `KeepAlive`, i.e. after the
  response body's encoder hit EOF and `writing` left `Writing::Body`
  (`conn.rs:1072-1089`, `:595-600`, `:1104-1116`).

Conclusion unchanged (long-polls ≤ 25 s, SSE sessions, body uploads and slow
responses are never under the timer, and the parked-SSE memory figure is
untouched because the `Sleep` is dropped at `conn.rs:273-277`), but cite the
state gate, not the handler future, in `h1_builder`'s doc and the WIRE-MATRIX
bullet; otherwise a future reader may believe an SSE session is only
protected while its handler runs.

### C4 (bench, not gated): the #269 CONTROL probe parks idle keep-alives past the new deadline

`bench/sse-probes/ka-slope.sh:38-52` opens N plain keep-alive connections
(one `GET /v1/debug/store` each, then idle) and holds them `asyncio.sleep(40)`;
the `PARKED` RSS sample is taken at `sleep 25` (`:59`). With the fix the
server closes each socket 30 s after its response, so connections opened at
the start of the ramp die at ~t=31 s and the 40 s hold no longer measures a
parked fleet; the 25 s sample survives only because the ramp starts at
~t=1 s. Section 6's "bench tools" bullet lists `bench.rs`/`livebench.rs`/
`edgesim` but misses this probe. Add `SSE_H1_HEADER_TIMEOUT_MS=120000` to the
probe's server environment (the knob is the plan's own escape hatch) or
shorten the hold, and say so in `bench/sse-probes/README.md`.

### C5 (nits, no change to the design)

- The item text names `docs/WIRE-MATRIX.md`; that file does not exist. The
  plan's `docs/refactor/WIRE-MATRIX.md` is the real one (§0 bullet at `:9`
  is the right anchor). Note the working tree already carries uncommitted
  edits to `docs/refactor/WIRE-MATRIX.md` and `docs/refactor/test-inventory.json`
  (git status) — regenerate/append on top of those, never from a clean
  checkout.
- The `run` reason re-decision is within the letter of the policy ("update
  its reason as the explicit reviewed decision") and measured clean, but the
  new text differs from the old only by a site count. If the reviewer objects
  that a count is not a decision, the honest fallback is design B, not a
  `let max_buf = &config.http;` shadow (agreed with the plan).
- `serve()` in the test module: `tasks.spawn(..).unwrap()` needs
  `SpawnRejected: Debug` — it derives `Debug` (`src/tasks.rs:83-84`); and
  `supervisor.shutdown(..).await;` as a bare statement is the existing
  pattern (`src/tasks/tests.rs:42`), `ShutdownReport` is not `#[must_use]`.
  Both compile as written.
- `closed_within`'s `Ok(Err(e)) if e.kind() == ConnectionReset => Ok(())`
  next to `Ok(Ok(0)) => Ok(())` is not a `match_same_arms` hit (guarded arms
  are skipped by that lint). Fine as written.
- With C1 applied the fixture edit is two lines; keep the commit-1 red run
  exactly as sequenced (plumbing + tests with `h1_builder` setting only
  `max_buf_size`), then commit 2 adds `.timer(TokioTimer::new())
  .header_read_timeout(http.h1_header_timeout)`. The red messages in
  section 3 are what `assert_eq!` prints for `Err("still open after 4s")`
  vs `Ok(())`; confirmed against the assertion text.
