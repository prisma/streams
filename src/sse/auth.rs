//! The SSE authorization boundary — moved verbatim from http.rs
//! (LIVE-FEED Stage 1). `GatedSseBody` remains the AUTHORITATIVE
//! per-frame lease gate; `LeaseWatch`/`SseLease` carry the
//! generation-stable proof, workload-token expiry and exactly-once
//! termination accounting established in rounds V4/3/4. Contract
//! tests transfer unchanged.

use crate::http::{AppState, InternalLease, SseSlot, err_resp};
use axum::http::StatusCode;
use axum::response::Response;
use bytes::Bytes;
use std::sync::Arc;

/// Review round 3: subscriber-side canary counters.
pub(crate) mod sse_stats {
    use std::sync::atomic::AtomicU64;
    pub static DELIVERED_RECORDS: AtomicU64 = AtomicU64::new(0);
    pub static BELOW_FLOOR_CATCHUPS: AtomicU64 = AtomicU64::new(0);
    pub static DISCONNECT_SEND_TIMEOUT: AtomicU64 = AtomicU64::new(0);
    pub static DISCONNECT_CLIENT_CLOSED: AtomicU64 = AtomicU64::new(0);
    // LiveFeed engine counters (follow-up review: field observability).
    /// Shared-admission refusals (zero budget or zero ring).
    pub static FEED_CAPACITY_REJECTED: AtomicU64 = AtomicU64::new(0);
    /// Source reads that returned an empty partial page.
    pub static FEED_NO_PROGRESS: AtomicU64 = AtomicU64::new(0);
    /// Source reads that failed outright.
    pub static FEED_SOURCE_FAILED: AtomicU64 = AtomicU64::new(0);
    /// Batches dropped without retention: larger than the feed ring.
    pub static FEED_OVERSIZE_DROPPED: AtomicU64 = AtomicU64::new(0);
    /// Publications without retention: process budget exhausted.
    pub static FEED_UNCACHED_PUBLISH: AtomicU64 = AtomicU64::new(0);
    /// Publications without retention: the PROJECT's own allowance
    /// exhausted (round-10 isolation — the offender takes the
    /// uncached posture; the cell ceiling was not the refusal).
    pub static FEED_PROJECT_CAP_UNCACHED: AtomicU64 = AtomicU64::new(0);
    /// Live sessions disconnected for genuine lag (below the floor
    /// AFTER having reached live).
    pub static FEED_LAG_DISCONNECTS: AtomicU64 = AtomicU64::new(0);
    /// Sessions disconnected WITHOUT a terminal control because the
    /// incarnation moved on, the topology was incompatible, a
    /// transition did not settle in bounds, or a raw session met a
    /// source swap (Stage 6 typed disconnect-and-resume).
    pub static FEED_TOPOLOGY_DISCONNECTS: AtomicU64 = AtomicU64::new(0);
    /// Typed source cutoffs by reason (Stage 7 canary telemetry).
    pub static FEED_CUTOFF_INCARNATION: AtomicU64 = AtomicU64::new(0);
    pub static FEED_CUTOFF_WRONG_OWNER: AtomicU64 = AtomicU64::new(0);
    pub static FEED_CUTOFF_INCOMPATIBLE: AtomicU64 = AtomicU64::new(0);
    pub static FEED_CUTOFF_TARGET_MISMATCH: AtomicU64 = AtomicU64::new(0);
    pub static FEED_CUTOFF_FLEET_AUTH: AtomicU64 = AtomicU64::new(0);
    pub static FEED_CUTOFF_REDIRECT_LOOP: AtomicU64 = AtomicU64::new(0);
    /// Initial-handoff durable re-catch-ups (the ring overtook a
    /// session that had not reached live yet — NOT a disconnect).
    pub static FEED_CATCHUP_RETRIES: AtomicU64 = AtomicU64::new(0);
    /// Feed version publications (one per actual state change).
    pub static FEED_VERSION_BUMPS: AtomicU64 = AtomicU64::new(0);
}

/// Review round 3 F1: lease terminations by reason (canary counter).
pub(crate) static LEASE_TERMINATIONS: [std::sync::atomic::AtomicU64; 10] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

pub(crate) fn lease_terminations_json() -> serde_json::Value {
    let mut m = serde_json::Map::new();
    for r in crate::auth::LeaseInvalidReason::ALL {
        m.insert(
            r.as_str().to_string(),
            serde_json::Value::from(
                LEASE_TERMINATIONS[r.index()].load(std::sync::atomic::Ordering::Relaxed),
            ),
        );
    }
    serde_json::Value::Object(m)
}

/// Round-4 finding 1: a subscription whose lease was ALREADY invalid
/// when its body was constructed is refused, never established. The
/// status classes mirror `auth_failure_response` (product.rs): 503 for
/// this cell's own feed staleness, 403 for verified-but-denied project
/// state, 401 for everything a fresh token fixes.
pub(crate) fn lease_refusal_response(r: crate::auth::LeaseInvalidReason) -> Response {
    use crate::auth::LeaseInvalidReason as R;
    let status = match r {
        R::PolicyStale | R::GrantsStale => StatusCode::SERVICE_UNAVAILABLE,
        R::ProjectMissing | R::ProjectNotActive => StatusCode::FORBIDDEN,
        _ => StatusCode::UNAUTHORIZED,
    };
    err_resp(
        status,
        r.as_str(),
        "authorization was invalidated before the subscription could be established",
    )
}

/// Round-4 finding 2: what a long-lived subscription re-proves. A
/// customer lease re-validates against the policy/grant feeds; an
/// internal (workload-JWT) lease enforces token expiry — the raw
/// surface has no feed-coupled identity to re-check yet, but a parked
/// connection must still die with its credential.
#[derive(Clone, Debug)]
pub(crate) enum SseLease {
    None,
    Customer(crate::auth::AuthLease),
    Internal(InternalLease),
}

impl SseLease {
    pub(crate) fn of(params: &crate::http::ReadParams) -> Self {
        match (&params.internal_lease, &params.lease) {
            (Some(i), _) => Self::Internal(i.clone()),
            (None, Some(c)) => Self::Customer(c.clone()),
            (None, None) => Self::None,
        }
    }
}

/// Review V4 + round 3 F1: bounded re-authorization for a live
/// subscription. One atomic load per wakeup; the full lease re-check
/// runs when the publication generation moved OR the lease's own
/// deadline (token/credential expiry, feed staleness boundary) passed.
pub(crate) struct LeaseWatch {
    pub(crate) lease: SseLease,
    pub(crate) last_gen: u64,
    pub(crate) next_deadline: i64,
    /// Round-4 finding 4: EXACT-ONCE termination accounting. The
    /// producer task and the response-body gate each hold their own
    /// watch for the SAME connection; unsynchronized, one invalidated
    /// subscription produced one OR TWO termination counts depending
    /// on scheduling — and either watcher alone can miss being first
    /// (the producer quitting hands the body a plain EOF). The record
    /// is SHARED per connection: the first detector wins the swap and
    /// records its observed reason exactly once.
    pub(crate) term: std::sync::Arc<TerminateOnce>,
}

/// Per-connection exactly-once termination record (round-4 finding 4).
#[derive(Default)]
pub(crate) struct TerminateOnce(std::sync::atomic::AtomicBool);

impl TerminateOnce {
    /// Record `r` unless another detector already recorded THIS
    /// connection's termination. True = this call is the one that
    /// counted.
    pub(crate) fn record_once(&self, r: crate::auth::LeaseInvalidReason) -> bool {
        if self.0.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return false;
        }
        LEASE_TERMINATIONS[r.index()].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    }
}

impl LeaseWatch {
    /// Round-4 finding 1: GENERATION-STABLE INITIAL VALIDATION. The
    /// old constructor only READ the current generation into
    /// `last_gen`; invalidation published after token verification but
    /// before this construction left `last_gen` already equal to the
    /// NEW generation, so the first `revoked()` fast path ("generation
    /// unchanged since construction") passed forever and the
    /// subscription started on authorization that was already dead.
    ///
    /// This constructor validates UNCONDITIONALLY before the watch is
    /// trusted; customer leases re-read in a loop until the generation
    /// is stable ACROSS the check (`lease_check` and `lease_deadline`
    /// read several snapshots, so a publication landing mid-check
    /// would otherwise mix pre- and post-publication facts). Returns
    /// the reason when the lease was already invalid at construction.
    pub(crate) fn new_checked(
        state: &AppState,
        lease: SseLease,
        term: std::sync::Arc<TerminateOnce>,
    ) -> Result<Self, crate::auth::LeaseInvalidReason> {
        let now = crate::shard::now_ms() / 1000;
        // Round-4 finding 2: an internal (workload-JWT) lease has no
        // feed-coupled identity to re-check yet — its single fact is
        // token expiry, one clock read with no snapshot window to
        // stabilize across.
        if let SseLease::Internal(l) = &lease {
            if now >= l.expires_at {
                return Err(crate::auth::LeaseInvalidReason::TokenExpired);
            }
            return Ok(Self {
                next_deadline: l.expires_at,
                last_gen: state.auth.auth_generation(),
                lease,
                term,
            });
        }
        let SseLease::Customer(lease) = lease else {
            return Ok(Self::unrestricted());
        };
        loop {
            let before = state.auth.auth_generation();
            let now = crate::shard::now_ms() / 1000;
            state.auth.lease_check(&lease, now)?;
            let deadline = state.auth.lease_deadline(&lease);
            let after = state.auth.auth_generation();
            if before == after {
                return Ok(Self {
                    lease: SseLease::Customer(lease),
                    last_gen: after,
                    next_deadline: deadline,
                    term,
                });
            }
        }
    }

    pub(crate) fn unrestricted() -> Self {
        Self {
            lease: SseLease::None,
            last_gen: 0,
            next_deadline: i64::MAX,
            term: std::sync::Arc::new(TerminateOnce::default()),
        }
    }

    /// True = terminate the subscription NOW.
    pub(crate) fn revoked(&mut self, state: &AppState) -> bool {
        let now = crate::shard::now_ms() / 1000;
        let g = state.auth.auth_generation();
        if g == self.last_gen && now < self.next_deadline {
            return false;
        }
        self.last_gen = g;
        match &self.lease {
            SseLease::None => false,
            // Round-4 finding 2: a workload-JWT subscription dies with
            // its token — nothing else can invalidate it yet.
            SseLease::Internal(l) => {
                if now >= l.expires_at {
                    // The identity fields ride the log: a fleet
                    // operator debugging terminations needs to know
                    // WHICH workload credential died, not just that
                    // one did.
                    tracing::info!(
                        subject = %l.subject,
                        cell = %l.cell_id,
                        operation = l.operation.claim(),
                        "workload-JWT subscription terminated at token expiry"
                    );
                    self.term
                        .record_once(crate::auth::LeaseInvalidReason::TokenExpired);
                    true
                } else {
                    self.next_deadline = l.expires_at;
                    false
                }
            }
            SseLease::Customer(l) => match state.auth.lease_check(l, now) {
                Ok(()) => {
                    self.next_deadline = state.auth.lease_deadline(l);
                    false
                }
                Err(r) => {
                    self.term.record_once(r);
                    true
                }
            },
        }
    }

    /// Nap until the next mandatory re-check, capped so a far-future
    /// deadline does not hold a giant timer.
    pub(crate) fn nap(&self) -> std::time::Duration {
        if matches!(self.lease, SseLease::None) {
            return std::time::Duration::from_secs(3600);
        }
        let now = crate::shard::now_ms() / 1000;
        std::time::Duration::from_secs((self.next_deadline - now).clamp(1, 3600) as u64)
    }
}

/// One queued SSE chunk plus its BILLABLE weight. Metering happens at
/// the AUTHORITATIVE yield boundary in `GatedSseBody` — producer-side
/// billing at enqueue time charged for frames the body gate then
/// discarded at a revocation/expiry cutoff (round-9 review: the
/// customer received no payload but read usage was charged). Status
/// controls, cursors, terminals and keep-alives carry zero weight.
pub(crate) struct SseChunk {
    pub(crate) bytes: Bytes,
    pub(crate) payload_bytes: u64,
    pub(crate) records: u64,
}

/// Review round 3 F2: the AUTHORITATIVE lease gate sits at the
/// response-body boundary — immediately before bytes leave the HTTP
/// body. The producer checks are an optimization that stops work; this
/// gate guarantees that no queued frame is yielded after a revocation,
/// expiry, or staleness boundary has been observed (the channel's
/// buffered items included). Wakes on: the next queued frame, an auth
/// generation change (watch), or the lease deadline. Round-9: it is
/// also the METERING boundary — `DELIVERED_RECORDS` and read billing
/// move exactly with the yield, so a discarded frame is never charged.
pub(crate) struct GatedSseBody {
    state: Arc<AppState>,
    rx: tokio::sync::mpsc::Receiver<SseChunk>,
    /// Billing identity resolution at YIELD time (ownership can move
    /// mid-connection; the identity is re-resolved per billed chunk,
    /// the same rule the hub producer applied per batch).
    desc: crate::registry::StreamDesc,
    _slot: SseSlot,
    watch: LeaseWatch,
    gen_rx: tokio::sync::watch::Receiver<u64>,
    gen_changed: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
    deadline: std::pin::Pin<Box<tokio::time::Sleep>>,
    /// Round-11.1: the body OWNS keep-alives — a blocked local read,
    /// remote page, topology refresh, or store operation can never
    /// suppress network heartbeats. Any successful outbound chunk
    /// resets the timer (no keep-alive right after data).
    heartbeat: std::pin::Pin<Box<tokio::time::Sleep>>,
    heartbeat_interval: std::time::Duration,
    ended: bool,
}

impl GatedSseBody {
    /// Round-4 finding 1: the watch arrives PRE-VALIDATED
    /// (`LeaseWatch::new_checked`) and the generation receiver is
    /// subscribed BEFORE that check runs, so a publication landing in
    /// between is still observed by THIS body on its first poll — no
    /// missed wakeup between "proved" and "parked".
    pub(crate) fn new(
        state: Arc<AppState>,
        rx: tokio::sync::mpsc::Receiver<SseChunk>,
        desc: crate::registry::StreamDesc,
        slot: SseSlot,
        watch: LeaseWatch,
        gen_rx: tokio::sync::watch::Receiver<u64>,
    ) -> Self {
        let nap = watch.nap();
        let heartbeat_interval =
            std::time::Duration::from_millis(state.livefeed.heartbeat_ms().max(50));
        Self {
            gen_rx,
            state,
            rx,
            desc,
            _slot: slot,
            watch,
            gen_changed: None,
            deadline: Box::pin(tokio::time::sleep(nap)),
            heartbeat: Box::pin(tokio::time::sleep(heartbeat_interval)),
            heartbeat_interval,
            ended: false,
        }
    }
}

impl futures_util::Stream for GatedSseBody {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if !matches!(this.watch.lease, SseLease::None) {
            // Generation change: (re)arm a changed() future on a clone
            // of the receiver so it can be held across polls.
            let mut fired = false;
            loop {
                if this.gen_changed.is_none() {
                    let mut rx = this.gen_rx.clone();
                    this.gen_changed = Some(Box::pin(async move {
                        let _ = rx.changed().await;
                    }));
                }
                match this.gen_changed.as_mut().unwrap().as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.gen_changed = None;
                        // Mark the value seen on OUR receiver too.
                        let _ = this.gen_rx.has_changed();
                        this.gen_rx.mark_unchanged();
                        fired = true;
                    }
                    Poll::Pending => break,
                }
            }
            if this.deadline.as_mut().poll(cx).is_ready() {
                fired = true;
                let nap = this.watch.nap();
                this.deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + nap);
                // re-register the fresh timer
                let _ = this.deadline.as_mut().poll(cx);
            }
            if fired && this.watch.revoked(&this.state) {
                this.ended = true;
                this.rx.close();
                return Poll::Ready(None);
            }
        }
        // Test failpoint: park THIS poll BEFORE dequeuing — the frame
        // stays IN the channel across an authorization cutoff, and the
        // resumed body must discard it unyielded AND unbilled (round-9
        // review's queued-frame billing leg). Sync poll context: park
        // by returning Pending and re-waking on release.
        #[cfg(test)]
        if crate::failpoints::hit(crate::failpoints::Fp::SseBodyBeforeYield, &this.desc.name) {
            let waker = cx.waker().clone();
            let name = this.desc.name.clone();
            tokio::spawn(async move {
                crate::failpoints::pause(crate::failpoints::Fp::SseBodyBeforeYield, &name).await;
                waker.wake();
            });
            return Poll::Pending;
        }
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(chunk)) => {
                // The authoritative per-frame gate: cheap (atomic +
                // deadline compare) unless something moved.
                if this.watch.revoked(&this.state) {
                    this.ended = true;
                    this.rx.close();
                    return Poll::Ready(None);
                }
                // Metering rides the YIELD (round-9 review): §4.2's
                // "each emitted payload" is what actually leaves the
                // body, never a queued frame the gate discards.
                if chunk.records > 0 {
                    sse_stats::DELIVERED_RECORDS
                        .fetch_add(chunk.records, std::sync::atomic::Ordering::Relaxed);
                    crate::billing::meter_read_chunk(
                        &this.state.billing_reads,
                        &crate::billing::identity_of(&this.state, &this.desc),
                        chunk.payload_bytes,
                        chunk.records,
                    );
                }
                // Data IS liveness: push the next keep-alive out.
                this.heartbeat
                    .as_mut()
                    .reset(tokio::time::Instant::now() + this.heartbeat_interval);
                Poll::Ready(Some(Ok(chunk.bytes)))
            }
            Poll::Ready(None) => {
                this.ended = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                // Round-11.1 body-owned keep-alive: producer progress
                // and network liveness are INDEPENDENT. The lease is
                // re-checked (cheap fast path) so a keep-alive can
                // never outlive a cutoff the deadline/generation arms
                // would have caught.
                if this.heartbeat.as_mut().poll(cx).is_ready() {
                    if this.watch.revoked(&this.state) {
                        this.ended = true;
                        this.rx.close();
                        return Poll::Ready(None);
                    }
                    this.heartbeat
                        .as_mut()
                        .reset(tokio::time::Instant::now() + this.heartbeat_interval);
                    let _ = this.heartbeat.as_mut().poll(cx);
                    return Poll::Ready(Some(Ok(Bytes::from_static(b": keep-alive\n\n"))));
                }
                Poll::Pending
            }
        }
    }
}
