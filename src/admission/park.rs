//! The request a handler serves, and how its waits park (shared cells M1).
//!
//! The transport serves every request through `InflightTicket::serve`, so
//! the request's own in-flight ticket is in scope for exactly as long as
//! its handler runs. A wait deep inside an application operation (the
//! read long-poll's tail wait, the consumer pull's wait, the watch wait)
//! parks THAT request with `park`, without every layer between the
//! transport and the wait carrying the ticket down. A verified request
//! also binds its project's share of the live pool (`bind_principal`)
//! once its project is admitted, so its waits park against the share too.
//!
//! A wait that cannot park (outside a served request, or with the pool or
//! the share full) waits as an active request, as every wait did before.
//!
//! The scope also carries the request's read memory (shared cells H3,
//! `read_memory::ReadHold`): bound at admission, released while the request
//! waits, taken again before a woken wait renders (`resume`, or at most
//! what a woken long-poll measures, `resume_at_most`), settled when its
//! page renders, and attached to the response body when the handler
//! returns, so the page's bytes stay counted until the body ends. A read
//! beside that page (a settle's dead-letter handoff) reserves its own bytes
//! without waiting (`try_reserve_read`) and holds them itself.
//!
//! And it counts the registry store work the request's own task makes
//! (public-beta phase B step 14(b)): `store_timing` charges each registry
//! operation's weight (`charge_store_weight`), and when the request is
//! served, or its handler dropped, the total is debited from its bound
//! project's store budget (`ParkShare::debit_store_ops`). Store work
//! outside a served request (the committer, sweeps, other tasks) is
//! charged to no one, and neither is work a request runs through
//! `uncharged_when_complete` that completes (the stream-count seed walk).
//!
//! And it counts what a consumer pull's walks read before they lease
//! (public-beta phase B C7, decided (a) by the owner on 2026-10-12): a walk
//! notes its coverage (`note_coverage`), and when a walk leased nothing
//! the pull's wait has the bound project pay what was noted from its
//! read-byte bucket, debt allowed (`pay_coverage`), and, woken, waits
//! parked while the project is in read debt (`out_of_read_debt`).

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use axum::response::Response;

use super::fair_shed::Verdict;
use super::read_memory::{ReadHold, ReadRefusal};
use super::{InflightTicket, ParkedTicket};
use crate::auth::RequestPrincipal;
use crate::quota::QuotaRegistry;
use crate::quota::parked::{ParkShare, ProjectParked};

tokio::task_local! {
    /// The request the current handler future serves.
    static REQUEST: Request;
}

struct Request {
    ticket: InflightTicket,
    share: OnceLock<ParkShare>,
    read: Arc<OnceLock<ReadHold>>,
    /// The weighted registry store work the request's task has made.
    weight: AtomicU64,
    /// The bytes its walks read that it has not paid for yet.
    coverage: AtomicU64,
    /// What fair shedding made of it (`judge_load`).
    verdict: OnceLock<Verdict>,
}

impl Drop for Request {
    /// The request was served (or its handler dropped, its store work
    /// done all the same): its bound project pays for that work.
    fn drop(&mut self) {
        if let Some(share) = self.share.get() {
            share.debit_store_ops(*self.weight.get_mut(), crate::shard::now_ms());
        }
    }
}

impl InflightTicket {
    /// Serve `handler` as the request this ticket admitted; the ticket
    /// drops when the handler's future completes or is dropped, and a read
    /// hold the request bound rides the response's body.
    pub(crate) async fn serve<F: Future<Output = Response>>(self, handler: F) -> Response {
        let read = Arc::new(OnceLock::new());
        let request = Request {
            ticket: self,
            share: OnceLock::new(),
            read: read.clone(),
            weight: AtomicU64::new(0),
            coverage: AtomicU64::new(0),
            verdict: OnceLock::new(),
        };
        let response = REQUEST.scope(request, handler).await;
        match Arc::into_inner(read).and_then(OnceLock::into_inner) {
            Some(hold) => hold.attach(response),
            None => response,
        }
    }
}

/// Bind the served request's project share: the verified principal's
/// project under the quotas it was admitted with (the exact snapshot the
/// request verified against). The first bind stands; a request with no
/// principal binds none. Returns whether a share is bound afterwards.
pub(crate) fn bind_principal(quotas: &QuotaRegistry, principal: Option<&RequestPrincipal>) -> bool {
    let share = principal.and_then(|p| quotas.park_share(&p.project_id, &p.quotas));
    REQUEST
        .try_with(|request| {
            if let Some(share) = share {
                request.share.get_or_init(|| share);
            }
            request.share.get().is_some()
        })
        .unwrap_or(false)
}

/// Judge the served request by its bound project's share of the cell's
/// offered requests, on a cell shared `share_k` ways, once, and keep the
/// verdict for the request's gates (`verdict`). A request with no bound
/// project, or outside a served request, has none.
pub(crate) fn judge_load(share_k: u64) {
    let _judged = REQUEST.try_with(|request| {
        let share = request.share.get()?;
        Some(
            *request
                .verdict
                .get_or_init(|| share.judge(share_k, crate::shard::now_ms())),
        )
    });
}

/// The served request's fair-shed verdict: `None` when it was not counted.
pub(crate) fn verdict() -> Option<Verdict> {
    REQUEST
        .try_with(|request| request.verdict.get().copied())
        .ok()
        .flatten()
}

/// Charge `weight` units of registry store work to the served request,
/// which its project pays once it is served; nothing outside one.
pub(crate) fn charge_store_weight(weight: u64) {
    REQUEST
        .try_with(|request| {
            request.weight.fetch_add(weight, Ordering::Relaxed);
        })
        .unwrap_or(());
}

/// Note `bytes` a consumer pull's walk read before it leased, which the
/// served request pays if the walk leased nothing (`pay_coverage`);
/// nothing outside one.
pub(crate) fn note_coverage(bytes: u64) {
    REQUEST
        .try_with(|request| {
            request.coverage.fetch_add(bytes, Ordering::Relaxed);
        })
        .unwrap_or(());
}

/// The served request's walk leased nothing: its bound project pays the
/// coverage noted since it last paid from its read-byte bucket. A request
/// with no bound project pays nothing.
pub(crate) fn pay_coverage() {
    REQUEST
        .try_with(|request| {
            let bytes = request.coverage.swap(0, Ordering::Relaxed);
            if let Some(share) = request.share.get() {
                share.debit_read(bytes, crate::shard::now_ms());
            }
        })
        .unwrap_or(());
}

/// Wait until the served request's bound project is out of read debt, or
/// `deadline`, the wait's own, passes; the caller keeps its `Parked`
/// across this. False: the deadline passed first. True outside a served
/// request or for a request with no bound project.
pub(crate) async fn out_of_read_debt(deadline: tokio::time::Instant) -> bool {
    loop {
        let debt = REQUEST
            .try_with(|request| {
                let share = request.share.get()?;
                share.read_debt(crate::shard::now_ms())
            })
            .ok()
            .flatten();
        let Some(debt) = debt else {
            return true;
        };
        let now = tokio::time::Instant::now();
        let until = now
            .checked_add(debt)
            .map_or(deadline, |at| at.min(deadline));
        tokio::time::sleep_until(until).await;
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
    }
}

/// Run `work` in the served request, which pays nothing for the registry
/// store work `work` makes if it completes (`Ok`): the weight charged
/// before it is restored once it returns. Work that fails, or is dropped
/// before it returns, stays charged. Outside a served request nothing is
/// charged either way.
pub(crate) async fn uncharged_when_complete<T, E>(
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let before = REQUEST.try_with(|request| request.weight.load(Ordering::Relaxed));
    let done = work.await;
    if done.is_ok()
        && let Ok(before) = before
    {
        REQUEST
            .try_with(|request| request.weight.store(before, Ordering::Relaxed))
            .unwrap_or(());
    }
    done
}

/// Bind the served request's read hold. The first bind stands; a hold that
/// does not bind (outside a served request, or beside an earlier hold)
/// drops here, released.
pub(crate) fn bind_read(hold: ReadHold) {
    REQUEST
        .try_with(|request| drop(request.read.set(hold)))
        .unwrap_or(());
}

/// A page of `served` bytes rendered for the served request: its hold
/// settles to the page, or `unreserved` makes one (a page no admission
/// reserved). Outside a served request no body holds the page.
pub(crate) fn settle_read(served: u64, unreserved: impl FnOnce() -> ReadHold) {
    REQUEST
        .try_with(|request| request.read.get_or_init(unreserved).settle(served))
        .unwrap_or(());
}

/// What a read beside the served request's page reserved
/// (`try_reserve_read`).
pub(crate) enum ReadReservation {
    /// The bytes, held in both ledgers until the hold drops.
    Reserved(ReadHold),
    /// No room now under the project's line or in the instance's read
    /// memory: the read is deferred.
    NoRoom,
    /// Outside a served request: nothing to reserve against, so the read is
    /// made as before.
    Unscoped,
}

/// Reserve `bytes` for a read the served request makes beside the page its
/// admission reserved (a settle's dead-letter handoff, public-beta phase B
/// item B7), in its bound project's read bytes under the project's line and
/// in the instance's read memory, without waiting. The hold is the
/// caller's, for as long as it holds what it read: it is never bound to the
/// request, where a second read's hold would not bind and so would drop at
/// once.
pub(crate) fn try_reserve_read(bytes: u64) -> ReadReservation {
    REQUEST
        .try_with(|request| {
            let ctl = &request.ticket.ctl;
            let project = request.share.get().map(ParkShare::read_bytes);
            let weight = request.share.get().map_or(1, ParkShare::byte_weight);
            ReadHold::try_reserve(ctl, project, ctl.project_memory_line(weight), bytes)
                .map_or(ReadReservation::NoRoom, |hold| {
                    ReadReservation::Reserved(hold.weighted(weight))
                })
        })
        .unwrap_or(ReadReservation::Unscoped)
}

/// Reserve `bytes` for a read the served request makes beside its page, as
/// `try_reserve_read` does, waiting for room until `deadline` (a
/// multiplexed wait's group walk, whose coverage the answer's hold does not
/// cover). `NoRoom`: none came by then.
pub(crate) async fn reserve_read_until(
    bytes: u64,
    deadline: tokio::time::Instant,
) -> ReadReservation {
    let Ok((ctl, project, weight)) = REQUEST.try_with(|request| {
        let share = request.share.get();
        (
            request.ticket.ctl.clone(),
            share.map(ParkShare::read_bytes),
            share.map_or(1, ParkShare::byte_weight),
        )
    }) else {
        return ReadReservation::Unscoped;
    };
    let line = ctl.project_memory_line(weight);
    ReadHold::reserve_until(&ctl, project, line, bytes, deadline)
        .await
        .map_or(ReadReservation::NoRoom, |hold| {
            ReadReservation::Reserved(hold.weighted(weight))
        })
}

/// The served request waits again with its read reservation taken back: it
/// is released, as `park` releases it, until its next wake takes it again.
pub(crate) fn release_read() {
    REQUEST
        .try_with(|request| {
            if let Some(hold) = request.read.get() {
                hold.unreserve();
            }
        })
        .unwrap_or(());
}

/// The byte weight of the served request's bound project (its memory line
/// scales by it, `AdmissionController::project_memory_line`): 1 with no
/// bound project or outside a served request.
pub(crate) fn byte_weight() -> u64 {
    REQUEST
        .try_with(|request| request.share.get().map_or(1, ParkShare::byte_weight))
        .unwrap_or(1)
}

/// Reserve `bytes` for what the served request is about to read and render
/// without an admission reservation (a claim's leased messages, layout 6A
/// package 1), in its bound project's read bytes under the project's line
/// and in the instance's read memory, waiting for room as a page read's
/// admission does (`ReadHold::reserve`), and bind the hold to the request,
/// so its response body holds it once it settles to the page. Outside a
/// served request nothing is reserved.
pub(crate) async fn reserve_read(bytes: u64) -> Result<(), ReadRefusal> {
    let Ok((ctl, project, weight)) = REQUEST.try_with(|request| {
        let share = request.share.get();
        let weight = share.map_or(1, ParkShare::byte_weight);
        (
            request.ticket.ctl.clone(),
            share.map(ParkShare::read_bytes),
            weight,
        )
    }) else {
        return Ok(());
    };
    let line = ctl.project_memory_line(weight);
    bind_read(
        ReadHold::reserve(&ctl, project, line, bytes)
            .await?
            .weighted(weight),
    );
    Ok(())
}

/// While held, the served request is parked in a wait.
pub(crate) struct Parked {
    _instance: ParkedTicket,
    _project: Option<ProjectParked>,
}

/// Park the served request for one wait, against the instance's live pool
/// and, when bound, its project's share. `None`: the wait stays active.
/// Either way the request's unrendered read reservation is released: a
/// waiting request materializes nothing.
pub(crate) fn park() -> Option<Parked> {
    REQUEST
        .try_with(|request| {
            if let Some(hold) = request.read.get() {
                hold.unreserve();
            }
            let project = match request.share.get() {
                Some(share) => Some(share.park()?),
                None => None,
            };
            Some(Parked {
                _instance: request.ticket.ctl.park()?,
                _project: project,
            })
        })
        .ok()
        .flatten()
}

/// The served request's wait ended with a page to render: take its whole
/// read reservation, released while it waited, again (`resume_at_most`
/// with no measure).
pub(crate) async fn resume(deadline: tokio::time::Instant) -> bool {
    resume_at_most(deadline, || None).await
}

/// The served request's wait ended with a page to render: take its read
/// reservation, released while it waited, again, at most what `at_most`
/// measures on each attempt (`ReadHold::resume_with`; a woken long-poll
/// measures what was appended while it waited, public-beta phase B item
/// B8), waiting for room until `deadline`, the wait's own; the caller
/// keeps its `Parked` across this, so the request waits for room parked.
/// False: no room came, and the wait must end as if nothing had arrived.
/// True outside a served request or for a request no admission reserved.
pub(crate) async fn resume_at_most(
    deadline: tokio::time::Instant,
    at_most: impl Fn() -> Option<u64>,
) -> bool {
    let Ok(read) = REQUEST.try_with(|request| request.read.clone()) else {
        return true;
    };
    match read.get() {
        Some(hold) => hold.resume_with(deadline, at_most).await,
        None => true,
    }
}

/// Park the served request against the instance's live pool only, for a
/// wait that already holds its place in its project's share (a watch
/// wait's live-subscription slot).
pub(crate) fn park_instance() -> Option<Parked> {
    REQUEST
        .try_with(|request| {
            Some(Parked {
                _instance: request.ticket.ctl.park()?,
                _project: None,
            })
        })
        .ok()
        .flatten()
}
