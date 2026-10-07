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
//! waits, taken again before a woken wait renders (`resume`), settled when
//! its page renders, and attached to the response body when the handler
//! returns, so the page's bytes stay counted until the body ends.

use std::future::Future;
use std::sync::{Arc, OnceLock};

use axum::response::Response;

use super::read_memory::ReadHold;
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

/// The served request's wait ended with a page to render: take its read
/// reservation, released while it waited, again (`ReadHold::resume`),
/// waiting for room until `deadline`, the wait's own; the caller keeps its
/// `Parked` across this, so the request waits for room parked. False: no
/// room came, and the wait must end as if nothing had arrived. True
/// outside a served request or for a request no admission reserved.
pub(crate) async fn resume(deadline: tokio::time::Instant) -> bool {
    let Ok(read) = REQUEST.try_with(|request| request.read.clone()) else {
        return true;
    };
    match read.get() {
        Some(hold) => hold.resume(deadline).await,
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
