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

use std::future::Future;
use std::sync::OnceLock;

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
}

impl InflightTicket {
    /// Serve `handler` as the request this ticket admitted; the ticket
    /// drops when the handler's future completes or is dropped.
    pub(crate) async fn serve<F: Future>(self, handler: F) -> F::Output {
        let request = Request {
            ticket: self,
            share: OnceLock::new(),
        };
        REQUEST.scope(request, handler).await
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

/// While held, the served request is parked in a wait.
pub(crate) struct Parked {
    _instance: ParkedTicket,
    _project: Option<ProjectParked>,
}

/// Park the served request for one wait, against the instance's live pool
/// and, when bound, its project's share. `None`: the wait stays active.
pub(crate) fn park() -> Option<Parked> {
    REQUEST
        .try_with(|request| {
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
