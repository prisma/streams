//! Which product operation a request names is decided once, here. The auth
//! gate authorizes that operation's §6.1 scope, and the entry dispatches
//! exactly these operations
//! (`security_operations::the_entry_dispatches_exactly_the_resolved_operations`).
//! A request that names none has no scope to lack: the entry refuses it
//! (404/405) after the gate has authenticated it and checked its prefix.
//! A separate route × verb × method scope matrix used to answer with
//! fallback arms for requests nothing dispatched, so a new entry arm
//! inherited whatever those arms guessed.
use axum::http::Method;

use super::ProductRoute;
use crate::tenant::Scope;

/// The suffixes the grammar splits off a final segment. Anything else after
/// a colon stays part of the collection name, because a colon is legal
/// inside one.
pub(crate) const VERBS: [&str; 7] = [
    "batch",
    "long-poll",
    "sse",
    "pull",
    "settle",
    "seal",
    "scan",
];

/// One variant per dispatch arm of `product_entry`, so a new operation
/// cannot reach the gate without `scope` deciding its scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProductOperation {
    Create,
    Metadata,
    Delete,
    Seal,
    Scan,
    Append,
    AppendBatch,
    Read,
    Subscribe,
    ConsumerPut,
    ConsumerGet,
    ConsumerDelete,
    ConsumerPull,
    ConsumerSettle,
    WatchList,
    WatchGet,
    WatchWait,
    Usage,
}

impl ProductOperation {
    /// The operation a request names; `None` exactly when the entry answers
    /// 404/405 for this route, verb and method.
    pub(crate) fn resolve(
        route: &ProductRoute,
        verb: Option<&str>,
        method: &Method,
    ) -> Option<Self> {
        let get = *method == Method::GET;
        match route {
            ProductRoute::Collection { .. } => match (method.clone(), verb) {
                (Method::PUT, None) => Some(Self::Create),
                (Method::GET, None) => Some(Self::Metadata),
                (Method::DELETE, None) => Some(Self::Delete),
                (Method::POST, Some("seal")) => Some(Self::Seal),
                (Method::GET, Some("scan")) => Some(Self::Scan),
                _ => None,
            },
            ProductRoute::Records { .. } => match (method.clone(), verb) {
                (Method::POST, None) => Some(Self::Append),
                (Method::POST, Some("batch")) => Some(Self::AppendBatch),
                (Method::GET, None | Some("long-poll")) => Some(Self::Read),
                (Method::GET, Some("sse")) => Some(Self::Subscribe),
                _ => None,
            },
            ProductRoute::Consumer { .. } => match (method.clone(), verb) {
                (Method::PUT, None) => Some(Self::ConsumerPut),
                (Method::GET, None) => Some(Self::ConsumerGet),
                (Method::DELETE, None) => Some(Self::ConsumerDelete),
                (Method::POST, Some("pull")) => Some(Self::ConsumerPull),
                (Method::POST, Some("settle")) => Some(Self::ConsumerSettle),
                _ => None,
            },
            // The entry ignores a verb on these routes; so does the
            // operation.
            ProductRoute::Watches { .. } => get.then_some(Self::WatchList),
            ProductRoute::Watch { .. } => get.then_some(Self::WatchGet),
            ProductRoute::WatchWait { .. } => get.then_some(Self::WatchWait),
            ProductRoute::Usage { .. } => get.then_some(Self::Usage),
        }
    }

    /// The §6.1 scope a request demands: its named operation's. `None` when
    /// it names no operation, because the entry refuses it by route after
    /// the gate has authenticated it and checked its prefix, and for the
    /// watch wait.
    pub(crate) fn demanded_scope(
        route: &ProductRoute,
        verb: Option<&str>,
        method: &Method,
    ) -> Option<Scope> {
        Self::resolve(route, verb, method).and_then(Self::scope)
    }

    /// §6.1. Scopes that depend on the body (watch definitions on create, a
    /// DLQ link on a consumer) are checked where the body is parsed. `None`
    /// only for the watch wait: its handler verifies a §15 capability or the
    /// stream key. The gate prefix-checks a bearer-authenticated wait; a
    /// capability carrier skips the gate and is bound to its stream by the
    /// capability's signature.
    fn scope(self) -> Option<Scope> {
        Some(match self {
            Self::Create => Scope::Create,
            // Reading a consumer's config/positions is stream metadata;
            // changing it is configuration.
            Self::Metadata | Self::ConsumerGet | Self::WatchList | Self::WatchGet => {
                Scope::MetadataRead
            }
            Self::Delete | Self::Seal => Scope::LifecycleManage,
            // `:scan` pages back decrypted record bodies: a bulk record
            // read, so a metadata-only credential (which a create/monitor
            // service legitimately holds, along with the stream key)
            // cannot export records, and revoking records.read cuts record
            // access.
            Self::Scan | Self::Read | Self::Subscribe => Scope::RecordsRead,
            Self::Append | Self::AppendBatch => Scope::RecordsAppend,
            Self::ConsumerPut | Self::ConsumerDelete => Scope::ConsumersConfigure,
            Self::ConsumerPull => Scope::ConsumersPull,
            Self::ConsumerSettle => Scope::ConsumersSettle,
            Self::Usage => Scope::UsageRead,
            Self::WatchWait => return None,
        })
    }
}

/// Whether a request will meet the write gate on its ordinary path, by its method and its path at
/// arrival: a product append or batch (`ProductOperation`; not a seal, which meets the gate only
/// when it carries a final record), a raw append or close (a POST to
/// a stream), a queue's send, batch or settle (the queue surface's `writes_like_append`). Such a
/// request enters the write gate's count at the gate, not at its arrival
/// (`admission::write_slots`, edge change #272).
pub(crate) fn meets_write_gate(method: &Method, path: &str) -> bool {
    use super::queues::Resource;
    use ProductOperation as Op;
    if *method != Method::POST {
        return false;
    }
    if let Some(rest) = path.strip_prefix("/v1/streams/") {
        let verb = super::strip_verb(rest).1;
        let op = super::classify_route(rest)
            .ok()
            .and_then(|route| Op::resolve(&route, verb, method));
        return matches!(op, Some(Op::Append | Op::AppendBatch));
    }
    if let Some(rest) = path.strip_prefix("/v1/queues/") {
        let resource = super::queues::parse(rest).ok().map(|(_, r)| r);
        return matches!(
            resource,
            Some(Resource::Messages | Resource::MessagesBatch | Resource::GroupVerb("settle"))
        );
    }
    path.strip_prefix("/v1/stream/")
        .is_some_and(|rest| !rest.is_empty() && !rest.starts_with("__ds/"))
}

/// Count `request` in at the transport (`AdmissionController::enter`, a data-plane route as an
/// arrival), and whether it will meet the write gate (`meets_write_gate`): its ticket and that.
pub(crate) fn count_in<B>(
    admission: &crate::admission::AdmissionController,
    request: &axum::http::Request<B>,
) -> (crate::admission::InflightTicket, bool) {
    let path = request.uri().path();
    let ticket = admission.enter(super::live_wait::data_plane(path));
    (ticket, meets_write_gate(request.method(), path))
}

#[cfg(test)]
mod tests {
    use super::meets_write_gate;
    use axum::http::Method;

    /// The routes that meet the write gate on their ordinary path enter at 0, by method and path:
    /// a product append and batch, a raw append or close, a queue's send, batch and settle.
    /// Every other route, and these paths under another method, counts from its arrival.
    #[test]
    fn the_routes_that_meet_the_write_gate_and_only_they_enter_at_zero() {
        let post = Method::POST;
        for path in [
            "/v1/streams/orders/records",
            "/v1/streams/orders/records:batch",
            "/v1/stream/orders",
            "/v1/queues/jobs/messages",
            "/v1/queues/jobs/messages:batch",
            "/v1/queues/jobs/groups/default:settle",
        ] {
            assert!(meets_write_gate(&post, path), "POST {path} meets the gate");
        }
        let not = [
            (Method::GET, "/v1/streams/orders/records"),
            (Method::PUT, "/v1/streams/orders"),
            (Method::DELETE, "/v1/streams/orders"),
            (Method::GET, "/v1/streams/orders:scan"),
            (Method::POST, "/v1/streams/orders/consumers/c1:pull"),
            (Method::POST, "/v1/streams/orders/consumers/c1:settle"),
            (Method::PUT, "/v1/stream/orders"),
            (Method::GET, "/v1/stream/orders"),
            (Method::POST, "/v1/stream/__ds/x"),
            (Method::POST, "/v1/streams/orders:seal"),
            (Method::POST, "/v1/queues/jobs/groups/default:claim"),
            (Method::PUT, "/v1/queues/jobs"),
            (Method::GET, "/v1/queues/jobs/messages"),
            (Method::POST, "/v1/streams"),
            (Method::POST, "/v1/containers/p1/live:wait"),
        ];
        for (method, path) in not {
            assert!(
                !meets_write_gate(&method, path),
                "{method} {path} counts from its arrival"
            );
        }
    }
}
