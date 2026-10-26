//! The multiplexed wait's request body and its limits
//! (docs/refactor/WIRE-MATRIX.md §2.24): one long-poll carrying a client's
//! tails, consumer-group pulls and watch waits. Every bound is checked
//! before anything is resolved, so a malformed body costs no store work.
use std::collections::HashSet;
use std::time::Duration;

use serde::Deserialize;

/// The longest wait, as every per-stream long-poll's.
pub(super) const MAX_WAIT: Duration = Duration::from_secs(25);
/// The answer's byte budget when the request names none, and its bounds.
pub(super) const DEFAULT_MAX_BYTES: usize = 256 << 10;
pub(super) const MIN_MAX_BYTES: usize = 4 << 10;
pub(super) const MAX_MAX_BYTES: usize = 1 << 20;
/// The most subscriptions one request carries.
pub(super) const MAX_SUBS: usize = 256;
/// The longest subscription id.
const MAX_ID: usize = 64;

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Body {
    wait_ms: Option<u64>,
    max_bytes: Option<usize>,
    subs: Vec<SubBody>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SubBody {
    id: String,
    kind: String,
    stream: String,
    key: Option<String>,
    cursor: Option<String>,
    routing_key: Option<String>,
    deliver: Option<String>,
    group: Option<String>,
    max: Option<usize>,
    visibility_ms: Option<u64>,
    watch: Option<String>,
    watch_key: Option<String>,
}

/// A validated request.
pub(super) struct Request {
    pub(super) wait: Duration,
    pub(super) max_bytes: usize,
    pub(super) subs: Vec<Sub>,
}

/// One subscription, as the client named it.
pub(super) struct Sub {
    pub(super) id: String,
    pub(super) stream: String,
    pub(super) spec: Spec,
}

pub(super) enum Spec {
    Tail(TailSpec),
    Group(GroupSpec),
    Watch(WatchSpec),
}

pub(super) struct TailSpec {
    pub(super) key: String,
    pub(super) cursor: Option<String>,
    pub(super) routing_key: String,
    pub(super) applied: bool,
}

pub(super) struct GroupSpec {
    pub(super) key: String,
    pub(super) group: String,
    pub(super) max: Option<usize>,
    pub(super) visibility_ms: Option<u64>,
}

pub(super) struct WatchSpec {
    pub(super) key: String,
    pub(super) watch: String,
    pub(super) watch_key: String,
    pub(super) cursor: String,
}

/// Why a body is refused (400 `invalid_body`, or 429
/// `live_subscription_limit` for too many subscriptions).
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Invalid {
    Body(String),
    TooMany(usize),
}

impl Request {
    pub(super) fn parse(bytes: &[u8]) -> Result<Self, Invalid> {
        let body: Body = serde_json::from_slice(bytes)
            .map_err(|e| Invalid::Body(format!("live wait request: {e}")))?;
        if body.subs.len() > MAX_SUBS {
            return Err(Invalid::TooMany(body.subs.len()));
        }
        if body.subs.is_empty() {
            return Err(Invalid::Body(
                "subs must name at least one subscription".into(),
            ));
        }
        let wait = Duration::from_millis(body.wait_ms.unwrap_or(25_000)).min(MAX_WAIT);
        let max_bytes = body
            .max_bytes
            .unwrap_or(DEFAULT_MAX_BYTES)
            .clamp(MIN_MAX_BYTES, MAX_MAX_BYTES);
        let mut ids = HashSet::with_capacity(body.subs.len());
        let mut subs = Vec::with_capacity(body.subs.len());
        for sub in body.subs {
            if sub.id.is_empty() || sub.id.len() > MAX_ID {
                return Err(Invalid::Body(format!(
                    "a subscription id is 1-{MAX_ID} bytes"
                )));
            }
            if !ids.insert(sub.id.clone()) {
                return Err(Invalid::Body(format!(
                    "subscription id {:?} is named twice",
                    sub.id
                )));
            }
            subs.push(sub.validated()?);
        }
        Ok(Self {
            wait,
            max_bytes,
            subs,
        })
    }
}

impl SubBody {
    fn validated(self) -> Result<Sub, Invalid> {
        let field = |kind: &str, name: &str| {
            Invalid::Body(format!(
                "subscription {:?}: a {kind} takes no {name}",
                self.id
            ))
        };
        let need = |kind: &str, name: &str| {
            Invalid::Body(format!("subscription {:?}: a {kind} needs {name}", self.id))
        };
        let spec = match self.kind.as_str() {
            "tail" => {
                if self.group.is_some() || self.max.is_some() || self.visibility_ms.is_some() {
                    return Err(field("tail", "group, max or visibilityMs"));
                }
                if self.watch.is_some() || self.watch_key.is_some() {
                    return Err(field("tail", "watch or watchKey"));
                }
                let applied = match self.deliver.as_deref() {
                    None | Some("durable") => false,
                    Some("applied") => true,
                    Some(_) => {
                        return Err(Invalid::Body(format!(
                            "subscription {:?}: deliver must be \"durable\" or \"applied\"",
                            self.id
                        )));
                    }
                };
                Spec::Tail(TailSpec {
                    key: self.key.clone().ok_or_else(|| need("tail", "key"))?,
                    cursor: self.cursor.clone(),
                    routing_key: self.routing_key.clone().unwrap_or_default(),
                    applied,
                })
            }
            "group" => {
                if self.cursor.is_some() || self.routing_key.is_some() || self.deliver.is_some() {
                    return Err(field("group", "cursor, routingKey or deliver"));
                }
                if self.watch.is_some() || self.watch_key.is_some() {
                    return Err(field("group", "watch or watchKey"));
                }
                Spec::Group(GroupSpec {
                    key: self.key.clone().ok_or_else(|| need("group", "key"))?,
                    group: self.group.clone().ok_or_else(|| need("group", "group"))?,
                    max: self.max,
                    visibility_ms: self.visibility_ms,
                })
            }
            "watch" => {
                if self.group.is_some() || self.max.is_some() || self.visibility_ms.is_some() {
                    return Err(field("watch", "group, max or visibilityMs"));
                }
                if self.routing_key.is_some() || self.deliver.is_some() {
                    return Err(field("watch", "routingKey or deliver"));
                }
                Spec::Watch(WatchSpec {
                    key: self.key.clone().ok_or_else(|| need("watch", "key"))?,
                    watch: self.watch.clone().ok_or_else(|| need("watch", "watch"))?,
                    watch_key: self
                        .watch_key
                        .clone()
                        .ok_or_else(|| need("watch", "watchKey"))?,
                    cursor: self.cursor.clone().unwrap_or_else(|| "now".into()),
                })
            }
            other => {
                return Err(Invalid::Body(format!(
                    "subscription {:?}: kind {other:?} is not tail, group or watch",
                    self.id
                )));
            }
        };
        Ok(Sub {
            id: self.id,
            stream: self.stream,
            spec,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MAX_BYTES, GroupSpec, Invalid, MAX_MAX_BYTES, MAX_SUBS, MAX_WAIT, MIN_MAX_BYTES,
        Request, Spec, TailSpec, WatchSpec,
    };
    use std::time::Duration;

    fn parse(body: &str) -> Result<Request, Invalid> {
        Request::parse(body.as_bytes())
    }

    #[test]
    fn a_request_takes_its_defaults_and_clamps_every_bound() {
        let r = parse(r#"{"subs":[{"id":"t","kind":"tail","stream":"a","key":"k"}]}"#).unwrap();
        assert_eq!(
            (r.wait, r.max_bytes, r.subs.len()),
            (MAX_WAIT, DEFAULT_MAX_BYTES, 1)
        );
        let r = parse(
            r#"{"waitMs":99999,"maxBytes":1,"subs":[{"id":"t","kind":"tail","stream":"a","key":"k"}]}"#,
        )
        .unwrap();
        assert_eq!((r.wait, r.max_bytes), (MAX_WAIT, MIN_MAX_BYTES));
        let r = parse(r#"{"waitMs":0,"subs":[{"id":"t","kind":"tail","stream":"a","key":"k"}]}"#)
            .unwrap();
        assert_eq!(r.wait, Duration::ZERO);
        let r = parse(
            r#"{"maxBytes":99999999,"subs":[{"id":"t","kind":"tail","stream":"a","key":"k"}]}"#,
        )
        .unwrap();
        assert_eq!(r.max_bytes, MAX_MAX_BYTES);
    }

    #[test]
    fn every_kind_names_its_own_fields_only() {
        let r = parse(
            r#"{"subs":[
              {"id":"t","kind":"tail","stream":"a","key":"k","cursor":"now","routingKey":"r","deliver":"applied"},
              {"id":"g","kind":"group","stream":"b","key":"k","group":"g1","max":3,"visibilityMs":500},
              {"id":"w","kind":"watch","stream":"c","key":"k","watch":"byId","watchKey":"00ff"}]}"#,
        )
        .unwrap();
        assert!(matches!(
            &r.subs[0].spec,
            Spec::Tail(TailSpec { cursor: Some(c), routing_key, applied: true, .. }) if c == "now" && routing_key == "r"
        ));
        assert!(matches!(
            &r.subs[1].spec,
            Spec::Group(GroupSpec { group, max: Some(3), visibility_ms: Some(500), .. }) if group == "g1"
        ));
        assert!(matches!(
            &r.subs[2].spec,
            Spec::Watch(WatchSpec { watch, cursor, key, .. }) if watch == "byId" && cursor == "now" && key == "k"
        ));
        for bad in [
            r#"{"subs":[{"id":"t","kind":"tail","stream":"a","key":"k","group":"g"}]}"#,
            r#"{"subs":[{"id":"g","kind":"group","stream":"a","key":"k","group":"g","cursor":"c"}]}"#,
            r#"{"subs":[{"id":"g","kind":"group","stream":"a","key":"k","group":"g","seen":"s"}]}"#,
            r#"{"subs":[{"id":"w","kind":"watch","stream":"a","watch":"x","watchKey":"1"}]}"#,
            r#"{"subs":[{"id":"t","kind":"tail","stream":"a"}]}"#,
            r#"{"subs":[{"id":"g","kind":"group","stream":"a","key":"k"}]}"#,
            r#"{"subs":[{"id":"w","kind":"watch","stream":"a","watch":"x"}]}"#,
            r#"{"subs":[{"id":"t","kind":"sse","stream":"a","key":"k"}]}"#,
            r#"{"subs":[{"id":"t","kind":"tail","stream":"a","key":"k","deliver":"soon"}]}"#,
            r#"{"subs":[{"id":"t","kind":"tail","stream":"a","key":"k","extra":1}]}"#,
            r#"{"lingerMs":5,"subs":[{"id":"t","kind":"tail","stream":"a","key":"k"}]}"#,
            r#"{"subs":[]}"#,
            r#"{"subs":[{"id":"","kind":"tail","stream":"a","key":"k"}]}"#,
            r#"{"subs":[{"id":"t","kind":"tail","stream":"a","key":"k"},{"id":"t","kind":"tail","stream":"b","key":"k"}]}"#,
        ] {
            assert!(matches!(parse(bad), Err(Invalid::Body(_))), "{bad}");
        }
    }

    #[test]
    fn more_subscriptions_than_the_cap_are_refused_as_too_many() {
        let subs: Vec<String> = (0..=MAX_SUBS)
            .map(|i| format!(r#"{{"id":"t{i}","kind":"tail","stream":"a","key":"k"}}"#))
            .collect();
        let body = format!(r#"{{"subs":[{}]}}"#, subs.join(","));
        assert_eq!(parse(&body).err(), Some(Invalid::TooMany(MAX_SUBS + 1)));
        let ok: Vec<String> = subs[..MAX_SUBS].to_vec();
        assert_eq!(
            parse(&format!(r#"{{"subs":[{}]}}"#, ok.join(",")))
                .unwrap()
                .subs
                .len(),
            MAX_SUBS
        );
    }
}
