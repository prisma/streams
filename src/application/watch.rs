//! Watch observation: credential proof precedes state diagnostics, and each
//! request occupies one project request slot for its entire wait.
use crate::auth::RequestPrincipal;
use crate::registry::{Registry, StreamDesc, WatchDefinition};
use crate::runtime::Clock;
use crate::tenant::{ProjectId, TenantStreamRef};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) enum WatchAccess<'a> {
    AdmittedAccount(&'a RequestPrincipal),
    CapabilityCarrier,
    Deployment,
}

pub(crate) struct WatchCredentials {
    pub(crate) capability: Option<String>,
    pub(crate) encryption_key: Option<String>,
}

/// Only this module can construct credential proof. Unverified capability
/// claims never become an identity for accounting or existence diagnostics.
pub(crate) struct VerifiedObservation<'a> {
    descriptor: StreamDesc,
    watch: String,
    key_hex: String,
    key: Option<crate::crypto::StreamKey>,
    access: WatchAccess<'a>,
}

#[derive(Debug)]
pub(crate) enum WatchFailure {
    Unauthorized(Option<ProjectId>),
    PolicyStale(ProjectId),
    ProjectInactive(ProjectId),
    Quota(ProjectId, crate::quota::QuotaRefusal),
    InvalidKey,
    Creating,
    NotFound,
    UnknownWatch,
    Storage(String),
}

pub(crate) enum Observation {
    Touched {
        cursor: String,
        proven: bool,
        stream_cursor: Option<String>,
    },
    Stale {
        cursor: String,
    },
    Timeout {
        cursor: String,
        stream_cursor: Option<String>,
    },
}

/// A per-runtime pre-verification lookup budget. Its refill uses elapsed time,
/// so clock corrections cannot bypass the cap or poison a later runtime.
struct LookupBudget {
    tokens: f64,
    at: crate::runtime::MonotonicNow,
}
impl LookupBudget {
    fn take(&mut self, now: crate::runtime::MonotonicNow) -> bool {
        self.tokens = (self.tokens + now.since(self.at).as_secs_f64() * 500.0).min(1000.0);
        self.at = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

pub(crate) struct WatchService {
    registry: Arc<Registry>,
    auth: Arc<crate::auth::AuthService>,
    quotas: crate::quota::QuotaRegistry,
    keys: Arc<crate::history::KeyCache>,
    touch: Arc<crate::touch::TouchRegistry>,
    clock: Arc<dyn Clock>,
    lookup: Mutex<LookupBudget>,
}

impl WatchService {
    pub(crate) fn new(
        registry: Arc<Registry>,
        auth: Arc<crate::auth::AuthService>,
        quotas: crate::quota::QuotaRegistry,
        keys: Arc<crate::history::KeyCache>,
        touch: Arc<crate::touch::TouchRegistry>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let lookup = Mutex::new(LookupBudget {
            tokens: 1000.0,
            at: clock.monotonic(),
        });
        Self {
            registry,
            auth,
            quotas,
            keys,
            touch,
            clock,
            lookup,
        }
    }

    /// Compile immutable watch definitions into the committer's deferred
    /// touch effect. Publication remains behind its durable acknowledgement.
    pub(crate) fn append_touch(
        &self,
        descriptor: &StreamDesc,
        records: &[bytes::Bytes],
    ) -> Option<crate::shard::TouchFeed> {
        if descriptor.watch_definitions.is_empty() {
            return None;
        }
        let mut key_ids = Vec::new();
        for record in records {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(record) {
                key_ids.extend(product_watch_ids(&descriptor.watch_definitions, &value));
            }
        }
        key_ids.sort_unstable();
        key_ids.dedup();
        if key_ids.is_empty() {
            return None;
        }
        let journal = self.touch.journal(
            descriptor.storage_hash(),
            crate::crypto::RouteHash::for_stream(&descriptor.sref()),
            &watch_pinned(descriptor),
        );
        Some(crate::shard::TouchFeed {
            journal,
            key_ids,
            next_offset: 0,
        })
    }

    fn alive(&self, descriptor: &StreamDesc) -> bool {
        !descriptor.deleted
            && !descriptor.soft_deleted
            && descriptor
                .expires_at_ms
                .is_none_or(|expiry| self.clock.now().ms() < expiry)
    }

    pub(crate) async fn definitions(
        &self,
        stream: &TenantStreamRef,
    ) -> Result<Vec<WatchDefinition>, WatchFailure> {
        let descriptor = self
            .registry
            .get(stream)
            .await
            .map_err(|error| WatchFailure::Storage(error.to_string()))?
            .filter(|descriptor| self.alive(descriptor))
            .ok_or(WatchFailure::NotFound)?;
        if descriptor.init.is_some() {
            return Err(WatchFailure::Creating);
        }
        Ok(descriptor.watch_definitions.clone())
    }

    pub(crate) async fn authenticate<'a>(
        &self,
        stream: &TenantStreamRef,
        watch: String,
        key_hex: String,
        credentials: WatchCredentials,
        access: WatchAccess<'a>,
    ) -> Result<VerifiedObservation<'a>, WatchFailure> {
        let capability_project = match &credentials.capability {
            Some(capability) => {
                let project = crate::crypto::watch_capability_project(capability)
                    .ok_or(WatchFailure::Unauthorized(None))?;
                if !self.lookup.lock().unwrap().take(self.clock.monotonic()) {
                    return Err(WatchFailure::Unauthorized(None));
                }
                Some(project)
            }
            None => None,
        };
        let lookup = capability_project
            .as_ref()
            .map(|project| project.stream_ref(stream.name().as_str()))
            .unwrap_or_else(|| stream.clone());
        let descriptor = self
            .registry
            .get(&lookup)
            .await
            .ok()
            .flatten()
            .filter(|descriptor| self.alive(descriptor))
            .ok_or(WatchFailure::Unauthorized(None))?;
        let key_hex = key_hex.trim_end_matches('/').to_ascii_lowercase();
        let epoch = descriptor.epoch();
        let key = credentials
            .encryption_key
            .as_deref()
            .and_then(|raw| crate::crypto::StreamKey::from_b64(raw).ok())
            .filter(|key| key.fingerprint(&epoch) == descriptor.key_fingerprint);
        let capability_ok = credentials.capability.as_deref().is_some_and(|capability| {
            use base64::Engine;
            let Some(encoded) = descriptor.watch_sig_key.as_deref() else {
                return false;
            };
            let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
                return false;
            };
            let Ok(signing_key) = <[u8; 32]>::try_from(raw.as_slice()) else {
                return false;
            };
            crate::crypto::verify_watch_capability(
                capability,
                &signing_key,
                &descriptor.sref(),
                &descriptor.stream_epoch,
                &watch,
                &key_hex,
                "GET",
                self.clock.now().ms() / 1000,
            )
        });
        if !capability_ok && key.is_none() {
            return Err(WatchFailure::Unauthorized(None));
        }
        if let WatchAccess::AdmittedAccount(principal) = &access
            && (principal.project_id != descriptor.project_id
                || principal.require_stream(&descriptor.name).is_err())
        {
            return Err(WatchFailure::Unauthorized(None));
        }
        if key_hex.len() != 16 || u64::from_str_radix(&key_hex, 16).is_err() {
            return Err(WatchFailure::InvalidKey);
        }
        Ok(VerifiedObservation {
            descriptor,
            watch,
            key_hex,
            key,
            access,
        })
    }

    pub(crate) async fn wait(
        &self,
        verified: VerifiedObservation<'_>,
        cursor: String,
        timeout: Duration,
    ) -> Result<Observation, WatchFailure> {
        let descriptor = &verified.descriptor;
        let project = &descriptor.project_id;
        // Every enforce-mode waiter uses current policy. Only capability
        // carriers need a request slot here: account admission is held by the
        // caller, whose proof is carried explicitly in WatchAccess.
        let (_request, _subscription) = if self.auth.mode == crate::auth::AuthMode::Enforce {
            let (status, quotas) = self
                .auth
                .status_and_quotas(project, self.clock.now().ms() / 1000)
                .map_err(|_| WatchFailure::PolicyStale(project.clone()))?
                .ok_or_else(|| WatchFailure::Unauthorized(Some(project.clone())))?;
            if !matches!(status, crate::project_policy::ProjectStatus::Active) {
                return Err(WatchFailure::ProjectInactive(project.clone()));
            }
            let request = match verified.access {
                WatchAccess::AdmittedAccount(_) => None,
                WatchAccess::CapabilityCarrier => Some(
                    self.quotas
                        .admit(project, &quotas, self.clock.now().ms())
                        .map_err(|error| WatchFailure::Quota(project.clone(), error))?,
                ),
                WatchAccess::Deployment => return Err(WatchFailure::Unauthorized(None)),
            };
            let subscription = self
                .quotas
                .admit_subscription(project, &quotas)
                .map_err(|error| WatchFailure::Quota(project.clone(), error))?;
            (request, Some(subscription))
        } else {
            (None, None)
        };
        if descriptor.init.is_some() {
            return Err(WatchFailure::Creating);
        }
        if !descriptor
            .watch_definitions
            .iter()
            .any(|definition| definition.name == verified.watch)
        {
            return Err(WatchFailure::UnknownWatch);
        }
        if let Some(key) = verified.key {
            self.keys
                .put(descriptor.storage_hash(), key, descriptor.epoch());
        }
        let journal = self.touch.journal(
            descriptor.storage_hash(),
            crate::crypto::RouteHash::for_stream(&descriptor.sref()),
            &watch_pinned(descriptor),
        );
        let outcome = journal
            .wait(
                &cursor,
                vec![crate::touch_keys::key_id_of(&verified.key_hex)],
                timeout.min(Duration::from_secs(25)),
            )
            .await;
        let stream_cursor = |end| {
            self.keys.get(&descriptor.storage_hash()).map(|(key, _)| {
                let route = descriptor.resolve_segment("");
                crate::product_cursor::KeyCursor {
                    epoch: descriptor.epoch(),
                    key_hash: crate::crypto::stream_hash(""),
                    seg_id: route.seg_id,
                    offset: end,
                }
                .encode(project, &key)
            })
        };
        Ok(match outcome {
            crate::touch::WaitOutcome::Touched {
                cursor,
                end_offset,
                proven,
                ..
            } => Observation::Touched {
                cursor,
                proven,
                stream_cursor: stream_cursor(end_offset),
            },
            crate::touch::WaitOutcome::Stale { cursor } => Observation::Stale { cursor },
            crate::touch::WaitOutcome::Timeout { cursor, end_offset } => Observation::Timeout {
                cursor,
                stream_cursor: stream_cursor(end_offset),
            },
        })
    }
}

/// Journal template registration shape for a stream's immutable watch
/// definitions.
pub(crate) fn watch_pinned(desc: &StreamDesc) -> Vec<(String, Vec<String>)> {
    desc.watch_definitions
        .iter()
        .map(|w| (w.name.clone(), w.fields.clone()))
        .collect()
}

/// Canonical watch-key value encoding (spec Stage 2 §3.3): JSON
/// serialization, so "1" (string), 1 (number), true, null, arrays and
/// objects are all distinct. A missing pointer produces NO key for the
/// definition.
///
/// This encoding is NORMATIVE and cross-language — the SDK derives the
/// same watch key offline (see `sdk/src/index.ts`), so the two must
/// agree byte for byte. Two places where a naive `to_string()` would
/// not: object keys are sorted (serde's map already is, JavaScript's
/// is not), and a float with no fractional part is written as an
/// integer, because serde writes `1.0` where JSON.stringify writes `1`.
pub(crate) fn canonical_arg(v: &serde_json::Value) -> String {
    use serde_json::Value as V;
    match v {
        V::Number(n) => match n.as_f64() {
            Some(f) if n.as_i64().is_none() && n.as_u64().is_none() && f.fract() == 0.0 => {
                format!("{}", f as i64)
            }
            _ => n.to_string(),
        },
        V::Array(a) => {
            let items: Vec<String> = a.iter().map(canonical_arg).collect();
            format!("[{}]", items.join(","))
        }
        V::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let items: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        V::String((*k).clone()),
                        canonical_arg(m.get(*k).unwrap_or(&V::Null))
                    )
                })
                .collect();
            format!("{{{}}}", items.join(","))
        }
        other => other.to_string(),
    }
}

fn watch_arg(v: Option<&serde_json::Value>) -> Option<String> {
    v.map(canonical_arg)
}

/// The 64-bit watch key for (definition, extracted values), hex16 on
/// the wire. Field order is significant and preserved (spec §3.2) —
/// the definition id hashes fields AS DECLARED.
#[cfg(test)]
pub(crate) fn watch_key_hex(name: &str, fields: &[String], values: &[String]) -> String {
    let tid = crate::touch_keys::template_id(name, fields);
    crate::touch_keys::key_hex(crate::touch_keys::watch_key(tid, values))
}

/// Watch-journal key ids for one committed JSON record.
pub(crate) fn product_watch_ids(
    defs: &[crate::registry::WatchDefinition],
    record: &serde_json::Value,
) -> Vec<u32> {
    let mut out = Vec::new();
    for def in defs {
        let mut values = Vec::with_capacity(def.fields.len());
        let mut complete = true;
        for ptr in &def.fields {
            match watch_arg(record.pointer(ptr)) {
                Some(v) => values.push(v),
                None => {
                    complete = false;
                    break;
                }
            }
        }
        if !complete {
            continue;
        }
        let tid = crate::touch_keys::template_id(&def.name, &def.fields);
        out.push(crate::touch_keys::key_id_of_u64(
            crate::touch_keys::watch_key(tid, &values),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_budget_is_runtime_local_and_refills_only_with_elapsed_time() {
        let clock = crate::runtime::ManualClock::at(0);
        let mut first = LookupBudget {
            tokens: 1000.0,
            at: clock.monotonic(),
        };
        let mut second = LookupBudget {
            tokens: 1000.0,
            at: clock.monotonic(),
        };
        for _ in 0..1000 {
            assert!(first.take(clock.monotonic()));
        }
        assert!(!first.take(clock.monotonic()));
        assert!(second.take(clock.monotonic()));
        for jump in [86_400_000, -172_800_000] {
            clock.jump_wall(jump);
            assert!(!first.take(clock.monotonic()));
        }
        clock.advance_monotonic(Duration::from_millis(100));
        for _ in 0..50 {
            assert!(first.take(clock.monotonic()));
        }
        assert!(!first.take(clock.monotonic()));
    }
}
