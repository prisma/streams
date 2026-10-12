//! Watch observation: credential proof precedes state diagnostics, and each
//! request occupies one project request slot for its entire wait. A waiter
//! parks only on the instance the ring assigns the stream's route shard;
//! every other instance replays it to that owner.
use crate::auth::RequestPrincipal;
use crate::registry::{Registry, StreamDesc, WatchDefinition};
use crate::runtime::Clock;
use crate::tenant::{ProjectId, TenantStreamRef};
use std::collections::HashMap;
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
    /// The stream's route shard is not served by this instance. Carried
    /// typed so the transport renders it through its one resolve mapping.
    Resolve(crate::shard_directory::ResolveError),
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

/// A token bucket's refill rate and its cap.
#[derive(Clone, Copy)]
struct Rate {
    per_sec: f64,
    burst: f64,
}

/// What the unverified capabilities claiming one project may spend on
/// store lookups (shared-cells M2, phase B step 15), for a project a
/// fresh policy feed places on this cell: a flood naming one project
/// spends only that project's lookups and the cell's.
const PROJECT_LOOKUPS: Rate = Rate {
    per_sec: 20.0,
    burst: 40.0,
};
/// What every unverified capability on this cell together may spend on
/// store lookups, whatever project it claims.
const CELL_LOOKUPS: Rate = Rate {
    per_sec: 100.0,
    burst: 200.0,
};
/// A project's lookup budget untouched this long is full again; adding
/// another project's drops it, so the map holds only recent claims.
const IDLE_LOOKUP_BUDGET: Duration = Duration::from_secs(60);

/// The answer to a carrier whose store lookup the budgets refuse: owner
/// question Q3 of phase B (step 15). Today's uniform, untagged 403
/// `watch_unauthorized`, which the SDK does not retry; the recommended
/// retryable 429 with `Retry-After: 1` is an untagged variant of its own
/// that `product.rs` renders. Never tagged with the claimed project: an
/// unverified claim is not an identity.
const LOOKUP_REFUSED: WatchFailure = WatchFailure::Unauthorized(None);

/// A per-runtime pre-verification lookup budget. Its refill uses elapsed time,
/// so clock corrections cannot bypass the cap or poison a later runtime.
struct LookupBudget {
    tokens: f64,
    at: crate::runtime::MonotonicNow,
}
impl LookupBudget {
    fn full(rate: Rate, at: crate::runtime::MonotonicNow) -> Self {
        Self {
            tokens: rate.burst,
            at,
        }
    }

    /// Refill to `now`; whether a whole token is there to spend.
    fn refill(&mut self, rate: Rate, now: crate::runtime::MonotonicNow) -> bool {
        self.tokens =
            (self.tokens + now.since(self.at).as_secs_f64() * rate.per_sec).min(rate.burst);
        self.at = now;
        self.tokens >= 1.0
    }
}

/// The cell's lookup budget and each claimed project's.
struct Budgets {
    cell: LookupBudget,
    projects: HashMap<ProjectId, LookupBudget>,
}

impl Budgets {
    /// One lookup from `project`'s budget and one from the cell's: both or
    /// neither.
    fn take(&mut self, project: Option<&ProjectId>, now: crate::runtime::MonotonicNow) -> bool {
        if !self.cell.refill(CELL_LOOKUPS, now) {
            return false;
        }
        if let Some(project) = project {
            let own = self.project(project, now);
            if !own.refill(PROJECT_LOOKUPS, now) {
                return false;
            }
            own.tokens -= 1.0;
        }
        self.cell.tokens -= 1.0;
        true
    }

    /// `project`'s budget, added full; adding one first drops every budget
    /// untouched for `IDLE_LOOKUP_BUDGET`.
    fn project(
        &mut self,
        project: &ProjectId,
        now: crate::runtime::MonotonicNow,
    ) -> &mut LookupBudget {
        if !self.projects.contains_key(project) {
            self.projects
                .retain(|_, budget| now.since(budget.at) < IDLE_LOOKUP_BUDGET);
        }
        self.projects
            .entry(project.clone())
            .or_insert_with(|| LookupBudget::full(PROJECT_LOOKUPS, now))
    }
}

/// The per-runtime budgets of store lookups made for capabilities that
/// have not verified: only such a lookup spends them, never a check
/// against a descriptor the cell holds.
struct LookupBudgets(Mutex<Budgets>);

impl LookupBudgets {
    fn new(at: crate::runtime::MonotonicNow) -> Self {
        Self(Mutex::new(Budgets {
            cell: LookupBudget::full(CELL_LOOKUPS, at),
            projects: HashMap::new(),
        }))
    }

    /// Spend one lookup of the claimed `project`'s budget (a project a
    /// fresh policy feed places on this cell; `None` outside enforce or
    /// under a stale feed, where only the cell's applies) and one of the
    /// cell's. A poisoned budget may hold a half-counted window, and
    /// recovering it could admit a lookup it should refuse, so it refuses.
    fn take(&self, project: Option<&ProjectId>, now: crate::runtime::MonotonicNow) -> bool {
        self.0
            .lock()
            .is_ok_and(|mut budgets| budgets.take(project, now))
    }
}

pub(crate) struct WatchService {
    registry: Arc<Registry>,
    auth: Arc<crate::auth::AuthService>,
    quotas: crate::quota::QuotaRegistry,
    keys: Arc<crate::history::KeyCache>,
    touch: Arc<crate::touch::TouchRegistry>,
    shards: crate::shard_directory::ShardDirectory,
    clock: Arc<dyn Clock>,
    lookup: LookupBudgets,
}

impl WatchService {
    #[expect(
        clippy::too_many_arguments,
        reason = "WatchService::new; the service takes its registry, shard directory, clock, key and limit collaborators separately as composition resolved them, the directory because a wait must prove route ownership before it reads a process-local journal; a builder would exist for this single call site"
    )]
    pub(crate) fn new(
        registry: Arc<Registry>,
        auth: Arc<crate::auth::AuthService>,
        quotas: crate::quota::QuotaRegistry,
        keys: Arc<crate::history::KeyCache>,
        touch: Arc<crate::touch::TouchRegistry>,
        shards: crate::shard_directory::ShardDirectory,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let lookup = LookupBudgets::new(clock.monotonic());
        Self {
            registry,
            auth,
            quotas,
            keys,
            touch,
            shards,
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
        let journal = self
            .touch
            .journal(descriptor.storage_hash(), descriptor.route_hash());
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

    #[expect(
        clippy::too_many_arguments,
        reason = "WatchService::authenticate; authentication takes the capability's signed fields separately as the wire presents them; a bundle struct would exist for this single call site"
    )]
    pub(crate) async fn authenticate<'a>(
        &self,
        stream: &TenantStreamRef,
        watch: String,
        key_hex: String,
        credentials: WatchCredentials,
        access: WatchAccess<'a>,
    ) -> Result<VerifiedObservation<'a>, WatchFailure> {
        let key_hex = key_hex.trim_end_matches('/').to_ascii_lowercase();
        let descriptor = self
            .carrier_descriptor(stream, &credentials, (&watch, &key_hex))
            .await?;
        let key = self.observation_proof(&descriptor, &credentials, (&watch, &key_hex), &access)?;
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

    /// The live descriptor a request's credentials are proved against.
    /// Without a capability it is the stream's own. A capability names its
    /// project, an unverified claim (shared-cells M2, phase B step 15):
    /// under enforce, a project a fresh policy feed does not place on this
    /// cell is refused before any read. Otherwise the capability must
    /// verify first, against the descriptor the cell holds or, when the
    /// cell holds nothing it can trust as current, against one budgeted
    /// store fetch that fills nothing. Only a carrier that verified reads
    /// through the cache, and the descriptor that read returns is the one
    /// the proof and the wait use, so a stale held entry never admits a
    /// capability the stream's current incarnation refuses.
    async fn carrier_descriptor(
        &self,
        stream: &TenantStreamRef,
        credentials: &WatchCredentials,
        (watch, key_hex): (&str, &str),
    ) -> Result<StreamDesc, WatchFailure> {
        let Some(capability) = credentials.capability.as_deref() else {
            return self.live_descriptor(stream).await;
        };
        let project = crate::crypto::watch_capability_project(capability)
            .ok_or(WatchFailure::Unauthorized(None))?;
        let now = self.clock.now().ms() / 1000;
        let served = match (self.auth.mode == crate::auth::AuthMode::Enforce)
            .then(|| self.auth.status_and_quotas(&project, now))
        {
            Some(Ok(None)) => return Err(WatchFailure::Unauthorized(None)),
            Some(Ok(Some(_))) => Some(&project),
            // A stale feed cannot place the claim, so only the cell's budget
            // applies; a carrier that verifies is then failed closed by the
            // wait with its project's retryable `policy_stale`, as before.
            Some(Err(_)) | None => None,
        };
        // mt-lint: allow(stream-ref-construction): explicit capability project is a lookup hint only; signature/key verification below must grant observation authority.
        let claimed = project.stream_ref(stream.name().as_str());
        let verifies = |descriptor: &StreamDesc| {
            self.capability_verifies(descriptor, capability, watch, key_hex)
        };
        let verified = match self.registry.held_descriptor(&claimed) {
            Some((Some(held), _)) if verifies(&held) => true,
            // A fresh entry is current: what it refuses, a fresh absence
            // included, is refused for free.
            Some((_, true)) => false,
            // An expired entry may be stale in a fleet (a stream recreated
            // elsewhere has a new verifier), and no entry proves nothing:
            // the store decides, under budget, and fills nothing.
            _ => {
                if !self.lookup.take(served, self.clock.monotonic()) {
                    return Err(LOOKUP_REFUSED);
                }
                self.registry
                    .fetch_unpublished(&claimed)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|fetched| verifies(&fetched))
            }
        };
        if !verified {
            return Err(WatchFailure::Unauthorized(None));
        }
        self.live_descriptor(&claimed).await
    }

    /// The stream's descriptor read through the registry's cache, while the
    /// stream is alive; anything else is the uniform refusal.
    async fn live_descriptor(&self, stream: &TenantStreamRef) -> Result<StreamDesc, WatchFailure> {
        self.registry
            .get(stream)
            .await
            .ok()
            .flatten()
            .filter(|descriptor| self.alive(descriptor))
            .ok_or(WatchFailure::Unauthorized(None))
    }

    /// The stream key a request presented, once it proved it may observe
    /// `watch` at `key_hex`: by a capability that verifies against the
    /// descriptor's persisted verifier, or by a matching stream key. A key
    /// proves record access only, so under enforce it observes only beside
    /// the principal the account gate verified, never in place of the
    /// capability a carrier failed to verify (shared-cells M5: a garbage
    /// capability plus a learned key used to observe without a token, past
    /// revocation). A principal observes only its own project's streams
    /// inside its prefix grant.
    fn observation_proof(
        &self,
        descriptor: &StreamDesc,
        credentials: &WatchCredentials,
        (watch, key_hex): (&str, &str),
        access: &WatchAccess<'_>,
    ) -> Result<Option<crate::crypto::StreamKey>, WatchFailure> {
        let epoch = descriptor.epoch();
        let key = credentials
            .encryption_key
            .as_deref()
            .and_then(|raw| crate::crypto::StreamKey::from_b64(raw).ok())
            .filter(|key| key.fingerprint(&epoch) == descriptor.key_fingerprint);
        let capability_ok = credentials.capability.as_deref().is_some_and(|capability| {
            self.capability_verifies(descriptor, capability, watch, key_hex)
        });
        let key_admits = key.is_some()
            && (self.auth.mode != crate::auth::AuthMode::Enforce
                || matches!(access, WatchAccess::AdmittedAccount(_)));
        if !capability_ok && !key_admits {
            return Err(WatchFailure::Unauthorized(None));
        }
        if let WatchAccess::AdmittedAccount(principal) = access
            && (principal.project_id != descriptor.project_id
                || principal.require_stream(&descriptor.name).is_err())
        {
            return Err(WatchFailure::Unauthorized(None));
        }
        Ok(key)
    }

    /// Whether `capability` verifies against the descriptor's persisted
    /// watch verifier for exactly this watch, key and method, now.
    fn capability_verifies(
        &self,
        descriptor: &StreamDesc,
        capability: &str,
        watch: &str,
        key_hex: &str,
    ) -> bool {
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
            watch,
            key_hex,
            "GET",
            self.clock.now().ms() / 1000,
        )
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
        // Touches are published only by the instance that commits the
        // append, so a waiter may park only where the ring places the
        // stream's route shard; anywhere else it is replayed to the owner.
        // Full resolution keeps the engine resident, so its close retires
        // this journal and wakes the waiter the moment the ring moves.
        let stream_route = descriptor.route_hash();
        self.shards
            .resolve(&stream_route.0, crate::shard_directory::Adoption::External)
            .await
            .map_err(WatchFailure::Resolve)?;
        if let Some(key) = verified.key {
            self.keys
                .put(descriptor.storage_hash(), key, descriptor.epoch());
        }
        let journal = self.touch.journal(descriptor.storage_hash(), stream_route);
        // The wait holds its project's live slot above; on the instance it
        // is parked, not a request in flight (shared cells M1).
        let _parked = crate::admission::park::park_instance();
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

/// Canonical watch-key value encoding (spec Stage 2 §3.3): JSON
/// serialization, so "1" (string), 1 (number), true, null, arrays and
/// objects are all distinct. A missing pointer produces NO key for the
/// definition.
///
/// This encoding is NORMATIVE and cross-language — the SDK derives the
/// same watch key offline (see `sdk/src/index.ts`), so the two must
/// agree byte for byte. Two places where a naive `to_string()` would
/// not: object keys are sorted (serde's map already is, JavaScript's
/// is not), and a number is written as JavaScript's `String(v)` writes
/// the f64 `JSON.parse` gives it (`sdk_number`).
pub(crate) fn canonical_arg(v: &serde_json::Value) -> String {
    use serde_json::Value as V;
    match v {
        V::Number(n) => n.as_f64().map_or_else(|| n.to_string(), sdk_number),
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

/// JavaScript's `String(v)` for a finite number (ECMA-262
/// `Number::toString`), which is how the SDK keys a watch value. Rust's
/// `{:e}` yields the same shortest round-trip digits JavaScript chooses;
/// only the layout differs: whole numbers below 1e21 in full, decimals from
/// 1e-6, and an exponent with an explicit sign otherwise. Every whole number
/// keeps its own digits, 2^63 and beyond included.
fn sdk_number(f: f64) -> String {
    if f == 0.0 {
        return "0".into(); // -0 too
    }
    let scientific = format!("{:e}", f.abs());
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    // ECMA-262's n: the decimal point sits after this many digits.
    let point = exponent.parse::<i32>().unwrap_or(0) + 1;
    let sign = if f < 0.0 { "-" } else { "" };
    let text = match usize::try_from(point) {
        Ok(n) if (digits.len()..=21).contains(&n) => {
            format!("{digits}{}", "0".repeat(n - digits.len()))
        }
        Ok(n @ 1..=21) => {
            let (whole, fraction) = digits.split_at(n);
            format!("{whole}.{fraction}")
        }
        _ if (-5..=0).contains(&point) => {
            format!(
                "0.{}{digits}",
                "0".repeat(usize::try_from(-point).unwrap_or(0))
            )
        }
        _ => {
            let (first, rest) = digits.split_at(1);
            let dot = if rest.is_empty() { "" } else { "." };
            format!("{first}{dot}{rest}e{:+}", point - 1)
        }
    };
    format!("{sign}{text}")
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
        let first = LookupBudgets::new(clock.monotonic());
        let second = LookupBudgets::new(clock.monotonic());
        for _ in 0..200 {
            assert!(first.take(None, clock.monotonic()));
        }
        assert!(!first.take(None, clock.monotonic()));
        assert!(second.take(None, clock.monotonic()));
        for jump in [86_400_000, -172_800_000] {
            clock.jump_wall(jump);
            assert!(!first.take(None, clock.monotonic()));
        }
        clock.advance_monotonic(Duration::from_millis(100));
        for _ in 0..10 {
            assert!(first.take(None, clock.monotonic()));
        }
        assert!(!first.take(None, clock.monotonic()));
    }

    /// A claimed project spends its own budget (20 a second, 40 at most)
    /// and the cell's (100 a second, 200 at most), both or neither: a
    /// lookup its project refuses costs the cell nothing, one the cell
    /// refuses costs its project nothing, and a project's budget untouched
    /// for a minute is dropped when another project's is added.
    #[test]
    fn a_claimed_project_spends_its_own_lookups_and_the_cells() {
        let clock = crate::runtime::ManualClock::at(0);
        let budgets = LookupBudgets::new(clock.monotonic());
        let project = |i: usize| ProjectId::new(&format!("prj-{i}")).unwrap();
        let spend = |i: usize| {
            std::iter::repeat_with(|| budgets.take(Some(&project(i)), clock.monotonic()))
                .take_while(|taken| *taken)
                .count()
        };
        let held = || {
            let budgets = budgets.0.lock().unwrap();
            let mut projects: Vec<String> =
                budgets.projects.keys().map(ToString::to_string).collect();
            projects.sort();
            (budgets.cell.tokens, projects)
        };
        assert_eq!((spend(0), spend(0)), (40, 0), "one project's burst");
        assert_eq!(held().0, 160.0, "the project's refusals cost the cell");
        let shares: Vec<usize> = (1..5).map(spend).collect();
        assert_eq!(shares, [40; 4], "the other projects keep theirs");
        assert_eq!(spend(5), 0, "the cell's burst is spent");
        assert_eq!(held().1.len(), 5, "the cell's refusal added a project");
        clock.advance_monotonic(Duration::from_secs(1));
        assert_eq!(spend(0), 20, "a project refills at 20 a second");
        clock.advance_monotonic(Duration::from_secs(59));
        assert_eq!(spend(6), 40);
        assert_eq!(
            held().1,
            ["prj-0", "prj-6"],
            "budgets untouched for a minute are dropped"
        );
    }

    /// The SDK keys a watch value with JavaScript's `String(v)` of the number
    /// `JSON.parse` gives it, so the server must render the correctly rounded
    /// f64 of every stored literal exactly that way: whole numbers of any size
    /// (2^63 and beyond saturated to `i64::MAX` here once), exponent forms from
    /// 1e21 and below 1e-6, and values a non-correctly-rounded parse moves.
    #[test]
    fn watch_keys_render_every_number_as_the_sdk_string_of_its_value() {
        let sdk = [
            ("0", "0"),
            ("-0", "0"),
            ("-0.0", "0"),
            ("1.0", "1"),
            ("1.50", "1.5"),
            ("1e2", "100"),
            ("-12", "-12"),
            ("0.1", "0.1"),
            ("0.000001", "0.000001"),
            ("1e-6", "0.000001"),
            ("1.5e-7", "1.5e-7"),
            ("1e-7", "1e-7"),
            ("123e-20", "1.23e-18"),
            ("1.7802719962921167e-19", "1.7802719962921167e-19"),
            ("-1.7802719962921167e-19", "-1.7802719962921167e-19"),
            ("9.999999999999999e-20", "9.999999999999999e-20"),
            ("0.8337463852537358", "0.8337463852537358"),
            ("9007199254740993", "9007199254740992"),
            ("9223372036854775807", "9223372036854776000"),
            ("9223372036854775808", "9223372036854776000"),
            ("9.3e18", "9300000000000000000"),
            ("-9.3e18", "-9300000000000000000"),
            ("18446744073709551615", "18446744073709552000"),
            ("1e20", "100000000000000000000"),
            ("123456789012345678901", "123456789012345680000"),
            ("1e21", "1e+21"),
            ("-1e21", "-1e+21"),
            ("1.5e300", "1.5e+300"),
            ("1.7976931348623157e308", "1.7976931348623157e+308"),
            ("5e-324", "5e-324"),
        ];
        let render = |literal: &str| canonical_arg(&serde_json::from_str(literal).unwrap());
        let wrong: Vec<String> = sdk
            .iter()
            .filter(|(literal, want)| render(literal) != *want)
            .map(|(literal, want)| format!("{literal}: {} is not {want}", render(literal)))
            .collect();
        assert!(wrong.is_empty(), "{wrong:#?}");
        // Math.random()-style values: in [1e-6, 1e21) JavaScript's String(v)
        // and Rust's shortest Display agree, so Display is the SDK oracle.
        let mut state = 0x5eed_u64;
        let mut drifted = Vec::new();
        for _ in 0..100_000 {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            let random = f64::from_bits(0x3ff0_0000_0000_0000 | ((z ^ (z >> 31)) >> 12)) - 1.0;
            if random < 1e-6 {
                continue;
            }
            let literal = random.to_string();
            if render(&literal) != literal {
                drifted.push(literal);
            }
        }
        assert!(
            drifted.is_empty(),
            "{} drifted, first {:?}",
            drifted.len(),
            drifted.first()
        );
    }
}
