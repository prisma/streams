//! Fleet coordination (COMPUTE-SPEC §2/§4, pilot-scaled).
//!
//! Every instance heartbeats `fleet/<instance>.json` every 2 s. One process
//! holds a renewable CAS lease, fans in the bounded heartbeat/router sets,
//! conditionally publishes an epoch-fenced `fleet.json`, and is the only
//! writer of `fleet/desired.json`. Other servers and routers consume the one
//! aggregate, keeping cell coordination O(N) instead of O(N²).
//!
//! Sleep interaction: a scale-to-zero'd instance stops heartbeating and ages
//! out of the live set within 10 s — exactly the semantics the ring wants,
//! since a sleeping instance serves nothing. The router waking instance N+1
//! re-adds it to the live set on its next heartbeat.

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use crate::http::AppState;
use crate::shard::now_ms;

const AGGREGATION_VERSION: u32 = 1;
const AGGREGATOR_LEASE_MS: i64 = 6_000;
const HEARTBEAT_FRESH_MS: i64 = 10_000;
const SNAPSHOT_FRESH_MS: i64 = 10_000;
const MAX_FLEET_INSTANCES: usize = 64;
const MAX_SHARDS_PER_HEARTBEAT: usize = 1_536;
const MAX_ROUTER_REPORTS: usize = 32;
pub const RING_PROTOCOL_VERSION: u32 = 1;
const LEGACY_LIVE_WRITER: u32 = 2;
const LEGACY_HISTORY_WRITER: u32 = 1;
const LEGACY_BACKUP_WRITER: u32 = 2;

static LIVE_HEARTBEATS: OnceLock<RwLock<Vec<Heartbeat>>> = OnceLock::new();

fn replace_live_heartbeats(heartbeats: Vec<Heartbeat>) {
    *LIVE_HEARTBEATS
        .get_or_init(|| RwLock::new(Vec::new()))
        .write()
        .unwrap() = heartbeats;
}

/// Last complete, fresh heartbeat fan-in observed by this process. The merge
/// actor consumes this in-memory snapshot rather than issuing a second N-way
/// object-store scan every two seconds.
pub fn live_heartbeats() -> Vec<Heartbeat> {
    LIVE_HEARTBEATS
        .get_or_init(|| RwLock::new(Vec::new()))
        .read()
        .unwrap()
        .clone()
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ShardActivity {
    pub shard: String,
    /// SlateDB manifest writer epoch. Changes on every engine reopen/fence.
    pub writer_epoch: u64,
    /// Payload bytes committed since this writer epoch opened.
    pub appended_bytes: u64,
}

/// Bounded release/storage compatibility declaration carried by every
/// heartbeat and preserved in the aggregate. An old instance or old
/// aggregator strips this field, which new canary tooling observes as the
/// all-zero legacy value instead of guessing compatibility.
#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct FleetCapabilities {
    pub version: u32,
    pub release_id: String,
    pub ring_protocol: u32,
    pub live_reader_min: u32,
    pub live_reader_max: u32,
    pub live_writer: u32,
    pub history_reader_min: u32,
    pub history_reader_max: u32,
    pub history_writer: u32,
    pub backup_reader_min: u32,
    pub backup_reader_max: u32,
    pub backup_writer: u32,
    pub backup_coordination_protocol: u32,
}

impl FleetCapabilities {
    pub fn current(history_writer: u8, backup_writer: u32) -> Result<Self, String> {
        let release_id = option_env!("STREAMS_RELEASE_ID")
            .unwrap_or(env!("CARGO_PKG_VERSION"))
            .to_string();
        let capabilities = Self {
            version: 1,
            release_id,
            ring_protocol: RING_PROTOCOL_VERSION,
            live_reader_min: 2,
            live_reader_max: 2,
            live_writer: 2,
            history_reader_min: 1,
            history_reader_max: 2,
            history_writer: u32::from(history_writer),
            backup_reader_min: 1,
            backup_reader_max: 3,
            backup_writer,
            backup_coordination_protocol: 2,
        };
        if capabilities.is_valid() && capabilities.version == 1 {
            Ok(capabilities)
        } else {
            Err("compiled release ID or configured storage capabilities are invalid".to_string())
        }
    }

    fn is_legacy_unknown(&self) -> bool {
        *self == Self::default()
    }

    fn is_valid(&self) -> bool {
        if self.is_legacy_unknown() {
            return true;
        }
        let valid_release = !self.release_id.is_empty()
            && self.release_id.len() <= 128
            && self.release_id.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'+')
            });
        let valid_range = |minimum: u32, maximum: u32, writer: u32| {
            minimum > 0
                && minimum <= maximum
                && maximum <= 1_000_000
                && (minimum..=maximum).contains(&writer)
        };
        self.version == 1
            && valid_release
            && (1..=1_000_000).contains(&self.ring_protocol)
            && valid_range(self.live_reader_min, self.live_reader_max, self.live_writer)
            && valid_range(
                self.history_reader_min,
                self.history_reader_max,
                self.history_writer,
            )
            && valid_range(
                self.backup_reader_min,
                self.backup_reader_max,
                self.backup_writer,
            )
            && (1..=1_000_000).contains(&self.backup_coordination_protocol)
    }
}

fn reader_covers(minimum: u32, maximum: u32, writer: u32) -> bool {
    (minimum..=maximum).contains(&writer)
}

/// Serving-time backstop for a deployment gate mistake. Legacy/unknown
/// members may coexist only during the old-writer adoption wave. Once any
/// configured writer flips, every member must explicitly declare compatible
/// ranges. Known members are checked in both directions because either one may
/// own a shard or become backup coordinator during the wave.
fn fleet_capabilities_are_compatible(
    local: &FleetCapabilities,
    heartbeats: &[Heartbeat],
) -> Result<(), String> {
    debug_assert!(local.is_valid() && !local.is_legacy_unknown());
    for heartbeat in heartbeats {
        let remote = &heartbeat.capabilities;
        if remote.is_legacy_unknown() {
            if local.live_writer != LEGACY_LIVE_WRITER
                || local.history_writer != LEGACY_HISTORY_WRITER
                || local.backup_writer != LEGACY_BACKUP_WRITER
            {
                return Err(format!(
                    "legacy capability member {} remains after a writer flip",
                    heartbeat.instance
                ));
            }
            continue;
        }
        if remote.ring_protocol != local.ring_protocol {
            return Err(format!(
                "ring protocol mismatch with {}",
                heartbeat.instance
            ));
        }
        if remote.backup_coordination_protocol != local.backup_coordination_protocol {
            return Err(format!(
                "backup coordination protocol mismatch with {}",
                heartbeat.instance
            ));
        }
        let compatible = reader_covers(
            local.live_reader_min,
            local.live_reader_max,
            remote.live_writer,
        ) && reader_covers(
            remote.live_reader_min,
            remote.live_reader_max,
            local.live_writer,
        ) && reader_covers(
            local.history_reader_min,
            local.history_reader_max,
            remote.history_writer,
        ) && reader_covers(
            remote.history_reader_min,
            remote.history_reader_max,
            local.history_writer,
        ) && reader_covers(
            local.backup_reader_min,
            local.backup_reader_max,
            remote.backup_writer,
        ) && reader_covers(
            remote.backup_reader_min,
            remote.backup_reader_max,
            local.backup_writer,
        );
        if !compatible {
            return Err(format!(
                "storage format compatibility mismatch with {}",
                heartbeat.instance
            ));
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Heartbeat {
    pub instance: String,
    pub ts_ms: i64,
    /// Online cross-cell move safety protocol. Old binaries deserialize this
    /// as absent and republish no capability, so the mover can reject a mixed
    /// fleet before changing a descriptor or copying data.
    #[serde(default)]
    pub cell_move_protocol: u32,
    #[serde(default)]
    pub capabilities: FleetCapabilities,
    pub rps: f64,
    /// p50 of commit durable-wait over the last 15 s across owned shards
    /// (ms). The latency dimension of the load vector (§4.2): rps alone
    /// deadlocks — a congested instance caps its own throughput signal.
    #[serde(default)]
    pub ack_p50_ms: f64,
    /// Measured process CPU (user+sys) over the last heartbeat interval,
    /// as % of one core. THE primary scaling signal: assumed-capacity
    /// constants go stale every time the engine changes speed (run 5
    /// scaled out at ~5 % utilization because SCALE_RPS_CAPACITY still
    /// described the pre-pacing engine). Utilization is workload- and
    /// version-independent.
    #[serde(default)]
    pub cpu_pct: f64,
    /// In-flight HTTP requests at heartbeat time / windowed peak since the
    /// last beat. Measures ADMITTED CONCURRENCY — the per-instance
    /// resource the platform edge actually bounds (runs 6–8: the fleet
    /// ceiling at 16–25 % CPU).
    #[serde(default)]
    pub inflight: i64,
    #[serde(default)]
    pub inflight_peak: i64,
    /// Resident set size (MB). The 1 GB boxes die at ~RSS cap (the
    /// block-cache epidemic); scaling and alarms need to see it.
    #[serde(default)]
    pub rss_mb: f64,
    /// O14a: WAL PUT latency at the object_store client over the last 15 s
    /// (ms) — the durable-commit path's raw store cost, same window as
    /// ack_p50_ms so excursions correlate sample-for-sample.
    #[serde(default)]
    pub wal_put_p50_ms: u64,
    #[serde(default)]
    pub wal_put_p99_ms: u64,
    /// Outbound object-store ops in flight now / peak. The platform egress
    /// budget (~50 concurrent per instance) gates these; a pinned peak
    /// during an ack excursion is the egress-exhaustion signature.
    #[serde(default)]
    pub out_inflight: i64,
    #[serde(default)]
    pub out_inflight_peak: i64,
    pub owned_shards: Vec<String>,
    /// One entry for every topology shard assigned to this instance, whether
    /// or not its engine is currently open. Automatic cold-merge evaluation
    /// accepts an entry only from the shard's current ring owner.
    #[serde(default)]
    pub shard_activity: Vec<ShardActivity>,
    pub draining: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct AggregatorLease {
    version: u32,
    owner: String,
    token: String,
    epoch: u64,
    lease_until_ms: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct FleetSnapshot {
    version: u32,
    lease_epoch: u64,
    sequence: u64,
    generated_at_ms: i64,
    heartbeats: Vec<Heartbeat>,
    edge_p50_ms: f64,
}

pub(crate) fn valid_instance_name(instance: &str) -> bool {
    !instance.is_empty()
        && instance.len() <= 128
        && instance
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

pub(crate) fn fleet_ordinal(instance: &str) -> Option<u64> {
    let ordinal = instance.strip_prefix("streams-")?.parse::<u64>().ok()?;
    (ordinal > 0).then_some(ordinal)
}

fn valid_prefix(prefix: &str) -> bool {
    prefix.len() <= 128 && prefix.bytes().all(|byte| byte == b'0' || byte == b'1')
}

fn heartbeat_shape_is_valid(heartbeat: &Heartbeat, expected_instance: &str) -> bool {
    if heartbeat.instance != expected_instance
        || !valid_instance_name(&heartbeat.instance)
        || heartbeat.cell_move_protocol > 1_000_000
        || !heartbeat.capabilities.is_valid()
        || heartbeat.owned_shards.len() > MAX_SHARDS_PER_HEARTBEAT
        || heartbeat.shard_activity.len() > MAX_SHARDS_PER_HEARTBEAT
        || !heartbeat.rps.is_finite()
        || !(0.0..=1_000_000_000.0).contains(&heartbeat.rps)
        || !heartbeat.ack_p50_ms.is_finite()
        || !(0.0..=3_600_000.0).contains(&heartbeat.ack_p50_ms)
        || !heartbeat.cpu_pct.is_finite()
        || !(0.0..=10_000.0).contains(&heartbeat.cpu_pct)
        || !heartbeat.rss_mb.is_finite()
        || !(0.0..=1_000_000_000.0).contains(&heartbeat.rss_mb)
        || !(0..=1_000_000_000).contains(&heartbeat.inflight)
        || !(0..=1_000_000_000).contains(&heartbeat.inflight_peak)
        || !(0..=1_000_000_000).contains(&heartbeat.out_inflight)
        || !(0..=1_000_000_000).contains(&heartbeat.out_inflight_peak)
        || heartbeat.ts_ms <= 0
    {
        return false;
    }
    let mut owned = HashSet::with_capacity(heartbeat.owned_shards.len());
    if heartbeat
        .owned_shards
        .iter()
        .any(|shard| !valid_prefix(shard) || !owned.insert(shard.as_str()))
    {
        return false;
    }
    let mut activity = HashSet::with_capacity(heartbeat.shard_activity.len());
    !heartbeat
        .shard_activity
        .iter()
        .any(|sample| !valid_prefix(&sample.shard) || !activity.insert(sample.shard.as_str()))
}

fn heartbeat_is_valid(heartbeat: &Heartbeat, expected_instance: &str, now: i64) -> bool {
    heartbeat_shape_is_valid(heartbeat, expected_instance)
        && now
            .checked_sub(heartbeat.ts_ms)
            .is_some_and(|age| (0..HEARTBEAT_FRESH_MS).contains(&age))
}

fn lease_is_valid(lease: &AggregatorLease) -> bool {
    lease.version == AGGREGATION_VERSION
        && valid_instance_name(&lease.owner)
        && lease.token.len() == 32
        && lease.token.bytes().all(|byte| byte.is_ascii_hexdigit())
        && lease.epoch > 0
        && lease.lease_until_ms > 0
}

fn snapshot_is_valid(snapshot: &FleetSnapshot, now: i64) -> bool {
    if snapshot.version != AGGREGATION_VERSION
        || snapshot.lease_epoch == 0
        || snapshot.sequence == 0
        || snapshot.heartbeats.is_empty()
        || snapshot.heartbeats.len() > MAX_FLEET_INSTANCES
        || !snapshot.edge_p50_ms.is_finite()
        || !(0.0..=3_600_000.0).contains(&snapshot.edge_p50_ms)
        || !now
            .checked_sub(snapshot.generated_at_ms)
            .is_some_and(|age| (0..SNAPSHOT_FRESH_MS).contains(&age))
    {
        return false;
    }
    let mut instances = HashSet::with_capacity(snapshot.heartbeats.len());
    snapshot.heartbeats.iter().all(|heartbeat| {
        instances.insert(heartbeat.instance.as_str())
            && heartbeat_shape_is_valid(heartbeat, &heartbeat.instance)
    })
}

fn desired_is_valid(desired: &Desired, fleet_max: u64, now: i64) -> bool {
    desired.count > 0
        && desired.count <= fleet_max
        && desired.reason.len() <= 4_096
        && !desired.reason.bytes().any(|byte| byte == 0)
        && desired.epoch > 0
        && desired.computed_at_ms > 0
        && desired.computed_at_ms <= now.saturating_add(60_000)
}

fn aggregator_token() -> String {
    let mut value = [0u8; 16];
    use rand::RngCore;
    rand::rng().fill_bytes(&mut value);
    crate::crypto::hex(&value)
}

async fn claim_aggregator(
    store: &Arc<dyn ObjectStore>,
    owner: &str,
    token: &str,
) -> Result<Option<AggregatorLease>, String> {
    let path = ObjPath::from("fleet/aggregate-lease.json");
    for _ in 0..5 {
        let now = now_ms();
        match store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let raw = result.bytes().await.map_err(|error| error.to_string())?;
                let current: AggregatorLease = serde_json::from_slice(&raw)
                    .map_err(|_| "malformed fleet aggregator lease".to_string())?;
                if !lease_is_valid(&current) {
                    return Err("malformed fleet aggregator lease".to_string());
                }
                if current.lease_until_ms > now.saturating_add(60_000)
                    || (current.token == token && current.owner != owner)
                {
                    return Err("malformed fleet aggregator lease".to_string());
                }
                if current.token != token && current.lease_until_ms > now {
                    return Ok(None);
                }
                let next = AggregatorLease {
                    version: AGGREGATION_VERSION,
                    owner: owner.to_string(),
                    token: token.to_string(),
                    epoch: if current.token == token {
                        current.epoch
                    } else {
                        current
                            .epoch
                            .checked_add(1)
                            .ok_or_else(|| "fleet aggregator epoch exhausted".to_string())?
                    },
                    lease_until_ms: now.saturating_add(AGGREGATOR_LEASE_MS),
                };
                match store
                    .put_opts(
                        &path,
                        PutPayload::from(
                            serde_json::to_vec(&next).expect("fleet aggregator lease json"),
                        ),
                        PutOptions::from(PutMode::Update(version)),
                    )
                    .await
                {
                    Ok(_) => return Ok(Some(next)),
                    Err(object_store::Error::Precondition { .. }) => continue,
                    Err(error) => return Err(error.to_string()),
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                let lease = AggregatorLease {
                    version: AGGREGATION_VERSION,
                    owner: owner.to_string(),
                    token: token.to_string(),
                    epoch: 1,
                    lease_until_ms: now.saturating_add(AGGREGATOR_LEASE_MS),
                };
                match store
                    .put_opts(
                        &path,
                        PutPayload::from(
                            serde_json::to_vec(&lease).expect("fleet aggregator lease json"),
                        ),
                        PutOptions::from(PutMode::Create),
                    )
                    .await
                {
                    Ok(_) => return Ok(Some(lease)),
                    Err(object_store::Error::AlreadyExists { .. }) => continue,
                    Err(error) => return Err(error.to_string()),
                }
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("fleet aggregator lease CAS retries exhausted".to_string())
}

async fn verify_aggregator(
    store: &Arc<dyn ObjectStore>,
    expected: &AggregatorLease,
) -> Result<(), String> {
    let result = store
        .get(&ObjPath::from("fleet/aggregate-lease.json"))
        .await
        .map_err(|error| error.to_string())?;
    let raw = result.bytes().await.map_err(|error| error.to_string())?;
    let current: AggregatorLease =
        serde_json::from_slice(&raw).map_err(|_| "malformed fleet aggregator lease".to_string())?;
    if !lease_is_valid(&current)
        || current.owner != expected.owner
        || current.token != expected.token
        || current.epoch != expected.epoch
        || current.lease_until_ms <= now_ms()
        || current.lease_until_ms > now_ms().saturating_add(60_000)
    {
        return Err("fleet aggregator lease was lost".to_string());
    }
    Ok(())
}

async fn collect_heartbeats(
    store: &Arc<dyn ObjectStore>,
    owner: &str,
    fleet_max: u64,
) -> Result<Vec<Heartbeat>, String> {
    let mut candidates: Vec<String> = (1..=fleet_max)
        .map(|ordinal| format!("streams-{ordinal}"))
        .collect();
    if !candidates.iter().any(|candidate| candidate == owner) {
        candidates.push(owner.to_string());
    }
    candidates.sort();
    candidates.dedup();
    if candidates.len() > MAX_FLEET_INSTANCES {
        return Err("fleet heartbeat candidate count exceeds cell bound".to_string());
    }
    let now = now_ms();
    let mut heartbeats = Vec::new();
    for instance in candidates {
        let path = ObjPath::from(format!("fleet/{instance}.json"));
        match store.get(&path).await {
            Ok(result) => {
                let raw = result.bytes().await.map_err(|error| error.to_string())?;
                let heartbeat: Heartbeat = serde_json::from_slice(&raw)
                    .map_err(|_| format!("malformed heartbeat for {instance}"))?;
                if !heartbeat_shape_is_valid(&heartbeat, &instance) {
                    return Err(format!("malformed heartbeat for {instance}"));
                }
                if heartbeat_is_valid(&heartbeat, &instance, now) && !heartbeat.draining {
                    heartbeats.push(heartbeat);
                }
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(heartbeats)
}

async fn collect_edge_p50(store: &Arc<dyn ObjectStore>) -> Result<f64, String> {
    let now = now_ms();
    let mut edge_p50 = 0.0f64;
    for ordinal in 1..=MAX_ROUTER_REPORTS {
        let router = format!("router-{ordinal}");
        let path = ObjPath::from(format!("routers/{router}.json"));
        let result = match store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(error) => return Err(error.to_string()),
        };
        let raw = result.bytes().await.map_err(|error| error.to_string())?;
        let value: serde_json::Value =
            serde_json::from_slice(&raw).map_err(|_| format!("malformed router report {path}"))?;
        if value["router"].as_str() != Some(&router) {
            return Err(format!("malformed router report {path}"));
        }
        let ts_ms = value["ts_ms"]
            .as_i64()
            .ok_or_else(|| format!("malformed router report {path}"))?;
        let client_p50_ms = value["client_p50_ms"]
            .as_f64()
            .filter(|value| value.is_finite() && (0.0..=3_600_000.0).contains(value))
            .ok_or_else(|| format!("malformed router report {path}"))?;
        if now
            .checked_sub(ts_ms)
            .is_some_and(|age| (0..HEARTBEAT_FRESH_MS).contains(&age))
        {
            edge_p50 = edge_p50.max(client_p50_ms);
        }
    }
    Ok(edge_p50)
}

async fn publish_snapshot(
    store: &Arc<dyn ObjectStore>,
    lease: &AggregatorLease,
    snapshot: &FleetSnapshot,
) -> Result<(), String> {
    if !snapshot_is_valid(snapshot, now_ms())
        || snapshot.lease_epoch != lease.epoch
        || snapshot.generated_at_ms <= 0
    {
        return Err("refusing malformed fleet snapshot".to_string());
    }
    let path = ObjPath::from("fleet.json");
    for _ in 0..5 {
        verify_aggregator(store, lease).await?;
        let mode = match store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let raw = result.bytes().await.map_err(|error| error.to_string())?;
                let current: FleetSnapshot = serde_json::from_slice(&raw)
                    .map_err(|_| "malformed fleet snapshot".to_string())?;
                if current.version != AGGREGATION_VERSION
                    || current.lease_epoch > snapshot.lease_epoch
                    || (current.lease_epoch == snapshot.lease_epoch
                        && current.sequence > snapshot.sequence)
                {
                    return Err("fleet snapshot publication was fenced".to_string());
                }
                if current.lease_epoch == snapshot.lease_epoch
                    && current.sequence == snapshot.sequence
                {
                    return if current == *snapshot {
                        Ok(())
                    } else {
                        Err("conflicting fleet snapshot sequence".to_string())
                    };
                }
                PutMode::Update(version)
            }
            Err(object_store::Error::NotFound { .. }) => PutMode::Create,
            Err(error) => return Err(error.to_string()),
        };
        match store
            .put_opts(
                &path,
                PutPayload::from(serde_json::to_vec(snapshot).expect("fleet snapshot json")),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("fleet snapshot CAS retries exhausted".to_string())
}

async fn load_snapshot(store: &Arc<dyn ObjectStore>) -> Result<FleetSnapshot, String> {
    let result = store
        .get(&ObjPath::from("fleet.json"))
        .await
        .map_err(|error| error.to_string())?;
    let raw = result.bytes().await.map_err(|error| error.to_string())?;
    let mut snapshot: FleetSnapshot =
        serde_json::from_slice(&raw).map_err(|_| "malformed fleet snapshot".to_string())?;
    let now = now_ms();
    if !snapshot_is_valid(&snapshot, now) {
        return Err("malformed or stale fleet snapshot".to_string());
    }
    snapshot
        .heartbeats
        .retain(|heartbeat| heartbeat_is_valid(heartbeat, &heartbeat.instance, now));
    if snapshot.heartbeats.is_empty() {
        return Err("fleet snapshot has no fresh heartbeats".to_string());
    }
    Ok(snapshot)
}

/// Current RSS in bytes. Linux (musl cloud build): /proc/self/statm.
/// macOS dev box: getrusage peak RSS as an approximation.
pub fn rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(pages) = statm
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
            {
                return pages * 4096;
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return 0;
        }
        ru.ru_maxrss as u64 // bytes on macOS
    }
}

/// Process CPU time (user+sys) in seconds via getrusage — portable across
/// the macOS dev box and the musl cloud build.
pub fn cpu_time_secs() -> f64 {
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return 0.0;
        }
        ru.ru_utime.tv_sec as f64
            + ru.ru_utime.tv_usec as f64 / 1e6
            + ru.ru_stime.tv_sec as f64
            + ru.ru_stime.tv_usec as f64 / 1e6
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Desired {
    pub count: u64,
    pub reason: String,
    pub epoch: u64,
    pub computed_at_ms: i64,
}

pub struct FleetCfg {
    pub instance: String,
    /// Legacy assumed-capacity dimension (req/s per instance). 0 disables
    /// it — measured CPU replaced it as the primary signal.
    pub capacity_rps: u64,
    /// Per-instance admitted-concurrency capacity (edge slots). Measured
    /// on Prisma Compute via a calibrated-latency ladder: the edge admits
    /// ~48-50 concurrent requests per instance and queues the rest
    /// (EXPERIMENT edge probe, 2026-07-15). Utilization = Σ in-flight /
    /// (slots × live); scale-out at target_util of it — i.e. BEFORE the
    /// edge queue forms. 0 disables.
    pub edge_slots: u64,
    /// Utilization target for scale-out capacity planning: desired ≥
    /// ceil(fleet core-equivalents in use / target_util). 0.75 = "scale
    /// when the fleet is at 75 % of maximum".
    pub target_util: f64,
    /// Projected post-shrink utilization ceiling for scale-in: shrinking
    /// to N-1 is allowed only if used/(N-1) stays under this. Must be
    /// meaningfully below target_util or the fleet flaps at the boundary.
    pub scale_in_util: f64,
    /// Hot-instance scale-out: one instance sustaining ≥ this CPU% asks
    /// for one more instance even while fleet-average utilization is low
    /// (skewed shards). Matches target_util by default (75).
    pub hot_cpu_pct: f64,
    /// How long the hot-instance breach must persist (transition churn
    /// briefly spikes CPU during shard handoffs).
    pub cpu_sustain: Duration,
    pub scale_in: Duration,
    /// Ack-p50 threshold (ms) above which a loaded instance demands
    /// scale-out regardless of the other dimensions.
    pub latency_ms: u64,
    /// Router-observed client-latency threshold (ms). The router
    /// publishes what clients actually experience (routers/*.json);
    /// server-side ack latency cannot see edge queueing. Breach ⇒ +1 and
    /// scale-in is blocked while hot.
    pub edge_latency_ms: u64,
    /// How long the latency breach must persist before it scales the fleet
    /// (transition churn spikes ack latency briefly; undamped, every scale
    /// event triggers the next one).
    pub latency_sustain: Duration,
    pub max: u64,
}

pub fn start(state: Arc<AppState>, store: Arc<dyn ObjectStore>, cfg: FleetCfg) {
    tokio::spawn(async move {
        let mut ewma_rps = 0.0f64;
        let mut last_ops = 0u64;
        let mut last_tick = Instant::now();
        let mut below_since: Option<Instant> = None;
        let mut lat_breach_since: Option<Instant> = None;
        let mut cpu_breach_since: Option<Instant> = None;
        let mut last_cpu = cpu_time_secs();
        let mut ewma_cpu = 0.0f64;
        let aggregator_token = aggregator_token();
        let mut aggregator_epoch = 0u64;
        let mut snapshot_sequence = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let ops = state.fleet_ops.load(Ordering::Relaxed);
            let dt = last_tick.elapsed().as_secs_f64().max(0.001);
            last_tick = Instant::now();
            let inst_rps = (ops - last_ops) as f64 / dt;
            last_ops = ops;
            ewma_rps = if ewma_rps == 0.0 {
                inst_rps
            } else {
                ewma_rps * 0.6 + inst_rps * 0.4
            };
            let cpu_now = cpu_time_secs();
            let inst_cpu = ((cpu_now - last_cpu) / dt * 100.0).max(0.0);
            last_cpu = cpu_now;
            ewma_cpu = if ewma_cpu == 0.0 {
                inst_cpu
            } else {
                ewma_cpu * 0.6 + inst_cpu * 0.4
            };

            // 1. Heartbeat (single writer per object: plain PUT).
            let topology = state.topology.read().unwrap().clone();
            let active = state.ring_active.read().unwrap().clone();
            let (owned, shard_activity, ack_p50_ms) = {
                let shards = state.shards.read().unwrap();
                let owned: Vec<String> = shards.keys().cloned().collect();
                // An empty ring is bootstrap uncertainty, not ownership.
                // Publish no activity claims until desired+liveness produce
                // an assignment; the merge actor fails closed in the gap.
                let shard_activity = topology
                    .shards
                    .iter()
                    .filter(|prefix| {
                        !active.is_empty()
                            && active[crate::http::ring_pick(prefix, &active)] == cfg.instance
                    })
                    .map(|prefix| {
                        let (writer_epoch, appended_bytes) = shards
                            .get(prefix)
                            .map(|engine| {
                                (
                                    engine.writer_epoch(),
                                    engine.stats_appended_bytes.load(Ordering::Relaxed),
                                )
                            })
                            .unwrap_or((0, 0));
                        ShardActivity {
                            shard: prefix.clone(),
                            writer_epoch,
                            appended_bytes,
                        }
                    })
                    .collect();
                let cutoff = now_ms() - 15_000;
                let mut waits: Vec<u32> = Vec::new();
                for eng in shards.values() {
                    waits.extend(
                        eng.timings
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|g| g.ts_ms >= cutoff)
                            .map(|g| g.durable_wait_us),
                    );
                }
                waits.sort_unstable();
                let p50 = waits
                    .get(waits.len() / 2)
                    .map(|us| *us as f64 / 1000.0)
                    .unwrap_or(0.0);
                (owned, shard_activity, (p50 * 10.0).round() / 10.0)
            };
            let inflight_now = state.inflight.load(Ordering::Relaxed);
            let inflight_peak = state.inflight_peak.swap(inflight_now, Ordering::Relaxed);
            let (wal_put_p50_ms, wal_put_p99_ms, out_inflight, out_inflight_peak) =
                crate::store_timing::heartbeat_summary();
            let hb = Heartbeat {
                instance: cfg.instance.clone(),
                ts_ms: now_ms(),
                cell_move_protocol: crate::cell_move_fence::PROTOCOL_VERSION,
                capabilities: state.fleet_capabilities.clone(),
                rps: (ewma_rps * 10.0).round() / 10.0,
                ack_p50_ms,
                cpu_pct: (ewma_cpu * 10.0).round() / 10.0,
                inflight: inflight_now,
                inflight_peak,
                rss_mb: (rss_bytes() as f64 / 1048576.0 * 10.0).round() / 10.0,
                wal_put_p50_ms,
                wal_put_p99_ms,
                out_inflight,
                out_inflight_peak,
                owned_shards: owned,
                shard_activity,
                draining: false,
            };
            let path = ObjPath::from(format!("fleet/{}.json", cfg.instance));
            if let Err(e) = store
                .put(&path, PutPayload::from(serde_json::to_vec(&hb).unwrap()))
                .await
            {
                tracing::warn!("heartbeat put failed: {e}");
                state.fleet_ready.store(false, Ordering::Release);
                continue;
            }

            // 2. Exactly one lease-fenced aggregator fans in N heartbeats and
            // router reports, then CAS-publishes fleet.json. Everyone else
            // consumes that one bounded snapshot: O(N), not O(N²).
            let lease = match claim_aggregator(&store, &cfg.instance, &aggregator_token).await {
                Ok(lease) => lease,
                Err(error) => {
                    tracing::warn!("fleet aggregator lease failed: {error}");
                    replace_live_heartbeats(Vec::new());
                    state.fleet_ready.store(false, Ordering::Release);
                    continue;
                }
            };
            let (live_heartbeats, edge_p50) = if let Some(lease) = lease.as_ref() {
                if aggregator_epoch != lease.epoch {
                    aggregator_epoch = lease.epoch;
                    snapshot_sequence = 0;
                }
                let heartbeats = match collect_heartbeats(&store, &cfg.instance, cfg.max).await {
                    Ok(heartbeats) => heartbeats,
                    Err(error) => {
                        tracing::warn!("fleet heartbeat aggregation failed: {error}");
                        replace_live_heartbeats(Vec::new());
                        state.fleet_ready.store(false, Ordering::Release);
                        continue;
                    }
                };
                let edge_p50 = match collect_edge_p50(&store).await {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::warn!("router report aggregation failed: {error}");
                        replace_live_heartbeats(Vec::new());
                        state.fleet_ready.store(false, Ordering::Release);
                        continue;
                    }
                };
                snapshot_sequence = snapshot_sequence.saturating_add(1).max(1);
                let snapshot = FleetSnapshot {
                    version: AGGREGATION_VERSION,
                    lease_epoch: lease.epoch,
                    sequence: snapshot_sequence,
                    generated_at_ms: now_ms(),
                    heartbeats: heartbeats.clone(),
                    edge_p50_ms: edge_p50,
                };
                if let Err(error) = publish_snapshot(&store, lease, &snapshot).await {
                    tracing::warn!("fleet snapshot publication failed: {error}");
                    replace_live_heartbeats(Vec::new());
                    state.fleet_ready.store(false, Ordering::Release);
                    continue;
                }
                (heartbeats, edge_p50)
            } else {
                match load_snapshot(&store).await {
                    Ok(snapshot) => (snapshot.heartbeats, snapshot.edge_p50_ms),
                    Err(error) => {
                        tracing::warn!("fleet snapshot read failed: {error}");
                        replace_live_heartbeats(Vec::new());
                        state.fleet_ready.store(false, Ordering::Release);
                        continue;
                    }
                }
            };
            replace_live_heartbeats(live_heartbeats.clone());
            if let Err(error) =
                fleet_capabilities_are_compatible(&state.fleet_capabilities, &live_heartbeats)
            {
                tracing::error!("fleet compatibility gate failed: {error}");
                state.fleet_ready.store(false, Ordering::Release);
                continue;
            }

            // 2b. Fleet load derived from the same aggregate every instance
            // uses for placement and automatic merge activity.
            let mut total_rps = 0.0f64;
            let mut total_cores_used = 0.0f64;
            let mut total_inflight = 0.0f64;
            let mut live = 0u64;
            let mut max_loaded_p50 = 0.0f64;
            let mut max_loaded_cpu = 0.0f64;
            let mut hb_age_ms: HashMap<String, i64> = HashMap::new();
            for other in &live_heartbeats {
                hb_age_ms.insert(other.instance.clone(), now_ms() - other.ts_ms);
                live += 1;
                total_rps += other.rps;
                total_cores_used += other.cpu_pct / 100.0;
                total_inflight += other.inflight.max(0) as f64;
                // Load-gated dims count only for instances doing real work
                // so idle cold-start CPU does not scale the cell.
                if other.rps >= 5.0 {
                    if other.ack_p50_ms > max_loaded_p50 {
                        max_loaded_p50 = other.ack_p50_ms;
                    }
                    if other.cpu_pct > max_loaded_cpu {
                        max_loaded_cpu = other.cpu_pct;
                    }
                }
            }

            // 3. Desired count — max over dimensions (§4.1/§4.2):
            //    utilization (primary): ceil(cores-in-use / target_util) —
            //    scale-out begins when the fleet nears 75 % of maximum.
            //    hot instance: one instance sustaining ≥ hot_cpu_pct wants
            //    +1 even when fleet-average is low (shard skew). Damped.
            //    latency: sustained ack-p50 breach wants +1 (congestion
            //    that doesn't show as CPU — e.g. object-store slowness).
            //    rps: legacy assumed-capacity dim, only if capacity_rps>0.
            let need_util = (total_cores_used / cfg.target_util).ceil() as u64;
            let need_rps = if cfg.capacity_rps > 0 {
                (total_rps / (cfg.target_util * cfg.capacity_rps as f64).max(1.0)).ceil() as u64
            } else {
                0
            };
            // Edge-slot dimension: in-flight requests vs the measured
            // per-instance admission budget. This is the resource that
            // actually bound runs 6-8 (servers at 16-25 % CPU): once
            // offered concurrency exceeds slots × live, clients queue at
            // the edge. Scaling at 75 % of the slot budget adds capacity
            // BEFORE the queue forms.
            let need_slots = if cfg.edge_slots > 0 {
                (total_inflight / (cfg.target_util * cfg.edge_slots as f64).max(1.0)).ceil() as u64
            } else {
                0
            };
            let breaching = max_loaded_p50 > cfg.latency_ms as f64;
            let need_latency = if breaching {
                let since = *lat_breach_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= cfg.latency_sustain {
                    live + 1
                } else {
                    0
                }
            } else {
                lat_breach_since = None;
                0
            };
            let edge_hot = edge_p50 > cfg.edge_latency_ms as f64 && total_rps >= 5.0;
            let need_edge = if edge_hot { live + 1 } else { 0 };
            let cpu_hot = max_loaded_cpu >= cfg.hot_cpu_pct;
            let need_hot = if cpu_hot {
                let since = *cpu_breach_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= cfg.cpu_sustain {
                    live + 1
                } else {
                    0
                }
            } else {
                cpu_breach_since = None;
                0
            };
            let need_shards = (topology.shards.len() as u64).div_ceil(32);
            let need = need_util
                .max(need_rps)
                .max(need_slots)
                .max(need_latency)
                .max(need_hot)
                .max(need_edge)
                .max(need_shards)
                .clamp(1, cfg.max);
            // Scale-in target uses the conservative divisor, and edge
            // congestion BLOCKS shrink outright: measured rps falls during
            // client-side queueing, and shrinking on that signal removes
            // capacity mid-collapse (observed in run 7: desired 4→2 while
            // client p50 was 1.6-2 s).
            let need_shrink = if edge_hot {
                cfg.max // sentinel: never below current -> shrink blocked
            } else {
                let slots_shrink = if cfg.edge_slots > 0 {
                    (total_inflight / (cfg.scale_in_util * cfg.edge_slots as f64).max(1.0)).ceil()
                        as u64
                } else {
                    0
                };
                ((total_cores_used / cfg.scale_in_util).ceil() as u64)
                    .max(need_rps)
                    .max(slots_shrink)
                    .max(need_latency)
                    .max(need_hot)
                    .max(need_shards)
                    .clamp(1, cfg.max)
            };

            let dpath = ObjPath::from("fleet/desired.json");
            let (cur, version): (Option<Desired>, Option<UpdateVersion>) =
                match store.get(&dpath).await {
                    Ok(r) => {
                        let v = UpdateVersion {
                            e_tag: r.meta.e_tag.clone(),
                            version: r.meta.version.clone(),
                        };
                        let raw = match r.bytes().await {
                            Ok(raw) => raw,
                            Err(error) => {
                                tracing::warn!("desired.json body failed: {error}");
                                state.fleet_ready.store(false, Ordering::Release);
                                continue;
                            }
                        };
                        let desired: Desired = match serde_json::from_slice(&raw) {
                            Ok(desired) => desired,
                            Err(_) => {
                                tracing::error!("malformed desired.json");
                                state.fleet_ready.store(false, Ordering::Release);
                                continue;
                            }
                        };
                        if !desired_is_valid(&desired, cfg.max, now_ms()) {
                            tracing::error!("malformed desired.json");
                            state.fleet_ready.store(false, Ordering::Release);
                            continue;
                        }
                        (Some(desired), Some(v))
                    }
                    Err(object_store::Error::NotFound { .. }) => (None, None),
                    Err(e) => {
                        tracing::warn!("desired.json get failed: {e}");
                        state.fleet_ready.store(false, Ordering::Release);
                        continue;
                    }
                };
            if cur.is_none() && lease.is_none() {
                state.fleet_ready.store(false, Ordering::Release);
                continue;
            }
            let cur_count = cur.as_ref().map(|d| d.count).unwrap_or(1);
            // Publish the ring's ACTIVE set for the R2 ownership check:
            // the first `desired` ordinal instances, dropping any that have
            // been heartbeat-dark >10 s (the liveness contract in §2; a
            // request would have woken a merely-sleeping instance)
            // a merely-sleeping one). Self is always fresh (just wrote).
            // Falls back to the unfiltered ordinal set if filtering empties
            // it (bootstrap: everyone asleep, first request must land).
            {
                let ordinal: Vec<String> = (1..=cur_count.max(1))
                    .map(|i| format!("streams-{i}"))
                    .collect();
                let mut active: Vec<String> = ordinal
                    .iter()
                    .filter(|n| {
                        **n == cfg.instance
                            || hb_age_ms.get(*n).map(|a| *a < 10_000).unwrap_or(false)
                    })
                    .cloned()
                    .collect();
                if active.is_empty() {
                    active = ordinal;
                }
                *state.ring_active.write().unwrap() = active;
            }

            // Scale-out publishes `need` immediately; scale-in publishes
            // the conservative `need_shrink` and only after the sustain
            // window — and only when even the conservative target is
            // below the current count (no flapping at the 75 % boundary).
            let (publish, publish_count) = if cur.is_none() {
                (true, need) // bootstrap: make the count observable
            } else if need > cur_count {
                below_since = None;
                (true, need)
            } else if need_shrink < cur_count {
                let since = *below_since.get_or_insert_with(Instant::now);
                (since.elapsed() >= cfg.scale_in, need_shrink)
            } else {
                below_since = None;
                (false, need)
            };

            if publish && let Some(aggregate_lease) = lease.as_ref() {
                if let Err(error) = verify_aggregator(&store, aggregate_lease).await {
                    tracing::warn!("desired-count publication fenced: {error}");
                    state.fleet_ready.store(false, Ordering::Release);
                    continue;
                }
                let next_epoch = match cur.as_ref() {
                    Some(desired) => match desired.epoch.checked_add(1) {
                        Some(epoch) => epoch,
                        None => {
                            tracing::error!("desired-count epoch exhausted");
                            state.fleet_ready.store(false, Ordering::Release);
                            continue;
                        }
                    },
                    None => 1,
                };
                let next = Desired {
                    count: publish_count,
                    reason: format!(
                        "cores_used={total_cores_used:.2} util->{need_util} inflight={total_inflight:.0} slots->{need_slots} hot_cpu={max_loaded_cpu:.0}% ({need_hot}) ack_p50={max_loaded_p50:.0}ms ({need_latency}) edge_p50={edge_p50:.0}ms ({need_edge}) rps={total_rps:.0} ({need_rps}) shards={} live={live}",
                        topology.shards.len(),
                    ),
                    epoch: next_epoch,
                    computed_at_ms: now_ms(),
                };
                let mode = match version {
                    Some(v) => PutMode::Update(v),
                    None => PutMode::Create,
                };
                match store
                    .put_opts(
                        &dpath,
                        PutPayload::from(serde_json::to_vec(&next).unwrap()),
                        PutOptions::from(mode),
                    )
                    .await
                {
                    Ok(_) => {
                        tracing::info!(
                            "fleet desired {} -> {} ({})",
                            cur_count,
                            publish_count,
                            next.reason
                        );
                        below_since = None;
                    }
                    // Lost the CAS: another instance published; converge next tick.
                    Err(object_store::Error::Precondition { .. })
                    | Err(object_store::Error::AlreadyExists { .. }) => {}
                    Err(e) => {
                        tracing::warn!("desired.json cas failed: {e}");
                        state.fleet_ready.store(false, Ordering::Release);
                        continue;
                    }
                }
            }
            state.fleet_ready.store(true, Ordering::Release);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn heartbeat(instance: &str) -> Heartbeat {
        Heartbeat {
            instance: instance.to_string(),
            ts_ms: now_ms(),
            cell_move_protocol: crate::cell_move_fence::PROTOCOL_VERSION,
            capabilities: FleetCapabilities::current(2, 3).unwrap(),
            rps: 1.0,
            ack_p50_ms: 2.0,
            cpu_pct: 3.0,
            inflight: 1,
            inflight_peak: 2,
            rss_mb: 4.0,
            wal_put_p50_ms: 5,
            wal_put_p99_ms: 6,
            out_inflight: 1,
            out_inflight_peak: 2,
            owned_shards: vec!["0".to_string()],
            shard_activity: vec![ShardActivity {
                shard: "0".to_string(),
                writer_epoch: 7,
                appended_bytes: 8,
            }],
            draining: false,
        }
    }

    #[test]
    fn heartbeat_and_snapshot_validation_is_bounded_and_strict() {
        let mut valid = heartbeat("streams-1");
        let now = now_ms();
        assert!(heartbeat_is_valid(&valid, "streams-1", now));
        valid.shard_activity.push(valid.shard_activity[0].clone());
        assert!(!heartbeat_is_valid(&valid, "streams-1", now));

        let mut malformed_capabilities = heartbeat("streams-1");
        malformed_capabilities.capabilities.history_reader_min = 0;
        assert!(!heartbeat_is_valid(
            &malformed_capabilities,
            "streams-1",
            now
        ));

        let encoded = serde_json::to_value(heartbeat("streams-1")).unwrap();
        let mut legacy = encoded.as_object().unwrap().clone();
        legacy.remove("capabilities");
        let mut legacy: Heartbeat = serde_json::from_value(legacy.into()).unwrap();
        legacy.ts_ms = now;
        assert_eq!(legacy.capabilities, FleetCapabilities::default());
        assert!(heartbeat_is_valid(&legacy, "streams-1", now));

        let duplicate = heartbeat("streams-1");
        let snapshot = FleetSnapshot {
            version: AGGREGATION_VERSION,
            lease_epoch: 1,
            sequence: 1,
            generated_at_ms: now,
            heartbeats: vec![duplicate.clone(), duplicate],
            edge_p50_ms: 1.0,
        };
        assert!(!snapshot_is_valid(&snapshot, now));
        assert!(!valid_instance_name("../streams-1"));
        assert_eq!(fleet_ordinal("streams-64"), Some(64));
        assert_eq!(fleet_ordinal("streams-0"), None);
        let desired = Desired {
            count: 2,
            reason: "unit".to_string(),
            epoch: 1,
            computed_at_ms: now,
        };
        assert!(desired_is_valid(&desired, 4, now));
        assert!(!desired_is_valid(
            &Desired {
                computed_at_ms: now + 60_001,
                ..desired
            },
            4,
            now
        ));
    }

    #[test]
    fn runtime_compatibility_allows_adoption_and_rejects_unsafe_flip() {
        let read_first = FleetCapabilities::current(1, 2).unwrap();
        let mut legacy = heartbeat("streams-1");
        legacy.capabilities = FleetCapabilities::default();
        assert!(fleet_capabilities_are_compatible(&read_first, &[legacy.clone()]).is_ok());

        let flipped_history = FleetCapabilities::current(2, 2).unwrap();
        assert!(fleet_capabilities_are_compatible(&flipped_history, &[legacy]).is_err());

        let mut compatible = heartbeat("streams-2");
        compatible.capabilities = FleetCapabilities::current(1, 2).unwrap();
        assert!(fleet_capabilities_are_compatible(&flipped_history, &[compatible.clone()]).is_ok());

        compatible.capabilities.history_reader_max = 1;
        assert!(fleet_capabilities_are_compatible(&flipped_history, &[compatible]).is_err());

        let mut ring_skew = heartbeat("streams-3");
        ring_skew.capabilities.ring_protocol += 1;
        assert!(fleet_capabilities_are_compatible(&flipped_history, &[ring_skew]).is_err());
    }

    #[tokio::test]
    async fn newer_aggregator_epoch_fences_delayed_snapshot_writer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = claim_aggregator(&store, "streams-1", "11111111111111111111111111111111")
            .await
            .unwrap()
            .unwrap();
        assert!(
            claim_aggregator(&store, "streams-2", "22222222222222222222222222222222")
                .await
                .unwrap()
                .is_none()
        );

        let path = ObjPath::from("fleet/aggregate-lease.json");
        let result = store.get(&path).await.unwrap();
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let raw = result.bytes().await.unwrap();
        let mut expired: AggregatorLease = serde_json::from_slice(&raw).unwrap();
        expired.lease_until_ms = 1;
        store
            .put_opts(
                &path,
                PutPayload::from(serde_json::to_vec(&expired).unwrap()),
                PutOptions::from(PutMode::Update(version)),
            )
            .await
            .unwrap();

        let second = claim_aggregator(&store, "streams-2", "22222222222222222222222222222222")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.epoch, first.epoch + 1);

        let newer = FleetSnapshot {
            version: AGGREGATION_VERSION,
            lease_epoch: second.epoch,
            sequence: 1,
            generated_at_ms: now_ms(),
            heartbeats: vec![heartbeat("streams-2")],
            edge_p50_ms: 1.0,
        };
        publish_snapshot(&store, &second, &newer).await.unwrap();

        let delayed = FleetSnapshot {
            version: AGGREGATION_VERSION,
            lease_epoch: first.epoch,
            sequence: 99,
            generated_at_ms: now_ms(),
            heartbeats: vec![heartbeat("streams-1")],
            edge_p50_ms: 99.0,
        };
        assert!(publish_snapshot(&store, &first, &delayed).await.is_err());
        let loaded = load_snapshot(&store).await.unwrap();
        assert_eq!(loaded.lease_epoch, second.epoch);
        assert_eq!(loaded.heartbeats[0].instance, "streams-2");

        let mut conflict = newer.clone();
        conflict.edge_p50_ms = 2.0;
        assert!(publish_snapshot(&store, &second, &conflict).await.is_err());
    }
}
