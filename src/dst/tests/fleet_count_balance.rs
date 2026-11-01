//! Count balance (edge change #236): an instance that owns more than the
//! fair share ceil(shards / members) moves one shard to a member below it,
//! through the overrides document a lag move writes, once its own move
//! cooldown (counted from its boot) and every override in the fleet are
//! older than the cooldown, and only after two consecutive passes saw the
//! imbalance. A target's lag must be under half the lag threshold.
//!
//! Every scenario runs `streams-1` over the shards 00, 01, 10 and 11 of a
//! ring whose rendezvous homes are 00 and 01 on `streams-1` and 10 and 11 on
//! `streams-2`, with 10 and 11 overridden to `streams-1`: it owns all four of
//! a share of two. The cooldown is the default 60 s on the runtime's
//! monotonic clock, which only the scenario advances; every wait is bounded.
//! A read that finds no move holds the next for a cooldown, and an instance
//! within the share reads nothing.
use super::super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::super::fixture_runtime::RigRuntime;
use super::super::fixture_storage::mem;
use super::settled;
use crate::shard::now_ms;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const PREFIXES: [&str; 4] = ["00", "01", "10", "11"];
/// The seeded overrides' age: past the 60 s move cooldown, short of
/// return-home's 300 s, so the tick keeps them.
const AGED_MS: i64 = 120_000;
/// Four count-balance passes: twice the passes a move needs.
const QUIET: Duration = Duration::from_secs(8);
/// The bound on the move once nothing holds it.
const MOVE_BUDGET: Duration = Duration::from_secs(20);

async fn put(store: &Arc<dyn ObjectStore>, path: &str, body: Vec<u8>) {
    store
        .put(&Path::from(path), PutPayload::from(body))
        .await
        .unwrap();
}

/// A live, non-draining heartbeat for `instance` with `lag_secs` of absorb
/// lag, stamped 60 s ahead so it stays fresh for the whole scenario.
async fn beat(store: &Arc<dyn ObjectStore>, instance: &str, lag_secs: u64) {
    let ts_ms = now_ms() + 60_000;
    let body = format!(
        r#"{{"instance":"{instance}","ts_ms":{ts_ms},"rps":0.0,"absorb_lag_max_secs":{lag_secs},"owned_shards":[],"draining":false,"url":"https://{instance}.invalid"}}"#
    );
    put(store, &format!("fleet/{instance}.json"), body.into_bytes()).await;
}

/// The desired count.
async fn seed_count(store: &Arc<dyn ObjectStore>, count: u64, epoch: u64) {
    let body = format!(r#"{{"count":{count},"epoch":{epoch},"reason":"seed","computed_at_ms":0}}"#);
    put(store, "fleet/desired.json", body.into_bytes()).await;
}

/// The overrides document: 10 and 11 to `streams-1`, `ages_ms` old.
async fn seed_overrides(store: &Arc<dyn ObjectStore>, ages_ms: [i64; 2]) {
    let now = now_ms();
    let overrides = crate::fleet::Overrides {
        entries: [("10", ages_ms[0]), ("11", ages_ms[1])]
            .into_iter()
            .map(|(prefix, age)| {
                let entry = crate::fleet::OverrideEntry {
                    to: "streams-1".to_string(),
                    ms: now - age,
                };
                (prefix.to_string(), entry)
            })
            .collect(),
        ..Default::default()
    };
    put(
        store,
        "fleet/overrides.json",
        serde_json::to_vec(&overrides).unwrap(),
    )
    .await;
}

/// The stored overrides document's entries: prefix -> (target, ms).
async fn entries(rig: &HttpRig) -> BTreeMap<String, (String, i64)> {
    let (overrides, _) = rig.state.fleet.read_overrides().await.unwrap();
    overrides
        .entries
        .into_iter()
        .map(|(prefix, entry)| (prefix, (entry.to, entry.ms)))
        .collect()
}

/// `streams-1` over `fleet` with shard 00 open, its fleet started under the
/// command line `cli` edits; returns once the tick has published a ring of
/// `members`.
async fn started(
    fleet: Arc<dyn ObjectStore>,
    members: usize,
    cli: fn(&mut crate::config::CliArgs),
) -> HttpRig {
    let ring = ["streams-1".to_string(), "streams-2".to_string()];
    let homes: Vec<usize> = PREFIXES
        .iter()
        .map(|prefix| crate::ownership::ring_pick(prefix, &ring))
        .collect();
    assert_eq!(homes, [0, 0, 1, 1], "the ring's homes the scenarios assume");
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some("streams-1".into()),
            prefixes: PREFIXES
                .iter()
                .map(|prefix| (*prefix).to_string())
                .collect(),
            cli,
            ..Default::default()
        },
    )
    .await;
    let crate::sharddir::OpenOutcome::Ready(engine) = rig
        .state
        .shards
        .open_or_wait("00", Duration::from_secs(5))
        .await
    else {
        panic!("shard 00 opens");
    };
    drop(engine);
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    settled(Duration::from_secs(20), || {
        rig.state.ownership.ring_active().len() == members
    })
    .await;
    assert_eq!(
        rig.state.ownership.ring_active().len(),
        members,
        "the tick publishes the ring"
    );
    rig
}

/// Wait out `QUIET` and assert the stored entries are still `seeded`.
async fn holds(rig: &HttpRig, seeded: &BTreeMap<String, (String, i64)>, why: &str) {
    tokio::time::sleep(QUIET).await;
    assert_eq!(&entries(rig).await, seeded, "{why}");
    assert!(rig.state.shards.is_open("00"), "{why}: 00 stays");
}

/// Wait, bounded, for the move of 00; assert it is the only change to
/// `seeded` and that 00 closed here.
async fn moves_00(rig: &HttpRig, seeded: &BTreeMap<String, (String, i64)>) {
    let deadline = tokio::time::Instant::now() + MOVE_BUDGET;
    let mut stored = entries(rig).await;
    while !stored.contains_key("00") && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        stored = entries(rig).await;
    }
    let moved = stored.remove("00").map(|(to, _)| to);
    assert_eq!(moved.as_deref(), Some("streams-2"), "00 moves to streams-2");
    assert_eq!(&stored, seeded, "the seeded overrides stay as they were");
    settled(Duration::from_secs(10), || !rig.state.shards.is_open("00")).await;
    assert!(
        !rig.state.shards.is_open("00"),
        "the moved shard closes here"
    );
}

async fn stop(rig: HttpRig) {
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// The positive path and its boot cooldown: nothing moves until this
/// instance's own cooldown has run from its boot, then exactly one shard,
/// 00, moves to the member below the share, and the next waits a cooldown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_count_move_waits_out_the_boot_cooldown_then_moves_one_shard() {
    let fleet = mem();
    seed_count(&fleet, 2, 1).await;
    seed_overrides(&fleet, [AGED_MS, AGED_MS]).await;
    beat(&fleet, "streams-2", 0).await;
    let rig = started(fleet, 2, |_| {}).await;
    let seeded = entries(&rig).await;
    assert_eq!(seeded.keys().collect::<Vec<_>>(), ["10", "11"]);
    holds(&rig, &seeded, "no move within the boot cooldown").await;
    rig.clock.advance_monotonic(Duration::from_secs(61));
    moves_00(&rig, &seeded).await;
    // streams-1 still owns three of a share of two, but its own cooldown and
    // the fleet's new override hold the next move.
    tokio::time::sleep(QUIET).await;
    assert_eq!(entries(&rig).await.len(), 3, "one move per cooldown");
    stop(rig).await;
}

/// A fleet store that counts the listings of the heartbeat namespace.
#[derive(Debug)]
struct Listings {
    inner: Arc<dyn ObjectStore>,
    fleet: AtomicU64,
}
impl std::fmt::Display for Listings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "counted-fleet-listings")
    }
}
#[async_trait::async_trait]
impl ObjectStore for Listings {
    async fn put_opts(
        &self,
        path: &Path,
        body: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(path, body, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(
        &self,
        path: &Path,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        if prefix.is_some_and(|prefix| prefix.as_ref() == "fleet") {
            self.fleet.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// A member within the share reads nothing for the count balance: no
/// listing of the heartbeats, which the tick itself makes only on its first
/// pass and every 30th after it (a minute).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_member_within_the_share_reads_nothing_for_the_count_balance() {
    let inner = mem();
    seed_count(&inner, 2, 1).await;
    put(
        &inner,
        "fleet/overrides.json",
        br#"{"entries":{}}"#.to_vec(),
    )
    .await;
    beat(&inner, "streams-2", 0).await;
    let fleet = Arc::new(Listings {
        inner,
        fleet: AtomicU64::new(0),
    });
    let rig = started(fleet.clone(), 2, |_| {}).await;
    rig.clock.advance_monotonic(Duration::from_secs(61));
    let listed = fleet.fleet.load(Ordering::SeqCst);
    assert!(listed > 0, "the tick's first pass lists the heartbeats");
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        fleet.fleet.load(Ordering::SeqCst),
        listed,
        "streams-1 owns 00 and 01, its share of two"
    );
    assert!(entries(&rig).await.is_empty(), "and moves nothing");
    stop(rig).await;
}

/// The target rule is the lag move's: a lag under half the threshold. At
/// half of the default 60 s nothing moves; a second under it, 00 does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_member_at_half_the_lag_threshold_receives_no_count_move() {
    let fleet = mem();
    seed_count(&fleet, 2, 1).await;
    seed_overrides(&fleet, [AGED_MS, AGED_MS]).await;
    beat(&fleet, "streams-2", 30).await;
    let rig = started(fleet.clone(), 2, |_| {}).await;
    let seeded = entries(&rig).await;
    rig.clock.advance_monotonic(Duration::from_secs(61));
    holds(&rig, &seeded, "a target at half the threshold").await;
    beat(&fleet, "streams-2", 29).await;
    // The read that found no target held the next for a cooldown.
    rig.clock.advance_monotonic(Duration::from_secs(61));
    moves_00(&rig, &seeded).await;
    stop(rig).await;
}

/// A ring of one holds every shard within its share: nothing moves. Once
/// the desired count names a second member, 00 does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_ring_of_one_makes_no_count_move() {
    let fleet = mem();
    seed_count(&fleet, 1, 1).await;
    seed_overrides(&fleet, [AGED_MS, AGED_MS]).await;
    beat(&fleet, "streams-2", 0).await;
    // The test process's CPU, which every concurrent test shares, would
    // otherwise scale the ring out to the beating streams-2.
    let rig = started(fleet.clone(), 1, |cli| cli.fleet_max = 1).await;
    let seeded = entries(&rig).await;
    rig.clock.advance_monotonic(Duration::from_secs(61));
    holds(&rig, &seeded, "a ring of one").await;
    seed_count(&fleet, 2, 2).await;
    moves_00(&rig, &seeded).await;
    stop(rig).await;
}

/// A heartbeat from an instance no desired count names is no ring member,
/// so it receives no shard however idle it is. Once the member streams-2
/// beats, 00 moves to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_heartbeat_outside_the_ring_receives_no_count_move() {
    let fleet = mem();
    seed_count(&fleet, 2, 1).await;
    seed_overrides(&fleet, [AGED_MS, AGED_MS]).await;
    beat(&fleet, "streams-9", 0).await;
    let rig = started(fleet.clone(), 1, |_| {}).await;
    let seeded = entries(&rig).await;
    rig.clock.advance_monotonic(Duration::from_secs(61));
    holds(&rig, &seeded, "only a non-member beats").await;
    beat(&fleet, "streams-2", 0).await;
    moves_00(&rig, &seeded).await;
    stop(rig).await;
}

/// Any override younger than the move cooldown, here a peer's move of 10,
/// holds every count move in the fleet. Once it has aged, 00 moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fresh_override_anywhere_holds_the_count_move() {
    let fleet = mem();
    seed_count(&fleet, 2, 1).await;
    seed_overrides(&fleet, [0, AGED_MS]).await;
    beat(&fleet, "streams-2", 0).await;
    let rig = started(fleet.clone(), 2, |_| {}).await;
    let fresh = entries(&rig).await;
    rig.clock.advance_monotonic(Duration::from_secs(61));
    holds(&rig, &fresh, "an override younger than the cooldown").await;
    seed_overrides(&fleet, [AGED_MS, AGED_MS]).await;
    let aged = entries(&rig).await;
    // The young override held the reads for the rest of its cooldown.
    rig.clock.advance_monotonic(Duration::from_secs(61));
    moves_00(&rig, &aged).await;
    stop(rig).await;
}
