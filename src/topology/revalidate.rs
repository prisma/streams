//! Revalidation of the cell's topology (S4-DESIGN §3.2). Once a fleet tick period, a read of
//! `topology.json` on the snapshot's ETag: an unchanged object changes nothing; a changed one
//! is installed unless it is older than the snapshot, which retires the engines of prefixes
//! that are no longer leaves. A cell without the fleet has no peer to publish a topology; its
//! split installs the one it wrote.
//!
//! On a fleet instance an install of a newer version first refreshes the ownership view's
//! overrides (GATE-DECISION-f2-children-on-actor-2026-11-16 A1): a split's actor names itself the
//! owner of its children in `fleet/overrides.json` before its topology CAS, so a member that
//! installs the new leaves serves them by that entry, never by the ring. The read, the refresh and
//! the install run under the directory's revalidation lock, which the split's overrides write and
//! the fleet tick's view publication also take. A refresh that cannot read the overrides refuses
//! the install: the member keeps its version and tries again at its next read.

use std::sync::Arc;
use std::time::Duration;

use crate::fleet::FleetRepository;
use crate::ownership::OwnershipService;
use crate::registry::Registry;
use crate::runtime::{Clock, MonotonicNow};
use crate::shard_directory::ShardDirectory;
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};

/// What a fleet instance's install refreshes first: the fleet's overrides document, into this
/// instance's ownership view.
#[derive(Clone)]
pub(crate) struct Refresh {
    fleet: FleetRepository,
    ownership: OwnershipService,
}

impl Refresh {
    /// The refresh of an instance whose fleet is `fleet`: none when it runs without one.
    pub(crate) fn of(fleet: &FleetRepository, ownership: &OwnershipService) -> Option<Self> {
        fleet.enabled().then(|| Self {
            fleet: fleet.clone(),
            ownership: ownership.clone(),
        })
    }

    /// Read the overrides and publish them with the document's own ring over the fleet tick's last
    /// candidates (§22 C2); before the tick's first pass, with the view's current active members,
    /// which it keeps exactly (so it never creates a view an instance awaiting its first one lacks).
    async fn refreshed(&self) -> anyhow::Result<()> {
        let (overrides, _) = self.fleet.read_overrides().await?;
        let ring = self.fleet.standing().ring_of(&overrides);
        let map = overrides
            .entries
            .into_iter()
            .map(|(prefix, entry)| (prefix, entry.to))
            .collect();
        let active = ring.unwrap_or_else(|| self.ownership.ring_active());
        self.ownership.set_view(active, map);
        Ok(())
    }
}

/// What the loop revalidates: the cell's registry, this instance's directory and its refresh.
pub(crate) struct Revalidation {
    pub(crate) registry: Arc<Registry>,
    pub(crate) shards: ShardDirectory,
    pub(crate) refresh: Option<Refresh>,
}

/// Start the revalidation loop: one read each `period` (the fleet tick's), each install refreshed
/// by the revalidation's refresh.
pub(crate) fn start(
    revalidation: Revalidation,
    clock: Arc<dyn Clock>,
    period: Duration,
    tasks: &TaskSupervisor,
) {
    let spawned = tasks.spawn("topology-revalidate", Policy::Noncritical, move |cancel| {
        run(revalidation, clock, period, cancel)
    });
    if let Err(rejected) = spawned {
        tracing::debug!(
            ?rejected,
            "runtime stopping: topology revalidation was not started"
        );
    }
}

async fn run(
    revalidation: Revalidation,
    clock: Arc<dyn Clock>,
    period: Duration,
    cancel: Cancellation,
) -> TaskResult {
    let Revalidation {
        registry,
        shards,
        refresh,
    } = revalidation;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            _ = tokio::time::sleep(period) => {}
        }
        let read = once(&registry, &shards, refresh.as_ref(), clock.monotonic());
        if let Err(e) = read.await {
            tracing::debug!("topology revalidation: {e}");
        }
    }
}

/// One revalidation at `now`, under the directory's revalidation lock: whether it installed the
/// stored topology.
pub(crate) async fn once(
    registry: &Registry,
    shards: &ShardDirectory,
    refresh: Option<&Refresh>,
    now: MonotonicNow,
) -> anyhow::Result<bool> {
    let _one = shards.revalidation().lock().await;
    installed(registry, shards, refresh, now).await
}

/// [`once`], for a caller that holds the revalidation lock: a newer stored version is installed
/// after `refresh` read the overrides, and not at all if it could not.
pub(crate) async fn installed(
    registry: &Registry,
    shards: &ShardDirectory,
    refresh: Option<&Refresh>,
    now: MonotonicNow,
) -> anyhow::Result<bool> {
    let held = shards.prefixes();
    let Some((topology, etag)) = registry.topology(held.etag.as_deref()).await? else {
        return Ok(false);
    };
    if let Some(refresh) = refresh
        && topology.shards.version > held.shards.version
    {
        refresh.refreshed().await?;
    }
    Ok(shards.install(topology.shards, etag, now))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use object_store::{ObjectStore, ObjectStoreExt};

    use crate::shard_directory::{OpenTiming, ShardDirectory};
    use crate::sharddir::{EngineIncarnation, OpenFn};
    use crate::topology::model::{Home, ShardSet, Topology};

    fn directory(shards: ShardSet) -> ShardDirectory {
        let opener: OpenFn = Box::new(|_prefix: String, _inc: EngineIncarnation| {
            Box::pin(async { anyhow::bail!("this test opens no shard") })
        });
        ShardDirectory::new(
            shards,
            crate::ownership::OwnershipService::new(String::new()),
            OpenTiming {
                open_deadline: Duration::from_secs(1),
                open_wait: Duration::from_millis(10),
            },
            |_notifier| opener,
        )
    }

    fn home(path: &str, prefix: &str, parents: Vec<u32>, born: u64) -> Home {
        Home {
            path: path.to_string(),
            prefix: prefix.to_string(),
            parents,
            born_version: born,
            retired_version: None,
        }
    }

    /// `v1` with prefix "1" split into "10" and "11" in homes 2 and 3, published as version 2.
    fn split_of_one(v1: &Topology) -> Topology {
        let mut t = v1.clone();
        t.version = 2;
        t.shards.version = 2;
        t.shards.shards = ["0", "10", "11"].map(str::to_string).to_vec();
        t.shards.homes.get_mut(&1).unwrap().retired_version = Some(2);
        t.shards
            .homes
            .insert(2, home("shards/10-2", "10", vec![1], 2));
        t.shards
            .homes
            .insert(3, home("shards/11-3", "11", vec![1], 2));
        t.shards.leaves.remove("1");
        t.shards.leaves.insert("10".to_string(), 2);
        t.shards.leaves.insert("11".to_string(), 3);
        t.shards.next_home = 4;
        t
    }

    async fn put(store: &Arc<dyn ObjectStore>, t: &Topology) {
        let raw = serde_json::to_vec(t).unwrap();
        let path = object_store::path::Path::from("topology.json");
        store.put(&path, raw.into()).await.unwrap();
    }

    /// **The started loop revalidates on its own.** With the topology stored and a 10 ms period,
    /// the directory, booted without an ETag, holds the stored topology's within 5 s.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn the_started_loop_revalidates_on_its_own() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards = crate::config::validation::InitialShards::new(2).unwrap();
        let v1 = crate::registry::load_or_init_topology(&store, shards, 1 << 20)
            .await
            .unwrap();
        let cell = crate::tenant::CellId::new("test-cell").unwrap();
        let registry = Arc::new(crate::registry::Registry::new(store.clone(), &cell));
        let dir = directory(v1.shards.clone());
        let clock: Arc<dyn crate::runtime::Clock> = Arc::new(crate::runtime::ManualClock::at(0));
        let tasks = crate::tasks::TaskSupervisor::new();
        let revalidation = super::Revalidation {
            registry,
            shards: dir.clone(),
            refresh: None,
        };
        super::start(revalidation, clock, Duration::from_millis(10), &tasks);
        let started = std::time::Instant::now();
        while dir.prefixes().etag.is_none() {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the loop never revalidated"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(tasks.shutdown(Duration::from_secs(1)).await);
    }

    /// **A revalidation installs a newer topology, and an unchanged or older one changes
    /// nothing.** The first read installs the stored topology and learns its ETag; the next
    /// finds it unchanged. A split published as version 2 is installed, and the directory then
    /// routes by its leaves; a version 1 stored after it is not installed (a version never goes
    /// back).
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_revalidation_installs_a_newer_topology_and_nothing_else() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards = crate::config::validation::InitialShards::new(2).unwrap();
        let v1 = crate::registry::load_or_init_topology(&store, shards, 1 << 20)
            .await
            .unwrap();
        let cell = crate::tenant::CellId::new("test-cell").unwrap();
        let registry = crate::registry::Registry::new(store.clone(), &cell);
        let dir = directory(v1.shards.clone());
        let clock = crate::runtime::ManualClock::at(0);
        let now = || crate::runtime::Clock::monotonic(&clock);
        assert!(dir.prefixes().etag.is_none(), "boot read no ETag");
        assert!(
            super::once(&registry, &dir, None, now()).await.unwrap(),
            "the first read installs"
        );
        let first = dir.prefixes();
        assert!(first.etag.is_some());
        assert!(
            !super::once(&registry, &dir, None, now()).await.unwrap(),
            "unchanged"
        );
        assert!(Arc::ptr_eq(&first, &dir.prefixes()), "the snapshot is kept");

        let v2 = split_of_one(&v1);
        put(&store, &v2).await;
        assert!(
            super::once(&registry, &dir, None, now()).await.unwrap(),
            "a newer version installs"
        );
        let held = dir.prefixes();
        assert_eq!(
            (held.shards.version, &*held.shards.shards),
            (2, &v2.shards.shards[..])
        );
        let routes: Vec<String> = [0x00u8, 0x80, 0xC0]
            .iter()
            .map(|b| dir.prefix_for(&[*b; 16]))
            .collect();
        assert_eq!(routes, ["0", "10", "11"]);

        put(&store, &v1).await;
        assert!(
            !super::once(&registry, &dir, None, now()).await.unwrap(),
            "an older version does not"
        );
        assert_eq!(dir.prefixes().shards.version, 2);
    }

    /// **A re-read of the held version under a new ETag installs without a refresh** (PR #158's
    /// survivor revalidate.rs:130:36, the refresh's `>` as `>=`). A fleet instance holding version 1
    /// with no override: version 1 stored again (a new ETag) with `fleet/overrides.json` unreadable
    /// still installs, as a refresh would have refused it; stored again with the document naming
    /// `a` for `10`, it installs and the view still holds no override. Only a newer version
    /// refreshes.
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_reread_of_the_held_version_installs_without_a_refresh() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards = crate::config::validation::InitialShards::new(2).unwrap();
        let v1 = crate::registry::load_or_init_topology(&store, shards, 1 << 20)
            .await
            .unwrap();
        let cell = crate::tenant::CellId::new("test-cell").unwrap();
        let registry = crate::registry::Registry::new(store.clone(), &cell);
        let dir = directory(v1.shards.clone());
        let fleet_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let fleet = crate::fleet::FleetRepository::new(Some(fleet_store.clone()));
        let ownership = fleet.ownership("a".to_string());
        ownership.set_view(vec!["a".to_string()], Default::default());
        let refresh = super::Refresh::of(&fleet, &ownership);
        let clock = crate::runtime::ManualClock::at(0);
        let now = || crate::runtime::Clock::monotonic(&clock);
        assert!(
            super::once(&registry, &dir, refresh.as_ref(), now())
                .await
                .unwrap()
        );
        let overrides = object_store::path::Path::from("fleet/overrides.json");
        fleet_store
            .put(&overrides, b"not json".to_vec().into())
            .await
            .unwrap();
        put(&store, &v1).await;
        let reread = super::once(&registry, &dir, refresh.as_ref(), now()).await;
        assert_eq!(
            (
                reread.map_err(|e| e.to_string()),
                dir.prefixes().shards.version
            ),
            (Ok(true), 1),
            "a re-read of the held version reads no overrides"
        );
        let named = br#"{"entries":{"10":{"to":"a","ms":1}}}"#.to_vec();
        fleet_store.put(&overrides, named.into()).await.unwrap();
        put(&store, &v1).await;
        assert!(
            super::once(&registry, &dir, refresh.as_ref(), now())
                .await
                .unwrap()
        );
        assert!(
            ownership.view().overrides.is_empty(),
            "no refresh on the same version"
        );
    }

    /// **An install whose overrides read fails is refused** (GATE-DECISION-f2-children-on-actor
    /// A1). A fleet instance holding version 1, its view of member `a`: with version 2 stored and
    /// `fleet/overrides.json` unreadable, the revalidation fails and the directory keeps version 1.
    /// Once the document reads, naming `a` for `10`, the next revalidation installs version 2, and
    /// the view holds that override with its active members unchanged.
    #[tokio::test(crate = "crate::test_rt")]
    async fn an_install_whose_overrides_read_fails_is_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards = crate::config::validation::InitialShards::new(2).unwrap();
        let v1 = crate::registry::load_or_init_topology(&store, shards, 1 << 20)
            .await
            .unwrap();
        let cell = crate::tenant::CellId::new("test-cell").unwrap();
        let registry = crate::registry::Registry::new(store.clone(), &cell);
        let dir = directory(v1.shards.clone());
        let fleet_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let fleet = crate::fleet::FleetRepository::new(Some(fleet_store.clone()));
        let ownership = fleet.ownership("a".to_string());
        ownership.set_view(vec!["a".to_string()], Default::default());
        let refresh = super::Refresh::of(&fleet, &ownership);
        assert!(refresh.is_some(), "a fleet instance refreshes");
        let clock = crate::runtime::ManualClock::at(0);
        let now = || crate::runtime::Clock::monotonic(&clock);
        assert!(
            super::once(&registry, &dir, refresh.as_ref(), now())
                .await
                .unwrap()
        );
        put(&store, &split_of_one(&v1)).await;
        let overrides = object_store::path::Path::from("fleet/overrides.json");
        fleet_store
            .put(&overrides, b"not json".to_vec().into())
            .await
            .unwrap();
        let refused = super::once(&registry, &dir, refresh.as_ref(), now()).await;
        assert!(refused.is_err(), "the install is refused");
        assert_eq!(dir.prefixes().shards.version, 1, "the version stays");
        let named = br#"{"entries":{"10":{"to":"a","ms":1}}}"#.to_vec();
        fleet_store.put(&overrides, named.into()).await.unwrap();
        assert!(
            super::once(&registry, &dir, refresh.as_ref(), now())
                .await
                .unwrap()
        );
        assert_eq!(dir.prefixes().shards.version, 2, "the next read installs");
        let view = ownership.view();
        assert_eq!(
            (view.active, view.overrides.into_iter().collect::<Vec<_>>()),
            (
                vec!["a".to_string()],
                vec![("10".to_string(), "a".to_string())]
            ),
            "the refreshed overrides, the active members kept"
        );
    }

    /// **A version's install instant is this process's first install of that version.** The
    /// boot set has none; the first read sets it; a re-read of the same version under a new
    /// ETag keeps it; a newer version sets a new one (S6 §4 step 1's grace is measured from
    /// it).
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_versions_install_instant_is_its_first_install_here() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards = crate::config::validation::InitialShards::new(2).unwrap();
        let v1 = crate::registry::load_or_init_topology(&store, shards, 1 << 20)
            .await
            .unwrap();
        let cell = crate::tenant::CellId::new("test-cell").unwrap();
        let registry = crate::registry::Registry::new(store.clone(), &cell);
        let dir = directory(v1.shards.clone());
        let clock = crate::runtime::ManualClock::at(0);
        let now = || crate::runtime::Clock::monotonic(&clock);
        assert_eq!(dir.prefixes().installed_at, None, "the boot set has none");
        let first = now();
        super::once(&registry, &dir, None, first).await.unwrap();
        clock.advance_monotonic(Duration::from_secs(60));
        put(&store, &v1).await;
        assert!(
            super::once(&registry, &dir, None, now()).await.unwrap(),
            "a new ETag re-reads"
        );
        assert_eq!(
            dir.prefixes().installed_at,
            Some(first),
            "the same version keeps it"
        );
        clock.advance_monotonic(Duration::from_secs(60));
        put(&store, &split_of_one(&v1)).await;
        let newer = now();
        super::once(&registry, &dir, None, newer).await.unwrap();
        assert_eq!(
            dir.prefixes().installed_at,
            Some(newer),
            "a newer version sets it"
        );
        assert_ne!(first, newer);
    }

    /// **An install's refresh pairs a newer document's entries with that document's ring** (§22
    /// C2, GATE-DECISION-s22-3b-step4-2026-11-21 §2.2), not with the ring last published. The view
    /// last published is `streams-1` alone, no override; the fleet tick's last pass read a count of
    /// two with both members live. The stored document now lists both and moves `10` to
    /// `streams-2`: the refresh publishes both members as the ring, beside that entry.
    #[tokio::test(crate = "crate::test_rt")]
    async fn an_installs_refresh_pairs_a_newer_documents_entries_with_its_own_ring() {
        let fleet_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let fleet = crate::fleet::FleetRepository::new(Some(fleet_store.clone()));
        let ownership = fleet.ownership("streams-1".to_string());
        ownership.set_view(vec!["streams-1".to_string()], Default::default());
        fleet.standing().note_pass(2, &["streams-1", "streams-2"]);
        let newer = br#"{"entries":{"10":{"to":"streams-2","ms":0}},"members":[{"name":"streams-1"},{"name":"streams-2"}]}"#;
        let path = object_store::path::Path::from("fleet/overrides.json");
        fleet_store.put(&path, newer.to_vec().into()).await.unwrap();
        let refresh = super::Refresh::of(&fleet, &ownership).unwrap();
        refresh.refreshed().await.unwrap();
        let both = vec!["streams-1".to_string(), "streams-2".to_string()];
        let moved = vec![("10".to_string(), "streams-2".to_string())];
        let overrides: Vec<(String, String)> = ownership.overrides().into_iter().collect();
        assert_eq!((ownership.ring_active(), overrides), (both, moved));
    }
}
