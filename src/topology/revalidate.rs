//! Revalidation of the cell's topology (S4-DESIGN §3.2). Once a fleet tick period, a read of
//! `topology.json` on the snapshot's ETag: an unchanged object changes nothing; a changed one
//! is installed unless it is older than the snapshot, which retires the engines of prefixes
//! that are no longer leaves. A cell without the fleet has no peer to publish a topology; its
//! split installs the one it wrote.

use std::sync::Arc;
use std::time::Duration;

use crate::registry::Registry;
use crate::runtime::{Clock, MonotonicNow};
use crate::shard_directory::ShardDirectory;
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};

/// Start the revalidation loop: one read each `period` (the fleet tick's).
pub(crate) fn start(
    registry: Arc<Registry>,
    shards: ShardDirectory,
    clock: Arc<dyn Clock>,
    period: Duration,
    tasks: &TaskSupervisor,
) {
    if let Err(rejected) = tasks.spawn("topology-revalidate", Policy::Noncritical, move |cancel| {
        run(registry, shards, clock, period, cancel)
    }) {
        tracing::debug!(
            ?rejected,
            "runtime stopping: topology revalidation was not started"
        );
    }
}

async fn run(
    registry: Arc<Registry>,
    shards: ShardDirectory,
    clock: Arc<dyn Clock>,
    period: Duration,
    cancel: Cancellation,
) -> TaskResult {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            _ = tokio::time::sleep(period) => {}
        }
        if let Err(e) = once(&registry, &shards, clock.monotonic()).await {
            tracing::debug!("topology revalidation: {e}");
        }
    }
}

/// One revalidation at `now`: whether it installed the stored topology.
pub(crate) async fn once(
    registry: &Registry,
    shards: &ShardDirectory,
    now: MonotonicNow,
) -> Result<bool, object_store::Error> {
    let held = shards.prefixes();
    match registry.topology(held.etag.as_deref()).await? {
        None => Ok(false),
        Some((topology, etag)) => Ok(shards.install(topology.shards, etag, now)),
    }
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
            super::once(&registry, &dir, now()).await.unwrap(),
            "the first read installs"
        );
        let first = dir.prefixes();
        assert!(first.etag.is_some());
        assert!(
            !super::once(&registry, &dir, now()).await.unwrap(),
            "unchanged"
        );
        assert!(Arc::ptr_eq(&first, &dir.prefixes()), "the snapshot is kept");

        let v2 = split_of_one(&v1);
        put(&store, &v2).await;
        assert!(
            super::once(&registry, &dir, now()).await.unwrap(),
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
            !super::once(&registry, &dir, now()).await.unwrap(),
            "an older version does not"
        );
        assert_eq!(dir.prefixes().shards.version, 2);
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
        super::once(&registry, &dir, first).await.unwrap();
        clock.advance_monotonic(Duration::from_secs(60));
        put(&store, &v1).await;
        assert!(
            super::once(&registry, &dir, now()).await.unwrap(),
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
        super::once(&registry, &dir, newer).await.unwrap();
        assert_eq!(
            dir.prefixes().installed_at,
            Some(newer),
            "a newer version sets it"
        );
        assert_ne!(first, newer);
    }
}
