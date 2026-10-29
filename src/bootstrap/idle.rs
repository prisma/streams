//! The supervised idle release: once the instance has been quiet, it lets go
//! of its SlateDB block caches and its postings slices, so the memory a load
//! left behind returns to the OS instead of holding the instance near its shed
//! line (Q0). The next read refills what it needs.
use super::AppState;
use crate::admission::AdmissionController;
use crate::registry::Registry;
use crate::runtime::{MonotonicNow, RuntimeCaps};
use crate::shard_directory::ShardDirectory;
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};
use std::sync::Arc;
use std::time::Duration;

/// How often the loop reads the instance.
const TICK: Duration = Duration::from_secs(2);
/// Quiet needed before a release: no data-plane request arrived. It is two to
/// four times any client long-poll or heartbeat cadence, and a measured drain
/// settles about 90 s after its backlog is absorbed (Q0 PROFILE §4).
const QUIET: Duration = Duration::from_secs(120);
/// Bytes written into the released caches (trailing compactions, the server's
/// own telemetry flushes) that earn another release while still quiet.
const REFILL_FLOOR: u64 = 1 << 20;
/// Bytes still owed to the history partitions below which the drain has
/// ended. Under `ABSORB_AGE_SECS=60` the server's own telemetry appends stay
/// owed for up to a minute at idle (760-3,066 bytes at almost every tick in
/// the Q0 rig's F3 posture), which would hold the release until a tick lands
/// in the instant after an absorb pass. A backlog this small costs the
/// absorber a handful of reads after a release.
const DRAINED: u64 = 1 << 20;
/// The least time between two releases in one quiet period.
const RERELEASE_AFTER: Duration = Duration::from_secs(60);
/// After a release, allocator collect rounds at +0, +30 and +60 s: the first
/// returns most of the freed pages, the later ones what trailing frees left
/// (Q0 PROFILE §4d: 1-7 MiB more).
const COLLECT_ROUNDS: u32 = 3;
const COLLECT_EVERY: Duration = Duration::from_secs(30);
/// The least time between two releases under memory pressure (RSS over the
/// shed line, quiet or not): the caches refill from the store between them.
const PRESSURE_RELEASE_AFTER: Duration = Duration::from_secs(30);

/// One tick's reading of the instance.
#[derive(Clone, Copy, Debug)]
struct Reading {
    now: MonotonicNow,
    /// Data-plane requests (`/v1/stream*`) that have arrived since boot.
    arrivals: u64,
    /// Requests in flight that are not parked in a wait.
    active: i64,
    /// Bytes the absorber still owes the history partitions.
    unabsorbed: u64,
    /// Bytes inserted into the caches since they were last released.
    refilled: u64,
    /// The sampled RSS is over a set shed line: writes are being shed.
    over_line: bool,
}

/// When to release, as a pure function of successive readings.
#[derive(Debug)]
struct IdlePolicy {
    arrivals: u64,
    quiet_since: MonotonicNow,
    /// The last release in the current quiet period.
    released_at: Option<MonotonicNow>,
    /// The last release, and the collect rounds requested since.
    collecting: Option<(MonotonicNow, u32)>,
    /// The last release under memory pressure.
    pressed_at: Option<MonotonicNow>,
}

impl IdlePolicy {
    fn new(first: &Reading) -> Self {
        Self {
            arrivals: first.arrivals,
            quiet_since: first.now,
            released_at: None,
            collecting: None,
            pressed_at: None,
        }
    }

    /// Whether the caches are released for memory pressure at `reading`:
    /// RSS is over the shed line, so writes are being shed, and no pressure
    /// release came in the last `PRESSURE_RELEASE_AFTER`. Quiet or not: a
    /// shed must lower what the instance holds, and the caches are the
    /// memory it can let go of at once.
    fn pressure_release_due(&mut self, reading: &Reading) -> bool {
        let due = reading.over_line
            && self
                .pressed_at
                .is_none_or(|at| reading.now.since(at) >= PRESSURE_RELEASE_AFTER);
        if due {
            self.pressed_at = Some(reading.now);
        }
        due
    }

    /// Whether the caches are released at `reading`. An arrival starts a new
    /// quiet period. A release needs `QUIET` of it with no request active and
    /// the absorb backlog drained (below `DRAINED`), so a drain holds it back
    /// until it ends. One
    /// release per quiet period, and another only once `REFILL_FLOOR` bytes
    /// were written back and `RERELEASE_AFTER` has passed.
    fn release_due(&mut self, reading: &Reading) -> bool {
        if self.arrived(reading) {
            return false;
        }
        if reading.active > 0
            || reading.unabsorbed >= DRAINED
            || reading.now.since(self.quiet_since) < QUIET
        {
            return false;
        }
        let due = self.released_at.is_none_or(|at| {
            reading.refilled >= REFILL_FLOOR && reading.now.since(at) >= RERELEASE_AFTER
        });
        if due {
            self.released_at = Some(reading.now);
            self.collecting = Some((reading.now, 0));
        }
        due
    }

    /// Starts a new quiet period when a data-plane request arrived since the
    /// last reading, which ends the last release's collect rounds. Whether
    /// one did.
    fn arrived(&mut self, reading: &Reading) -> bool {
        if reading.arrivals == self.arrivals {
            return false;
        }
        self.arrivals = reading.arrivals;
        self.quiet_since = reading.now;
        self.released_at = None;
        self.collecting = None;
        true
    }

    /// Whether an allocator collect round is requested at `now`: rounds at
    /// `COLLECT_EVERY` steps from the last release, `COLLECT_ROUNDS` of them.
    /// An arrival ends them (`arrived`), so no round holds the workers
    /// that serve resumed traffic.
    fn collect_due(&mut self, now: MonotonicNow) -> bool {
        let Some((released, rounds)) = self.collecting else {
            return false;
        };
        let due = rounds < COLLECT_ROUNDS && now.since(released) >= COLLECT_EVERY * rounds;
        if due {
            self.collecting = Some((released, rounds + 1));
        }
        due
    }
}

/// Starts the `idle-release` loop under `tasks`, over the instance's
/// admission, shards and runtime. Losing it costs memory hygiene, not
/// service; a refused spawn (only while stopping) is logged.
pub(crate) fn start(state: &AppState, tasks: &TaskSupervisor) {
    let admission = state.admission.clone();
    let shards = state.shards.clone();
    let runtime = state.runtime.clone();
    let registry = state.registry.clone();
    if let Err(rejected) = tasks.spawn("idle-release", Policy::Noncritical, move |cancel| {
        run(admission, shards, runtime, registry, cancel)
    }) {
        tracing::warn!("idle-release not spawned: {rejected:?}");
    }
}

async fn run(
    admission: AdmissionController,
    shards: ShardDirectory,
    runtime: RuntimeCaps,
    registry: Arc<Registry>,
    cancel: Cancellation,
) -> TaskResult {
    let mut policy = IdlePolicy::new(&read(&admission, &shards, &runtime));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            _ = runtime.clock.sleep(TICK) => {}
        }
        let reading = read(&admission, &shards, &runtime);
        if policy.release_due(&reading) {
            tokio::select! {
                _ = cancel.cancelled() => return TaskResult::Done,
                () = flush_memtables(&shards) => {}
            }
            // A request that arrived during the flush ends the quiet period,
            // and with it this release and its collect rounds.
            if !policy.arrived(&read(&admission, &shards, &runtime)) {
                release(&runtime, &registry, "idle");
            }
        }
        // Under load the workers are not held for a collect: each collects
        // its empty pages at its next park.
        if policy.pressure_release_due(&reading) {
            release(&runtime, &registry, "over the shed line");
            super::allocator::request_collect();
        }
        if policy.collect_due(reading.now) {
            super::allocator::request_collect();
            super::allocator::collect_on_every_worker(&tokio::runtime::Handle::current()).await;
            super::allocator::collect_on_every_worker(super::slatedb_runtime().handle()).await;
        }
    }
}

fn read(
    admission: &AdmissionController,
    shards: &ShardDirectory,
    runtime: &RuntimeCaps,
) -> Reading {
    Reading {
        now: runtime.clock.monotonic(),
        arrivals: admission.data_plane_arrivals(),
        active: admission.active(),
        unabsorbed: crate::backpressure::snapshot(shards).unabsorbed_bytes_instance,
        refilled: [&runtime.shared_cache, &runtime.telemetry.cache]
            .map(|cache| cache.inserted_bytes())
            .iter()
            .sum(),
        over_line: over_shed_line(admission.rss_mb(), admission.rss_shed_mb()),
    }
}

/// Whether a sampled RSS of `rss_mb` is over a shed line of `line_mb` (0:
/// no line), as the write gate measures it without the absorber's
/// reservation: strictly above.
fn over_shed_line(rss_mb: u64, line_mb: u64) -> bool {
    line_mb > 0 && rss_mb > line_mb
}

/// Seals every open shard (layout 6): its unsealed pages go to a segment
/// object and its memtable to an L0 table after an awaited WAL flush, so
/// what the queue collector and the billing drain wrote once appends stopped
/// does not stay in memory at idle. The seal runs before the caches are
/// emptied, so the blocks it caches on write go with them. A shard whose
/// seal fails keeps its memtable and its unsealed pages; its sealer retries.
async fn flush_memtables(shards: &ShardDirectory) {
    for engine in shards.engines() {
        if let Err(error) = engine.seal_requested().await {
            tracing::warn!(shard = %engine.prefix, "idle: seal failed: {error}");
        }
    }
}

/// Releases the two block caches and sweeps the registry's stale
/// descriptor slots, at idle or under memory pressure (`why`); the release
/// is tested after a quiet period and beside live keyed reads, appends and
/// installs (DST `idle_release`).
fn release(runtime: &RuntimeCaps, registry: &Registry, why: &'static str) {
    let shared = runtime.shared_cache.release();
    let telemetry = runtime.telemetry.cache.release();
    let descriptors = registry.sweep_cache();
    // The byte counts are what writes inserted into each released generation
    // since the release before it (evicted entries included), not what it
    // held: foyer does not expose its usage.
    tracing::info!(
        release = runtime.shared_cache.releases(),
        shared_inserted_bytes = shared,
        telemetry_inserted_bytes = telemetry,
        descriptor_slots = descriptors,
        why,
        "released the block caches"
    );
}

#[cfg(test)]
mod tests;
