//! The process's allocator posture.
//!
//! Transparent huge pages are off for the whole process. With them on, the
//! kernel's khugepaged collapses the 4 KiB holes mimalloc purges back into
//! 2 MiB pages, so memory freed after a load returns to RSS within minutes
//! (Q0: an idle instance climbed from 119-122 to 154-162 MiB within 90 s of
//! releasing its caches, against 79-81 MiB with huge pages off).
//!
//! After the idle release, every long-lived runtime thread collects its
//! empty allocator pages once per requested round.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// What the kernel reports after the process asked it to stop backing this
/// process's memory with transparent huge pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThpPosture {
    /// The kernel reads the flag back as set: no huge page backs this
    /// process's anonymous memory, at fault time or by collapse.
    Off,
    /// The kernel refused the request, with this errno.
    Refused(i32),
    /// The request succeeded but the flag read back as this value.
    Unconfirmed(i32),
    /// Not Linux: there are no transparent huge pages to turn off.
    NotLinux,
}

/// Turns transparent huge pages off for this process and its future
/// children, then reads the flag back. The binary calls this first, before
/// the allocator maps the arenas the service runs on. The flag holds under
/// both kernel modes (`always` and `madvise`), whatever the allocator asks
/// for, which an allocator option alone does not. One body serves every
/// target, so no variant of it is compiled out where it is tested.
pub fn disable_transparent_huge_pages() -> ThpPosture {
    #[cfg(target_os = "linux")]
    let posture = prctl_disable_transparent_huge_pages();
    #[cfg(not(target_os = "linux"))]
    let posture = ThpPosture::NotLinux;
    posture
}

/// Sets `PR_SET_THP_DISABLE` and reads it back.
#[cfg(target_os = "linux")]
fn prctl_disable_transparent_huge_pages() -> ThpPosture {
    let (zero, one): (libc::c_ulong, libc::c_ulong) = (0, 1);
    // SAFETY: PR_SET_THP_DISABLE takes only integer arguments and changes
    // this process's huge-page eligibility; no memory is passed to the
    // kernel. The unused arguments are passed as zero, as it requires.
    if unsafe { libc::prctl(libc::PR_SET_THP_DISABLE, one, zero, zero, zero) } != 0 {
        let errno = std::io::Error::last_os_error().raw_os_error();
        return ThpPosture::Refused(errno.unwrap_or_default());
    }
    // SAFETY: as above; PR_GET_THP_DISABLE reads the flag back.
    match unsafe { libc::prctl(libc::PR_GET_THP_DISABLE, zero, zero, zero, zero) } {
        1 => ThpPosture::Off,
        other => ThpPosture::Unconfirmed(other),
    }
}

/// Collect rounds requested since boot. A thread collects once per round it
/// has not yet collected, the next time it parks or ticks.
static COLLECT_EPOCH: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// The last round this thread collected for.
    static SEEN: Cell<u64> = const { Cell::new(0) };
}

/// Asks every long-lived runtime thread to return its empty allocator pages
/// to the OS: each worker of both runtimes when [`collect_on_every_worker`]
/// reaches it or it next parks, and the thread that runs the served future
/// within a second. Requested after the
/// idle release, never under load: after a release most pages are empty
/// but held by the thread-local heap of the thread that last used them,
/// and only that thread can return them (Q0 PROFILE §4b-4d: 25-50 MiB of
/// Linux RSS).
pub(crate) fn request_collect() {
    COLLECT_EPOCH.fetch_add(1, Ordering::Relaxed);
}

/// Runs `mi_collect(true)` on this thread when a round was requested since
/// it last did. Between rounds it costs one relaxed load and one TLS read,
/// so it can run on every park.
pub(crate) fn collect_if_requested() {
    if SEEN.with(|seen| collect_due(COLLECT_EPOCH.load(Ordering::Relaxed), seen)) {
        // SAFETY: mi_collect is thread-safe and takes no pointers; it frees
        // only pages that hold no live block.
        unsafe { libmimalloc_sys::mi_collect(true) };
    }
}

/// Whether a thread that last collected for round `seen` collects for
/// `epoch`, recording it when it does.
fn collect_due(epoch: u64, seen: &Cell<u64>) -> bool {
    if seen.get() >= epoch {
        return false;
    }
    seen.set(epoch);
    true
}

/// How long a probe holds its worker for the others to start. An idle worker
/// takes its probe within a millisecond of being woken; a worker still busy
/// past this misses the round and collects at its next park.
const PROBE_HOLD: Duration = Duration::from_millis(100);

/// Runs a requested round on every worker of the runtime behind `runtime`,
/// parked or not. A park hook runs only when a worker goes to park, and an
/// idle worker stays parked: under a few periodic timers one worker in four
/// parks at all (Q0 RESULT.md, round 1), so the pages the others hold would
/// stay.
pub(crate) async fn collect_on_every_worker(runtime: &tokio::runtime::Handle) {
    on_every_worker(runtime, collect_if_requested).await;
}

/// Runs `probe` once on each worker of the runtime behind `runtime` and
/// returns what each returned. Every probe holds its worker until all have
/// started or `PROBE_HOLD` passes: a worker that holds one cannot take
/// another, so the rest wake the parked workers and run there.
#[expect(
    clippy::disallowed_methods,
    reason = "on_every_worker; a probe must run as an ordinary task on each worker of a runtime, the storage runtime included, which no supervisor owns; each ends within PROBE_HOLD and is joined here"
)]
async fn on_every_worker<T: Send + 'static>(
    runtime: &tokio::runtime::Handle,
    probe: fn() -> T,
) -> Vec<T> {
    let workers = runtime.metrics().num_workers();
    let started = Arc::new(AtomicUsize::new(0));
    let probes: Vec<_> = (0..workers)
        .map(|_| {
            let started = Arc::clone(&started);
            runtime.spawn(async move {
                let out = probe();
                hold_until_started(&started, workers);
                out
            })
        })
        .collect();
    let mut outs = Vec::with_capacity(workers);
    for probe in probes {
        if let Ok(out) = probe.await {
            outs.push(out);
        }
    }
    outs
}

/// Counts this probe as started, then holds its worker until `workers`
/// probes have, or `PROBE_HOLD` passes.
fn hold_until_started(started: &AtomicUsize, workers: usize) {
    started.fetch_add(1, Ordering::AcqRel);
    let deadline = Instant::now() + PROBE_HOLD;
    while started.load(Ordering::Acquire) < workers && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The thread and the last round it collected for, on every worker of the
/// runtime behind `runtime`; reading it collects nothing.
#[cfg(test)]
pub(crate) async fn rounds_seen_on_every_worker(
    runtime: &tokio::runtime::Handle,
) -> Vec<(std::thread::ThreadId, u64)> {
    on_every_worker(runtime, || (std::thread::current().id(), seen_round())).await
}

/// The latest round requested.
#[cfg(test)]
pub(crate) fn current_round() -> u64 {
    COLLECT_EPOCH.load(Ordering::Relaxed)
}

/// The last round this thread collected for.
#[cfg(test)]
pub(crate) fn seen_round() -> u64 {
    SEEN.with(Cell::get)
}

/// Waits on 10 ms sleeps, bounded at 3 s, until the thread polling it has
/// collected for `round`. Whether it did.
#[cfg(test)]
pub(crate) async fn collected_here(round: u64) -> bool {
    for _ in 0..300 {
        if seen_round() >= round {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    false
}

#[cfg(test)]
mod collect_tests {
    use super::{collect_due, current_round, request_collect, seen_round};
    use std::cell::Cell;

    #[test]
    fn a_thread_collects_once_per_requested_round() {
        let seen = Cell::new(0);
        assert!(!collect_due(0, &seen), "no round requested yet");
        assert!(collect_due(1, &seen));
        assert!(!collect_due(1, &seen), "round 1 is done");
        assert_eq!(seen.get(), 1);
        assert!(collect_due(3, &seen), "missed rounds collect once");
        assert!(!collect_due(3, &seen));
        assert!(!collect_due(2, &seen), "never for an older round");
        assert_eq!(seen.get(), 3);
    }

    /// A round reaches every worker of an idle runtime, the ones that stay
    /// parked included.
    #[test]
    fn a_round_runs_on_every_worker_of_an_idle_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .on_thread_park(super::collect_if_requested)
            .build()
            .expect("runtime");
        // Every worker parks once at start; nothing wakes it afterwards.
        std::thread::sleep(std::time::Duration::from_millis(100));
        request_collect();
        let round = current_round();
        runtime.block_on(super::collect_on_every_worker(runtime.handle()));
        let seen = runtime.block_on(super::rounds_seen_on_every_worker(runtime.handle()));
        let threads: std::collections::HashSet<_> = seen.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            threads.len(),
            4,
            "a probe did not run on its own worker: {seen:?}"
        );
        let missed: Vec<_> = seen.iter().filter(|(_, last)| *last < round).collect();
        assert!(
            missed.is_empty(),
            "workers that did not collect for round {round}: {missed:?}"
        );
    }

    /// A probe holds its worker while another has not started, and only up
    /// to the hold; the last one to start releases at once.
    #[test]
    fn a_probe_holds_its_worker_until_every_probe_has_started_or_the_hold_passes() {
        let started = std::sync::atomic::AtomicUsize::new(0);
        let first = std::time::Instant::now();
        super::hold_until_started(&started, 2);
        let held = first.elapsed();
        assert!(
            held >= super::PROBE_HOLD,
            "the first probe held only {held:?}"
        );
        assert!(
            held < 20 * super::PROBE_HOLD,
            "the first probe held {held:?}, past its hold"
        );
        let last = std::time::Instant::now();
        super::hold_until_started(&started, 2);
        let held = last.elapsed();
        assert!(
            held < super::PROBE_HOLD / 2,
            "the last probe to start held {held:?}"
        );
        assert_eq!(started.load(std::sync::atomic::Ordering::Acquire), 2);
    }

    #[test]
    fn a_request_starts_a_round_this_thread_collects_once() {
        let before = current_round();
        request_collect();
        let round = current_round();
        assert!(round > before, "a request starts a new round");
        super::collect_if_requested();
        assert!(seen_round() >= round);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{ThpPosture, disable_transparent_huge_pages};

    #[test]
    fn the_process_posture_turns_transparent_huge_pages_off() {
        let posture = disable_transparent_huge_pages();
        let zero: libc::c_ulong = 0;
        // SAFETY: PR_GET_THP_DISABLE takes only integer arguments and reads
        // this process's flag; no memory is passed to the kernel.
        let flag = unsafe { libc::prctl(libc::PR_GET_THP_DISABLE, zero, zero, zero, zero) };
        assert_eq!(flag, 1);
        assert_eq!(posture, ThpPosture::Off);
    }
}
