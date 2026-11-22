#![cfg(test)]
//! A move's release on a target that has just opened (Addendum 16 of the s22 join ruling,
//! 2026-11-22, after Addendum 27 (a) of the forward-wedge ruling): package 4's cold-engine
//! diagnosis (`store_first_creates`) shaped as the 40,000 step's scale-out. The ramp touches each
//! stream once a minute, so the moved leaf takes about 333 touches a second: the giver's 2 s hold
//! piles about 667, and for a minute after the move nearly every touch names a stream the target
//! has never loaded.
//!
//! **The target is the product.** One project's streams are seeded on a store with no latency:
//! the touched ones sealed, and 667 others left unsealed, which the sealer's open loads and holds
//! (`seal/open.rs`) as on the leg's target. The server restarts over the same objects 20 ms away,
//! behind the production store wrapper (`TimingStore`) and its pools as built, at the in-flight
//! cap of 512 with its project's line the cap (a cell shared eight ways), and opens its leaf.
//!
//! **The giver, the third member and the clients are modelled here.**
//! - Each forwarding member has 128 forward places, each held for its forward's life.
//! - A request entering a member with no free place is answered `409`; its client resends it after
//!   the `Retry-After` of 1 s, to the owner the `409` names (the target, as a direct request: the
//!   rig's driver since its relearn commit) or to the same member (the SDK's), one run each.
//! - The pile was parked at the giver over its hold and is released at once. After it, the touches
//!   arrive at their rate, round robin over the giver, the third member and the target itself.
//! - The release as today (a wave: each parked request takes a free place or is answered
//!   `409`), and the release in step with the forward places (each waits for one, in order).
//! - The driver's catch-up, off and on: the touches arrive at 1.5 times the leaf's rate for the
//!   2 s after the release (the leg's figure), and otherwise open loop.
//! - Not modelled: the forward hop's own latency, which shortens how long a forward place is held.
//!
//! It reports:
//! - the gate's verdicts and line, the answers, the sheds and the `409`s;
//! - each parked request's time from its park to its answer, and those over 2 s as a share of a
//!   60 s chunk's touches at this rate (what the bar's p99 turns on);
//! - each tenth of a second on the target: requests in flight, forwards and direct requests
//!   arriving, sheds, `409`s, and the commit groups' requests acked and their longest queue wait;
//! - each request's own wait for a count permit by store class, and the other tasks';
//! - the general pool's holders by class, each second;
//! - the commit groups and the cold loads' bursts.
//!
//! Nothing is asserted. **Measured 2026-11-22** (release profile, one run per shape; p50 / p99 are
//! each parked request's park to answer, the 2 s hold included; "over 2 s" is the share of a 60 s
//! chunk's 19,980 touches):
//!
//! | release | resend | catch-up | sheds | `409`s (pile's) | peak | p50 / p99 | over 2 s |
//! |---|---|---|---|---|---|---|---|
//! | wave | owner | off | 18 | 554 (539) | 553 at 1.1 s | 1,954 / 3,180 ms | 1.49% |
//! | wave | owner | on | 0 | 556 (539) | 598 at 1.1 s | 1,940 / 2,707 ms | 1.45% |
//! | wave | entry | off | 0 | 1,544 (1,472) | ≤ 160 | 3,225 / 5,948 ms | 2.86% |
//! | wave | entry | on | 0 | 1,574 (1,497) | ≤ 160 | 3,163 / 7,089 ms | 2.81% |
//! | paced | owner | off | 0 | 44 (0) | 149 | 1,275 / 2,092 ms | 0.19% |
//! | paced | owner | on | 0 | 62 (0) | ≤ 160 | 1,268 / 2,088 ms | 0.19% |
//! | paced | entry | off | 0 | 42 (0) | ≤ 160 | 1,270 / 2,097 ms | 0.20% |
//! | paced | entry | on | 0 | 92 (0) | ≤ 160 | 1,405 / 2,654 ms | 0.31% |
//!
//! The wave's only tenth over the line is its `409`s resent to their owner one `Retry-After`
//! later, outside every forward place: 561 direct arrivals at 1.0 s, 553 in flight at 1.1 s. The
//! paced release drains the pile through the places in about 0.4 s and has no such tenth.

use super::fixture_cell::{Cell, CellSpec, open_cell};
use super::fixture_http::engine_shutdown;
use super::store_first_creates::{
    Call, Stages, append, create, quantiles, read_name, seeded, timed,
};
use crate::dst::fault_store::{FaultPlan, FaultStore};
use crate::shard::GroupTiming;
use crate::store_timing::{StoreResources, TimingStore, classify};
use object_store::ObjectStore;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The streams the pile and the touches name, each once: sealed before the restart.
const TOUCHED: usize = 4_000;
/// The streams left unsealed, which the target's sealer loads at its open (the leg's 667).
const UNSEALED: usize = 667;
/// The giver's 2 s hold at the leaf's rate.
const PILE: usize = 667;
/// The leaf's touches a second after the release.
const RATE: u64 = 333;
/// How long the touches arrive after the release.
const AFTER: Duration = Duration::from_secs(8);
/// The driver's catch-up, when on: from the release the touches arrive at this multiple of `RATE`
/// (3/2) for this long, the leg's figure.
const CATCH_UP: (u64, u64, Duration) = (3, 2, Duration::from_secs(2));
/// The touches due in one of the driver's 60 s chunks at `RATE`: what the bar's p99 is over.
const CHUNK: u64 = 60 * RATE;
/// The instance's in-flight cap, its production value.
const CAP: i64 = 512;
/// A burst of one task's reads ends at a gap this long with none in flight.
const GAP: Duration = Duration::from_millis(5);

/// The store as the target sees it: a recorder above `TimingStore` (a call's start, its permit
/// wait included), the production wrapper and its pools, and a recorder beneath it (a call's
/// dispatch, once it holds its permit), over `objects` 20 ms away.
struct Chain {
    above: Arc<Stages>,
    beneath: Arc<Stages>,
    io: Arc<StoreResources>,
}

fn chain(objects: Arc<dyn ObjectStore>) -> Chain {
    let plan = FaultPlan {
        latency_pct: 100,
        latency_ms: (20, 20),
        ..FaultPlan::CLEAN
    };
    let beneath = Arc::new(Stages {
        inner: FaultStore::uniform(objects, 0x5427, plan),
        calls: std::sync::Mutex::default(),
    });
    let io = Arc::new(StoreResources::new(&crate::config::StorageConfig::default()));
    let timing: Arc<dyn ObjectStore> = Arc::new(TimingStore::new(beneath.clone(), io.clone()));
    let above = Arc::new(Stages {
        inner: timing,
        calls: std::sync::Mutex::default(),
    });
    Chain { above, beneath, io }
}

/// The target: project 0 seeds `TOUCHED` streams sealed and `UNSEALED` more unsealed over
/// `objects` with no latency, and the server restarts over `measured`, having served none of them.
async fn target(
    objects: Arc<dyn ObjectStore>,
    measured: Arc<dyn ObjectStore>,
    cli: fn(&mut crate::config::CliArgs),
) -> Cell {
    let spec = |store: Arc<dyn ObjectStore>| CellSpec {
        store,
        quotas: seeded,
        cli,
        ..CellSpec::open(1)
    };
    let before = open_cell(spec(objects)).await;
    let all: Vec<usize> = (0..TOUCHED + UNSEALED).collect();
    for chunk in all.chunks(64) {
        let made = chunk.iter().map(|n| create(&before, 0, format!("s{n}")));
        assert!(
            futures_util::future::join_all(made)
                .await
                .iter()
                .all(|s| *s == 201)
        );
        let written = chunk.iter().map(|n| append(&before, 0, format!("s{n}")));
        assert!(
            futures_util::future::join_all(written)
                .await
                .iter()
                .all(|s| *s == 200)
        );
    }
    let engine = before.state.shards.open("").expect("the leaf is open");
    for _ in 0..1_000 {
        match engine
            .seal_once(crate::shard::seal::Trigger::Requested)
            .await
        {
            Ok(report) if report.object.is_some() => {}
            Ok(_) => break,
            Err(e) => panic!("a seal of the seeded streams: {e:?}"),
        }
    }
    for chunk in all[TOUCHED..].chunks(64) {
        let written = chunk.iter().map(|n| append(&before, 0, format!("s{n}")));
        assert!(
            futures_util::future::join_all(written)
                .await
                .iter()
                .all(|s| *s == 200)
        );
    }
    drop(engine);
    engine_shutdown(&before.state).await;
    before.state.rollup.get().unwrap().db.close().await.unwrap();
    open_cell(CellSpec {
        incarnation: 1,
        ..spec(measured)
    })
    .await
}

/// The members a touch enters by, in order: the giver, the third member and the target itself.
const ENTRIES: usize = 3;
/// A member's forward places.
const PLACES: usize = 128;
/// The client's wait before it resends a `409`: its `Retry-After`.
const RETRY: Duration = Duration::from_secs(1);
/// The giver's hold, over which its pile arrived.
const HOLD: Duration = Duration::from_secs(2);
/// The tenths of a second reported, from the release.
const TENTHS: usize = 100;

/// Where a client resends a `409`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Resend {
    /// To the owner the `409` names, the target, as a direct request: the rig's driver since its
    /// relearn commit.
    Owner,
    /// To the member it entered by: the SDK's.
    Entry,
}

/// One run's shape: how the giver releases, where a `409` is resent, and whether the driver
/// catches up.
#[derive(Clone, Copy, Debug)]
struct Shape {
    release: Release,
    resend: Resend,
    catch_up: bool,
}
/// How the giver releases its pile.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Release {
    /// As today: each parked request takes a free forward place or is answered `409`.
    Wave,
    /// In step with the forward places: each parked request waits for one, in order.
    Paced,
}

/// The two forwarding members as the target sees them: their forward places, and when each
/// forward, each direct request and each `409` happened.
struct Members {
    places: [tokio::sync::Semaphore; 2],
    forwards: std::sync::Mutex<Vec<Instant>>,
    direct: std::sync::Mutex<Vec<Instant>>,
    refused: std::sync::Mutex<Vec<Instant>>,
}

/// One touch's end: when it was due (a parked one's arrival at the giver), when its last answer
/// came, its status and code, and how many `409`s it was answered first.
struct Touched {
    due: Instant,
    answered: Instant,
    status: u16,
    code: Option<String>,
    refusals: u32,
}

/// When each touch is due from the hold's start, whether it was parked, and the member it enters
/// by: the pile's arrived at the giver over its hold; the rest arrive from the release at `RATE`,
/// or at the catch-up's multiple of it for its time, round robin over the giver, the third member
/// and the target.
fn schedule(catch_up: bool) -> Vec<(Duration, bool, usize)> {
    let pile = u64::try_from(PILE).unwrap();
    let mut due: Vec<(Duration, bool, usize)> = (0..pile)
        .map(|k| (Duration::from_micros(k * 2_000_000 / pile), true, 0))
        .collect();
    let (times, over, fast) = CATCH_UP;
    let quick = if catch_up { fast } else { Duration::ZERO };
    let quick_rate = RATE * times / over;
    let quick_n = quick_rate * quick.as_secs();
    let rest_n = RATE * (AFTER - quick).as_secs();
    let offsets = (0..quick_n)
        .map(|i| Duration::from_micros(i * 1_000_000 / quick_rate))
        .chain((0..rest_n).map(|j| quick + Duration::from_micros(j * 1_000_000 / RATE)));
    for (n, at) in offsets.enumerate() {
        due.push((HOLD + at, false, n % ENTRIES));
    }
    due
}

/// The touch `k`, due `due` after the hold began (`t0 - HOLD`; a parked one is released at `t0`),
/// entering by `entry`. A request entering a forwarding member takes one of its places for its
/// forward's life, or is answered `409` and resent after `RETRY` where `shape` says; a parked one
/// under a paced release waits for a place instead.
async fn touch(
    (cell, members): (&Cell, &Members),
    k: usize,
    (due, pile, mut entry): (Duration, bool, usize),
    (shape, t0): (Shape, tokio::time::Instant),
) -> Touched {
    let due = t0 - HOLD + due;
    tokio::time::sleep_until(due.max(t0)).await;
    let mut refusals = 0;
    loop {
        let place = match members.places.get(entry) {
            None => Ok(None),
            Some(places) if pile && shape.release == Release::Paced => {
                Ok(Some(places.acquire().await.expect("the places stay open")))
            }
            Some(places) => places.try_acquire().map(Some),
        };
        let Ok(place) = place else {
            refusals += 1;
            members.refused.lock().unwrap().push(Instant::now());
            if shape.resend == Resend::Owner {
                entry = ENTRIES - 1;
            }
            tokio::time::sleep(RETRY).await;
            continue;
        };
        let arrivals = if place.is_some() {
            &members.forwards
        } else {
            &members.direct
        };
        arrivals.lock().unwrap().push(Instant::now());
        let path = format!("/v1/streams/s{k}/records");
        let (_, status, code, _) = timed(cell, format!("s{k}"), "POST", path, br#"{"n":1}"#).await;
        drop(place);
        return Touched {
            due: due.into_std(),
            answered: Instant::now(),
            status,
            code,
            refusals,
        };
    }
}

/// What a release left: each touch's end in order, the target's requests in flight at their
/// peak each tenth of a second, the commit groups seen, the gate's verdicts it judged, the
/// members' record, and when it began.
struct Released {
    touched: Vec<Touched>,
    peaks: Vec<i64>,
    groups: Vec<GroupTiming>,
    verdicts: [u64; 4],
    members: Members,
    began: Instant,
    began_ms: i64,
}

/// The commit groups `engine` has recorded (`ShardEngine::timings`) that `seen` has not, added to
/// `groups`.
fn kept(
    engine: &crate::shard::ShardEngine,
    seen: &mut HashSet<String>,
    groups: &mut Vec<GroupTiming>,
) {
    for group in engine.timings.lock().unwrap().iter() {
        if seen.insert(format!("{group:?}")) {
            groups.push(*group);
        }
    }
}

/// The pile released as `shape` says and the touches after it, to `cell` whose leaf `engine` has
/// just opened; the requests in flight and the commit groups sampled meanwhile.
async fn release(cell: &Cell, engine: &crate::shard::ShardEngine, shape: Shape) -> Released {
    let before = cell.state.admission.gate_verdicts();
    let members = Members {
        places: [(); 2].map(|()| tokio::sync::Semaphore::new(PLACES)),
        forwards: std::sync::Mutex::default(),
        direct: std::sync::Mutex::default(),
        refused: std::sync::Mutex::default(),
    };
    let (t0, began_ms) = (tokio::time::Instant::now(), crate::shard::now_ms());
    let began = t0.into_std();
    let touches = schedule(shape.catch_up)
        .into_iter()
        .enumerate()
        .map(|(k, due)| touch((cell, &members), k, due, (shape, t0)));
    let stop = AtomicBool::new(false);
    let sample = async {
        let (mut peaks, mut groups, mut seen) = (vec![0i64; TENTHS], Vec::new(), HashSet::new());
        for tick in 0u64.. {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let tenth = usize::try_from(began.elapsed().as_millis() / 100).unwrap();
            if let Some(peak) = peaks.get_mut(tenth) {
                *peak = (*peak).max(cell.state.admission.active());
            }
            if tick % 20 == 0 {
                kept(engine, &mut seen, &mut groups);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        kept(engine, &mut seen, &mut groups);
        (peaks, groups)
    };
    let sent = async {
        let touched = futures_util::future::join_all(touches).await;
        stop.store(true, Ordering::Relaxed);
        touched
    };
    let ((peaks, groups), touched) = futures_util::future::join(sample, sent).await;
    let after = cell.state.admission.gate_verdicts();
    Released {
        touched,
        peaks,
        groups,
        verdicts: [0, 1, 2, 3].map(|i| after[i] - before[i]),
        members,
        began,
        began_ms,
    }
}

/// A shed on the product surface: `429 rate_limited`.
fn shed(touched: &Touched) -> bool {
    touched.status == 429 && touched.code.as_deref() == Some("rate_limited")
}

/// The gate's line for what it judged (`fair_shed`: a spared write's is a quarter over the cap),
/// the answers, the sheds and the `409`s, and each parked request's time from its park to its
/// answer, with those over `HOLD` as a share of a 60 s chunk's touches.
fn answers(run: &Released) -> String {
    let [blamed, spared, even, none] = run.verdicts;
    let line = if spared > 0 && blamed == 0 {
        CAP * 5 / 4
    } else {
        CAP
    };
    let mut statuses: BTreeMap<(u16, Option<String>), usize> = BTreeMap::new();
    for touched in &run.touched {
        *statuses
            .entry((touched.status, touched.code.clone()))
            .or_default() += 1;
    }
    let parked = &run.touched[..PILE];
    let waited: Vec<u128> = parked
        .iter()
        .map(|t| t.answered.saturating_duration_since(t.due).as_millis())
        .collect();
    let over = waited.iter().filter(|ms| **ms > HOLD.as_millis()).count();
    let share = u64::try_from(over).unwrap() * 10_000 / CHUNK;
    format!(
        "verdicts (blamed, spared, even, none) ({blamed}, {spared}, {even}, {none}), line {line}; \
         {} touches, answers {statuses:?}; sheds {}; 409s {} ({} of the pile); the pile's {PILE}, \
         park to answer {}, {over} over {} ms ({}.{:02}% of a 60 s chunk's {CHUNK} touches)",
        run.touched.len(),
        run.touched.iter().filter(|t| shed(t)).count(),
        run.touched.iter().map(|t| t.refusals).sum::<u32>(),
        parked.iter().map(|t| t.refusals).sum::<u32>(),
        quantiles(waited),
        HOLD.as_millis(),
        share / 100,
        share % 100,
    )
}

/// Counts of `times` by tenth of a second from `began`.
fn by_tenth(times: impl Iterator<Item = Instant>, began: Instant) -> Vec<usize> {
    let mut by = vec![0usize; TENTHS];
    for at in times {
        let tenth = usize::try_from(at.saturating_duration_since(began).as_millis() / 100).unwrap();
        if let Some(n) = by.get_mut(tenth) {
            *n += 1;
        }
    }
    by
}

/// The commit groups acked in each tenth of a second from the release: their requests, and their
/// longest queue wait in ms (the cold loads of their prepare included).
fn groups_by_tenth(run: &Released) -> (Vec<u32>, Vec<u32>) {
    let (mut reqs, mut queue) = (vec![0u32; TENTHS], vec![0u32; TENTHS]);
    for group in &run.groups {
        let at = usize::try_from((group.ts_ms - run.began_ms).max(0) / 100).unwrap();
        if let (Some(r), Some(q)) = (reqs.get_mut(at), queue.get_mut(at)) {
            *r += group.reqs;
            *q = (*q).max(group.queue_wait_us / 1_000);
        }
    }
    (reqs, queue)
}

/// Each tenth of a second from the release on the target: its requests in flight at their peak,
/// the forwards and the direct requests that arrived, the sheds answered, the `409`s the members
/// answered, and the commit groups acked; one line a second, the tenths with nothing left out.
fn tenths(run: &Released) -> String {
    let times = |m: &std::sync::Mutex<Vec<Instant>>| m.lock().unwrap().clone();
    let forwards = by_tenth(times(&run.members.forwards).into_iter(), run.began);
    let direct = by_tenth(times(&run.members.direct).into_iter(), run.began);
    let refused = by_tenth(times(&run.members.refused).into_iter(), run.began);
    let sheds = by_tenth(
        run.touched.iter().filter(|t| shed(t)).map(|t| t.answered),
        run.began,
    );
    let (acked, queued) = groups_by_tenth(run);
    let mut seconds = Vec::new();
    for second in 0..TENTHS / 10 {
        let shown: Vec<String> = (second * 10..second * 10 + 10)
            .filter(|i| {
                run.peaks[*i] > 0 || forwards[*i] + direct[*i] + sheds[*i] + refused[*i] > 0
            })
            .map(|i| {
                format!(
                    "{i}: in {} fwd {} dir {} shed {} 409 {} acked {} (queue {} ms)",
                    run.peaks[i], forwards[i], direct[i], sheds[i], refused[i], acked[i], queued[i]
                )
            })
            .collect();
        if !shown.is_empty() {
            seconds.push(shown.join("; "));
        }
    }
    format!(
        "by tenth of a second from the release (in flight at the peak, forwards and direct \
         requests arriving, sheds, 409s, the commit groups' requests acked and their longest queue \
         wait, the cold loads of their prepare included; the forward hop's latency is not \
         modelled, which shortens how long a forward place is held):\nMOVE_RELEASE     {}",
        seconds.join("\nMOVE_RELEASE     ")
    )
}

/// A task and a call's path: the key a call above `TimingStore` and its dispatch beneath share.
type Key = (Option<tokio::task::Id>, String);

/// A call's task, its store class and its wait for a count permit in milliseconds.
type Waited = (Option<tokio::task::Id>, u8, u128);

/// Each call's wait for its count permit (its start above `TimingStore` to its dispatch beneath
/// it, matched in order by task and path: the pools hand out permits in order), with its task and
/// store class; and how many calls above found no dispatch.
fn waits(above: &[Call], beneath: &[Call]) -> (Vec<Waited>, usize) {
    let mut dispatched: HashMap<Key, VecDeque<Instant>> = HashMap::new();
    let mut under = beneath.to_vec();
    under.sort_by_key(|call| call.3);
    for (task, _, path, at, _) in under {
        dispatched.entry((task, path)).or_default().push_back(at);
    }
    let mut over = above.to_vec();
    over.sort_by_key(|call| call.3);
    let (mut out, mut unmatched) = (Vec::new(), 0);
    for (task, _, path, began, _) in over {
        let class = classify(&path);
        match dispatched
            .get_mut(&(task, path))
            .and_then(VecDeque::pop_front)
        {
            Some(at) => out.push((task, class, at.saturating_duration_since(began).as_millis())),
            None => unmatched += 1,
        }
    }
    (out, unmatched)
}

/// The requests' tasks: the task each descriptor read ran in names its stream's request.
fn request_tasks(above: &[Call]) -> HashSet<tokio::task::Id> {
    above
        .iter()
        .filter(|(_, kind, path, _, _)| *kind == "GET" && read_name(path).is_some())
        .filter_map(|(task, _, _, _, _)| *task)
        .collect()
}

/// Each request's own wait for a permit by class (its calls' waits summed), and the other tasks'
/// calls and waits by class.
fn permit_waits(above: &[Call], beneath: &[Call]) -> String {
    let requests = request_tasks(above);
    let (waited, unmatched) = waits(above, beneath);
    let mut own: BTreeMap<u8, HashMap<tokio::task::Id, u128>> = BTreeMap::new();
    let mut others: BTreeMap<u8, Vec<u128>> = BTreeMap::new();
    for (task, class, ms) in waited {
        match task.filter(|t| requests.contains(t)) {
            Some(t) => *own.entry(class).or_default().entry(t).or_default() += ms,
            None => others.entry(class).or_default().push(ms),
        }
    }
    let own: Vec<String> = own
        .into_iter()
        .map(|(class, by)| {
            let n = by.len();
            format!(
                "class {class} ({n} requests) {}",
                quantiles(by.into_values().collect())
            )
        })
        .collect();
    let others: Vec<String> = others
        .into_iter()
        .map(|(class, ms)| format!("class {class} ({} calls) {}", ms.len(), quantiles(ms)))
        .collect();
    format!(
        "permit waits: each request's own, by class [{}]; the other tasks', by class [{}]; {} \
         calls unmatched ({} requests' tasks)",
        own.join("; "),
        others.join("; "),
        unmatched,
        requests.len()
    )
}

/// The general pool's holders (classes 2 and 4, from dispatch to answer beneath the wrapper) at
/// their peak each second, by class.
fn pool_holders(beneath: &[Call], began: Instant) -> String {
    let mut seconds = Vec::new();
    for class in [2u8, 4] {
        let mut events: Vec<(Instant, i64)> = beneath
            .iter()
            .filter(|(_, _, path, _, _)| classify(path) == class)
            .flat_map(|(_, _, _, at, ended)| [(*at, 1), (*ended, -1)])
            .collect();
        events.sort();
        let (mut held, mut peaks) = (0i64, vec![0i64; 12]);
        for (at, step) in events {
            held += step;
            let s = usize::try_from(at.saturating_duration_since(began).as_secs()).unwrap();
            if let Some(peak) = peaks.get_mut(s) {
                *peak = (*peak).max(held);
            }
        }
        seconds.push(format!("class {class} {peaks:?}"));
    }
    format!(
        "general pool's holders at their peak, each second: {}",
        seconds.join("; ")
    )
}

/// One task's bursts of calls: the calls in flight from one to the next gap of `GAP` with none,
/// each its start from `began`, its length, its calls and its most in flight at once.
fn bursts(mut calls: Vec<(Instant, Instant)>, began: Instant) -> Vec<(u128, u128, usize, usize)> {
    calls.sort();
    let mut out: Vec<(u128, u128, usize, usize)> = Vec::new();
    let mut open: Vec<Instant> = Vec::new();
    let mut end = began;
    for (at, ended) in calls {
        open.retain(|e| *e > at);
        if (open.is_empty() && at.saturating_duration_since(end) >= GAP) || out.is_empty() {
            out.push((at.saturating_duration_since(began).as_millis(), 0, 0, 0));
        }
        open.push(ended);
        end = end.max(ended);
        let last = out.last_mut().unwrap();
        last.1 = end.saturating_duration_since(began).as_millis() - last.0;
        last.2 += 1;
        last.3 = last.3.max(open.len());
    }
    out
}

/// The cold loads: the tasks besides the requests' that read the shard's log most, the bursts of
/// the first (the committer's prepare), and the commit groups after the release.
fn cold_loads(above: &[Call], run: &Released) -> String {
    let requests = request_tasks(above);
    let mut by_task: HashMap<tokio::task::Id, Vec<(Instant, Instant)>> = HashMap::new();
    for (task, kind, path, at, ended) in above {
        if let Some(t) = task.filter(|t| !requests.contains(t))
            && *kind == "GET"
            && classify(path) == 2
        {
            by_task.entry(t).or_default().push((*at, *ended));
        }
    }
    let mut tasks: Vec<_> = by_task.into_iter().collect();
    tasks.sort_by_key(|(_, calls)| std::cmp::Reverse(calls.len()));
    let top: Vec<usize> = tasks.iter().take(3).map(|(_, calls)| calls.len()).collect();
    let shown: Vec<String> = tasks
        .first()
        .map(|(_, calls)| bursts(calls.clone(), run.began))
        .unwrap_or_default()
        .iter()
        .take(16)
        .map(|(at, took, calls, most)| format!("+{at} {took} ms {calls} reads ({most} at once)"))
        .collect();
    let mut groups = run.groups.clone();
    groups.sort_by_key(|g| g.ts_ms);
    let ms = |us: u32| us / 1_000;
    let first: Vec<String> = groups
        .iter()
        .take(16)
        .map(|g| {
            format!(
                "+{} {} reqs: queue {} encode {} write {} durable {}",
                g.ts_ms - run.began_ms,
                g.reqs,
                ms(g.queue_wait_us),
                ms(g.encode_us),
                ms(g.write_us),
                ms(g.durable_wait_us)
            )
        })
        .collect();
    let queued = groups
        .iter()
        .map(|g| u128::from(ms(g.queue_wait_us)))
        .collect();
    format!(
        "cold loads: SST reads by the three tasks that read most {top:?}; the first's bursts \
         [{}]\nMOVE_RELEASE   commit groups {} ({} reqs), queue wait {}; the first [{}]",
        shown.join("; "),
        groups.len(),
        groups.iter().map(|g| g.reqs).sum::<u32>(),
        quantiles(queued),
        first.join("; ")
    )
}

/// One release shaped by `shape` on a target over a fresh store, its project's line the cap (a
/// cell shared eight ways): its lines.
async fn released(shape: Shape) -> String {
    let objects = super::fixture_storage::mem();
    let chain = chain(objects.clone());
    let shared: fn(&mut crate::config::CliArgs) = |cli| cli.project_share_k = 8;
    let cell = target(objects, chain.above.clone(), shared).await;
    cell.state.admission.set_max_inflight(CAP);
    let opening = Instant::now();
    let engine = match cell
        .state
        .shards
        .open_or_wait("", Duration::from_secs(60))
        .await
    {
        crate::sharddir::OpenOutcome::Ready(engine) => engine,
        _ => panic!("the target opens its leaf"),
    };
    let open_ms = opening.elapsed().as_millis();
    chain.above.calls.lock().unwrap().clear();
    chain.beneath.calls.lock().unwrap().clear();
    let run = release(&cell, &engine, shape).await;
    drop(engine);
    engine_shutdown(&cell.state).await;
    let above = std::mem::take(&mut *chain.above.calls.lock().unwrap());
    let beneath = std::mem::take(&mut *chain.beneath.calls.lock().unwrap());
    format!(
        "MOVE_RELEASE {shape:?}: the leaf opened in {open_ms} ms; {}; pools (peak, waits) general \
         {:?} commit {:?}\nMOVE_RELEASE   {}\nMOVE_RELEASE   {}\nMOVE_RELEASE   {}\n\
         MOVE_RELEASE   {}",
        answers(&run),
        chain.io.count_seen()[0],
        chain.io.count_seen()[1],
        tenths(&run),
        permit_waits(&above, &beneath),
        pool_holders(&beneath, run.began),
        cold_loads(&above, &run),
    )
}

/// **A move's release on a target that has just opened** (a diagnosis for Addendum 16 of the s22
/// join ruling, after Addendum 27 (a) of the forward-wedge ruling; run under the machine's lock;
/// ignored in the suite): the pile a 2 s hold collects at the leaf's rate, released as today (a
/// wave: a forward place or a `409`, resent a second later) and in step with the forward places;
/// each `409` resent to its owner (the rig's driver) and to its entry (the SDK); with the driver's
/// catch-up off and on; beside the touches at that rate entering by three members, each on a
/// sealed stream the target has never loaded, at the in-flight cap of 512 with the store's pools
/// as built. Nothing is asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a diagnosis, run under the machine's lock"]
async fn a_moves_release_on_a_target_that_has_just_opened() {
    let mut shapes = Vec::new();
    for release in [Release::Wave, Release::Paced] {
        for resend in [Resend::Owner, Resend::Entry] {
            shapes.extend([false, true].map(|catch_up| Shape {
                release,
                resend,
                catch_up,
            }));
        }
    }
    for shape in shapes {
        println!("{}", released(shape).await);
    }
}
