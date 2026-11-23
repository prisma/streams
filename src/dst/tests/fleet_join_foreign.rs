//! A foreign write of the overrides document (C18, GATE-DECISION-s22-join-2026-11-20 Addendum 33):
//! a write that drops `members`, or a running member's boot id, is repaired in place by the
//! running members. No member unlists itself on C13's boot-time verdict after its decision, so
//! no engine is fenced and no shard is yielded, and the written entries are honoured meanwhile.
//! The blind write is the cert's old steering (`bench/fleet/livefeed-cert.mjs`, #183's red). A
//! member that loses the race to relist it keeps itself in its own ring meanwhile (C18b, Addendum
//! 35).

use super::super::fixture_http::HttpRig;
use super::super::fixture_runtime::RigRuntime;
use super::super::fixture_storage::mem;
use super::{LEAVES, YieldWatch, desired, member, nothing_fenced, stop, stored, until};
use crate::dst::fault_store::{FaultProfile, FaultStore};
use crate::dst::{ObjClass, StoreOp};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// The tick passes (2 s each) a dropped `members` gets to be repaired: C17's listing at one
/// member's next pass, each other member's own stamp at its next. At most twice the worst restore
/// `a_write_that_drops_members_is_repaired_in_place_without_a_fence` printed over ten runs
/// (`C18_RESTORE`, 4019-4133 ms; the join ruling, Addenda 34 and 35).
const MEMBERS_PASSES: u64 = 4;

/// The passes a dropped boot id gets: the member's own stamp at its next pass. At most twice the
/// worst of ten runs (1960-2020 ms; Addendum 35).
const BOOT_ID_PASSES: u64 = 2;

/// The overrides document, under the fleet prefix the rigs' fleet stores are rooted at.
const OVERRIDES: &str = "fleet/overrides.json";

/// Three members serving, each over its own fault store on the one fleet store.
struct Serving {
    rigs: Vec<HttpRig>,
    /// Their listing in order: name and boot id.
    listing: Vec<(String, Option<String>)>,
    /// Each member's fleet store, unarmed.
    stores: Vec<Arc<FaultStore>>,
}

/// Three members, seeded joined (the join ruling, Addendum 23 §2 (b): the join is not the
/// subject), each serving the leaves it owns but `unopened`.
async fn three_serving(fleet: &Arc<dyn ObjectStore>, unopened: &str) -> Serving {
    let data = mem();
    desired(fleet, 3, 1).await;
    let names = ["streams-1", "streams-2", "streams-3"];
    let runtimes: Vec<RigRuntime> = (0..3).map(RigRuntime::incarnation).collect();
    let seeded: Vec<(&str, &RigRuntime)> = names.iter().copied().zip(runtimes.iter()).collect();
    super::super::fixture_members::seed_members(fleet, &seeded).await;
    let listing: Vec<(String, Option<String>)> = names
        .iter()
        .zip(&runtimes)
        .map(|(name, runtime)| {
            let boot = runtime.caps.identity.boot_id.clone();
            ((*name).to_string(), Some(boot))
        })
        .collect();
    let stores: Vec<Arc<FaultStore>> = (0..3)
        .map(|seed| FaultStore::new(fleet.clone(), 0xC18 + seed, FaultProfile::clean()))
        .collect();
    let mut rigs = Vec::new();
    for ((name, runtime), store) in names.iter().zip(runtimes).zip(&stores) {
        rigs.push(member(&data, store.clone(), name, runtime).await);
    }
    let all: Vec<&HttpRig> = rigs.iter().collect();
    super::super::fixture_members::joined(fleet, &all, Duration::from_secs(60)).await;
    for rig in &rigs {
        serve_owned(rig, unopened).await;
    }
    Serving {
        rigs,
        listing,
        stores,
    }
}

/// `rig` opens each leaf it owns but `unopened`.
async fn serve_owned(rig: &HttpRig, unopened: &str) {
    let me = rig.state.config.cli.instance_name.as_str();
    for leaf in LEAVES.iter().filter(|leaf| **leaf != unopened) {
        if rig.state.ownership.effective_owner(leaf).as_deref() == Some(me) {
            let opened = rig
                .state
                .shards
                .open_or_wait(leaf, Duration::from_secs(5))
                .await;
            assert!(
                matches!(opened, crate::sharddir::OpenOutcome::Ready(_)),
                "{leaf}"
            );
        }
    }
}

/// For `passes` passes after a foreign write made at `written`: whether a listing the product
/// wrote ever lacked one of `listing`'s names (a document with no `members` is the foreign write
/// itself, or C17's moment before its relisting, and is not judged), and how long after the write
/// the document came back to exactly `listing` and stayed (`None`: not by the end), printed as
/// `C18_RESTORE`.
async fn repaired(
    fleet: &Arc<dyn ObjectStore>,
    listing: &[(String, Option<String>)],
    (case, written, passes): (&str, tokio::time::Instant, u64),
) -> (bool, Option<Duration>) {
    let names: Vec<&String> = listing.iter().map(|(name, _)| name).collect();
    let (mut unlisted, mut back) = (false, None);
    let deadline = written + Duration::from_secs(2 * passes);
    while tokio::time::Instant::now() < deadline {
        let (members, _) = stored(fleet).await;
        if !members.is_empty() {
            let has = |name: &&String| members.iter().any(|(listed, _)| listed == *name);
            unlisted |= !names.iter().all(has);
        }
        back = if members.as_slice() == listing {
            back.or(Some(written.elapsed()))
        } else {
            None
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ms = back.map(|took| took.as_millis());
    eprintln!("C18_RESTORE case={case} ms={ms:?}");
    (unlisted, back)
}

/// Every rig's tick yields since `watches` began.
fn yields(rigs: &[HttpRig], watches: &[YieldWatch]) -> Vec<u64> {
    rigs.iter()
        .zip(watches)
        .map(|(rig, watch)| watch.yields(rig))
        .collect()
}

/// **A write that drops `members` is repaired in place** (C18, C18b). Three members serving; an
/// unconditional write of `{ entries }` alone, giving `01` to `streams-3` (not its ring home; its
/// holder has not opened it, since a leaf a written entry moves is yielded at its holder's tick,
/// as any override's always was). Each member's relisting CAS (C17) is parked until all three are,
/// then `streams-2`'s lands first and the other two lose theirs: each publishes the document it
/// reads after the loss, which lists it with no boot id. Within [`MEMBERS_PASSES`] passes the
/// document lists the same three with the same boot ids, no listing the product writes lacks one
/// of them, nothing is fenced or yielded, and every member's view gives `01` to `streams-3`. Red
/// without C18 (3d3dba96e): a member that finds itself relisted unstamped unlists itself on its
/// boot-time verdict, and its shards are yielded. Red without C18b: each loser leaves its own ring
/// for that pass and yields its open shards at its tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_write_that_drops_members_is_repaired_in_place_without_a_fence() {
    let fleet = mem();
    let Serving {
        rigs,
        listing,
        stores,
    } = three_serving(&fleet, "01").await;
    let watches: Vec<YieldWatch> = rigs.iter().map(YieldWatch::of).collect();
    let parked: Vec<Arc<AtomicU64>> = stores
        .iter()
        .map(|store| store.hold_under_after(StoreOp::Put, ObjClass::Fleet, OVERRIDES, 0))
        .collect();
    let now = crate::shard::now_ms();
    let blind = format!(r#"{{"entries":{{"01":{{"to":"streams-3","ms":{now}}}}}}}"#);
    let written = tokio::time::Instant::now();
    let path = Path::from(OVERRIDES);
    fleet.put(&path, PutPayload::from(blind)).await.unwrap();
    until("every member's relisting CAS is parked", async || {
        parked.iter().all(|seen| seen.load(Ordering::SeqCst) >= 1)
    })
    .await;
    stores[1].release_hold();
    until("streams-2's relisting lands", async || {
        !stored(&fleet).await.0.is_empty()
    })
    .await;
    stores[0].release_hold();
    stores[2].release_hold();
    let case = ("members dropped", written, MEMBERS_PASSES);
    let (unlisted, back) = repaired(&fleet, &listing, case).await;
    let all: Vec<&HttpRig> = rigs.iter().collect();
    let honoured = rigs
        .iter()
        .all(|rig| rig.state.ownership.effective_owner("01").as_deref() == Some("streams-3"));
    assert_eq!(
        (
            unlisted,
            back.is_some(),
            nothing_fenced(&all),
            yields(&rigs, &watches),
            honoured
        ),
        (false, true, vec![(0, 0); 3], vec![0; 3], true),
        "(a member unlisted after the blind write, members back with their boot ids within {MEMBERS_PASSES} passes, fences, tick yields, the written entry honoured)"
    );
    for rig in rigs {
        stop(rig).await;
    }
}

/// **A write that drops one running member's boot id is repaired in place** (C18). Three members
/// serving; `streams-3`'s listing written back without its boot id, the others intact. Within
/// [`BOOT_ID_PASSES`] passes `streams-3` stamps its own boot id back, no listing ever lacks it, and
/// nothing is fenced or yielded. Red without C18: `streams-3` unlists itself on its boot-time
/// verdict and rejoins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_write_that_drops_a_members_boot_id_is_repaired_in_place() {
    let fleet = mem();
    let Serving { rigs, listing, .. } = three_serving(&fleet, "").await;
    let watches: Vec<YieldWatch> = rigs.iter().map(YieldWatch::of).collect();
    let path = Path::from(OVERRIDES);
    let got = fleet.get(&path).await.unwrap().bytes().await.unwrap();
    let mut document: crate::fleet::Overrides = serde_json::from_slice(&got).unwrap();
    for listed in document.members.iter_mut().flatten() {
        if listed.name == "streams-3" {
            listed.boot_id = None;
        }
    }
    let body = serde_json::to_vec(&document).unwrap();
    let written = tokio::time::Instant::now();
    fleet.put(&path, PutPayload::from(body)).await.unwrap();
    let case = ("a boot id dropped", written, BOOT_ID_PASSES);
    let (unlisted, back) = repaired(&fleet, &listing, case).await;
    let all: Vec<&HttpRig> = rigs.iter().collect();
    assert_eq!(
        (
            unlisted,
            back.is_some(),
            nothing_fenced(&all),
            yields(&rigs, &watches)
        ),
        (false, true, vec![(0, 0); 3], vec![0; 3]),
        "(streams-3 unlisted after its boot id was dropped, members back within {BOOT_ID_PASSES} passes, fences, tick yields)"
    );
    for rig in rigs {
        stop(rig).await;
    }
}
