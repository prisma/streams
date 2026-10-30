#![cfg(test)]
//! Merges (DESIGN-6A §2.3): which objects the next merge takes (each
//! input at most a quarter of the budget, together at most half, at least
//! two, `fan_in` of the lowest level that has them); what the merged object
//! keeps and counts; and that the loop's merge step merges until none is
//! due.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rand::{SeedableRng, rngs::StdRng};

use super::merge::{Input, merge_inputs, merged};
use super::{FoldSettings, Reclaimed, finish};
use crate::shard::seal::Trigger;
use crate::shard::seal::tests::{append_recorded, manual, random_bytes, rig_with};
use crate::spill::directory::{DirEntry, KeySummary};
use crate::spill::object::ObjectId;
use crate::spill::runindex::format::{self, ObjectWriter, TRAILER_LEN};
use crate::spill::runindex::{Orphans, Pointer};

fn pointer(seq: u64, level: u8, len: u64) -> (ObjectId, Pointer) {
    let id = ObjectId { epoch: 1, seq };
    let p = Pointer {
        id,
        level,
        len,
        footer_off: 0,
        footer_len: 0,
        runs: 1,
        entries: 1,
        seals: 1,
        orphans: Orphans::default(),
        created_ms: 0,
    };
    (id, p)
}

/// The sequence numbers of the next merge's inputs over objects of
/// `(level, len)`, numbered from 1, under a 1000-byte budget.
fn picked(objects: &[(u8, u64)], fan_in: [usize; 2]) -> Option<Vec<u64>> {
    let set: BTreeMap<ObjectId, Pointer> = (1..)
        .zip(objects)
        .map(|(seq, &(level, len))| pointer(seq, level, len))
        .collect();
    let s = FoldSettings {
        fan_in,
        merge_bytes: 1000,
        ..FoldSettings::default()
    };
    merge_inputs(&set, &s).map(|inputs| inputs.iter().map(|p| p.id.seq).collect())
}

#[test]
fn a_merge_takes_the_oldest_inputs_within_its_budget() {
    assert_eq!(picked(&[(0, 100), (0, 100)], [2, 2]), Some(vec![1, 2]));
    assert_eq!(picked(&[(0, 100)], [2, 2]), None, "one object is no merge");
    assert_eq!(
        picked(&[(0, 251), (0, 100), (0, 100)], [2, 2]),
        Some(vec![2, 3]),
        "an object over a quarter of the budget is never an input"
    );
    assert_eq!(
        picked(&[(0, 200), (0, 200), (0, 100)], [3, 2]),
        Some(vec![1, 2, 3]),
        "inputs filling exactly half the budget"
    );
    assert_eq!(
        picked(&[(0, 250), (0, 250), (0, 250)], [3, 2]),
        Some(vec![1, 2]),
        "the inputs stop before passing half the budget"
    );
    assert_eq!(
        picked(&[(1, 100), (0, 100), (1, 100)], [3, 2]),
        Some(vec![1, 3]),
        "a level short of its fan-in waits; the next level merges"
    );
    assert_eq!(
        picked(&[(2, 100), (2, 100)], [2, 2]),
        None,
        "daily objects never merge"
    );
}

fn entry(seq: u64, first: u64, last: u64) -> DirEntry {
    DirEntry {
        object: ObjectId { epoch: 1, seq },
        chunk_off: 0,
        chunk_len: 100,
        first,
        last,
        page_count: 1,
        keys: KeySummary::Keys(vec![1]),
        anchors: Vec::new(),
    }
}

/// An input object of `streams`, read back as a merge reads it.
fn input(seq: u64, seals: u64, orphans: Orphans, streams: &[([u8; 16], Vec<DirEntry>)]) -> Input {
    let mut writer = ObjectWriter::new();
    for (hash, entries) in streams {
        writer.push(*hash, entries).unwrap();
    }
    let built = finish(ObjectId { epoch: 1, seq }, writer, 0, seals, orphans)
        .unwrap()
        .unwrap();
    let body = Bytes::from(built.bytes);
    let split = body.len() - TRAILER_LEN;
    let (trailer, crc) = format::decode_trailer(&body[split..]).unwrap();
    let start = usize::try_from(trailer.footer_off).unwrap();
    let footer = format::checked_footer(&body[start..split], &trailer, crc).unwrap();
    Input {
        pointer: built.pointer,
        body,
        footer,
    }
}

#[test]
fn a_merge_drops_the_entries_a_later_chunk_contains_and_sums_its_inputs() {
    let (a, b) = ([0xA1; 16], [0xB2; 16]);
    let o = |chunks, bytes| Orphans { chunks, bytes };
    let inputs = [
        input(
            10,
            2,
            o(1, 10),
            &[
                (a, vec![entry(1, 0, 3), entry(2, 4, 7)]),
                (b, vec![entry(3, 0, 0)]),
            ],
        ),
        input(11, 3, o(2, 20), &[(a, vec![entry(4, 0, 7)])]),
    ];
    let built = merged(
        ObjectId { epoch: 1, seq: 12 },
        1,
        &inputs,
        &Reclaimed::new(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        built.dropped, 2,
        "the two chunks the consolidated one holds"
    );
    let p = &built.pointer;
    assert_eq!((p.level, p.runs, p.entries, p.seals), (1, 2, 2, 5));
    assert_eq!(p.orphans, o(3, 30));
    let reclaimed = Reclaimed::from([(a, 8)]);
    let built = merged(ObjectId { epoch: 1, seq: 13 }, 1, &inputs, &reclaimed)
        .unwrap()
        .unwrap();
    assert_eq!(
        (built.dropped, built.pointer.runs, built.pointer.entries),
        (3, 1, 1),
        "a dead stream's entries below its reclaimed offset leave too"
    );
}

/// The loop's merge step merges until none is due: four fold objects
/// under a fan-in of two become one daily object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_merge_step_merges_until_none_is_due() {
    let rig = rig_with("merge-logged", manual(), 0, Duration::from_millis(2)).await;
    rig.engine.layout.seal.fold.configure(|s| {
        s.fan_in = [2, 2];
        s.auto = false;
    });
    let oracle = std::sync::Mutex::new(crate::shard::seal::tests::Oracle::new());
    let mut rng = StdRng::seed_from_u64(0x3E6);
    for _ in 0..4 {
        let records = vec![random_bytes(&mut rng, 300)];
        append_recorded(&rig.engine, &oracle, [0x3E; 16], records).await;
        rig.engine.seal_once(Trigger::Requested).await.unwrap();
        rig.engine.fold_once().await.unwrap();
    }
    let levels = |engine: &Arc<crate::shard::ShardEngine>| {
        let set = engine.layout.seal.fold.set();
        set.values().map(|p| p.level).collect::<Vec<_>>()
    };
    assert_eq!(levels(&rig.engine), vec![0; 4]);
    tokio::time::timeout(Duration::from_secs(10), rig.engine.merge_logged())
        .await
        .expect("the merges settle");
    assert_eq!(levels(&rig.engine), vec![2]);
    rig.engine.begin_close();
}
