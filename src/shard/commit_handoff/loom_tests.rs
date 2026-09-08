//! Actual CommitHandoff transitions under Loom mutexes, not a rewritten model.
//! Three spawned threads maximum, preemption bound 2, 1,000 branches per
//! execution; no duration/permutation truncation. Tokio channel delivery and
//! the rest of the server remain covered by held-WAL integration tests.
use super::*;
use loom::sync::{Arc, Mutex};

fn explore(f: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    model.max_threads = 4;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.max_permutations = None;
    model.max_duration = None;
    model.check(f);
}

fn group(seq: u64) -> InFlightGroup {
    InFlightGroup {
        seq,
        written_at: std::time::Instant::now(),
        queue_wait_us: 0,
        encode_us: 0,
        write_us: 0,
        reqs: 0,
        records_n: 0,
        bytes: 0,
        effects: DurableEffects::default(),
    }
}

#[test]
fn quality_loom_held_wal_retry_cannot_turn_retirement_into_durability() {
    explore(|| {
        let mut handoff = CommitHandoff::default();
        handoff.publication().unwrap().push(group(1));
        let state = Arc::new(Mutex::new(handoff));
        let retry = state.clone();
        let retry = loom::thread::spawn(move || {
            let mut effects = DurableEffects::default();
            assert!(!matches!(
                retry.lock().unwrap().attach(&mut effects),
                Attachment::Durable
            ));
        });
        let close = state.clone();
        let close = loom::thread::spawn(move || {
            let drained = close.lock().unwrap().retire().unwrap();
            assert_eq!(drained.len(), 1);
        });
        let dispatch = state.clone();
        let dispatch = loom::thread::spawn(move || {
            assert!(dispatch.lock().unwrap().take_durable(0).is_empty());
        });
        retry.join().unwrap();
        close.join().unwrap();
        dispatch.join().unwrap();
        let mut state = state.lock().unwrap();
        assert!(state.pending().is_empty());
        assert!(state.publication().is_none());
        assert!(matches!(
            state.attach(&mut DurableEffects::default()),
            Attachment::Retired
        ));
    });
}

#[test]
fn quality_loom_publication_and_durable_claim_have_one_terminal_owner() {
    explore(|| {
        let state = Arc::new(Mutex::new(CommitHandoff::default()));
        let publish = state.clone();
        let publish = loom::thread::spawn(move || {
            if let Some(slot) = publish.lock().unwrap().publication() {
                slot.push(group(1));
                1
            } else {
                0
            }
        });
        let close = state.clone();
        let close = loom::thread::spawn(move || close.lock().unwrap().retire().unwrap().len());
        let dispatch = state.clone();
        let dispatch = loom::thread::spawn(move || dispatch.lock().unwrap().take_durable(1).len());
        let published = publish.join().unwrap();
        let retired = close.join().unwrap();
        let durable = dispatch.join().unwrap();
        assert_eq!(published, retired + durable);
        let mut state = state.lock().unwrap();
        assert!(state.pending().is_empty());
        assert!(state.retire().is_none());
        assert!(state.take_durable(u64::MAX).is_empty());
    });
}

#[test]
fn quality_handoff_attachment_moves_each_reply_once() {
    let mut handoff = CommitHandoff::default();
    handoff.publication().unwrap().push(group(1));
    let (reply, _receiver) = tokio::sync::oneshot::channel();
    let (queue_reply, _queue_receiver) = tokio::sync::oneshot::channel();
    let mut effects = DurableEffects::default();
    effects
        .acks
        .push((reply, Err(super::super::AppendErr::Moved)));
    effects
        .queue_acks
        .push((queue_reply, Err("probe".to_owned())));
    assert!(matches!(handoff.attach(&mut effects), Attachment::Pending));
    assert!(effects.acks.is_empty());
    assert!(effects.queue_acks.is_empty());
    assert!(handoff.take_durable(0).is_empty());
    let claimed = handoff.take_durable(1);
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].effects.acks.len(), 1);
    assert_eq!(claimed[0].effects.queue_acks.len(), 1);
    assert!(matches!(handoff.attach(&mut effects), Attachment::Durable));
    assert!(handoff.retire().unwrap().is_empty());
    assert!(matches!(handoff.attach(&mut effects), Attachment::Retired));
}
