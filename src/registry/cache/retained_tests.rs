//! The descriptor cache's retained tier: a stream whose hot slot was swept
//! or evicted revalidates against the store with the ETag it was last
//! served under, so an hourly-touched stream costs a 304, not a body.
#![cfg(test)]
use std::sync::Arc;

use crate::registry::Registry;
use crate::registry::tests::{CountingStore, desc, ts};

const EPOCH: &str = "00000000000000000000000000000001";

fn rig() -> (Arc<Registry>, Arc<CountingStore>) {
    let store = Arc::new(CountingStore {
        inner: Arc::new(object_store::memory::InMemory::new()),
        gets: Default::default(),
        conditional: Default::default(),
        not_modified: Default::default(),
        puts: Default::default(),
        omit_etag: Default::default(),
        lose_put_reply: Default::default(),
    });
    let reg = Arc::new(Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    ));
    (reg, store)
}

/// (GETs, conditional GETs, 304s) the store has seen.
trait Reads {
    fn reads(&self) -> (u64, u64, u64);
}

impl Reads for CountingStore {
    fn reads(&self) -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::SeqCst;
        (
            self.gets.load(SeqCst),
            self.conditional.load(SeqCst),
            self.not_modified.load(SeqCst),
        )
    }
}

fn delta(after: (u64, u64, u64), before: (u64, u64, u64)) -> (u64, u64, u64) {
    (after.0 - before.0, after.1 - before.1, after.2 - before.2)
}

/// Past the sweep floor, every slot expired and swept: the next touch of
/// the first stream sends one GET, conditional on the ETag its create was
/// served under, and the store answers 304; no body is read.
#[tokio::test(crate = "crate::test_rt")]
async fn a_swept_stream_revalidates_with_a_304_not_a_body() {
    let (reg, store) = rig();
    let names: Vec<String> = (0..5_000).map(|i| format!("r{i:05}")).collect();
    for name in &names {
        reg.create(desc(name, EPOCH, false)).await.unwrap();
    }
    for name in &names {
        reg.expire_cache_for_tests(&ts(name));
    }
    assert_eq!(reg.sweep_cache(), 5_000, "every expired slot swept");
    let before = store.reads();
    let got = reg.get(&ts(&names[0])).await.unwrap().expect("live");
    assert_eq!(got.stream_epoch, EPOCH);
    assert_eq!(
        delta(store.reads(), before),
        (1, 1, 1),
        "(GETs, conditional, 304s) of one touch after the sweep"
    );
}

/// Expire and sweep every slot of `names` out of the hot tier.
fn sweep_out(reg: &Registry, names: &[String]) {
    for name in names {
        reg.expire_cache_for_tests(&ts(name));
    }
    assert_eq!(reg.sweep_cache(), names.len(), "every expired slot swept");
}

fn names(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}{i:05}")).collect()
}

/// A write through this instance drops what was retained: the read after
/// it fetches the body unconditionally and sees the tombstone, never the
/// retained live descriptor.
#[tokio::test(crate = "crate::test_rt")]
async fn a_write_drops_the_retained_descriptor() {
    let (reg, store) = rig();
    let n = names("w", 2);
    for name in &n {
        reg.create(desc(name, EPOCH, false)).await.unwrap();
    }
    sweep_out(&reg, &n);
    let outcome = reg
        .mutate_incarnation(&ts(&n[0]), EPOCH, |d| {
            let mut next = d.to_persisted();
            next.deleted = true;
            crate::registry::Mutation::Write(next, ())
        })
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        crate::registry::MutationResult::Applied(())
    ));
    assert_eq!(
        reg.retained_for_tests().0,
        1,
        "only the untouched stream stays retained"
    );
    let before = store.reads();
    let got = reg.get(&ts(&n[0])).await.unwrap().expect("tombstone");
    assert!(got.deleted, "the read sees the delete");
    let after = delta(store.reads(), before);
    assert_eq!(after.2, 0, "no 304: {after:?}");
}

/// A write on another instance leaves this one's retained ETag stale: the
/// conditional GET is answered with the new body (200), and the read sees
/// the tombstone.
#[tokio::test(crate = "crate::test_rt")]
async fn another_instances_write_is_caught_by_the_etag() {
    let (reg, store) = rig();
    let other = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    let n = names("x", 1);
    reg.create(desc(&n[0], EPOCH, false)).await.unwrap();
    sweep_out(&reg, &n);
    let outcome = other
        .mutate_incarnation(&ts(&n[0]), EPOCH, |d| {
            let mut next = d.to_persisted();
            next.deleted = true;
            crate::registry::Mutation::Write(next, ())
        })
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        crate::registry::MutationResult::Applied(())
    ));
    let before = store.reads();
    let got = reg.get(&ts(&n[0])).await.unwrap().expect("tombstone");
    assert!(got.deleted, "the stale ETag fetched the tombstone");
    assert_eq!(
        delta(store.reads(), before),
        (1, 1, 0),
        "one conditional GET, no 304"
    );
}

/// A descriptor that is not ordinary (here: with a watch) is not retained:
/// its next read after the sweep is an unconditional GET.
#[tokio::test(crate = "crate::test_rt")]
async fn a_descriptor_with_watches_is_not_retained() {
    let (reg, store) = rig();
    let n = names("v", 1);
    let mut d = desc(&n[0], EPOCH, false);
    d.watch_definitions = vec![crate::registry::WatchDefinition {
        name: "w".into(),
        fields: vec!["/a".into()],
    }];
    reg.create(d).await.unwrap();
    sweep_out(&reg, &n);
    let before = store.reads();
    assert!(reg.get(&ts(&n[0])).await.unwrap().is_some());
    assert_eq!(
        delta(store.reads(), before),
        (1, 0, 0),
        "an unconditional GET"
    );
}

/// The retained key is the project and the name: another project's stream
/// of the same name finds nothing retained (its read is unconditional and
/// answers absence).
#[tokio::test(crate = "crate::test_rt")]
async fn another_projects_stream_of_the_same_name_finds_nothing_retained() {
    let (reg, store) = rig();
    let n = names("p", 1);
    reg.create(desc(&n[0], EPOCH, false)).await.unwrap();
    sweep_out(&reg, &n);
    let other = crate::tenant::TenantStreamRef::new(
        crate::tenant::ProjectId::new("proj-other").unwrap(),
        crate::tenant::CanonicalStreamName::new(&n[0]).unwrap(),
    );
    let before = store.reads();
    assert!(reg.get(&other).await.unwrap().is_none(), "absent there");
    assert_eq!(
        delta(store.reads(), before),
        (1, 0, 0),
        "an unconditional GET"
    );
}

/// Every field an ordinary descriptor carries survives the compaction
/// byte for byte (as its JSON), the production shapes packed and other
/// text kept as it came; the ETag too.
#[test]
fn the_compaction_round_trips_every_field() {
    use base64::Engine;
    let sref = ts("rt");
    let mut base = desc("rt", EPOCH, false);
    base.account_id = Some("acct-1".into());
    base.key_fingerprint = crate::crypto::hex(&[7u8; 16]);
    base.watch_sig_key = Some(base64::engine::general_purpose::STANDARD.encode([9u8; 32]));
    base.expires_at_ms = Some(99);
    base.ttl_secs = Some(3600);
    base.seal_gen_counter = 5;
    let mut odd = base.clone();
    odd.key_fingerprint = "fp".into();
    odd.watch_sig_key = Some("wsk-1".into());
    odd.content_type = "text/plain; charset=utf-8".into();
    odd.account_id = None;
    let mut tomb = base.clone();
    tomb.deleted = true;
    tomb.parent_ref_pending = true;
    tomb.logical_close_ms = Some(123);
    let mut sealed = base.clone();
    sealed.sealed = true;
    sealed.seal_op = Some("op-9".into());
    let mut soft = base.clone();
    soft.soft_deleted = true;
    for d in [base, odd, tomb, sealed, soft] {
        for etag in ["\"0123456789abcdef0123456789abcdef\"", "17", "W/\"weak\""] {
            let (back, tag) = super::retained::round_trip(&d, etag, &sref).expect("ordinary");
            assert_eq!(
                serde_json::to_value(&back).unwrap(),
                serde_json::to_value(&d).unwrap()
            );
            assert_eq!(tag, etag);
        }
    }
}

/// A production-shaped ordinary entry (hex epoch and fingerprint, base64
/// verifier, quoted MD5 ETag, an account) is charged at most 256 bytes, so
/// the 64 MiB tier holds at least 262,144 of them.
#[test]
fn an_ordinary_entry_is_charged_at_most_256_bytes() {
    use base64::Engine;
    let mut d = desc("sz", EPOCH, false);
    d.account_id = Some("acct-1".into());
    d.key_fingerprint = crate::crypto::hex(&[7u8; 16]);
    d.watch_sig_key = Some(base64::engine::general_purpose::STANDARD.encode([9u8; 32]));
    let charged = super::retained::charged(&d, "\"0123456789abcdef0123456789abcdef\"");
    assert!(charged <= 256, "charged {charged} bytes");
}

/// Past its budget the tier sheds the least recently kept entries first:
/// with room for about 100 entries, of 400 swept out in order the first
/// ones are gone (an unconditional GET) and the last ones still revalidate
/// (a 304).
#[tokio::test(crate = "crate::test_rt")]
async fn past_its_budget_the_tier_sheds_the_least_recently_kept() {
    let (reg, store) = rig();
    reg.retain_budget_for_tests(100 * super::retained::entry_size());
    let n = names("b", 400);
    for name in &n {
        reg.create(desc(name, EPOCH, false)).await.unwrap();
    }
    for name in &n {
        reg.expire_cache_for_tests(&ts(name));
        reg.sweep_cache();
    }
    let held = reg.retained_for_tests();
    assert!(held.0 > 0 && held.0 <= 100, "entries {held:?}");
    assert!(
        held.1 <= 100 * super::retained::entry_size(),
        "bytes {held:?}"
    );
    let before = store.reads();
    assert!(reg.get(&ts(&n[0])).await.unwrap().is_some());
    assert_eq!(
        delta(store.reads(), before),
        (1, 0, 0),
        "the oldest was shed"
    );
    let before = store.reads();
    assert!(reg.get(&ts(&n[399])).await.unwrap().is_some());
    assert_eq!(
        delta(store.reads(), before),
        (1, 1, 1),
        "the newest revalidates"
    );
}

/// Sized at boot (`REGISTRY_RETAIN_MB`, `Registry::set_retain_budget`), the
/// tier sheds its least recently kept entries down to the new budget: of
/// 400 kept, a budget of about 50 entries keeps at most 50, and the newest
/// still revalidates with a 304 while the oldest pays a GET.
#[tokio::test(crate = "crate::test_rt")]
async fn a_lower_retain_budget_sheds_the_least_recently_kept() {
    let (reg, store) = rig();
    let n = names("s", 400);
    for name in &n {
        reg.create(desc(name, EPOCH, false)).await.unwrap();
    }
    for name in &n {
        reg.expire_cache_for_tests(&ts(name));
        reg.sweep_cache();
    }
    assert_eq!(
        reg.retained_for_tests().0,
        400,
        "all kept under the default budget"
    );
    reg.set_retain_budget(50 * super::retained::entry_size());
    let held = reg.retained_for_tests();
    assert!(held.0 > 0 && held.0 <= 50, "entries {held:?}");
    let before = store.reads();
    assert!(reg.get(&ts(&n[399])).await.unwrap().is_some());
    assert_eq!(
        delta(store.reads(), before),
        (1, 1, 1),
        "the newest revalidates"
    );
    let before = store.reads();
    assert!(reg.get(&ts(&n[0])).await.unwrap().is_some());
    assert_eq!(
        delta(store.reads(), before),
        (1, 0, 0),
        "the oldest was shed"
    );
}
