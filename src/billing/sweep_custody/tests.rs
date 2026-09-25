//! The custody handshake's unit tests and its Loom model.
use super::{CustodyWord, SweepCustody};
use std::sync::atomic::Ordering;

impl CustodyWord for loom::sync::atomic::AtomicU64 {
    fn load(&self, order: Ordering) -> u64 {
        loom::sync::atomic::AtomicU64::load(self, order)
    }
    fn store(&self, value: u64, order: Ordering) {
        loom::sync::atomic::AtomicU64::store(self, value, order);
    }
    fn swap(&self, value: u64, order: Ordering) -> u64 {
        loom::sync::atomic::AtomicU64::swap(self, value, order)
    }
    fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        loom::sync::atomic::AtomicU64::compare_exchange(self, current, new, success, failure)
    }
}

/// Item 63: a customer's stamp and a sweep's install race with nothing
/// ordering them (the gate's Ready path stamps outside the map guard, and
/// the install holds none). Whatever the interleaving, once both have
/// returned the sweep must not hold custody: custody over external history
/// lets a later debt-free sweep retire an engine a customer adopted. One
/// spawned stamper plus the installing model thread, preemption bound 2.
/// The close CAS runs under the directory's write guard and stays covered
/// by the DST revoked-close rig; it is not modelled here.
#[test]
fn quality_loom_external_stamp_never_coexists_with_custody() {
    use loom::sync::Arc;
    use loom::sync::atomic::AtomicU64;
    let mut model = loom::model::Builder::new();
    model.max_threads = 2;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.max_permutations = None;
    model.max_duration = None;
    model.check(|| {
        let custody = Arc::new(SweepCustody::<AtomicU64>::default());
        let customer = custody.clone();
        let stamp = loom::thread::spawn(move || customer.stamp_external(1));
        let installed = custody.install(2);
        stamp.join().unwrap();
        if installed {
            assert!(
                !custody.holds(2),
                "install 2 kept custody over an external stamp"
            );
        }
        assert!(!custody.held(), "an external stamp left custody installed");
    });
}

#[test]
fn install_holds_exactly_its_own_value() {
    let custody: SweepCustody = SweepCustody::default();
    assert!(!custody.held());
    assert!(custody.install(5));
    assert!(custody.held());
    assert!(custody.holds(5));
    assert!(!custody.holds(6));
}

#[test]
fn an_earlier_external_stamp_declines_the_install() {
    let custody: SweepCustody = SweepCustody::default();
    custody.stamp_external(1);
    assert!(custody.externally_resolved());
    assert!(!custody.install(2));
    assert!(!custody.held());
}

#[test]
fn an_external_stamp_revokes_installed_custody() {
    let custody: SweepCustody = SweepCustody::default();
    assert!(custody.install(5));
    custody.stamp_external(6);
    assert!(!custody.held());
    assert!(!custody.revoke_if(5));
}

#[test]
fn revoke_releases_only_the_installers_value() {
    let custody: SweepCustody = SweepCustody::default();
    assert!(custody.install(5));
    assert!(!custody.revoke_if(4));
    assert!(custody.holds(5));
    assert!(custody.revoke_if(5));
    assert!(!custody.held());
}
