//! The custody handshake's unit tests.
use super::SweepCustody;

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
