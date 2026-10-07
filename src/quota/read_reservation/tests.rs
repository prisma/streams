#![cfg(test)]

use crate::project_policy::ProjectQuotas;
use crate::quota::QuotaRegistry;
use crate::tenant::ProjectId;

const MIB: u64 = 1 << 20;

fn tracked(registry: &QuotaRegistry, project: &str) -> ProjectId {
    let id = ProjectId::new(project).unwrap();
    drop(
        registry
            .admit(&id, &ProjectQuotas::default(), 0)
            .expect("admitted"),
    );
    id
}

/// Under a 16 MiB line, 8 MiB reservations fit while the project holds at
/// most 8 MiB: the first two are taken, the third does not fit (its final
/// refusal counts as a shed), and a release makes room again. The estimate counts the
/// held bytes exactly.
#[test]
fn reservations_fit_under_the_line_and_the_estimate_counts_them() {
    let registry = QuotaRegistry::default();
    let id = tracked(&registry, "proj-a");
    let bytes = registry.read_bytes(&id).unwrap();
    assert!(bytes.try_reserve(8 * MIB, 16 * MIB));
    assert!(bytes.try_reserve(8 * MIB, 16 * MIB));
    assert!(!bytes.try_reserve(1, 16 * MIB), "a byte past the line");
    bytes.refused();
    let entry = registry.pressure_handle(&id).unwrap();
    assert_eq!(
        (bytes.held(), entry.estimated_pressure_bytes()),
        (16 * MIB, 16 * MIB)
    );
    bytes.release(8 * MIB);
    assert!(bytes.try_reserve(8 * MIB, 16 * MIB), "room after a release");
    bytes.release(16 * MIB);
    assert_eq!((bytes.held(), entry.estimated_pressure_bytes()), (0, 0));
    let shed = registry.memory_pressure_json(1, 8)["project_memory_shed_total"].clone();
    assert_eq!(shed, 1);
}

/// A read alone always fits, whatever its size; a 0 line is no line; a
/// charge is never refused; a release never goes below zero.
#[test]
fn a_lone_read_always_fits_and_charges_are_unconditional() {
    let registry = QuotaRegistry::default();
    let id = tracked(&registry, "proj-a");
    let bytes = registry.read_bytes(&id).unwrap();
    assert!(bytes.try_reserve(8 * MIB, MIB), "alone, over the line");
    assert!(!bytes.try_reserve(1, MIB), "beside it");
    bytes.release(8 * MIB);
    for _ in 0..4 {
        assert!(bytes.try_reserve(8 * MIB, 0), "no line");
    }
    bytes.charge(3);
    assert_eq!(bytes.held(), 32 * MIB + 3);
    bytes.release(64 * MIB);
    assert_eq!(bytes.held(), 0);
}

/// Two projects' read bytes are their own: one project at its line never
/// refuses the other.
#[test]
fn a_projects_line_never_refuses_its_neighbour() {
    let registry = QuotaRegistry::default();
    let a = registry.read_bytes(&tracked(&registry, "proj-a")).unwrap();
    let b = registry.read_bytes(&tracked(&registry, "proj-b")).unwrap();
    assert!(a.try_reserve(8 * MIB, 8 * MIB));
    assert!(!a.try_reserve(MIB, 8 * MIB));
    assert!(b.try_reserve(8 * MIB, 8 * MIB));
    assert_eq!((a.held(), b.held()), (8 * MIB, 8 * MIB));
    assert!(
        registry
            .read_bytes(&ProjectId::new("proj-c").unwrap())
            .is_none()
    );
}
