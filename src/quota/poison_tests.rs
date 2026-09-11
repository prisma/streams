#![cfg(test)]

use super::{ProjectAdmission, ProjectId, ProjectQuotas, QuotaRegistry};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

fn tracked_project() -> (
    QuotaRegistry,
    ProjectId,
    ProjectQuotas,
    Arc<ProjectAdmission>,
) {
    let registry = QuotaRegistry::default();
    let project = ProjectId::new("poisoned").unwrap();
    let quotas = ProjectQuotas {
        requests_per_sec: 10,
        append_bytes_per_sec: 10,
        append_records_per_sec: 10,
        read_bytes_per_sec: 10,
        max_streams: 2,
        ..Default::default()
    };
    drop(registry.admit(&project, &quotas, 1_000).unwrap());
    let admission = registry.tracked(&project).unwrap();
    (registry, project, quotas, admission)
}

fn poison<T>(lock: &Mutex<T>, update: impl FnOnce(&mut T)) {
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let mut state = lock.lock().unwrap();
            update(&mut state);
            panic!("interrupted quota update");
        }))
        .is_err()
    );
    assert!(lock.is_poisoned());
}

#[test]
fn poisoned_project_map_cannot_republish_or_report_partial_state() {
    let (registry, project, quotas, _admission) = tracked_project();
    poison(&registry.projects, |projects| projects.clear());
    assert!(catch_unwind(AssertUnwindSafe(|| registry.admit(&project, &quotas, 1_000))).is_err());
    assert!(
        catch_unwind(AssertUnwindSafe(
            || registry.admit_append(&project, &quotas, 1, 1, 1_000)
        ))
        .is_err()
    );
    assert!(catch_unwind(AssertUnwindSafe(|| registry.pressure_handle(&project))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| registry.stats())).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| registry.memory_pressure_json(10, 10))).is_err());
    let (healthy, _, _, _) = tracked_project();
    assert_eq!(healthy.stats(), (1, 0));
}

#[test]
fn poisoned_request_bucket_cannot_refill_and_admit() {
    let (registry, project, quotas, admission) = tracked_project();
    poison(&admission.bucket, |bucket| bucket.level = 0.0);
    assert!(catch_unwind(AssertUnwindSafe(|| registry.admit(&project, &quotas, 2_000))).is_err());
    let neighbor = ProjectId::new("neighbor").unwrap();
    assert!(registry.admit(&neighbor, &quotas, 2_000).is_ok());
}

#[test]
fn either_poisoned_append_bucket_prevents_a_partial_charge() {
    for poison_bytes in [true, false] {
        let (registry, project, quotas, admission) = tracked_project();
        let bucket = if poison_bytes {
            &admission.append_bytes
        } else {
            &admission.append_records
        };
        poison(bucket, |bucket| bucket.level = 0.0);
        assert!(
            catch_unwind(AssertUnwindSafe(
                || registry.admit_append(&project, &quotas, 1, 1, 2_000)
            ))
            .is_err()
        );
    }
}

#[test]
fn poisoned_read_debt_cannot_be_checked_or_debited() {
    let (registry, project, quotas, admission) = tracked_project();
    poison(&admission.read_bytes, |bucket| bucket.level = -10.0);
    assert!(
        catch_unwind(AssertUnwindSafe(
            || registry.check_read(&project, &quotas, 2_000)
        ))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(
            || registry.debit_read(&project, &quotas, 1, 2_000)
        ))
        .is_err()
    );
}

#[test]
fn poisoned_stream_count_cannot_seed_reserve_release_or_cancel() {
    let (registry, project, quotas, admission) = tracked_project();
    let reservation = registry
        .reserve_stream(&project, &quotas, Some(0))
        .unwrap()
        .unwrap();
    poison(&admission.streams, |streams| {
        streams.count = 0;
        streams.seeded = false;
    });
    assert!(catch_unwind(AssertUnwindSafe(|| registry.needs_stream_seed(&project))).is_err());
    assert!(
        catch_unwind(AssertUnwindSafe(|| registry.reserve_stream(
            &project,
            &quotas,
            Some(0)
        )))
        .is_err()
    );
    assert!(catch_unwind(AssertUnwindSafe(|| registry.release_stream(&project))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| drop(reservation))).is_err());
}
