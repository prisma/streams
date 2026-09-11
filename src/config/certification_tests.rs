//! Certification checks typed settings; the success notice remains redacted JSON.
use super::{CliArgs, MapEnvironment, ServerConfig, notice::ConfigNotice};

#[test]
fn uncertified_profile_reports_every_mismatched_measurement_in_order() {
    let cfg = ServerConfig::load(
        CliArgs::deterministic(),
        &MapEnvironment::from([("MEMPROFILE_CERT", "compute-1g")]),
    );
    let mut notices = Vec::new();
    let errors = super::profile::certified_memprofile_errors(&cfg, &mut notices);
    let expected = [
        "bytes_to_fetch=2097152 (certified 1048576)",
        "max_concurrent_compactions=4 (certified 1)",
        "max_fetch_tasks=4 (certified 1)",
        "max_sst_size=268435456 (certified 33554432)",
        "max_subcompactions=4 (certified 1)",
        "store_bulk_inflight_max_bytes=0 (certified 33554432)",
        "worker_max_concurrent_compactions=4 (certified 1)",
    ];
    assert_eq!(errors.len(), expected.len());
    for (error, measurement) in errors.iter().zip(expected) {
        assert!(error.starts_with(&format!("MEMPROFILE_CERT=compute-1g but {measurement}")));
    }
    assert!(notices.is_empty());
}

#[test]
fn certification_notice_requires_the_complete_profile() {
    let cfg = ServerConfig::load(
        CliArgs::deterministic(),
        &MapEnvironment::from([
            ("MEMPROFILE_CERT", "compute-1g"),
            ("COMPACTOR_MAX_CONCURRENT", "1"),
            ("COMPACT_MAX_SUBCOMPACTIONS", "1"),
            ("COMPACT_MAX_FETCH_TASKS", "1"),
            ("COMPACT_BYTES_TO_FETCH", "1048576"),
            ("COMPACT_MAX_SST_SIZE_BYTES", "33554432"),
            ("STORE_BULK_INFLIGHT_MAX_BYTES", "33554432"),
        ]),
    );
    let mut notices = Vec::new();
    assert!(super::profile::certified_memprofile_errors(&cfg, &mut notices).is_empty());
    let [ConfigNotice::MemoryProfileCertified { profile }] = notices.as_slice() else {
        panic!("certification must produce exactly one success notice");
    };
    assert_eq!(
        *profile,
        super::profile::compactor_profile_json(&cfg).to_string()
    );
    let mut disabled = cfg;
    disabled.runtime.memprofile_cert = None;
    let mut notices = Vec::new();
    assert!(super::profile::certified_memprofile_errors(&disabled, &mut notices).is_empty());
    assert!(notices.is_empty());
}
