//! Certification checks typed settings; the success notice remains redacted JSON.
use super::{CliArgs, MapEnvironment, ServerConfig, notice::ConfigNotice};

/// The binary's defaults are the certified profile: a server that sets
/// nothing but the certification passes it.
#[test]
fn the_default_configuration_is_the_certified_profile() {
    let cfg = ServerConfig::load(
        CliArgs::deterministic(),
        &MapEnvironment::from([("MEMPROFILE_CERT", "compute-1g")]),
    );
    let mut notices = Vec::new();
    let errors = super::profile::certified_memprofile_errors(&cfg, &mut notices);
    assert_eq!(errors, Vec::<String>::new());
    assert!(matches!(
        notices.as_slice(),
        [ConfigNotice::MemoryProfileCertified { .. }]
    ));
}

#[test]
fn uncertified_profile_reports_every_mismatched_measurement_in_order() {
    // SlateDB's own worker values and the bulk gate off. The concurrency
    // is clap's, so the argument carries it.
    let mut cli = CliArgs::deterministic();
    cli.compactor_max_concurrent = 4;
    let cfg = ServerConfig::load(
        cli,
        &MapEnvironment::from([
            ("MEMPROFILE_CERT", "compute-1g"),
            ("COMPACT_MAX_SUBCOMPACTIONS", "4"),
            ("COMPACT_MAX_FETCH_TASKS", "4"),
            ("COMPACT_BYTES_TO_FETCH", "2097152"),
            ("COMPACT_MAX_SST_SIZE_BYTES", "268435456"),
            ("STORE_BULK_INFLIGHT_MAX_BYTES", "0"),
        ]),
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

/// A certified process judges the concurrency its argument asks for: 8
/// on argv is refused with the two lines that name it, and no family
/// adds a line, because every family carries the same resolved worker.
#[test]
fn certification_judges_the_concurrency_given_on_argv() {
    let mut cli = CliArgs::deterministic();
    cli.compactor_max_concurrent = 8;
    let cfg = ServerConfig::load(
        cli,
        &MapEnvironment::from([("MEMPROFILE_CERT", "compute-1g")]),
    );
    let mut notices = Vec::new();
    let errors = super::profile::certified_memprofile_errors(&cfg, &mut notices);
    let expected = [
        "MEMPROFILE_CERT=compute-1g but max_concurrent_compactions=8 (certified 1)",
        "MEMPROFILE_CERT=compute-1g but worker_max_concurrent_compactions=8 (certified 1)",
    ];
    assert_eq!(errors.len(), expected.len(), "{errors:?}");
    for (error, line) in errors.iter().zip(expected) {
        assert!(error.starts_with(line), "{error}");
    }
    assert!(notices.is_empty());
}

#[test]
fn certification_notice_requires_the_complete_profile() {
    let cfg = ServerConfig::load(
        CliArgs::deterministic(),
        &MapEnvironment::from([
            ("MEMPROFILE_CERT", "compute-1g"),
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
