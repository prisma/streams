//! Configuration-validation tests (PR 3.2.1: split beside their
//! subject; `use super::*` sees validation's private internals).

use super::*;

#[cfg(test)]
mod memprofile_tests {
    use clap::Parser;

    /// Structural: every production settings family hands its builder
    /// the ONE resolved worker profile — a family that regresses to
    /// `Settings::default().compactor_options` (history, R29) or
    /// `..Default::default()` (telemetry, R28) fails here.
    #[test]
    fn every_db_family_carries_the_resolved_compactor_profile() {
        let cfg = crate::config::ServerConfig::load(
            crate::config::CliArgs::parse_from([
                "streams-slate",
                "--s3-endpoint",
                "http://127.0.0.1:1",
            ]),
            &crate::config::MapEnvironment::empty(),
        );
        let cert = super::resolved_compactor_options(&cfg.engine)
            .worker
            .clone()
            .unwrap_or_default();
        for (family, co) in super::production_settings_families(&cfg) {
            let co = co.unwrap_or_else(|| panic!("{family}: compactor disabled"));
            let w = co.worker.clone().unwrap_or_default();
            assert_eq!(w.max_subcompactions, cert.max_subcompactions, "{family}");
            assert_eq!(w.max_fetch_tasks, cert.max_fetch_tasks, "{family}");
            assert_eq!(w.bytes_to_fetch, cert.bytes_to_fetch, "{family}");
            assert_eq!(w.max_sst_size, cert.max_sst_size, "{family}");
            assert_eq!(
                w.max_concurrent_compactions, cert.max_concurrent_compactions,
                "{family}"
            );
        }
    }
}

#[cfg(test)]
mod config_validation_tests {
    use super::*;
    use clap::Parser;

    /// CHAOS-2: the shipped defaults must be openable. The old
    /// L0_SST_SIZE_BYTES default (32 MiB) exceeded the
    /// MAX_UNFLUSHED_BYTES default (16 MiB), so a bare `streams-slate`
    /// with no environment booted, reported `/health` ok, accepted
    /// stream creation, and then failed EVERY append with a 500 for as
    /// long as the process lived.
    /// SR3-1 (round-3 finding 1): the release posture carries NO
    /// permanent shared credential, validated GLOBALLY — the same
    /// rules whether or not fleet mode is on.
    #[test]
    fn release_posture_refuses_every_static_credential_shape() {
        let parse = |extra: &[&str]| {
            let mut v = vec!["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"];
            v.extend_from_slice(extra);
            // try_parse_from: a parse error must FAIL THE TEST, not
            // process::exit(2) the whole suite binary.
            CliArgs::try_parse_from(v).expect("test args must parse")
        };
        // Workload + release + a coexisting static token: refused.
        let a = parse(&[
            "--fleet-auth-mode",
            "workload",
            "--workload-token-file",
            "/run/w.jwt",
            "--streams-auth-mode",
            "enforce",
            "--release-posture",
            "--fleet-internal-token",
            "legacy-token-0123456789",
        ]);
        assert!(
            validate_fleet_auth(&a, false).is_err(),
            "release+workload must refuse a coexisting static token"
        );
        // Static mode under release: refused even single-instance.
        let a = parse(&[
            "--fleet-auth-mode",
            "static",
            "--streams-auth-mode",
            "enforce",
            "--release-posture",
            "--fleet-internal-token",
            "legacy-token-0123456789",
        ]);
        assert!(
            validate_fleet_auth(&a, false).is_err(),
            "release posture must refuse static mode without fleet mode too"
        );
        // Release workload posture without enforce: refused.
        let a = parse(&[
            "--fleet-auth-mode",
            "workload",
            "--workload-token-file",
            "/run/w.jwt",
            "--release-posture",
        ]);
        assert!(
            validate_fleet_auth(&a, false).is_err(),
            "release posture requires STREAMS_AUTH_MODE=enforce"
        );
        // The clean release shape passes, single-instance AND fleet.
        let a = parse(&[
            "--fleet-auth-mode",
            "workload",
            "--workload-token-file",
            "/run/w.jwt",
            "--streams-auth-mode",
            "enforce",
            "--release-posture",
        ]);
        assert!(validate_fleet_auth(&a, false).is_ok());
        assert!(validate_fleet_auth(&a, true).is_ok());
        // Non-release migration coexistence stays allowed (boot only;
        // the runtime gate still refuses the static bearer in
        // workload mode).
        let a = parse(&[
            "--fleet-auth-mode",
            "workload",
            "--workload-token-file",
            "/run/w.jwt",
            "--fleet-internal-token",
            "legacy-token-0123456789",
        ]);
        assert!(validate_fleet_auth(&a, false).is_ok());
        // Static fleet mode off-release keeps its existing rules.
        let a = parse(&["--fleet-internal-token", "legacy-token-0123456789"]);
        assert!(validate_fleet_auth(&a, true).is_ok());
        let a = parse(&[]);
        assert!(
            validate_fleet_auth(&a, true).is_err(),
            "static fleet mode still requires the token"
        );
    }

    /// Round-11.6: the seal-publication delay is a certification
    /// instrument — armed without STREAMS_CERTIFICATION_MODE=1 it
    /// refuses boot; unset and malformed shapes behave predictably.
    #[test]
    fn cert_sealed_publish_delay_is_gated_on_certification_mode() {
        assert_eq!(cert_sealed_publish_delay_from(None, None).unwrap(), 0);
        assert_eq!(cert_sealed_publish_delay_from(Some("0"), None).unwrap(), 0);
        assert!(cert_sealed_publish_delay_from(Some("500"), None).is_err());
        assert!(cert_sealed_publish_delay_from(Some("500"), Some("0")).is_err());
        assert_eq!(
            cert_sealed_publish_delay_from(Some("500"), Some("1")).unwrap(),
            500
        );
        assert!(cert_sealed_publish_delay_from(Some("abc"), Some("1")).is_err());
    }

    /// Round-10 review: the release posture requires a per-record
    /// payload ceiling whose worst-case prepared SSE frame fits the
    /// feed ring.
    #[test]
    fn release_posture_requires_a_ring_consistent_record_ceiling() {
        let sse = crate::config::SseConfig::default();
        // Off-release: no ceiling required.
        assert!(validate_record_ceiling(&sse, false, None).is_ok());
        // Release without a ceiling: refused.
        assert!(validate_record_ceiling(&sse, true, None).is_err());
        // Round-10e: ZERO is the unlimited sentinel — refused.
        assert!(validate_record_ceiling(&sse, true, Some(0)).is_err());
        // Round-10e: an overflow-inducing ceiling is refused, not
        // wrapped.
        assert!(validate_record_ceiling(&sse, true, Some(usize::MAX)).is_err());
        // Release with a ceiling whose frame exceeds the ring: refused.
        let ring = crate::sse::budget::feed_ring_bytes(&sse);
        assert!(validate_record_ceiling(&sse, true, Some(ring)).is_err());
        // Release with a fitting ceiling: accepted (an eighth of the
        // ring leaves headroom under the 6x worst-case text framing).
        assert!(validate_record_ceiling(&sse, true, Some(ring / 8)).is_ok());
        // The bound covers the TRUE worst framing (round-10e): a
        // newline-heavy text payload (6 bytes of SSE output per input
        // byte), lossy invalid UTF-8 (3 bytes per byte), JSON and
        // binary all stay under worst_prepared_charge.
        let bound = |n: usize| crate::sse::feed::worst_prepared_charge(n).expect("plausible size");
        let text_desc = {
            let mut d = crate::sse::feed::tests::test_desc("wcase");
            d.content_type = "text/plain".into();
            d
        };
        let newlines = vec![b'\n'; 1024];
        assert!(
            crate::sse::wire::sse_data_event(&text_desc, &newlines).len() <= bound(1024),
            "newline-heavy text must fit the worst-case bound"
        );
        let invalid = vec![0xFFu8; 1024];
        assert!(
            crate::sse::wire::sse_data_event(&text_desc, &invalid).len() <= bound(1024),
            "lossy invalid UTF-8 must fit the worst-case bound"
        );
        let bin_desc = {
            let mut d = crate::sse::feed::tests::test_desc("wcase2");
            d.content_type = "application/octet-stream".into();
            d
        };
        assert!(crate::sse::wire::sse_data_event(&bin_desc, &invalid).len() <= bound(1024));
        let json_desc = crate::sse::feed::tests::test_desc("wcase3");
        assert!(
            crate::sse::wire::sse_data_event(&json_desc, &newlines).len() <= bound(1024),
            "json framing must fit the worst-case bound"
        );
    }

    #[test]
    fn shipped_defaults_are_a_valid_engine_configuration() {
        let args = CliArgs::parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]);
        validate_engine_settings(
            "shard",
            &shard_settings(&args, &crate::config::EngineConfig::default()),
        )
        .expect("default shard settings must open");
        let cfg = crate::config::ServerConfig::load(
            CliArgs::parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]),
            &crate::config::MapEnvironment::empty(),
        );
        validate_engine_settings(
            "history",
            &crate::history::history_settings(&cfg.history, &cfg.engine.compactor_options()),
        )
        .expect("default history settings must open");
    }

    /// Follow-up review finding 4 (red): the release-safe hub-budget
    /// maximum is PROFILE-specific. The 64-MiB rung was exercised but
    /// produced RSS shed on the 1-GiB class, so it must not be that
    /// class's release-safe ceiling.
    #[test]
    fn hub_budget_maximum_is_profile_specific() {
        const THIRTY_TWO_MIB: &str = "33554432";
        // Round-12: the LiveFeed retention model is exactly accounted
        // (bounded reservation per retained batch, released on
        // eviction), and the perf study certified 64 MiB on the 1-GiB
        // class: 10,000 parked subscribers idle at 307 MB, peak
        // 399 MB, 101 MB below the shed line (docs/PERF-LIVEFEED.md
        // §3). The old 16 MiB ceiling was the HUB's uncertainty.
        let mut cap = 10_000;
        validate_release_capacity(
            true,
            Some("compute-1g"),
            Some("67108864"),
            &mut cap,
            u32::MAX as u64,
        )
        .unwrap();
        // Above the newly certified 64 MiB: still refused.
        let mut cap = 10_000;
        assert!(
            validate_release_capacity(
                true,
                Some("compute-1g"),
                Some("134217728"),
                &mut cap,
                u32::MAX as u64
            )
            .is_err(),
            "the 1-GiB profile must refuse a feed budget above its certified 64 MiB"
        );
        // Unknown/default profile + release + 32 MiB: allowed (largest
        // EXERCISED envelope until that tier certifies its own).
        let mut cap = 10_000;
        validate_release_capacity(true, None, Some(THIRTY_TWO_MIB), &mut cap, u32::MAX as u64)
            .unwrap();
        // Non-release warns only.
        let mut cap = 10_000;
        validate_release_capacity(
            false,
            Some("compute-1g"),
            Some(THIRTY_TWO_MIB),
            &mut cap,
            u32::MAX as u64,
        )
        .unwrap();
        // The certified posture lands everywhere.
        let mut cap = 10_000;
        validate_release_capacity(
            true,
            Some("compute-1g"),
            Some("16777216"),
            &mut cap,
            u32::MAX as u64,
        )
        .unwrap();
    }

    /// Round-4 follow-up review, finding 1 (red): the runtime reads
    /// SSE_MAX_CONNECTIONS=0 as UNLIMITED, so neither the validator's
    /// own clamp nor an explicit zero may ever produce it under the
    /// release posture. A degraded platform fails closed.
    #[test]
    fn release_capacity_never_turns_the_sse_gate_off() {
        // Explicit cap 0 + release posture: boot refusal.
        let mut cap = 0u64;
        assert!(
            validate_release_capacity(true, None, None, &mut cap, u32::MAX as u64).is_err(),
            "release posture must refuse an unlimited subscription budget"
        );
        // Non-release cap 0 remains allowed and untouched.
        let mut cap = 0u64;
        validate_release_capacity(false, None, None, &mut cap, 4_096).unwrap();
        assert_eq!(cap, 0);
        // A degraded ceiling must not clamp DOWN to zero (=unlimited):
        // nofile_hard == FD_RESERVE refuses; below it refuses too.
        let mut cap = 10_000;
        assert!(validate_release_capacity(true, None, None, &mut cap, FD_RESERVE).is_err());
        let mut cap = 10_000;
        assert!(validate_release_capacity(true, None, None, &mut cap, FD_RESERVE - 1).is_err());
        // Non-release only warns.
        let mut cap = 10_000;
        validate_release_capacity(false, None, None, &mut cap, FD_RESERVE).unwrap();
        assert_eq!(cap, 10_000);
        // The first usable ceiling above the reserve clamps to it.
        let mut cap = 10_000;
        validate_release_capacity(true, None, None, &mut cap, FD_RESERVE + 1).unwrap();
        assert_eq!(cap, 1);
        // The observed Compute-class shape is unchanged.
        let mut cap = 10_000;
        validate_release_capacity(true, None, None, &mut cap, 4_096).unwrap();
        assert_eq!(cap, 3_072);
    }

    /// Round-4 review: release-posture capacity validation — the hub
    /// budget stays inside the field-certified envelope, a typo'd byte
    /// count never silently becomes the default, and the SSE
    /// connection cap clamps to what nofile_hard can carry.
    #[test]
    fn release_capacity_validates_hub_budget_and_fd_ceiling() {
        // An explicit hub budget above the certified envelope: refused
        // under the release posture, warned outside it.
        let mut cap = 10_000u64;
        assert!(validate_release_capacity(true, None, Some("134217728"), &mut cap, 0).is_err());
        assert!(validate_release_capacity(false, None, Some("134217728"), &mut cap, 0).is_ok());
        // The certified postures pass (16 MiB default is implicit).
        let mut cap = 10_000;
        assert!(validate_release_capacity(true, None, Some("16777216"), &mut cap, 65_536).is_ok());
        assert!(validate_release_capacity(true, None, None, &mut cap, 65_536).is_ok());
        // A typo'd value must not silently become the default.
        assert!(validate_release_capacity(false, None, Some("16 MiB"), &mut cap, 0).is_err());
        // The Compute-class ceiling: hard 4,096 with a 1,024 reserve
        // clamps the configured 10k to 3,072 under the release posture.
        let mut cap = 10_000;
        validate_release_capacity(true, None, None, &mut cap, 4_096).unwrap();
        assert_eq!(
            cap, 3_072,
            "effective cap must fit nofile_hard minus reserve"
        );
        // Outside the release posture the configured value stands.
        let mut cap = 10_000;
        validate_release_capacity(false, None, None, &mut cap, 4_096).unwrap();
        assert_eq!(cap, 10_000);
        // A generous ceiling leaves the cap alone.
        let mut cap = 10_000;
        validate_release_capacity(true, None, None, &mut cap, u32::MAX as u64).unwrap();
        assert_eq!(cap, 10_000);
    }

    /// CHAOS-3: the body ceiling is a capacity knob. Lowering it must
    /// shrink the absorber reservation that every gather holds against
    /// the shed line, and it must never be raisable above the pin.
    #[test]
    fn body_ceiling_sizes_the_absorber_reservation_and_only_lowers() {
        use crate::history::worst_frame_transient_for;
        let pinned = crate::http::MAX_BODY_BYTES;
        let at_pin = worst_frame_transient_for(pinned);
        assert!(
            at_pin > 96 * 1024 * 1024,
            "the pinned reservation is the ~96 MiB measured in the field, got {at_pin}"
        );
        let lowered = worst_frame_transient_for(1024 * 1024);
        assert!(
            lowered * 25 < at_pin,
            "a 1 MiB ceiling must shrink the reservation by more than 25x: \
             {lowered} vs {at_pin}"
        );

        // The live wiring reads the same rule, so the freed bytes are
        // real admission headroom and not a floor that clamps back.
        assert_eq!(
            crate::history::absorb_worst_frame_transient(),
            worst_frame_transient_for(crate::http::max_body_bytes())
        );
        assert_eq!(
            crate::history::floored_budget_capacity(0),
            crate::history::absorb_worst_frame_transient()
        );

        assert!(
            validate_body_ceiling(pinned + 1).is_err(),
            "the protocol ceiling must not be raisable"
        );
        assert!(validate_body_ceiling(1024).is_err(), "floor holds");
        assert_eq!(
            crate::http::max_body_bytes(),
            pinned,
            "nothing was installed by validation (it is pure)"
        );
    }

    #[test]
    fn unflushed_at_or_below_l0_is_rejected_before_any_engine_opens() {
        let mut args =
            CliArgs::parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]);
        args.l0_sst_size_bytes = 32 * 1024 * 1024;
        args.max_unflushed_bytes = 16 * 1024 * 1024;
        let err = validate_engine_settings(
            "shard",
            &shard_settings(&args, &crate::config::EngineConfig::default()),
        )
        .expect_err("l0 above unflushed must be refused at startup");
        let msg = format!("{err}");
        assert!(msg.contains("max_unflushed_bytes"), "unhelpful: {msg}");
        assert!(msg.contains("L0_SST_SIZE_BYTES"), "no remedy named: {msg}");

        // Equality is just as fatal as inversion — SlateDB requires a
        // strict inequality.
        args.max_unflushed_bytes = args.l0_sst_size_bytes;
        validate_engine_settings(
            "shard",
            &shard_settings(&args, &crate::config::EngineConfig::default()),
        )
        .expect_err("equal sizes must be refused too");
    }
}

#[cfg(test)]
mod validate_boundary_tests {
    //! The central-validator matrix (PR 3.2.1 review: helper tests do
    //! not prove the AGGREGATOR calls every required validation — these
    //! drive `ServerConfig::validate()` itself for every rejection
    //! category, plus the multi-error and default-valid cases).

    use crate::config::{CliArgs, MapEnvironment, ServerConfig};

    fn base() -> CliArgs {
        CliArgs::deterministic()
    }

    fn validate_with(
        mutate: impl FnOnce(&mut CliArgs),
        env: &[(&str, &str)],
    ) -> Result<super::ValidatedServerConfig, super::ConfigError> {
        let mut cli = base();
        mutate(&mut cli);
        ServerConfig::load(cli, &MapEnvironment::from(env.iter().copied())).validate()
    }

    fn rejects(mutate: impl FnOnce(&mut CliArgs), env: &[(&str, &str)], marker: &str) {
        let err = validate_with(mutate, env)
            .err()
            .unwrap_or_else(|| panic!("validate() must reject (marker {marker:?})"));
        let text = err.to_string();
        assert!(
            text.contains(marker),
            "rejection must name the problem: wanted {marker:?} in\n{text}"
        );
    }

    #[test]
    fn deterministic_default_configuration_is_valid() {
        let v = validate_with(|_| {}, &[]).expect("the shipped defaults must validate");
        assert_eq!(v.config().cli.project_id, "proj_local");
        assert_eq!(v.cell_id.as_str(), "local");
        assert_eq!(v.initial_shards.get(), 1, "standalone default");
    }

    #[test]
    fn validation_rejects_invalid_cell_id() {
        rejects(|c| c.cell_id = "bad cell!".into(), &[], "CELL_ID");
    }

    #[test]
    fn validation_rejects_non_power_of_two_initial_shards() {
        rejects(|c| c.initial_shards = Some(3), &[], "power of two");
        rejects(|c| c.initial_shards = Some(0), &[], "INITIAL_SHARDS");
    }

    #[test]
    fn validation_resolves_the_fleet_mode_shard_default() {
        let v = validate_with(
            |c| {
                c.fleet_prefix = Some("fleet/".into());
                c.fleet_max = 3;
                c.fleet_internal_token = Some("0123456789abcdef".into());
            },
            &[],
        )
        .expect("fleet default must validate");
        assert_eq!(v.initial_shards.get(), 16, "next_power_of_two(4x3)");
    }

    #[test]
    fn validation_rejects_invalid_body_limit() {
        rejects(
            |c| c.max_request_body_bytes = crate::protocol_pin::MAX_BODY_BYTES + 1,
            &[],
            "pinned protocol ceiling",
        );
        rejects(|c| c.max_request_body_bytes = 1024, &[], "floor");
    }

    #[test]
    fn validation_rejects_missing_required_billing_identity() {
        rejects(
            |c| {
                c.billing_mode = "required".into();
                c.usage_stream_key = Some("k".into());
            },
            &[],
            "local placeholders",
        );
        rejects(
            |c| c.billing_mode = "required".into(),
            &[],
            "USAGE_STREAM_KEY",
        );
    }

    #[test]
    fn validation_rejects_invalid_project_id_and_reserved_tenant() {
        rejects(|c| c.project_id = "bad id!".into(), &[], "PROJECT_ID");
    }

    #[test]
    fn validation_rejects_enforce_without_auth_files() {
        rejects(
            |c| {
                c.streams_auth_mode = "enforce".into();
                c.project_id = "proj_real".into();
            },
            &[],
            "STREAMS_AUTH_KEYS_FILE",
        );
    }

    #[test]
    fn validation_rejects_slow_auth_refresh_cadence() {
        rejects(
            |c| {
                c.streams_auth_mode = "shadow".into();
                c.project_id = "proj_real".into();
                c.streams_auth_keys_file = Some("/k".into());
                c.streams_auth_policy_file = Some("/p".into());
                c.streams_auth_grants_file = Some("/g".into());
                c.streams_auth_refresh_secs = 10_000_000;
            },
            &[],
            "STREAMS_AUTH_REFRESH_SECS",
        );
    }

    #[test]
    fn validation_rejects_starved_sweep_residency() {
        rejects(
            |_| {},
            &[("SWEEP_MAINT_RESIDENT", "0")],
            "SWEEP_MAINT_RESIDENT",
        );
    }

    #[test]
    fn validation_rejects_bad_cursor_key() {
        rejects(
            |c| c.streams_cursor_key = Some("!not-base64!".into()),
            &[],
            "STREAMS_CURSOR_KEY",
        );
        rejects(
            |c| c.streams_cursor_key = Some("c2hvcnQ=".into()),
            &[],
            "32 bytes",
        );
    }

    #[test]
    fn validation_rejects_release_posture_without_record_ceiling() {
        rejects(
            |c| {
                c.release_posture = true;
                c.streams_auth_mode = "enforce".into();
                c.project_id = "proj_real".into();
                c.streams_auth_keys_file = Some("/k".into());
                c.streams_auth_policy_file = Some("/p".into());
                c.streams_auth_grants_file = Some("/g".into());
                c.fleet_auth_mode = "workload".into();
                c.workload_token_file = Some("/w".into());
            },
            &[],
            "MAX_RECORD_PAYLOAD_BYTES",
        );
    }

    #[test]
    fn validation_rejects_static_fleet_auth_under_release_posture() {
        rejects(|c| c.release_posture = true, &[], "FLEET_AUTH_MODE=static");
    }

    #[test]
    fn validation_rejects_cert_delay_without_certification_mode() {
        rejects(
            |_| {},
            &[("STREAMS_CERT_SEALED_PUBLISH_DELAY_MS", "500")],
            "STREAMS_CERTIFICATION_MODE",
        );
    }

    #[test]
    fn validation_rejects_uncertified_memory_profile() {
        rejects(
            |_| {},
            &[
                ("MEMPROFILE_CERT", "compute-1g"),
                ("COMPACT_MAX_SUBCOMPACTIONS", "4"),
            ],
            "MEMPROFILE_CERT",
        );
    }

    #[test]
    fn validation_rejects_engine_settings_slatedb_would_refuse() {
        rejects(
            |c| {
                c.l0_sst_size_bytes = 32 * 1024 * 1024;
                c.max_unflushed_bytes = 16 * 1024 * 1024;
            },
            &[],
            "max_unflushed_bytes",
        );
    }

    /// Every problem is collected — one boot attempt reports one
    /// complete list, not the first refusal.
    #[test]
    fn validation_collects_every_problem() {
        let err = validate_with(
            |c| {
                c.cell_id = "bad cell!".into();
                c.initial_shards = Some(3);
                c.max_request_body_bytes = 1024;
            },
            &[("SWEEP_MAINT_RESIDENT", "0")],
        )
        .err()
        .expect("must reject");
        let text = err.to_string();
        for marker in ["CELL_ID", "power of two", "floor", "SWEEP_MAINT_RESIDENT"] {
            assert!(text.contains(marker), "missing {marker:?} in\n{text}");
        }
    }
}
