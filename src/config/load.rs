//! Loading: parse the environment ONCE into a [`ServerConfig`]
//! (WP-01 PR 3.1). `load()` = `with_knob_defaults()` + overlay, so the
//! no-environment configuration is provably the knob-default value.

use crate::config::cli::CliArgs;
use crate::config::environment::Environment;
use crate::config::model::ServerConfig;
use std::str::FromStr;
use std::time::Duration;

fn env_parse<T: FromStr>(env: &dyn Environment, k: &str) -> Option<T> {
    env.get(k).and_then(|v| v.parse().ok())
}

impl ServerConfig {
    /// Parse `environment` once over the knob defaults. Called exactly
    /// once per process, at the composition root.
    pub fn load(cli: CliArgs, environment: &dyn Environment) -> Self {
        let mut cfg = Self::with_knob_defaults(cli);
        cfg.overlay_env(environment);
        cfg
    }

    fn overlay_env(&mut self, env: &dyn Environment) {
        self.overlay_storage(env);
        self.overlay_engine(env);
        self.overlay_shard_runtime(env);
        self.overlay_history(env);
        self.overlay_postings(env);
        self.overlay_sse(env);
        self.overlay_http(env);
        self.overlay_billing_telemetry_rollup(env);
        self.overlay_fleet(env);
        self.overlay_scaler(env);
        self.overlay_admission_usage_limits(env);
        self.overlay_crypto(env);
        self.overlay_runtime_certification(env);
    }

    fn overlay_storage(&mut self, env: &dyn Environment) {
        if let Some(v) = env_parse(env, "POOL_IDLE_SECS") {
            self.storage.pool_idle_secs = v;
        }
        if let Some(v) = env_parse(env, "STORE_MAX_CONCURRENT") {
            self.storage.store_max_concurrent = v;
        }
        if let Some(v) = env_parse(env, "STORE_BULK_INFLIGHT_MAX_BYTES") {
            self.storage.bulk_inflight_max_bytes = v;
        }
        if let Some(v) = env_parse::<u64>(env, "COMPACT_MAX_SST_SIZE_BYTES") {
            // One env name feeds BOTH knobs, with different defaults
            // (256 MiB compactor roll vs 8 MiB nominal GET weight) —
            // preserved divergence, do not "fix" here.
            self.storage.bulk_nominal_get_bytes = v;
        }
        if let Some(v) = env_parse::<usize>(env, "COMPACT_MAX_SST_SIZE_BYTES") {
            self.engine.compact_max_sst_size = v;
        }
    }

    fn overlay_engine(&mut self, env: &dyn Environment) {
        if let Some(v) = env_parse(env, "COMPACTOR_POLL_MS") {
            self.engine.compactor_poll_ms = v;
        }
        if let Some(v) = env_parse(env, "COMPACTOR_MAX_CONCURRENT") {
            self.engine.compactor_max_concurrent = v;
        }
        if let Some(v) = env_parse(env, "COMPACT_MAX_SUBCOMPACTIONS") {
            self.engine.compact_max_subcompactions = v;
        }
        if let Some(v) = env_parse(env, "COMPACT_MAX_FETCH_TASKS") {
            self.engine.compact_max_fetch_tasks = v;
        }
        if let Some(v) = env_parse(env, "COMPACT_BYTES_TO_FETCH") {
            self.engine.compact_bytes_to_fetch = v;
        }
        if let Some(v) = env_parse(env, "SLATEDB_RT_THREADS") {
            self.engine.slatedb_rt_threads = v;
        }
    }

    fn overlay_shard_runtime(&mut self, env: &dyn Environment) {
        if let Some(v) = env_parse::<u64>(env, "SHARD_OPEN_DEADLINE_MS") {
            self.shard.open_deadline = Duration::from_millis(v);
        }
        if let Some(v) = env_parse(env, "SHARD_OPEN_WAIT_MS") {
            self.shard.open_wait_ms = v;
        }
        if let Some(v) = env_parse(env, "UNREADY_EXIT_AFTER_SECS") {
            self.shard.unready_exit_after_secs = v;
        }
    }

    fn overlay_history(&mut self, env: &dyn Environment) {
        if env.get("ABSORB_PAUSE").as_deref() == Some("1") {
            self.history.absorb_pause_initial = true;
        }
        if let Some(v) = env_parse(env, "ABSORB_GLOBAL_BUDGET_BYTES") {
            self.history.absorb_global_budget_bytes = v;
        }
        if let Some(v) = env_parse::<usize>(env, "ABSORB_GLOBAL_GATHERS") {
            self.history.absorb_global_gathers = v.max(1);
        }
        if let Some(v) = env_parse(env, "HISTORY_CACHE_BYTES") {
            self.history.cache_bytes = v;
        }
        if env
            .get("HISTORY_COMPACTOR")
            .map(|v| v == "off")
            .unwrap_or(false)
        {
            self.history.compactor_off = true;
        }
        {
            // Current name first, legacy alias as fallback; 0 disables.
            let secs = env
                .get("HISTORY_GC_INTERVAL_SECS")
                .or_else(|| env.get("HISTORY_GC_MAX_INTERVAL_SECS"))
                .and_then(|v| v.parse::<u64>().ok());
            if let Some(secs) = secs {
                self.history.gc_interval = (secs > 0).then(|| Duration::from_secs(secs));
            }
        }
    }

    fn overlay_postings(&mut self, env: &dyn Environment) {
        if let Some(v) = env_parse(env, "POSTINGS_CACHE_BYTES") {
            self.postings.cache_bytes = v;
        }
    }

    fn overlay_sse(&mut self, env: &dyn Environment) {
        if let Some(raw) = env.get("SSE_FEED_RING_BYTES") {
            self.sse.feed_ring_bytes = raw.trim().parse().unwrap_or_else(|_| {
                tracing::warn!(
                    "SSE_FEED_RING_BYTES={raw:?} does not parse as a byte count; \
                     using the 1 MiB default"
                );
                1024 * 1024
            });
        }
        self.sse.feed_total_bytes_raw = env.get("SSE_FEED_TOTAL_BYTES");
        if let Some(raw) = env.get("SSE_FEED_TOTAL_BYTES") {
            self.sse.feed_total_bytes = raw.trim().parse().unwrap_or_else(|_| {
                tracing::warn!(
                    "SSE_FEED_TOTAL_BYTES={raw:?} does not parse as a byte count; \
                     using the 16 MiB default"
                );
                16 * 1024 * 1024
            });
        }
        self.sse.feed_project_bytes_raw = env.get("SSE_FEED_PROJECT_BYTES");
        if let Some(v) = env_parse::<u64>(env, "SSE_HEARTBEAT_MS").filter(|v| *v > 0) {
            self.sse.heartbeat_ms = v;
        }
    }

    fn overlay_http(&mut self, env: &dyn Environment) {
        if let Some(v) = env_parse::<usize>(env, "TAIL_MAX_BYTES").filter(|v| *v > 0) {
            self.http.tail_max_bytes = v;
        }
        self.http.debug_timing = env.get("STREAMS_DEBUG_TIMING").as_deref() == Some("1");
        self.http.debug_exit = env.get("STREAMS_DEBUG_EXIT").as_deref() == Some("1");
        if let Some(v) = env.get("APP_BINARY_SHA256") {
            self.http.binary_sha256 = v;
        }
        if let Some(v) = env_parse(env, "SSE_H1_MAX_BUF") {
            self.http.h1_max_buf = v;
        }
    }

    fn overlay_billing_telemetry_rollup(&mut self, env: &dyn Environment) {
        self.billing.mode_env = env.get("BILLING_MODE");
        self.billing.meter_enabled = env.get("BILLING_METER").map(|v| v != "off").unwrap_or(true);
        self.billing.rollup_env = env.get("ROLLUP");
        self.billing.path_prefix_env = env.get("PATH_PREFIX");
        if let Some(v) = env_parse(env, "OUTBOX_SWEEP_SECS") {
            self.billing.outbox_sweep_secs = v;
        }
        if let Some(v) = env_parse(env, "TELEMETRY_DRAIN_SECS") {
            self.billing.telemetry_drain_secs = v;
        }
        if let Some(v) = env_parse(env, "METRICS_INTERVAL_SECS") {
            self.billing.metrics_interval_secs = v;
        }
        if let Some(v) = env_parse(env, "MONTH_CLOSE_GRACE_MS") {
            self.billing.month_close_grace_ms = v;
        }
        if let Some(v) = env_parse(env, "TELEMETRY_CACHE_BYTES") {
            self.billing.telemetry_cache_bytes = v;
        }
        if let Some(v) = env_parse(env, "SWEEP_DISCOVERY_MAX") {
            self.billing.sweep_discovery_max = v;
        }
        if let Some(v) = env_parse::<usize>(env, "SWEEP_MAINT_RESIDENT") {
            // Stored RAW: the binary refuses 0 at boot (0 would starve
            // cold-debt drain); the billing adapter floors at the use
            // site (`sweep_resident_budget`, max(1)).
            self.billing.sweep_maint_resident = v;
        }
        if let Some(v) = env_parse::<usize>(env, "SWEEP_RESIDENT_QUANTUM") {
            // Raw here too; floored at the use site (max(1)).
            self.billing.sweep_resident_quantum = v;
        }
        if let Some(v) = env_parse(env, "ALERT_USAGE_OUTBOX_DIRTY") {
            self.billing.alert_usage_outbox_dirty = v;
        }
    }

    fn overlay_fleet(&mut self, env: &dyn Environment) {
        self.fleet.allow_http_peers = env.get("FLEET_ALLOW_HTTP_PEERS").as_deref() == Some("1");
        self.fleet.peer_domains_raw = env.get("FLEET_PEER_DOMAINS");
        if let Some(v) = env_parse(env, "REBALANCE_LAG_SECS") {
            self.fleet.rebalance_lag_secs = v;
        }
        if let Some(v) = env_parse(env, "REBALANCE_MOVE_COOLDOWN_SECS") {
            self.fleet.rebalance_move_cooldown_secs = v;
        }
        if let Some(v) = env.get("SELF_URL") {
            self.fleet.self_url = v;
        }
        if let Some(v) = env_parse::<u64>(env, "FLEET_MIN") {
            self.fleet.fleet_min = v.max(1);
        }
        if let Some(v) = env_parse(env, "REBALANCE_RETURN_SECS") {
            self.fleet.rebalance_return_secs = v;
        }
    }

    fn overlay_scaler(&mut self, env: &dyn Environment) {
        // envf: f64 parse, cast at the same points as the old
        // scaler3 PolicyOnceLock.
        let envf = |k: &str, d: f64| env_parse(env, k).unwrap_or(d);
        if env.get("SCALE_EVAL_SECS").is_some() {
            self.scaler.eval_secs = envf("SCALE_EVAL_SECS", 10.0) as u64;
        }
        if env.get("SCALE_RATE_WINDOW_SECS").is_some() {
            self.scaler.rate_window_secs = envf("SCALE_RATE_WINDOW_SECS", 120.0);
        }
        if env.get("SCALE_HOT_PCT").is_some() {
            self.scaler.hot_pct = envf("SCALE_HOT_PCT", 75.0) / 100.0;
        }
        if env.get("SCALE_COLD_PCT").is_some() {
            self.scaler.cold_pct = envf("SCALE_COLD_PCT", 15.0) / 100.0;
        }
        if env.get("SCALE_HOT_EVALS").is_some() {
            self.scaler.hot_evals = envf("SCALE_HOT_EVALS", 2.0) as u32;
        }
        if env.get("SCALE_COLD_EVALS").is_some() {
            self.scaler.cold_evals = envf("SCALE_COLD_EVALS", 180.0) as u32;
        }
        if env.get("SCALE_COOLDOWN_SECS").is_some() {
            self.scaler.cooldown_secs = envf("SCALE_COOLDOWN_SECS", 600.0) as i64;
        }
        if env.get("MAX_SEGMENTS_PER_STREAM").is_some() {
            self.scaler.max_segments = envf("MAX_SEGMENTS_PER_STREAM", 64.0) as usize;
        }
    }

    fn overlay_admission_usage_limits(&mut self, env: &dyn Environment) {
        if let Some(v) = env_parse(env, "MAX_UNABSORBED_BYTES_PER_INSTANCE") {
            self.admission.unabsorbed_bytes_instance = v;
        }
        if let Some(v) = env_parse(env, "MAX_UNABSORBED_BYTES_PER_SHARD") {
            self.admission.unabsorbed_bytes_shard = v;
        }
        if let Some(v) = env_parse(env, "MAX_ABSORB_LAG_SECS") {
            self.admission.absorb_lag_secs = v;
        }
        if let Some(v) = env_parse::<u64>(env, "MAINT_BACKPRESSURE_RELEASE_PCT") {
            self.admission.maint_release_pct = v.min(100);
        }
        if let Some(v) = env_parse(env, "LIMIT_BYTES_PER_SEC") {
            self.admission.limit_bytes_per_sec = v;
        }
        if let Some(v) = env_parse(env, "LIMIT_REQS_PER_SEC") {
            self.admission.limit_reqs_per_sec = v;
        }
        if let Some(v) = env_parse(env, "LIMIT_RECS_PER_SEC") {
            self.admission.limit_recs_per_sec = v;
        }
        if let Some(v) = env_parse(env, "LIMIT_BURST_SECS") {
            self.admission.limit_burst_secs = v;
        }
    }

    fn overlay_crypto(&mut self, env: &dyn Environment) {
        self.crypto.frame_compress = env
            .get("FRAME_COMPRESS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
    }

    fn overlay_runtime_certification(&mut self, env: &dyn Environment) {
        self.runtime.memprofile_cert = env.get("MEMPROFILE_CERT");
        self.runtime.cert_sealed_publish_delay_ms_raw =
            env.get("STREAMS_CERT_SEALED_PUBLISH_DELAY_MS");
        self.runtime.certification_mode = env.get("STREAMS_CERTIFICATION_MODE");
    }
}
