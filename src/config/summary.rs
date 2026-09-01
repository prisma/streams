//! The redacted configuration summary (WP-01 PR 3.1).
//!
//! This is an EXPLICIT projection, not a derived serialization of the
//! whole graph: a new field on `ServerConfig` does NOT appear in the
//! summary until someone adds it here deliberately. `cli` is excluded
//! wholesale — it carries key material and tokens. The sentinel test in
//! `config::tests` proves no secret channel leaks.

use crate::config::model::ServerConfig;

impl ServerConfig {
    /// The effective configuration, safe to log. Every key below is a
    /// deliberate, reviewed inclusion.
    pub fn redacted_summary(&self) -> serde_json::Value {
        serde_json::json!({
            "storage": {
                "pool_idle_secs": self.storage.pool_idle_secs,
                "store_max_concurrent": self.storage.store_max_concurrent,
                "bulk_inflight_max_bytes": self.storage.bulk_inflight_max_bytes,
                "bulk_nominal_get_bytes": self.storage.bulk_nominal_get_bytes,
            },
            "engine": {
                "compactor_poll_ms": self.engine.compactor_poll_ms,
                "compactor_max_concurrent": self.engine.compactor_max_concurrent,
                "compact_max_subcompactions": self.engine.compact_max_subcompactions,
                "compact_max_fetch_tasks": self.engine.compact_max_fetch_tasks,
                "compact_bytes_to_fetch": self.engine.compact_bytes_to_fetch,
                "compact_max_sst_size": self.engine.compact_max_sst_size,
                "slatedb_rt_threads": self.engine.slatedb_rt_threads,
            },
            "shard": {
                "open_deadline_ms": self.shard.open_deadline.as_millis() as u64,
                "open_wait_ms": self.shard.open_wait_ms,
                "unready_exit_after_secs": self.shard.unready_exit_after_secs,
            },
            "history": {
                "absorb_pause_initial": self.history.absorb_pause_initial,
                "absorb_global_budget_bytes": self.history.absorb_global_budget_bytes,
                "absorb_global_gathers": self.history.absorb_global_gathers,
                "cache_bytes": self.history.cache_bytes,
                "compactor_off": self.history.compactor_off,
                "gc_interval_ms": self.history.gc_interval.map(|d| d.as_millis() as u64),
            },
            "postings": { "cache_bytes": self.postings.cache_bytes },
            "sse": {
                "feed_ring_bytes": self.sse.feed_ring_bytes,
                "feed_total_bytes": self.sse.feed_total_bytes,
                "feed_project_bytes": &self.sse.feed_project_bytes_raw,
                "heartbeat_ms": self.sse.heartbeat_ms,
            },
            "http": {
                "tail_max_bytes": self.http.tail_max_bytes,
                "debug_timing": self.http.debug_timing,
                "debug_exit": self.http.debug_exit,
                "h1_max_buf": self.http.h1_max_buf,
                "binary_sha256": &self.http.binary_sha256,
            },
            "billing": {
                "meter_enabled": self.billing.meter_enabled,
                "mode_env": &self.billing.mode_env,
                "rollup_env": &self.billing.rollup_env,
                "outbox_sweep_secs": self.billing.outbox_sweep_secs,
                "telemetry_drain_secs": self.billing.telemetry_drain_secs,
                "metrics_interval_secs": self.billing.metrics_interval_secs,
                "month_close_grace_ms": self.billing.month_close_grace_ms,
                "telemetry_cache_bytes": self.billing.telemetry_cache_bytes,
                "sweep_discovery_max": self.billing.sweep_discovery_max,
                "sweep_maint_resident": self.billing.sweep_maint_resident,
                "sweep_resident_quantum": self.billing.sweep_resident_quantum,
                "alert_usage_outbox_dirty": self.billing.alert_usage_outbox_dirty,
                "path_prefix_env": &self.billing.path_prefix_env,
            },
            "fleet": {
                "allow_http_peers": self.fleet.allow_http_peers,
                "peer_domains": &self.fleet.peer_domains_raw,
                "rebalance_lag_secs": self.fleet.rebalance_lag_secs,
                "rebalance_move_cooldown_secs": self.fleet.rebalance_move_cooldown_secs,
                "self_url": &self.fleet.self_url,
                "fleet_min": self.fleet.fleet_min,
                "rebalance_return_secs": self.fleet.rebalance_return_secs,
            },
            "scaler": {
                "eval_secs": self.scaler.eval_secs,
                "rate_window_secs": self.scaler.rate_window_secs,
                "hot_pct": self.scaler.hot_pct,
                "cold_pct": self.scaler.cold_pct,
                "hot_evals": self.scaler.hot_evals,
                "cold_evals": self.scaler.cold_evals,
                "cooldown_secs": self.scaler.cooldown_secs,
                "max_segments": self.scaler.max_segments,
            },
            "admission": {
                "unabsorbed_bytes_instance": self.admission.unabsorbed_bytes_instance,
                "unabsorbed_bytes_shard": self.admission.unabsorbed_bytes_shard,
                "absorb_lag_secs": self.admission.absorb_lag_secs,
                "maint_release_pct": self.admission.maint_release_pct,
                "limit_bytes_per_sec": self.admission.limit_bytes_per_sec,
                "limit_reqs_per_sec": self.admission.limit_reqs_per_sec,
                "limit_recs_per_sec": self.admission.limit_recs_per_sec,
                "limit_burst_secs": self.admission.limit_burst_secs,
            },
            "crypto": { "frame_compress": self.crypto.frame_compress },
            "runtime": {
                "memprofile_cert": &self.runtime.memprofile_cert,
                "certification_mode": &self.runtime.certification_mode,
            },
        })
    }
}
