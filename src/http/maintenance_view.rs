//! The per-engine maintenance view of `/v1/debug/load`
//! (`maintenance_shards`): each local shard's backlog, its maintenance shed
//! and its L0 watermark.

use std::sync::Arc;

use super::{AppState, ShardEngine};

/// Per-engine maintenance state for /v1/debug/load (R25-C).
pub(super) fn maintenance_shards_json(state: &AppState) -> serde_json::Value {
    let engines: Vec<Arc<ShardEngine>> = state.shards.engines();
    let now = crate::shard::now_ms();
    let shards_engaged = engines
        .iter()
        .filter(|e| {
            e.maintenance_shard_shed
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .count();
    serde_json::json!({
        "owned_shards": engines.len(),
        // R27-1: the per-shard machine's engaged count, reported next
        // to (not merged with) the instance machine's state.
        "shards_engaged": shards_engaged,
        // The L0 watermark: shards shedding writes now, and the most L0s
        // any local shard log holds.
        "l0_shards_shedding": engines.iter().filter(|e| e.l0_gauge().shedding).count(),
        "l0_max": engines.iter().map(|e| e.l0_gauge().count).max().unwrap_or(0),
        // R26-7: the exact cumulative frame-byte totals (actual
        // quantities per R26-2 — a mixed group counts both sides), so a
        // campaign can compute the corrected absorption ratio from the
        // field instead of a payload-unit artifact. Committed and
        // retired here are the SAME unit as unabsorbed_frame_bytes:
        // encoded frame bytes.
        "ingest_frame_bytes_total": crate::shard::INGEST_FRAME_BYTES_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        "absorbed_frame_bytes_total": crate::shard::ABSORBED_FRAME_BYTES_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        "shards": engines.iter().map(|e| {
            let m = e.maintenance_snapshot();
            serde_json::json!({
                "prefix": e.prefix,
                "unabsorbed_frame_bytes": m.unabsorbed_frame_bytes,
                "backlog_started_ms": m.backlog_started_ms,
                "last_progress_ms": m.last_progress_ms,
                "no_progress_secs": m.no_progress_secs(now),
                "shard_shed": e.maintenance_shard_shed
                    .load(std::sync::atomic::Ordering::Relaxed),
                // The shard log's L0 watermark (`shard::layout::l0_watermark`).
                "l0": e.l0_json(),
            })
        }).collect::<Vec<_>>(),
    })
}
