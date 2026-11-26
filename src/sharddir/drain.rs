//! The open gate under a planned drain (F6, GATE-DECISION-f6-prep-rows-rev2-2026-11-17, addendum
//! 2026-11-18 "drain fences ahead of the handoff", G1, G2 and G6).
//!
//! A drain hands every shard this instance holds off first and announces `draining` last. The
//! announcement is safe only when this instance owns nothing, and stays so only if it then
//! opens nothing: a routed request that opened a leaf here after the last check would be fenced
//! by the peer the ring then names. So the check and the refusal are one step under the gate's
//! state: [`OpenGate::seal_for_drain`] finds nothing resident, no open in flight and no close
//! unsettled, and stops the gate starting opens, as a stop does. A request that resolves a leaf
//! here afterwards is answered `503 shard_closing`, `Retry-After: 1` (the stop's answer), and
//! its replay meets the successor.
//!
//! [`OpenGate::resident_fenced`] is the fence's observation point (G6): an engine this instance
//! still serves whose storage closed fenced, judged before the eviction drops it.

use std::sync::atomic::Ordering;

use super::{EngineIncarnation, OpenGate};

impl OpenGate {
    /// Stop starting opens if this instance holds nothing: no resident, no open in flight, no
    /// close unsettled, judged under the gate's state and then the serving map, the one order.
    /// Whether it sealed; a gate already stopping is sealed.
    pub(crate) fn seal_for_drain(&self) -> bool {
        let Ok(state) = self.inner.st.lock() else {
            return false;
        };
        let Ok(serving) = self.inner.shards.read() else {
            return false;
        };
        let unsettled = |gate: &super::PrefixGate| {
            let closing = gate.closing.as_ref();
            gate.inflight.is_some() || closing.is_some_and(|engine| !engine.terminated())
        };
        if !serving.is_empty() || state.values().any(unsettled) {
            return false;
        }
        self.inner.stopping.store(true, Ordering::SeqCst);
        true
    }

    /// Whether the resident of `prefix` is the engine of `incarnation` and its storage closed
    /// fenced: a newer writer opened the shard under it.
    pub(crate) fn resident_fenced(&self, prefix: &str, incarnation: EngineIncarnation) -> bool {
        let Ok(serving) = self.inner.shards.read() else {
            return false;
        };
        let fenced = |resident: &super::Resident| {
            let status = resident.engine.db.subscribe();
            let closed = matches!(
                status.borrow().close_reason,
                Some(slatedb::CloseReason::Fenced)
            );
            resident.incarnation == incarnation && closed
        };
        serving.get(prefix).is_some_and(fenced)
    }

    /// Whether the gate starts no more opens for good: a stop, a drain's seal, or its evacuation;
    /// a leaver's reversible seal is not one.
    pub(crate) fn sealed(&self) -> bool {
        self.inner.stopping.stopped()
    }
}
