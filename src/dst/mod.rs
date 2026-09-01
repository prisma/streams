//! Simulation testing for the shard data plane (docs/DST.md).
//!
//! **Scope, stated honestly.** This is a *seeded fault-injection suite*
//! over the real single-node data plane, not yet whole-system deterministic
//! simulation in the TigerBeetle sense. What the seed controls is the
//! **fault schedule**: which object-store operation is delayed, which
//! fails, and which succeeds but loses its response. Task scheduling is
//! Tokio's. See docs/DST.md for exactly which guarantees hold today and
//! what closing the gap costs.
//!
//! Two design choices are load-bearing:
//!
//! *Faults are keyed, not drawn in sequence.* The decision for an
//! operation is a pure function of `(seed, path, op, occurrence)`. With one
//! shared RNG stream — the obvious implementation — the *identity* of the
//! operation consuming each random number depends on which task reaches
//! the mutex first, so a seed does not in fact reproduce a fault
//! placement under concurrency. Keying removes that dependency.
//!
//! *Records are identified by attempt, not by payload.* A client retrying
//! an ambiguous append resends the same bytes, so payload equality cannot
//! tell "the system duplicated my write" from "I deliberately wrote it
//! twice". Every attempt carries `(op, attempt)`, which makes that
//! distinction exactly.
//!
//! Invariants:
//!
//!   I1  every acknowledged append is readable
//!   I2  per routing key, acknowledged order is preserved
//!   I3  no attempt is stored twice
//!   I4  a fenced owner acknowledges nothing
//!   I5  a definitively rejected append never appears
//!   I6  an idempotent producer's retry commits at most once

// PR 3.2.1 Commit A: dst.rs had grown into a 2,750-line catch-all. The
// cohesive subsystems now own their files; every previously-public path
// survives via the re-exports below.

mod fault_store;
mod runtime;
mod trace_store;

pub use fault_store::*;
pub use runtime::*;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use object_store::path::Path as ObjPath;
use object_store::{GetOptions, ObjectStore, PutMultipartOptions, PutOptions, PutPayload};

// ---- semantic classification ----------------------------------------

/// Object-store verb.
///
/// `head` is absent deliberately: `ObjectStoreExt::head` is implemented on
/// top of `get_opts`, so a HEAD arrives here as a `Get`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreOp {
    Put,
    Get,
    Delete,
    List,
    Copy,
}

/// Semantic class of the object being touched, from the SAME classifier
/// production telemetry uses (`store_timing::classify`) — so a scenario
/// that targets "the WAL" targets what `/v1/debug/store` calls the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjClass {
    Wal,
    Manifest,
    Sst,
    Fleet,
    Other,
}

impl ObjClass {
    pub fn of(path: &str) -> Self {
        match crate::store_timing::classify(path) {
            0 => ObjClass::Wal,
            1 => ObjClass::Manifest,
            2 => ObjClass::Sst,
            3 => ObjClass::Fleet,
            _ => ObjClass::Other,
        }
    }
}

// ---- mechanism coverage ---------------------------------------------

/// Named counters for the mechanisms a scenario claims to exercise.
///
/// A fencing scenario in which nothing was ever fenced is not a passing
/// run, it is an invalid one. The docker ladder taught this expensively:
/// D3 and D4 passed their order checks for several passes while never once
/// triggering the mechanism under test (`bench/docker/harness/README.md`).
#[derive(Debug, Default)]
pub struct Coverage {
    counters: Mutex<HashMap<&'static str, u64>>,
}

impl Coverage {
    pub fn hit(&self, name: &'static str) {
        *self.counters.lock().unwrap().entry(name).or_insert(0) += 1;
    }

    pub fn get(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    pub fn snapshot(&self) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> = self
            .counters
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect();
        v.sort();
        v
    }

    /// Fail the scenario if a mechanism it claims to test never fired.
    pub fn require(&self, names: &[&str]) -> Result<(), String> {
        let missing: Vec<&str> = names.iter().copied().filter(|n| self.get(n) == 0).collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "scenario never exercised {missing:?}; coverage={:?}",
                self.snapshot()
            ))
        }
    }
}

/// Mechanism names. Scenarios `require` the ones they claim to test.
pub mod mech {
    pub const STORE_ERROR: &str = "store_error_before_dispatch";
    pub const STORE_LOST_RESPONSE: &str = "store_success_response_lost";
    pub const STORE_LATENCY: &str = "store_latency_injected";
    pub const APPEND_ACKED: &str = "append_acked";
    pub const APPEND_REJECTED: &str = "append_rejected";
    pub const APPEND_UNKNOWN: &str = "append_unknown_outcome";
    pub const APPEND_RETRIED: &str = "append_retried";
    pub const PRODUCER_DUPLICATE: &str = "producer_duplicate_suppressed";
    pub const OLD_OWNER_FENCED: &str = "old_owner_fenced";
    pub const AFTER_DURABLE_BEFORE_ACK: &str = "after_durable_before_ack";
    pub const CLIENT_DEADLINE_EXPIRED: &str = "client_deadline_expired";
    pub const IN_FLIGHT_AT_FENCE: &str = "append_in_flight_at_fence";
    pub const READ_FROM_HISTORY: &str = "read_served_from_history";
}

#[cfg(test)]
mod dst_tests;
