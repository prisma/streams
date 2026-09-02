//! Typed configuration advisories (PR 4.1.1: moved out of validation.rs
//! — a notice is a general configuration DIAGNOSTIC, consumed by the
//! profile certification, validation and bootstrap alike, not
//! validation's private detail; keeping it there made profile.rs and
//! validation.rs import each other).
//!
//! Validation emits NO logs — it must not announce one subsection as
//! certified before a later subsection rejects the whole configuration
//! — so notices are collected and emitted by bootstrap only after the
//! entire configuration was accepted. Every message carries its own
//! numbers: this module depends on nothing else in the crate.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigNotice {
    MemoryProfileCertified {
        profile: String,
    },
    FleetAuthStaticBridge,
    FeedBudgetAboveReleaseMax {
        configured: u64,
        max: u64,
        profile: String,
    },
    CoarseInitialShards {
        configured: usize,
        fleet_max: u64,
        suggested: usize,
    },
    /// The descriptor ceiling leaves nothing below the reserve held
    /// for storage clients, peer pools, maintenance, stdio, listener.
    DescriptorReserveTight {
        nofile_hard: u64,
        reserve: u64,
    },
    SseCapClamped {
        configured: u64,
        nofile_hard: u64,
        reserve: u64,
        effective: u64,
    },
    SseCapExceedsDescriptors {
        configured: u64,
        nofile_hard: u64,
        reserve: u64,
    },
    /// The platform reported no descriptor ceiling (no `getrlimit`), so
    /// the release posture's SSE cap could not be resolved against it.
    DescriptorCeilingUnknown {
        configured: u64,
    },
}

impl ConfigNotice {
    /// Severity for the emitter: `true` = warning, `false` = info.
    pub fn is_warning(&self) -> bool {
        !matches!(self, Self::MemoryProfileCertified { .. })
    }
}

impl std::fmt::Display for ConfigNotice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryProfileCertified { profile } => write!(
                f,
                "memory profile certified: compute-1g (all DB families) profile={profile}"
            ),
            Self::FleetAuthStaticBridge => write!(
                f,
                "FLEET_AUTH_MODE=static: the shared bridge token is a NAMED legacy \
                 posture; the release posture requires workload identity (§14.1)"
            ),
            Self::FeedBudgetAboveReleaseMax {
                configured,
                max,
                profile,
            } => write!(
                f,
                "SSE_FEED_TOTAL_BYTES={configured} exceeds the {max}-byte release-safe \
                 maximum for memory profile {profile:?}"
            ),
            Self::CoarseInitialShards {
                configured,
                fleet_max,
                suggested,
            } => write!(
                f,
                "INITIAL_SHARDS={configured} < 4×FLEET_MAX={fleet_max}: a fresh topology \
                 this coarse draws unevenly under rendezvous and the rebalancer flaps \
                 against return-home; use >= {suggested}"
            ),
            Self::DescriptorReserveTight {
                nofile_hard,
                reserve,
            } => write!(
                f,
                "nofile_hard={nofile_hard} leaves no safe SSE connection capacity after \
                 the {reserve}-descriptor reserve"
            ),
            Self::SseCapClamped {
                configured,
                nofile_hard,
                reserve,
                effective,
            } => write!(
                f,
                "SSE_MAX_CONNECTIONS={configured} exceeds what nofile_hard={nofile_hard} can \
                 carry with a {reserve}-descriptor reserve; clamping the effective cap to \
                 {effective} (raise RLIMIT_NOFILE or lower SSE_MAX_CONNECTIONS)"
            ),
            Self::SseCapExceedsDescriptors {
                configured,
                nofile_hard,
                reserve,
            } => write!(
                f,
                "SSE_MAX_CONNECTIONS={configured} exceeds what nofile_hard={nofile_hard} can \
                 carry with a {reserve}-descriptor reserve; descriptor exhaustion wedges \
                 parked subscriptions (~1.5k seen in the field)"
            ),
            Self::DescriptorCeilingUnknown { configured } => write!(
                f,
                "the platform reported no descriptor ceiling; the release posture's \
                 SSE_MAX_CONNECTIONS={configured} stands unresolved against RLIMIT_NOFILE"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reserve travels IN the notice: the message is complete on
    /// its own, wherever it is rendered.
    #[test]
    fn descriptor_notices_render_their_own_reserve() {
        let n = ConfigNotice::SseCapClamped {
            configured: 10_000,
            nofile_hard: 4_096,
            reserve: 1_024,
            effective: 3_072,
        };
        assert!(n.is_warning());
        let s = n.to_string();
        assert!(s.contains("1024-descriptor reserve"), "{s}");
        assert!(s.contains("clamping the effective cap to 3072"), "{s}");
        assert!(
            !ConfigNotice::MemoryProfileCertified {
                profile: "compute-1g".into()
            }
            .is_warning()
        );
    }
}
