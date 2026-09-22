//! TTL renewal has explicit admission/completion; retries keep one epoch/target.
use super::{CreationService, StreamDesc};
use crate::{
    application::request_work::{Action, Key, Kind, Ticket, WorkError},
    registry::{Mutation, Registry},
};
use std::sync::Arc;

/// The longest idle window either create surface admits: 2^32 - 1 seconds,
/// about 136 years. "Never expires" is spelled by omitting the TTL, so a
/// larger number carries no policy a stream could act on. Under this bound
/// every derived instant is exact in an f64 and a JavaScript number, inside
/// chrono's calendar, and six orders of magnitude clear of the i64 edge.
const MAX_TTL_SECS: u64 = u32::MAX as u64;

/// The one admission rule `Stream-TTL` and `expiry.idle` share. A window past
/// the ceiling is refused, never clamped: two different requests must not
/// compare as the same configuration.
pub(crate) fn admit_ttl(ttl_secs: u64) -> Option<u64> {
    (ttl_secs <= MAX_TTL_SECS).then_some(ttl_secs)
}

/// An idle window in milliseconds, total over every `u64`. Descriptors written
/// before the ceiling existed, and the forks that inherit them, carry windows no
/// parser vouched for; they saturate to "never" instead of wrapping into the past.
#[warn(clippy::arithmetic_side_effects)]
fn window_ms(ttl_secs: u64) -> i64 {
    i64::try_from(ttl_secs)
        .unwrap_or(i64::MAX)
        .saturating_mul(1000)
}

/// The instant a window opened at `now_ms` closes.
#[warn(clippy::arithmetic_side_effects)]
fn expiry_after(now_ms: i64, ttl_secs: u64) -> i64 {
    now_ms.saturating_add(window_ms(ttl_secs))
}

/// A fresh descriptor's expiry: its window opens when creation reads the clock.
pub(super) fn expiry_from_now(ttl_secs: u64) -> i64 {
    expiry_after(crate::shard::now_ms(), ttl_secs)
}

pub(crate) struct TtlMutation {
    registry: Arc<Registry>,
    stream: crate::tenant::TenantStreamRef,
    epoch: String,
    ttl: u64,
    target: i64,
}
impl TtlMutation {
    pub(crate) async fn run(self) -> Result<(), WorkError> {
        if self.target <= crate::shard::now_ms() {
            return Err(WorkError::TimedOut);
        }
        self.registry
            .mutate_incarnation(&self.stream, &self.epoch, |current| {
                if current.deleted
                    || current.soft_deleted
                    || current.ttl_secs != Some(self.ttl)
                    || !current
                        .expires_at_ms
                        .is_some_and(|expires| expires < self.target)
                {
                    return Mutation::Decline(());
                }
                let mut next = current.to_persisted();
                next.expires_at_ms = Some(self.target);
                Mutation::Write(next, ())
            })
            .await
            .map_err(|error| WorkError::Storage(error.to_string()))?;
        Ok(())
    }
}
impl Drop for TtlMutation {
    fn drop(&mut self) {
        // Also revalidate after an ambiguous cancelled CAS.
        self.registry.invalidate(&self.stream);
    }
}
impl CreationService {
    pub(crate) fn touch_ttl(self: &Arc<Self>, desc: &StreamDesc) -> Result<Ticket, WorkError> {
        let (Some(ttl), Some(expires)) = (desc.ttl_secs, desc.expires_at_ms) else {
            return Ok(Ticket::complete());
        };
        let now = crate::shard::now_ms();
        let window = window_ms(ttl);
        if desc.deleted
            || desc.soft_deleted
            || expires <= now
            || expires.saturating_sub(now) >= window - window / 4
        {
            return Ok(Ticket::complete());
        }
        let key = Key {
            stream: desc.sref(),
            epoch: desc.stream_epoch.clone(),
            kind: Kind::Ttl,
        };
        let action = Action::Ttl(TtlMutation {
            registry: self.registry.clone(),
            stream: key.stream.clone(),
            epoch: key.epoch.clone(),
            ttl,
            target: expiry_after(now, ttl),
        });
        self.runtime.request_work.submit(key, action)
    }
    pub(crate) async fn renew_ttl(self: &Arc<Self>, desc: &StreamDesc) -> Result<(), WorkError> {
        self.touch_ttl(desc)?.wait().await
    }
    #[cfg(test)]
    pub(crate) fn pending_ttl_for_tests(&self) -> usize {
        self.runtime.request_work.pending_ttl()
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_TTL_SECS, admit_ttl, expiry_after, expiry_from_now, window_ms};

    #[test]
    fn the_ceiling_is_the_documented_number_and_is_inclusive() {
        assert_eq!(MAX_TTL_SECS, 4_294_967_295);
        assert_eq!(admit_ttl(0), Some(0));
        assert_eq!(admit_ttl(MAX_TTL_SECS), Some(MAX_TTL_SECS));
        assert_eq!(admit_ttl(MAX_TTL_SECS + 1), None);
        assert_eq!(admit_ttl(u64::MAX), None);
    }

    #[test]
    fn an_expiry_is_total_over_every_persisted_window() {
        let now = 1_790_000_000_000_i64;
        assert_eq!(expiry_after(now, 0), now);
        assert_eq!(expiry_after(now, 3600), now + 3_600_000);
        assert_eq!(expiry_after(now, MAX_TTL_SECS), now + 4_294_967_295_000);
        // The multiply saturates (was: debug panic, release wrap to the past).
        assert_eq!(window_ms(9_223_372_036_854_776), i64::MAX);
        assert_eq!(expiry_after(now, 9_223_372_036_854_776), i64::MAX);
        // The add saturates on its own: the product fits, the sum does not.
        assert_eq!(window_ms(9_223_372_036_854_775), 9_223_372_036_854_775_000);
        assert_eq!(expiry_after(now, 9_223_372_036_854_775), i64::MAX);
        // The cast no longer sign-wraps (was: -1000 ms, a past expiry in every profile).
        assert_eq!(window_ms(u64::MAX), i64::MAX);
        assert_eq!(expiry_after(now, u64::MAX), i64::MAX);
    }

    #[test]
    fn a_fresh_expiry_opens_its_window_at_the_clock() {
        let before = crate::shard::now_ms();
        let at = expiry_from_now(60);
        let after = crate::shard::now_ms();
        assert!(
            (before + 60_000..=after + 60_000).contains(&at),
            "{before} {at} {after}"
        );
    }
}
