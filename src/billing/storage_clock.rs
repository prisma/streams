//! A segment billing row's storage clock: the byte-time integral it
//! advances on every billed commit, closing each UTC month it crosses.

use super::{SegmentBillingMetaV1, month_start_ms, next_month, utc_year_month};

impl SegmentBillingMetaV1 {
    pub(crate) fn month_byte_ms(&self) -> u128 {
        self.month_storage_byte_ms.parse().unwrap_or(0)
    }

    /// Advance the storage integral to `now_ms`, splitting at UTC month
    /// boundaries. Each closed month's final accumulators are handed to
    /// `on_month_close` BEFORE the month fields reset — the committer
    /// stages them into the usage outbox in the same WriteBatch.
    ///
    /// The month fields always name the month that holds the clock, so each
    /// boundary lies past the accounted-through instant. A debug build (the
    /// tests, and the mutation leg's quality profile) asserts it, so month
    /// math that does not advance panics instead of closing months without
    /// end. A release build keeps the bounded catch-up: returning early
    /// would leave storage unbilled, since a storage close zeroes the gauge
    /// after this advance.
    pub(crate) fn advance_storage_clock(
        &mut self,
        now_ms: i64,
        mut on_month_close: impl FnMut(&SegmentBillingMetaV1),
    ) {
        if self.storage_accounted_through_ms == 0 {
            // First observation: start the clock, open the month.
            let (y, m) = utc_year_month(now_ms);
            self.storage_accounted_through_ms = now_ms;
            self.month_year = y;
            self.month_month = m;
            return;
        }
        if now_ms <= self.storage_accounted_through_ms {
            return;
        }
        loop {
            let (ny, nm) = next_month(self.month_year, self.month_month);
            let boundary = month_start_ms(ny, nm);
            debug_assert!(
                boundary > self.storage_accounted_through_ms,
                "storage clock month {}-{} ends at {boundary}, not past {}",
                self.month_year,
                self.month_month,
                self.storage_accounted_through_ms
            );
            let upto = now_ms.min(boundary);
            let dt = u128::try_from((upto - self.storage_accounted_through_ms).max(0)).unwrap_or(0);
            let add = dt * self.owned_frame_bytes_current as u128;
            let cur = self.month_byte_ms();
            self.month_storage_byte_ms = (cur + add).to_string();
            self.storage_accounted_through_ms = upto;
            if upto < boundary {
                return;
            }
            // Month closed: report final numbers, then open the next.
            on_month_close(self);
            self.month_year = ny;
            self.month_month = nm;
            self.month_ingest_payload_bytes = 0;
            self.month_ingest_records = 0;
            self.month_storage_byte_ms = "0".to_string();
        }
    }
}
