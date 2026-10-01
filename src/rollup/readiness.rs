//! The rollup's member of the billing-readiness report
//! (`/operator/billing.json` `rollup`; RUNBOOK "Billing pipeline
//! runbooks"): cursor progress, close debt and the publication outboxes,
//! with the monthly artifact outbox split by publishability (bug #7, C1;
//! edge record #93).

use super::{K_OLDEST_UNCLOSED, UsageRollup, pending_artifact_row};
use serde::Serialize;

/// The report's cap: each outbox scan stops after this many publishable
/// rows, as `pendingArtifacts` always did.
const REPORTED_PENDING: usize = 1000;

/// The monthly artifact outbox as one capped scan sees it: the rows the
/// publisher publishes, the rows it skips forever because their key or
/// body does not decode (an operator repairs them), and both together.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ArtifactOutbox {
    pub(super) publishable: usize,
    pub(super) blocked_corrupt: usize,
    pub(super) total: usize,
}

/// The `rollup` member of `/operator/billing.json` on the instance that
/// runs the rollup. serde_json keeps object members sorted, so the field
/// order here never reaches the wire.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RollupReadiness {
    running: bool,
    last_apply_ms: i64,
    last_apply_age_secs: i64,
    oldest_unclosed_month: Option<String>,
    pending_artifacts: usize,
    pending_artifacts_blocked_corrupt: usize,
    pending_artifacts_total: usize,
    pending_correction_artifacts: usize,
}

impl UsageRollup {
    /// Counts `artifact-pending/` rows, classifying each as the publisher
    /// does (`pending_artifact_row`, which logs every blocked row), and
    /// stops after the first row at which `max_publishable` rows are
    /// publishable: the row where `pending_artifacts` stops.
    pub(super) async fn artifact_outbox(
        &self,
        max_publishable: usize,
    ) -> anyhow::Result<ArtifactOutbox> {
        let mut outbox = ArtifactOutbox::default();
        let mut iter = self.db.scan_prefix(&b"artifact-pending/"[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            outbox.total += 1;
            if pending_artifact_row(&kv.key, &kv.value).is_some() {
                outbox.publishable += 1;
            } else {
                outbox.blocked_corrupt += 1;
            }
            if outbox.publishable >= max_publishable {
                break;
            }
        }
        Ok(outbox)
    }

    /// The report member at `now_ms` for a rollup whose last ledger apply
    /// landed at `last_apply_ms` (0: none yet). A scan or read that fails
    /// reports zero or null, as the report always has.
    pub(crate) async fn readiness(&self, now_ms: i64, last_apply_ms: i64) -> RollupReadiness {
        let outbox = self
            .artifact_outbox(REPORTED_PENDING)
            .await
            .unwrap_or_default();
        let pending_correction_artifacts = self
            .pending_correction_artifacts(REPORTED_PENDING)
            .await
            .map(|v| v.len())
            .unwrap_or(0);
        let oldest_unclosed_month = self
            .db
            .get(K_OLDEST_UNCLOSED)
            .await
            .ok()
            .flatten()
            .map(|v| String::from_utf8_lossy(&v).to_string());
        RollupReadiness {
            running: true,
            last_apply_ms,
            last_apply_age_secs: if last_apply_ms > 0 {
                (now_ms - last_apply_ms) / 1000
            } else {
                -1
            },
            oldest_unclosed_month,
            pending_artifacts: outbox.publishable,
            pending_artifacts_blocked_corrupt: outbox.blocked_corrupt,
            pending_artifacts_total: outbox.total,
            pending_correction_artifacts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UsageRollup;
    use serde_json::Value;
    use slatedb::Db;
    use std::sync::Arc;

    /// The report member's values for a rollup with an applied ledger, a
    /// closed month, a pending correction and one blocked artifact row; the
    /// age is whole seconds since the last apply, truncated.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_report_member_reads_the_rollup_and_ages_the_last_apply() {
        let db = Arc::new(
            Db::builder("readiness", Arc::new(object_store::memory::InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let r = UsageRollup {
            db: db.clone(),
            close_rows_visited: Default::default(),
        };
        for (key, body) in [
            ("meta/oldest-unclosed-month", "2026-08"),
            ("corr-pending/2026-07/a/p/s/c1", "{}"),
            ("artifact-pending/2026-07/a/p/s", "{}"),
            ("artifact-pending/2026-07/a/p/t", "not json"),
        ] {
            db.put(key, body).await.unwrap();
        }
        let member = |readiness| serde_json::to_value(readiness).unwrap();
        let aged = member(r.readiness(10_999, 3_000).await);
        let expected = [
            ("running", Value::from(true)),
            ("lastApplyMs", Value::from(3_000)),
            ("lastApplyAgeSecs", Value::from(7)),
            ("oldestUnclosedMonth", Value::from("2026-08")),
            ("pendingArtifacts", Value::from(1)),
            ("pendingArtifactsBlockedCorrupt", Value::from(1)),
            ("pendingArtifactsTotal", Value::from(2)),
            ("pendingCorrectionArtifacts", Value::from(1)),
        ];
        for (name, value) in expected {
            assert_eq!(aged[name], value, "{name}");
        }
        assert_eq!(aged.as_object().unwrap().len(), 8, "{aged}");
        let never = member(r.readiness(10_999, 0).await);
        assert_eq!(never["lastApplyAgeSecs"], Value::from(-1), "no apply yet");
        db.close().await.unwrap();
    }
}
