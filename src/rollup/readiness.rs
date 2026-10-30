//! The rollup's member of the billing-readiness report
//! (`/operator/billing.json` `rollup`; RUNBOOK "Billing pipeline
//! runbooks"): cursor progress, close debt and the publication outboxes,
//! with the monthly artifact outbox split by publishability (bug #7, C1;
//! edge record #93), and the stored figures admission holds (stored-cap
//! B3, edge record #157).

use super::{K_OLDEST_UNCLOSED, UsageRollup, pending_artifact_row};
use crate::quota::stored::StoredTable;
use serde::Serialize;

/// The report's cap: each outbox scan stops after this many publishable
/// rows, as `pendingArtifacts` always did.
const REPORTED_PENDING: usize = 1000;

/// The projects `stored.largest` lists: those that wrote the most.
const REPORTED_LARGEST: usize = 10;

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
    stored: Option<StoredReport>,
}

/// The stored figures admission holds, as the rollup last published them:
/// the projects they cover, their totals, when, and the projects that
/// wrote the most, largest first (ties by project id).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredReport {
    projects: usize,
    total_bytes: u64,
    written_bytes: u64,
    as_of_ms: i64,
    largest: Vec<LargestProject>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LargestProject {
    project: String,
    stored_bytes: u64,
    written_bytes: u64,
}

impl StoredReport {
    /// The report of `table`; none before the rollup first published.
    fn of(table: &StoredTable) -> Option<Self> {
        let as_of_ms = table.published_ms();
        if as_of_ms == 0 {
            return None;
        }
        let mut figures = table.figures();
        figures
            .sort_by(|a, b| (b.written, a.project.as_str()).cmp(&(a.written, b.project.as_str())));
        Some(Self {
            projects: figures.len(),
            total_bytes: figures.iter().map(|f| f.bytes).sum(),
            written_bytes: figures.iter().map(|f| f.written).sum(),
            as_of_ms,
            largest: figures
                .into_iter()
                .take(REPORTED_LARGEST)
                .map(|f| LargestProject {
                    project: f.project.as_str().to_owned(),
                    stored_bytes: f.bytes,
                    written_bytes: f.written,
                })
                .collect(),
        })
    }
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
        let mut iter = self
            .db
            .scan_prefix_with_options(
                &b"artifact-pending/"[..],
                ..,
                &crate::rollup::telemetry_scan(),
            )
            .await?;
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
    /// landed at `last_apply_ms` (0: none yet) and which publishes into
    /// admission's `stored` table. A scan or read that fails reports zero
    /// or null, as the report always has.
    pub(crate) async fn readiness(
        &self,
        now_ms: i64,
        last_apply_ms: i64,
        stored: &StoredTable,
    ) -> RollupReadiness {
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
            stored: StoredReport::of(stored),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UsageRollup;
    use crate::quota::stored::{StoredFigure, StoredTable};
    use serde_json::Value;
    use slatedb::Db;
    use std::sync::Arc;

    /// The report member's values for a rollup with an applied ledger, a
    /// closed month, a pending correction and one blocked artifact row; the
    /// age is whole seconds since the last apply, truncated. `stored` is
    /// null until the rollup first publishes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
        let table = StoredTable::default();
        let aged = member(r.readiness(10_999, 3_000, &table).await);
        let expected = [
            ("running", Value::from(true)),
            ("lastApplyMs", Value::from(3_000)),
            ("lastApplyAgeSecs", Value::from(7)),
            ("oldestUnclosedMonth", Value::from("2026-08")),
            ("pendingArtifacts", Value::from(1)),
            ("pendingArtifactsBlockedCorrupt", Value::from(1)),
            ("pendingArtifactsTotal", Value::from(2)),
            ("pendingCorrectionArtifacts", Value::from(1)),
            ("stored", Value::Null),
        ];
        for (name, value) in expected {
            assert_eq!(aged[name], value, "{name}");
        }
        assert_eq!(aged.as_object().unwrap().len(), 9, "{aged}");
        let never = member(r.readiness(10_999, 0, &table).await);
        assert_eq!(never["lastApplyAgeSecs"], Value::from(-1), "no apply yet");
        db.close().await.unwrap();
    }

    /// One `largest` entry: (project, storedBytes, writtenBytes).
    type Listed = (String, u64, u64);

    /// `stored`'s (projects, totalBytes, writtenBytes, asOfMs), and its
    /// `largest` as (project, storedBytes, writtenBytes).
    fn stored_member(member: &Value) -> ([Value; 4], Vec<Listed>) {
        let stored = &member["stored"];
        let totals =
            ["projects", "totalBytes", "writtenBytes", "asOfMs"].map(|m| stored[m].clone());
        let largest = stored["largest"].as_array().unwrap().iter().map(|e| {
            let project = e["project"].as_str().unwrap().to_owned();
            (
                project,
                e["storedBytes"].as_u64().unwrap(),
                e["writtenBytes"].as_u64().unwrap(),
            )
        });
        (totals, largest.collect())
    }

    /// `stored` totals every published project's figures, dates them, and
    /// lists the ten that wrote the most, largest first and ties by project
    /// id; a later publication moves them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn the_stored_member_totals_the_figures_and_lists_the_ten_that_wrote_most() {
        let db = Arc::new(
            Db::builder("stored", Arc::new(object_store::memory::InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let r = UsageRollup {
            db: db.clone(),
            close_rows_visited: Default::default(),
        };
        let figure = |i: u64, bytes, written| StoredFigure {
            project: crate::tenant::ProjectId::new(&format!("p{i:02}")).unwrap(),
            bytes,
            written,
        };
        // p00..p11 wrote 100..1,200 and store half; p05 stores none and ties p06 at 700.
        let mut figures: Vec<_> = (0..12)
            .map(|i| figure(i, 50 * (i + 1), 100 * (i + 1)))
            .collect();
        figures[5] = figure(5, 0, 700);
        let table = StoredTable::default();
        table.publish(&figures, 4_000);
        let member = serde_json::to_value(r.readiness(5_000, 4_000, &table).await).unwrap();
        let (totals, largest) = stored_member(&member);
        let want = [12, 50 * 78 - 300, 100 * 78 + 100, 4_000].map(Value::from);
        assert_eq!(totals, want, "projects, totalBytes, writtenBytes, asOfMs");
        let listed = |i: usize| {
            let f = &figures[i];
            (f.project.as_str().to_owned(), f.bytes, f.written)
        };
        assert_eq!(largest, [11, 10, 9, 8, 7, 5, 6, 4, 3, 2].map(listed));
        assert_eq!(member["stored"].as_object().unwrap().len(), 5);
        table.publish(&[figure(0, 0, 5_000)], 4_500);
        let member = serde_json::to_value(r.readiness(5_000, 4_500, &table).await).unwrap();
        let (totals, largest) = stored_member(&member);
        let want = [12, 50 * 78 - 300 - 50, 100 * 78 + 100 - 100 + 5_000, 4_500];
        assert_eq!(totals, want.map(Value::from));
        assert_eq!(largest.len(), 10);
        assert_eq!(largest[0], ("p00".to_owned(), 0, 5_000));
        db.close().await.unwrap();
    }
}
