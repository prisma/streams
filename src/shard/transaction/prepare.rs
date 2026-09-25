use super::*;
impl CommitTransaction<'_> {
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::expand; the expansion nests the trim cursor walk inside the trim-tick arm of the op match; flattening it would separate the walk from the tick that schedules it"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::expand; a poisoned trim debt or cursor may hold a half-recorded stream set; recovering it could trim a stream twice or never"
    )]
    pub(super) fn expand(engine: &ShardEngine, ops: Vec<CommitOp>) -> Vec<CommitOp> {
        const TRIM_STREAMS_PER_TICK: usize = 64;
        let mut expanded: Vec<CommitOp> = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                CommitOp::AbsorbedBatch { streams, v2 } => {
                    expanded.extend(streams.into_iter().map(|(hash, upto, bytes)| {
                        CommitOp::Absorbed {
                            hash,
                            upto,
                            bytes,
                            v2,
                        }
                    }));
                }
                CommitOp::TrimTick => {
                    use std::ops::Bound;
                    let debt = engine.trim_debt.lock().unwrap();
                    if debt.is_empty() {
                        continue;
                    }
                    let mut cur = engine.trim_cursor.lock().unwrap();
                    let picked: Vec<[u8; 16]> = debt
                        .range((Bound::Excluded(*cur), Bound::Unbounded))
                        .chain(debt.range((Bound::Unbounded, Bound::Included(*cur))))
                        .take(TRIM_STREAMS_PER_TICK)
                        .copied()
                        .collect();
                    if let Some(last) = picked.last() {
                        *cur = *last;
                    }
                    expanded.extend(picked.into_iter().map(|hash| CommitOp::TrimStep { hash }));
                }
                other => expanded.push(other),
            }
        }
        expanded
    }
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::billing_rows; the row load nests the failure capture inside the uncached-hash branch; flattening it would separate the failure from the hash it stops at"
    )]
    pub(super) async fn billing_rows(
        engine: &ShardEngine,
        ops: &[CommitOp],
    ) -> Result<HashMap<[u8; 16], Option<crate::billing::SegmentBillingMetaV1>>, String> {
        let mut billing_rows = HashMap::new();
        let mut billing_failure = None;
        for op in ops {
            let hash = match op {
                CommitOp::Append(r) if r.billing.is_some() => Some(r.hash),
                CommitOp::UsageAck { hash, .. }
                | CommitOp::BillingClose { hash, .. }
                | CommitOp::BillingRetained { hash, .. } => Some(*hash),
                _ => None,
            };
            if let Some(hash) = hash
                && !billing_rows.contains_key(&hash)
            {
                match engine.load_billing_meta(hash).await {
                    Ok(row) => {
                        billing_rows.insert(hash, row);
                    }
                    Err(error) => {
                        billing_failure = Some(error);
                        break;
                    }
                }
            }
        }
        if let Some(error) = billing_failure {
            return Err(error.to_string());
        }
        Ok(billing_rows)
    }
}
