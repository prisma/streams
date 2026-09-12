use super::*;
impl CommitTransaction<'_> {
    async fn config_record(
        &self,
        local: &StreamOverlay,
        hash: &[u8; 16],
        consumer: &str,
    ) -> Result<Option<ConsumerRecord>, String> {
        if let Some(record) = local.queue.configs.get(consumer) {
            return Ok(Some(record.clone()));
        }
        self.engine
            .db
            .get(config_key(hash, consumer))
            .await
            .map_err(|error| error.to_string())?
            .map(|value| {
                decode_consumer_record(&value)
                    .map_err(|error| format!("consumer_config_corrupt: {error}"))
            })
            .transpose()
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "CommitTransaction::config_put; a config put takes the overlay, stream, consumer, config and reply as the queue dispatch resolved them; a request struct would exist only for this signature"
    )]
    pub(super) async fn config_put(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        consumer: String,
        cfg: ConsumerConfig,
        resp: QueueReply,
    ) {
        let existing = match self.config_record(local, &hash, &consumer).await {
            Ok(record) => record,
            Err(error) => {
                self.effects.queue_acks.push((resp, Err(error)));
                return;
            }
        };
        let out = match existing {
            Some(rec) if rec.state == ConsumerLifecycle::Active && rec.config == cfg => {
                QueueOut::Config {
                    rec: Some(rec),
                    created: false,
                    conflict: false,
                }
            }
            Some(rec) if rec.state == ConsumerLifecycle::Active => QueueOut::Config {
                rec: Some(rec),
                created: false,
                conflict: true,
            },
            Some(rec) if rec.state == ConsumerLifecycle::Deleting => QueueOut::Config {
                rec: Some(rec),
                created: false,
                conflict: true,
            },
            other => {
                let Some(cgen) = other.map_or(Some(1), |r| r.generation.checked_add(1)) else {
                    self.effects
                        .queue_acks
                        .push((resp, Err("consumer generation exhausted".into())));
                    return;
                };
                let rec = ConsumerRecord {
                    generation: cgen,
                    state: ConsumerLifecycle::Active,
                    config: cfg,
                };
                let enc = serde_json::to_vec(&rec).unwrap_or_default();
                self.batch.put(config_key(&hash, &consumer), enc);
                self.extra_writes = true;
                local.queue.configs.insert(consumer.clone(), rec.clone());
                QueueOut::Config {
                    rec: Some(rec),
                    created: true,
                    conflict: false,
                }
            }
        };
        self.effects.queue_acks.push((resp, Ok(out)));
    }
    pub(super) async fn config_get(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        consumer: String,
        resp: QueueReply,
    ) {
        let rec = match self.config_record(local, &hash, &consumer).await {
            Ok(record) => record,
            Err(error) => {
                self.effects.queue_acks.push((resp, Err(error)));
                return;
            }
        };
        self.effects.queue_acks.push((
            resp,
            Ok(QueueOut::Config {
                rec,
                created: false,
                conflict: false,
            }),
        ));
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "CommitTransaction::config_lifecycle; a lifecycle step takes the overlay, stream, consumer, expected generation, deletion flag and reply as the queue dispatch resolved them; a request struct would exist only for this signature"
    )]
    pub(super) async fn config_lifecycle(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        consumer: String,
        expect_gen: u64,
        deleting: bool,
        resp: QueueReply,
    ) {
        let existing = match self.config_record(local, &hash, &consumer).await {
            Ok(record) => record,
            Err(error) => {
                self.effects.queue_acks.push((resp, Err(error)));
                return;
            }
        };
        let Some(rec) = existing else {
            self.effects.queue_acks.push((
                resp,
                Err("consumer_not_found: no record for lifecycle change".into()),
            ));
            return;
        };
        if rec.generation != expect_gen {
            self.effects.queue_acks.push((
                resp,
                Err(format!(
                    "consumer_generation_conflict: record gen {} != expected {}",
                    rec.generation, expect_gen
                )),
            ));
            return;
        }
        let target = if deleting {
            ConsumerLifecycle::Deleting
        } else {
            ConsumerLifecycle::Deleted
        };
        let legal = matches!(
            (rec.state, target),
            (ConsumerLifecycle::Active, ConsumerLifecycle::Deleting)
                | (ConsumerLifecycle::Deleting, ConsumerLifecycle::Deleting)
                | (ConsumerLifecycle::Deleting, ConsumerLifecycle::Deleted)
                | (ConsumerLifecycle::Deleted, ConsumerLifecycle::Deleted)
        );
        if !legal {
            self.effects.queue_acks.push((
                resp,
                Err(format!(
                    "consumer_lifecycle_conflict: {:?} -> {:?}",
                    rec.state, target
                )),
            ));
            return;
        }
        let mut next = rec.clone();
        next.state = target;
        if next != rec {
            let enc = serde_json::to_vec(&next).unwrap_or_default();
            self.batch.put(config_key(&hash, &consumer), enc);
            self.extra_writes = true;
        }
        local.queue.configs.insert(consumer.clone(), next.clone());
        self.effects.queue_acks.push((
            resp,
            Ok(QueueOut::Config {
                rec: Some(next),
                created: false,
                conflict: false,
            }),
        ));
    }
}
