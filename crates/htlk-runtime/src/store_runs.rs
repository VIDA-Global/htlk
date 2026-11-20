//! Durable host receipt and initial run publication transactions.
use super::*;
use crate::{identity::ArtifactId, records::ManifestEntry, run::PreparedRun};
use std::sync::Arc;

pub(crate) struct HostRequest {
    pub principal: String,
    pub method: String,
    pub target: String,
    pub key: String,
    pub digest: Digest,
}
pub(crate) enum AdmissionTime {
    Relative(i64),
    Absolute {
        not_after_ms: i64,
        observed: i64,
        clock: Arc<dyn crate::clock::CoordinatorClock>,
    },
}
pub(crate) struct RunIdBlock {
    namespace: String,
    remaining: std::ops::RangeInclusive<i64>,
}
impl SqliteStore {
    pub(crate) async fn coordinator_time(&self, observed: i64) -> Result<i64, StoreError> {
        timestamp(observed)?;
        self.connection
            .read(move |db| observed_time(db, observed))
            .await
            .map_err(StoreError::from_worker)
    }
    pub(crate) async fn reserve_run_id(&self) -> Result<RunId, StoreError> {
        let mut block = self.run_ids.lock().await;
        if block
            .as_ref()
            .is_none_or(|block| block.remaining.is_empty())
        {
            *block = Some(
                self.connection
                    .call(move |db| -> Result<RunIdBlock, StoreError> {
                        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                        let (namespace, previous) = tx.query_row(
                            "SELECT namespace,next_run FROM runtime_identity WHERE singleton=1",
                            [],
                            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
                        )?;
                        if namespace.len() != 32
                            || !namespace
                                .bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                            || previous < 0
                        {
                            return Err(StoreError::Corrupt);
                        }
                        let next = previous.checked_add(1).ok_or(StoreError::Invalid)?;
                        let end = previous.saturating_add(64);
                        tx.execute(
                            "UPDATE runtime_identity SET next_run=?1 WHERE singleton=1",
                            [end],
                        )?;
                        tx.commit()?;
                        Ok(RunIdBlock {
                            namespace,
                            remaining: next..=end,
                        })
                    })
                    .await
                    .map_err(StoreError::from_worker)?,
            );
        }
        let block = block.as_mut().ok_or(StoreError::Corrupt)?;
        let next = block.remaining.next().ok_or(StoreError::Corrupt)?;
        RunId::new(
            format!("run:{}:{next}", block.namespace),
            &self.config.limits,
        )
        .map_err(|_| StoreError::Invalid)
    }
    pub(crate) async fn host_receipt(
        &self,
        request: &HostRequest,
        observed: i64,
    ) -> Result<Option<RunId>, StoreError> {
        request.check(&self.config.limits)?;
        let request = request.owned();
        let limits = self.config.limits.clone();
        self.connection
            .read(move |db| -> Result<Option<RunId>, StoreError> {
                let tx = db.transaction()?;
                let now = observed_time(&tx, observed)?;
                let receipt = receipt(&tx, &request, now, &limits, false)?;
                tx.commit()?;
                Ok(receipt)
            })
            .await
            .map_err(StoreError::from_worker)
    }
    pub(crate) async fn initialize_run(
        &self,
        prepared: PreparedRun,
        request: HostRequest,
        time: AdmissionTime,
        retention: i64,
    ) -> Result<RunId, StoreError> {
        request.check(&self.config.limits)?;
        let limits = self.config.limits.clone();
        self.connection.call(move|db|->Result<RunId,StoreError>{
            let tx=db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (observed, not_after_ms) = match time {
                AdmissionTime::Relative(observed) => (observed, None),
                AdmissionTime::Absolute { not_after_ms, observed, clock } => {
                    let current = clock.now_ms().map_err(|_| StoreError::Invalid)?;
                    timestamp(current)?;
                    (current.max(observed), Some(not_after_ms))
                }
            };
            let now=observed_time(&tx,observed)?;
            if let Some(old)=receipt(&tx,&request,now,&limits,not_after_ms.is_some())?{tx.commit()?;return Ok(old)}
            if not_after_ms.is_some_and(|deadline| now >= deadline) {
                return Err(StoreError::Deadline(FailureCode::Timeout));
            }
            let expires=now.checked_add(retention).ok_or(StoreError::Invalid)?;
            if retention<=0{return Err(StoreError::Invalid)}
            let graph:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM graphs WHERE fingerprint=?1)",[prepared.fingerprint.to_string()],|r|r.get(0))?;
            if !graph{return Err(StoreError::Missing)}
            let root=prepared.root()?;
            let inserted=tx.execute("INSERT OR IGNORE INTO runs(run_id,fingerprint,root_scope,status,sequence,fence,last_time_ms) VALUES(?1,?2,?3,'running',1,0,?4)",params![prepared.run_id.as_str(),prepared.fingerprint.to_string(),root.to_string(),now])?;
            if inserted!=1{return Err(StoreError::Conflict)}
            let created=cbor::encode(&Value::Array(vec![Value::Text(prepared.fingerprint.to_string()),Value::Text(root.to_string())]),&limits)?;
            append_event(&tx,prepared.run_id.as_str(),1,EventMeta{kind:"run_created",actor:&request.principal,timestamp:now,causation:"start_run",command:None},&created,&limits)?;
            let records=prepared.initial_records(now,not_after_ms)?;
            let failed=prepared.failure.is_some();
            let mut transition=Transition{context:TransitionContext{command_id:request.key.clone(),actor:request.principal.clone(),causation:request.method.clone(),timestamp_ms:now},writes:records,next_status:if failed{Some(RunStatus::Failed)}else{None},root:None,expected_fingerprint:Some(prepared.fingerprint)};
            let body=transitions::transition_body(&mut transition,&limits)?;
            for write in &transition.writes {
                if write.expected_revision.is_some(){return Err(StoreError::Invalid)}
                artifacts::check_artifact_write(&tx,&prepared.run_id,write,&limits)?;
                let payload=cbor::encode(&write.payload,&limits)?;
                transitions::write_checkpoint(&tx, &prepared.run_id, write, 1, &payload)?;
            }
            // Existing artifact aliases must still be accepted and present when
            // the new active manifest becomes a retention root.
            for (_,entry) in prepared.inputs.entries(){
                if let ManifestEntry::Value{artifact_id,content_digest,security_labels}=entry {
                    check_input_reference(&tx,*artifact_id,*content_digest,security_labels,&limits)?;
                }
            }
            let host=Value::Array(vec![Value::Text(request.principal.clone()),Value::Text(request.method.clone()),Value::Text(request.target.clone()),Value::Text(request.key.clone()),Value::Text(request.digest.to_string()),Value::Integer(expires)]);
            let payload=cbor::encode(&Value::Array(vec![body,host]),&limits)?;
            tx.execute("UPDATE runs SET status=?2,sequence=2,last_time_ms=?3 WHERE run_id=?1",params![prepared.run_id.as_str(),if failed{"failed"}else{"running"},now])?;
            append_event(&tx,prepared.run_id.as_str(),2,EventMeta{kind:"run_initialized",actor:&request.principal,timestamp:now,causation:&request.method,command:Some(&request.key)},&payload,&limits)?;
            tx.execute("INSERT INTO host_receipts(principal,method,target,idempotency_key,request_digest,run_id,expires_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(principal,method,target,idempotency_key) DO UPDATE SET request_digest=excluded.request_digest,run_id=excluded.run_id,expires_at_ms=excluded.expires_at_ms",params![request.principal,request.method,request.target,request.key,request.digest.to_string(),prepared.run_id.as_str(),expires])?;
            tx.commit()?;Ok(prepared.run_id)
        }).await.map_err(StoreError::from_worker)
    }
}
impl HostRequest {
    fn owned(&self) -> Self {
        Self {
            principal: self.principal.clone(),
            method: self.method.clone(),
            target: self.target.clone(),
            key: self.key.clone(),
            digest: self.digest,
        }
    }
    pub(crate) fn check(&self, limits: &Limits) -> Result<(), StoreError> {
        let mut accounting = htlk_executable::record_accounting::RecordAccounting::new(limits)?;
        accounting
            .collection(5, 0)
            .map_err(|_| StoreError::Invalid)?;
        for text in [
            &self.principal,
            &self.method,
            &self.target,
            &self.key,
            &self.digest.to_string(),
        ] {
            accounting.text(text, 1).map_err(|_| StoreError::Invalid)?;
        }
        if self.key.is_empty() {
            return Err(StoreError::Invalid);
        }
        Ok(())
    }
}
pub(super) fn observed_time(db: &rusqlite::Connection, observed: i64) -> Result<i64, StoreError> {
    timestamp(observed)?;
    let (watermark, committed): (i64, i64) = db.query_row(
        "SELECT last_time_ms,(SELECT coalesce(max(last_time_ms),0) FROM runs) FROM runtime_identity WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    Ok(observed.max(watermark).max(committed))
}
fn receipt(
    tx: &rusqlite::Transaction<'_>,
    request: &HostRequest,
    now: i64,
    limits: &Limits,
    refuse_reuse: bool,
) -> Result<Option<RunId>, StoreError> {
    let old=tx.query_row("SELECT request_digest,run_id,expires_at_ms FROM host_receipts WHERE principal=?1 AND method=?2 AND target=?3 AND idempotency_key=?4",params![request.principal,request.method,request.target,request.key],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?))).optional()?;
    let Some((digest, run, expiry)) = old else {
        return Ok(None);
    };
    if now >= expiry && !refuse_reuse {
        return Ok(None);
    }
    if digest != request.digest.to_string() {
        return Err(StoreError::IdempotencyConflict);
    }
    if now >= expiry {
        return Err(StoreError::Expired);
    }
    Ok(Some(
        RunId::new(run, limits).map_err(|_| StoreError::Corrupt)?,
    ))
}
fn check_input_reference(
    tx: &rusqlite::Transaction<'_>,
    id: ArtifactId,
    content: crate::identity::ValueDigest,
    labels: &crate::records::SecurityLabels,
    limits: &Limits,
) -> Result<(), StoreError> {
    if retention::expired(tx, id, limits)?.is_some() {
        return Err(StoreError::Expired);
    }
    let mut statement =
        tx.prepare("SELECT payload,final FROM records WHERE family='artifact' AND record_id=?1")?;
    let mut rows = statement.query([id.to_string()])?;
    let row = rows.next()?.ok_or(StoreError::Missing)?;
    if !row.get::<_, bool>(1)? {
        return Err(StoreError::Corrupt);
    }
    let value = cbor::decode(
        row.get_ref(0)?.as_blob().map_err(|_| StoreError::Corrupt)?,
        limits,
    )?;
    let metadata = crate::artifact::ArtifactMetadata::from_value(&value, limits)
        .map_err(|_| StoreError::Corrupt)?;
    if metadata.artifact_id() != id
        || metadata.content_digest() != content
        || metadata.security_labels() != labels
    {
        return Err(StoreError::Corrupt);
    }
    let bytes_present: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM value_blobs WHERE blob_digest=?1)",
        [metadata.storage_reference().to_string()],
        |r| r.get(0),
    )?;
    if !bytes_present {
        return Err(StoreError::Corrupt);
    }
    Ok(())
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::records::InputManifest;
    use htlk_executable::PortTable;

    async fn setup() -> (SqliteStore, PreparedRun, HostRequest) {
        let (admitted, bytes) = registration::tests::executable();
        let store = SqliteStore::in_memory(StoreConfig::default())
            .await
            .unwrap();
        store
            .store_graph(&admitted, &bytes, &Value::Null)
            .await
            .unwrap();
        let run = store.reserve_run_id().await.unwrap();
        let request = HostRequest {
            principal: "host".into(),
            method: "start_run".into(),
            target: admitted.fingerprint().to_string(),
            key: "request".into(),
            digest: Digest::from_bytes([7; 32]),
        };
        let prepared = PreparedRun {
            audit_retention_ms: 1000,
            run_id: run,
            fingerprint: admitted.fingerprint(),
            definition: admitted.document().fields().root_scope,
            principal: "host".into(),
            inputs: InputManifest::freeze(
                &PortTable::default(),
                vec![],
                crate::records::SecurityLabels::default(),
                &Limits::default(),
            )
            .unwrap(),
            artifacts: vec![],
            failure: None,
            run_limits: admitted.policy().fields().defaults.clone(),
            scope_limits: admitted.policy().fields().defaults.clone(),
            requested_limits: Default::default(),
            limits: Limits::default(),
        };
        (store, prepared, request)
    }
    #[test]
    fn failed_host_receipt_commit_rolls_back_run_scope_timers_and_all_notices() {
        actix::System::new().block_on(async{
            let (store,prepared,request)=setup().await;
            let run=prepared.run_id.clone();
            store.connection.call(|db|db.execute_batch("CREATE TRIGGER fail_receipt BEFORE INSERT ON host_receipts BEGIN SELECT RAISE(ABORT,'injected'); END;")).await.unwrap();
            assert_eq!(store.initialize_run(prepared,request.owned(),AdmissionTime::Relative(1000),100).await,Err(StoreError::Database));
            assert_eq!(store.run_header(&run).await,Err(StoreError::Missing));
            assert!(store.pending_outbox(100).await.unwrap().is_empty());
            assert!(store.host_receipt(&request,1000).await.unwrap().is_none());
            let counts=store.connection.call(|db|->Result<_,StoreError>{Ok((db.query_row("SELECT count(*) FROM records",[],|r|r.get::<_,i64>(0))?,db.query_row("SELECT count(*) FROM journal",[],|r|r.get::<_,i64>(0))?))}).await.unwrap();
            assert_eq!(counts,(0,0));
        });
    }
    #[test]
    fn allocator_is_unique_and_clock_includes_known_committed_run_timestamps() {
        actix::System::new().block_on(async {
            let (store, prepared, request) = setup().await;
            let original = prepared.run_id.clone();
            store
                .initialize_run(prepared, request, AdmissionTime::Relative(1000), 100)
                .await
                .unwrap();
            let a = store.clone();
            let b = store.clone();
            let before = store.write_metrics();
            let a = actix::spawn(async move { a.reserve_run_id().await });
            let b = actix::spawn(async move { b.reserve_run_id().await });
            let a = a.await.unwrap().unwrap();
            let b = b.await.unwrap().unwrap();
            assert_ne!(a, b);
            assert_ne!(a, original);
            assert_ne!(b, original);
            assert_eq!(store.write_metrics(), before);
            assert_eq!(store.coordinator_time(2100).await.unwrap(), 2100);
            assert_eq!(store.write_metrics(), before);
            assert_eq!(store.coordinator_time(500).await.unwrap(), 1000);
            store
                .acquire(&original, "owner".into(), 2000, 3000)
                .await
                .unwrap();
            assert_eq!(store.coordinator_time(500).await.unwrap(), 2000);
        });
    }
    #[test]
    fn replay_initialization_matches_authority_root_and_host_retention_receipt() {
        actix::System::new().block_on(async {
            let (store, prepared, request) = setup().await;
            let expected = request.digest;
            let run = store
                .initialize_run(prepared, request, AdmissionTime::Relative(1000), 100)
                .await
                .unwrap();
            let events = store.journal(&run, 0, 100).await.unwrap();
            let mut replay = ReplayState::begin(&events[0], &Limits::default()).unwrap();
            replay.apply(&events[1], &Limits::default()).unwrap();
            assert_eq!(replay.header(), &store.run_header(&run).await.unwrap());
            let receipt = replay.start_receipt().unwrap();
            assert_eq!(receipt.request_digest, expected);
            assert_eq!(receipt.expires_at_ms, 1100);
            let key = RecordKey {
                family: RecordFamily::Scope,
                id: replay.header().root_scope.to_string(),
            };
            assert_eq!(
                replay.record(&key).unwrap(),
                &store.record(&run, &key).await.unwrap()
            );
            let mut invalid = events[1].clone();
            invalid.actor = "other".into();
            let mut replay = ReplayState::begin(&events[0], &Limits::default()).unwrap();
            assert_eq!(
                replay.apply(&invalid, &Limits::default()),
                Err(StoreError::Corrupt)
            );
        });
    }
}
