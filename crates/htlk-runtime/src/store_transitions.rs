//! Atomic revisioned record transitions and recoverable journal/outbox delivery.
use super::*;
use htlk_executable::record_accounting::RecordAccounting;
use std::collections::BTreeSet;

macro_rules! families {
    ($($name:ident => $wire:literal),+ $(,)?) => {
        /// Closed authoritative runtime record families; each payload is validated
        /// by its coordinator consumer before committing this storage transition.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
        pub enum RecordFamily { $(#[doc = $wire] $name),+ }
        impl RecordFamily {
            /// Stable private database family spelling.
            pub const fn as_str(self) -> &'static str { match self { $(Self::$name => $wire),+ } }
            pub(crate) fn parse(value: &str) -> Result<Self, StoreError> {
                match value { $($wire => Ok(Self::$name)),+, _ => Err(StoreError::Corrupt) }
            }
        }
    };
}
families! {
    Scope => "scope", Invocation => "invocation", Guard => "guard", Binding => "binding",
    Manifest => "manifest", Attempt => "attempt", Response => "response", Wait => "wait",
    Reply => "reply", Approval => "approval", Reservation => "reservation", Usage => "usage",
    Artifact => "artifact", Cancellation => "cancellation", Extension => "extension",
    Loop => "loop", Timer => "timer", Observation => "observation", Tombstone => "tombstone",
}
/// Exact record key within a run. IDs are private names, not source-language paths.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordKey {
    /// Typed record family.
    pub family: RecordFamily,
    /// Exact opaque record name.
    pub id: String,
}
/// A trusted coordinator's compare-and-swap proposal. A finalized record cannot
/// subsequently be revised. This supplies storage integrity, not host authorization.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordWrite {
    /// Exact run-local key.
    pub key: RecordKey,
    /// None inserts; Some(n) requires an existing nonfinal revision n.
    pub expected_revision: Option<i64>,
    /// Make this record immutable after the transaction commits.
    pub final_record: bool,
    /// Bounded, consumer-validated canonical runtime record.
    pub payload: Value,
}
/// An authoritative bounded runtime record.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredRecord {
    /// Exact run-local key.
    pub key: RecordKey,
    /// Initial insertion is revision one.
    pub revision: i64,
    /// Whether further mutation is prohibited.
    pub final_record: bool,
    /// Protected canonical runtime record data.
    pub payload: Value,
}
/// Trusted attribution and internal idempotent command identity.
#[derive(Clone, Debug)]
pub struct TransitionContext {
    /// Stable unique command key scoped by the target run.
    pub command_id: String,
    /// Authenticated principal or trusted runtime actor.
    pub actor: String,
    /// Stable cause/request identity retained across command retries.
    pub causation: String,
    /// Trusted observed time, clamped to committed coordinator time.
    pub timestamp_ms: i64,
}
/// An already validated additive installation's new active root pointer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootReplacement {
    /// Registered composed executable.
    pub fingerprint: Digest,
    /// Fresh extended-root scope instance.
    pub root_scope: ScopeInstanceId,
}
/// One atomic decision, complete record changes and optional run terminal/root
/// update. The service validates operation semantics before this trusted boundary.
#[derive(Clone, Debug)]
pub struct Transition {
    /// Attribution and idempotent command key.
    pub context: TransitionContext,
    /// Unique record keys; storage canonicalizes their order.
    pub writes: Vec<RecordWrite>,
    /// Optional immutable terminal run result.
    pub next_status: Option<RunStatus>,
    /// Optional new root, after runtime additive validation.
    pub root: Option<RootReplacement>,
    /// Compare active graph before a root installation or other guarded decision.
    pub expected_fingerprint: Option<Digest>,
}
/// Stable result of an internal command, including repeated identical commands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitReceipt {
    /// The one authoritative journal sequence for this command.
    pub sequence: i64,
}
/// Closed version-0.1 journal transition schema selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalEvent {
    /// Terminal audit-window expiry records one immutable artifact tombstone.
    ArtifactExpired,
    /// Immutable host receipt appended after a terminal outcome, without work.
    TerminalReceipt,
    /// Initial root/inputs, contract result and authenticated host start receipt.
    RunInitialized,
    /// Initial fingerprint/root; sequence one, no owner.
    RunCreated,
    /// New owner, higher fence and expiry.
    OwnershipAcquired,
    /// Same owner/fence with renewed expiry.
    OwnershipRenewed,
    /// A verified quiescent run released the live owner until an external wake or timer.
    OwnershipParked,
    /// Complete revisioned records and optional terminal/root update.
    RecordsCommitted,
}
impl JournalEvent {
    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "artifact_expired" => Ok(Self::ArtifactExpired),
            "terminal_receipt" => Ok(Self::TerminalReceipt),
            "run_initialized" => Ok(Self::RunInitialized),
            "run_created" => Ok(Self::RunCreated),
            "ownership_acquired" => Ok(Self::OwnershipAcquired),
            "ownership_renewed" => Ok(Self::OwnershipRenewed),
            "ownership_parked" => Ok(Self::OwnershipParked),
            "records_committed" => Ok(Self::RecordsCommitted),
            _ => Err(StoreError::Corrupt),
        }
    }
}
/// One complete committed journal entry. Payloads are protected runtime data.
#[derive(Clone, Debug, PartialEq)]
pub struct JournalEntry {
    /// Target run.
    pub run_id: RunId,
    /// Strictly increasing per-run sequence.
    pub sequence: i64,
    /// Closed event schema.
    pub event: JournalEvent,
    /// Version, always exactly 0.1.
    pub record_version: &'static str,
    /// Authenticated or trusted actor attribution.
    pub actor: String,
    /// Authoritative persisted nondecreasing time.
    pub timestamp_ms: i64,
    /// Stable causal identity.
    pub causation: String,
    /// Internal idempotent command identity, if applicable.
    pub command_id: Option<String>,
    /// Complete typed-event canonical payload.
    pub payload: Value,
}
/// Recoverable hint for journal consumers. Redelivery cannot create a new result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxNotice {
    /// Target run.
    pub run_id: RunId,
    /// Committed journal event to inspect.
    pub sequence: i64,
}

pub(crate) struct GraphResponseCommit {
    pub namespace: String,
    pub attempt: crate::identity::AttemptId,
    pub response_digest: Digest,
    pub sequence: i64,
    pub first: Option<(i64, Transition)>,
}

impl SqliteStore {
    pub(crate) async fn commit_graph_response(
        &self,
        lease: &RunLease,
        request: GraphResponseCommit,
        clock: std::sync::Arc<dyn crate::clock::CoordinatorClock>,
    ) -> Result<CommitReceipt, StoreError> {
        let limits = self.config.limits.clone();
        let lease = lease.clone();
        let first = request
            .first
            .map(|(deadline, transition)| {
                Ok::<_, StoreError>((
                    deadline,
                    PreparedTransition::new(&lease, transition, &limits)?,
                ))
            })
            .transpose()?;
        self.connection.write_run(lease.run_id().to_string(), move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let namespace: String = tx.query_row("SELECT namespace FROM runtime_identity WHERE singleton=1", [], |row| row.get(0))?;
            if namespace != request.namespace { return Err(StoreError::Invalid); }
            let header = load_header(&tx, lease.run_id())?;
            let now = runs::observed_time(&tx, clock.now_ms().map_err(|_| StoreError::Invalid)?)?;
            check_authority(&header, &lease, now)?;
            // Covers the exact checkpoints used by preparation and policy, including
            // response labels, active ancestry and the outstanding reservation.
            if header.sequence != request.sequence { return Err(StoreError::Conflict); }
            let existing = tx.query_row(
                "SELECT payload,final FROM records WHERE run_id=?1 AND family='response' AND record_id=?2",
                params![lease.run_id().as_str(), request.attempt.to_string()],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, bool>(1)?)),
            ).optional()?;
            if let Some((bytes, final_record)) = existing {
                let Value::Map(response) = decode_stored(&bytes, &limits)? else { return Err(StoreError::Corrupt); };
                if !final_record { return Err(StoreError::Corrupt); }
                if response.get("host_recovery_digest") != Some(&Value::Text(request.response_digest.to_string())) {
                    return Err(StoreError::IdempotencyConflict);
                }
                let sequence = tx.query_row(
                    "SELECT sequence FROM commands WHERE run_id=?1 AND command_id=?2",
                    params![lease.run_id().as_str(), format!("recover-response:{}", request.attempt)],
                    |row| row.get(0),
                )?;
                tx.commit()?;
                return Ok(CommitReceipt { sequence });
            }
            if header.status.is_terminal() { return Err(StoreError::Terminal); }
            let (deadline, mut prepared) = first.ok_or(StoreError::Conflict)?;
            if now >= deadline { return Err(StoreError::Deadline(FailureCode::Timeout)); }
            prepared.transition.context.timestamp_ms = now;
            let receipt = prepared.apply_at_sequence(&tx, &lease, &limits, Some(request.sequence), false)?;
            tx.commit()?;
            Ok(receipt)
        }).await.map_err(StoreError::from_worker)
    }

    pub(crate) async fn commit_pure_batch(
        &self,
        lease: &RunLease,
        sequence: i64,
        observed: i64,
        deadline: i64,
        steps: Vec<Transition>,
        blobs: Vec<crate::artifact::PreparedArtifact>,
    ) -> Result<i64, StoreError> {
        timestamp(observed)?;
        if !(1..=8).contains(&steps.len())
            || steps
                .iter()
                .any(|step| step.next_status.is_some() || step.root.is_some())
        {
            return Err(StoreError::Invalid);
        }
        let limits = self.config.limits.clone();
        let prepared: Vec<_> = steps
            .into_iter()
            .map(|step| PreparedTransition::new(lease, step, &limits))
            .collect::<Result<_, _>>()?;
        let observed = prepared.iter().fold(observed, |time, step| {
            time.max(step.transition.context.timestamp_ms)
        });
        for blob in &blobs {
            blob.metadata()
                .verify_value(blob.bytes(), &limits)
                .map_err(artifacts::artifact_error)?;
        }
        let lease = lease.clone();
        self.connection
            .write_run(lease.run_id().to_string(), move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let header = load_header(&tx, lease.run_id())?;
                let now = runs::observed_time(&tx, observed)?;
                check_authority(&header, &lease, now)?;
                if header.sequence != sequence || header.status.is_terminal() {
                    return Err(StoreError::Conflict);
                }
                if now >= deadline {
                    return Err(StoreError::Deadline(FailureCode::Timeout));
                }
                for blob in blobs {
                    artifacts::stage_bytes(&tx, blob.metadata().storage_reference(), blob.bytes())?;
                }
                let mut sequence = sequence;
                let mut receipts = Vec::with_capacity(prepared.len());
                let mut seen = std::collections::BTreeMap::new();
                for transition in &prepared {
                    if let Some(digest) =
                        seen.insert(transition.command_id(), transition.request_digest)
                    {
                        return Err(if digest == transition.request_digest {
                            StoreError::Conflict
                        } else {
                            StoreError::IdempotencyConflict
                        });
                    }
                    if transition.receipt(&tx, &lease)?.is_some() {
                        return Err(StoreError::Conflict);
                    }
                    transition.check_header(&header)?;
                    transition.apply_records(&tx, &lease, now, &limits)?;
                    sequence = sequence.checked_add(1).ok_or(StoreError::Invalid)?;
                    append_journal(
                        &tx,
                        lease.run_id().as_str(),
                        sequence,
                        transition.event(now),
                        &transition.event_payload,
                        &limits,
                    )?;
                    receipts.push((
                        transition.command_id(),
                        transition.request_digest.to_string(),
                        sequence,
                    ));
                }
                // Pure batches cannot change the root, status or owner. Keep the
                // final high-water marks in one run-row/index update.
                tx.execute(
                    "UPDATE runs SET sequence=?2,last_time_ms=?3 WHERE run_id=?1",
                    params![lease.run_id().as_str(), sequence, now],
                )?;
                insert_batch_notices(&tx, lease.run_id(), &receipts)?;
                tx.commit()?;
                Ok(sequence)
            })
            .await
            .map_err(StoreError::from_worker)
    }
    /// Atomically compare/revise records, update run status/root, append the complete
    /// decision and enqueue its outbox hint. An identical command returns its old
    /// receipt; a changed request under that key fails. Timestamp and worker lease
    /// are excluded from request identity so recovery can reconcile with new authority.
    /// An existing receipt identifies the original accepted transition; returning it
    /// does not establish a new synchronous replication acknowledgment for that write.
    ///
    /// # Errors
    /// Returns stale authority/revision, final record/run, idempotency conflict,
    /// active-fingerprint conflict, complete record limits or database failure.
    /// [`StoreError::CommitUnknown`] preserves the original operation's uncertainty;
    /// do not infer rollback or automatically repeat external work.
    pub async fn commit(
        &self,
        lease: &RunLease,
        transition: Transition,
    ) -> Result<CommitReceipt, StoreError> {
        self.commit_transition(lease, transition, None, false).await
    }

    pub(crate) async fn commit_at_sequence(
        &self,
        lease: &RunLease,
        transition: Transition,
        sequence: Option<i64>,
    ) -> Result<CommitReceipt, StoreError> {
        self.commit_transition(lease, transition, sequence, true)
            .await
    }

    async fn commit_transition(
        &self,
        lease: &RunLease,
        transition: Transition,
        sequence: Option<i64>,
        coordinated: bool,
    ) -> Result<CommitReceipt, StoreError> {
        let limits = self.config.limits.clone();
        let prepared = PreparedTransition::new(lease, transition, &limits)?;
        let lease = lease.clone();
        self.connection
            .write_run(
                lease.run_id().to_string(),
                move |db| -> Result<CommitReceipt, StoreError> {
                    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let receipt =
                        prepared.apply_at_sequence(&tx, &lease, &limits, sequence, coordinated)?;
                    tx.commit()?;
                    Ok(receipt)
                },
            )
            .await
            .map_err(StoreError::from_worker)
    }
    /// Reads one authoritative record under the configured representation limits.
    ///
    /// # Errors
    /// Returns missing record, invalid/corrupt stored representation or SQLite failure.
    pub async fn record(&self, run: &RunId, key: &RecordKey) -> Result<StoredRecord, StoreError> {
        self.check_text(&key.id)?;
        let (run, key, limits) = (run.clone(), key.clone(), self.config.limits.clone());
        self.connection.read(move |db|->Result<StoredRecord,StoreError>{
            let mut statement=db.prepare("SELECT revision,final,payload FROM records WHERE run_id=?1 AND family=?2 AND record_id=?3")?;
            let mut rows=statement.query(params![run.as_str(),key.family.as_str(),key.id])?;
            let row=rows.next()?.ok_or(StoreError::Missing)?;
            let revision:i64=row.get(0)?;
            let final_record:bool=row.get(1)?;
            let bytes=row.get_ref(2)?.as_blob().map_err(|_|StoreError::Corrupt)?;
            let payload=decode_stored(bytes,&limits)?;
            if revision<1 {return Err(StoreError::Corrupt)}
            Ok(StoredRecord{key,revision,final_record,payload})
        }).await.map_err(StoreError::from_worker)
    }
    /// Reads a bounded journal page after an exclusive sequence cursor. A page may
    /// contain fewer than the requested rows to respect the aggregate byte budget.
    /// Continue from its last sequence. This never executes external operations.
    ///
    /// # Errors
    /// Returns invalid cursor/page size, corrupt records or SQLite failures.
    pub async fn journal(
        &self,
        run: &RunId,
        after: i64,
        page_size: usize,
    ) -> Result<Vec<JournalEntry>, StoreError> {
        self.journal_until(run, after, i64::MAX, page_size).await
    }
    pub(super) async fn journal_until(
        &self,
        run: &RunId,
        after: i64,
        through: i64,
        page_size: usize,
    ) -> Result<Vec<JournalEntry>, StoreError> {
        let count = self.page_size(page_size)?;
        if after < 0 {
            return Err(StoreError::Invalid);
        }
        let (run, limits) = (run.clone(), self.config.limits.clone());
        self.connection.read(move |db|->Result<Vec<JournalEntry>,StoreError>{
            let mut statement=db.prepare("SELECT sequence,event_type,record_version,actor,timestamp_ms,causation,command_id,payload FROM journal WHERE run_id=?1 AND sequence>?2 AND sequence<=?4 ORDER BY sequence LIMIT ?3")?;
            let mut rows=statement.query(params![run.as_str(),after,count,through])?;
            let mut result=Vec::new();
            let mut bytes=0usize;
            while let Some(row)=rows.next()? {
                if text(row,2,&limits)?!="0.1" {return Err(StoreError::Corrupt)}
                let raw=row.get_ref(7)?.as_blob().map_err(|_|StoreError::Corrupt)?;
                if raw.len()>limits.max_document_bytes{return Err(StoreError::Invalid)}
                let actor=text(row,3,&limits)?;
                let causation=text(row,5,&limits)?;
                let command_id=if matches!(row.get_ref(6)?,rusqlite::types::ValueRef::Null){None}else{Some(text(row,6,&limits)?)};
                let kind=text(row,1,&limits)?;
                let event=JournalEvent::parse(&kind)?;
                let sequence:i64=row.get(0)?;
                let timestamp_ms:i64=row.get(4)?;
                if sequence<1||timestamp_ms<0{return Err(StoreError::Corrupt)}
                let metadata=EventMeta{kind:&kind,actor:&actor,timestamp:timestamp_ms,causation:&causation,command:command_id.as_deref()};
                let size=cbor::encode(&event_value(run.as_str(),sequence,&metadata,raw),&limits)?.len();
                if bytes.checked_add(size).is_none_or(|n|n>limits.max_document_bytes) {
                    if result.is_empty(){return Err(StoreError::Invalid)}else{break}
                }
                bytes+=size;
                result.push(JournalEntry{run_id:run.clone(),sequence,event,record_version:"0.1",actor,timestamp_ms,causation,command_id,payload:decode_stored(raw,&limits)?});
            }
            Ok(result)
        }).await.map_err(StoreError::from_worker)
    }
    /// Lists bounded pending delivery hints. Repeated reads intentionally return
    /// the same hints until their delivery is acknowledged.
    ///
    /// # Errors
    /// Returns invalid page size, corrupt IDs or SQLite failure.
    pub async fn pending_outbox(&self, page_size: usize) -> Result<Vec<OutboxNotice>, StoreError> {
        let count = self.page_size(page_size)?;
        let limits = self.config.limits.clone();
        self.connection.read(move |db|->Result<Vec<OutboxNotice>,StoreError>{
            let mut statement=db.prepare("SELECT run_id,sequence,span,pending FROM outbox WHERE pending<>0 ORDER BY run_id,sequence LIMIT ?1")?;
            let mut rows=statement.query([count])?;
            let mut result=Vec::new();
            let mut bytes=1usize;
            while let Some(row)=rows.next()? {
                let run=RunId::new(text(row,0,&limits)?,&limits).map_err(|_|StoreError::Corrupt)?;
                let range=outbox_range(row.get(1)?,row.get(2)?,row.get(3)?)?;
                for offset in 0..range.span {
                    if range.pending & (1 << offset) == 0 { continue; }
                    let sequence=range.first+offset;
                    let size=cbor::encode(&Value::Array(vec![Value::Text(run.to_string()),Value::Integer(sequence)]),&limits)?.len();
                    let header_growth=page_header(result.len()+1)-page_header(result.len());
                    if bytes.checked_add(size).and_then(|n|n.checked_add(header_growth)).is_none_or(|n|n>limits.max_document_bytes) {
                        return if result.is_empty(){Err(StoreError::Invalid)}else{Ok(result)};
                    }
                    bytes+=size+header_growth;
                    result.push(OutboxNotice{run_id:run.clone(),sequence});
                    if result.len() == count as usize { return Ok(result); }
                }
            }
            Ok(result)
        }).await.map_err(StoreError::from_worker)
    }
    /// Idempotently acknowledges notification delivery. This changes no run state
    /// or journal outcome and grants no graph-execution authority.
    ///
    /// # Errors
    /// Returns an unknown notice or SQLite failure.
    pub async fn acknowledge(&self, notice: &OutboxNotice) -> Result<(), StoreError> {
        let notice = notice.clone();
        self.connection
            .write_run(notice.run_id.to_string(), move |db| -> Result<(), StoreError> {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let (end,span,pending) = tx.query_row(
                    "SELECT sequence,span,pending FROM outbox WHERE run_id=?1 AND sequence>=?2 ORDER BY sequence LIMIT 1",
                    params![notice.run_id.as_str(), notice.sequence],
                    |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
                ).optional()?.ok_or(StoreError::Missing)?;
                let range = outbox_range(end,span,pending)?;
                if notice.sequence < range.first {
                    return Err(StoreError::Missing);
                }
                let next = pending & !(1 << (notice.sequence - range.first));
                if next != pending && tx.execute("UPDATE outbox SET pending=?3 WHERE run_id=?1 AND sequence=?2", params![notice.run_id.as_str(), end, next])? != 1 {
                    return Err(StoreError::Corrupt);
                }
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(StoreError::from_worker)
    }
    pub(super) fn page_size(&self, page_size: usize) -> Result<i64, StoreError> {
        if page_size == 0 || page_size > self.config.max_page_size {
            return Err(StoreError::Invalid);
        }
        i64::try_from(page_size).map_err(|_| StoreError::Invalid)
    }
}

struct OutboxRange {
    first: i64,
    span: i64,
    pending: i64,
}
fn outbox_range(end: i64, span: i64, pending: i64) -> Result<OutboxRange, StoreError> {
    if !(1..=8).contains(&span) || end < span || pending < 0 || pending >= 1 << span {
        return Err(StoreError::Corrupt);
    }
    Ok(OutboxRange {
        first: end - span + 1,
        span,
        pending,
    })
}

// At most eight commands. Parameter 1 is shared by every row; values never
// enter the SQL string. The journal retains every sequence and both indexes
// retain individually addressable receipts/acknowledgements.
fn insert_batch_notices(
    tx: &rusqlite::Transaction<'_>,
    run: &RunId,
    receipts: &[(&str, String, i64)],
) -> Result<(), StoreError> {
    use std::fmt::Write as _;
    let mut commands =
        String::from("INSERT INTO commands(run_id,command_id,request_digest,sequence) VALUES");
    let run_id = run.as_str();
    let mut command_params: Vec<&dyn rusqlite::ToSql> = vec![&run_id];
    for (i, (id, digest, sequence)) in receipts.iter().enumerate() {
        if i > 0 {
            commands.push(',');
        }
        let start = i * 3 + 2;
        write!(commands, "(?1,?{start},?{},?{})", start + 1, start + 2)
            .expect("infallible String formatting");
        command_params.extend([id as &dyn rusqlite::ToSql, digest, sequence]);
    }
    let span = receipts.len();
    let end = receipts.last().ok_or(StoreError::Invalid)?.2;
    if tx.execute(&commands, command_params.as_slice())? != span
        || tx.execute(
            "INSERT INTO outbox(run_id,sequence,span,pending) VALUES(?1,?2,?3,?4)",
            params![run_id, end, span, (1_i64 << span) - 1],
        )? != 1
    {
        return Err(StoreError::Database);
    }
    Ok(())
}

// Pure acceptance predicates are evaluated at the transaction's authoritative
// time, even if proposal construction checked an earlier clock observation.
fn check_acceptance_deadline(write: &RecordWrite, now: i64) -> Result<(), StoreError> {
    if matches!(
        write.key.family,
        RecordFamily::Scope | RecordFamily::Invocation
    ) && let Value::Map(record) = &write.payload
        && record.get("status") == Some(&Value::Text("succeeded".into()))
        && matches!(record.get("deadline_ms"), Some(Value::Integer(expiry)) if now >= *expiry)
    {
        return Err(StoreError::Deadline(FailureCode::Timeout));
    }
    if write.key.family != RecordFamily::Wait {
        return Ok(());
    }
    let Value::Map(wait) = &write.payload else {
        return Err(StoreError::Invalid);
    };
    if wait.get("state") != Some(&Value::Text("accepted".into())) {
        return Ok(());
    }
    let Some(Value::Integer(expiry)) = wait.get("expires_at") else {
        return Err(StoreError::Invalid);
    };
    if now >= *expiry {
        let code = if wait.get("purpose") == Some(&Value::Text("approval".into())) {
            FailureCode::ApprovalDenied
        } else {
            FailureCode::WaitExpired
        };
        return Err(StoreError::Deadline(code));
    }
    Ok(())
}

// Pages are capped at 4096 rows by StoreConfig validation.
fn page_header(count: usize) -> usize {
    if count < 24 {
        1
    } else if count < 256 {
        2
    } else {
        3
    }
}

pub(super) struct PreparedTransition {
    transition: Transition,
    request_digest: Digest,
    event_payload: Vec<u8>,
    writes: Vec<Vec<u8>>,
}
impl PreparedTransition {
    pub fn new(
        lease: &RunLease,
        mut transition: Transition,
        limits: &Limits,
    ) -> Result<Self, StoreError> {
        timestamp(transition.context.timestamp_ms)?;
        for text in [
            &transition.context.command_id,
            &transition.context.actor,
            &transition.context.causation,
        ] {
            RecordAccounting::new(limits)?
                .text(text, 0)
                .map_err(|_| StoreError::Invalid)?;
        }
        let body = transition_body(&mut transition, limits)?;
        let request_digest = command_digest(
            lease.run_id(),
            &transition.context.actor,
            &transition.context.causation,
            &body,
            limits,
        )?;
        let event_payload = cbor::encode(
            &Value::Array(vec![
                Value::Text(lease.owner.clone()),
                Value::Integer(lease.fence),
                body,
            ]),
            limits,
        )?;
        let writes = transition
            .writes
            .iter()
            .map(|write| Ok(cbor::encode(&write.payload, limits)?))
            .collect::<Result<Vec<_>, StoreError>>()?;
        Ok(Self {
            transition,
            request_digest,
            event_payload,
            writes,
        })
    }
    // The same transactional trust boundary is used by ordinary graph commits and
    // callback accounting. The caller owns the transaction and commits all facts together.
    pub fn apply(
        self,
        tx: &rusqlite::Transaction<'_>,
        lease: &RunLease,
        limits: &Limits,
    ) -> Result<CommitReceipt, StoreError> {
        self.apply_at_sequence(tx, lease, limits, None, false)
    }

    fn command_id(&self) -> &str {
        &self.transition.context.command_id
    }

    fn event(&self, timestamp: i64) -> EventMeta<'_> {
        EventMeta {
            kind: "records_committed",
            actor: &self.transition.context.actor,
            timestamp,
            causation: &self.transition.context.causation,
            command: Some(self.command_id()),
        }
    }

    fn apply_at_sequence(
        self,
        tx: &rusqlite::Transaction<'_>,
        lease: &RunLease,
        limits: &Limits,
        sequence: Option<i64>,
        coordinated: bool,
    ) -> Result<CommitReceipt, StoreError> {
        let header = load_header(tx, &lease.run_id)?;
        let mut now = self
            .transition
            .context
            .timestamp_ms
            .max(header.last_time_ms);
        if coordinated {
            now = runs::observed_time(tx, now)?;
        }
        check_authority(&header, lease, now)?;
        if sequence.is_some_and(|expected| header.sequence != expected) {
            return Err(StoreError::Conflict);
        }
        if let Some(sequence) = self.receipt(tx, lease)? {
            return Ok(CommitReceipt { sequence });
        }
        self.check_header(&header)?;
        self.apply_records(tx, lease, now, limits)?;
        let sequence = header.sequence.checked_add(1).ok_or(StoreError::Invalid)?;
        let transition = &self.transition;
        let fingerprint = transition
            .root
            .as_ref()
            .map_or(header.fingerprint, |root| root.fingerprint);
        let root = transition
            .root
            .as_ref()
            .map_or(header.root_scope, |root| root.root_scope);
        tx.execute("UPDATE runs SET fingerprint=?2,root_scope=?3,status=?4,sequence=?5,last_time_ms=?6 WHERE run_id=?1",
            params![lease.run_id.as_str(), fingerprint.to_string(), root.to_string(), transition.next_status.unwrap_or(header.status).as_str(), sequence, now])?;
        append_event(
            tx,
            lease.run_id.as_str(),
            sequence,
            self.event(now),
            &self.event_payload,
            limits,
        )?;
        tx.execute(
            "INSERT INTO commands(run_id,command_id,request_digest,sequence) VALUES(?1,?2,?3,?4)",
            params![
                lease.run_id.as_str(),
                self.command_id(),
                self.request_digest.to_string(),
                sequence
            ],
        )?;
        Ok(CommitReceipt { sequence })
    }

    fn receipt(
        &self,
        tx: &rusqlite::Transaction<'_>,
        lease: &RunLease,
    ) -> Result<Option<i64>, StoreError> {
        let old = tx
            .query_row(
                "SELECT request_digest,sequence FROM commands WHERE run_id=?1 AND command_id=?2",
                params![lease.run_id.as_str(), self.command_id()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        if let Some((digest, sequence)) = old {
            if digest != self.request_digest.to_string() {
                return Err(StoreError::IdempotencyConflict);
            }
            return Ok(Some(sequence));
        }
        Ok(None)
    }

    fn check_header(&self, header: &RunHeader) -> Result<(), StoreError> {
        let transition = &self.transition;
        if header.status.is_terminal() {
            return Err(StoreError::Terminal);
        }
        if transition
            .expected_fingerprint
            .is_some_and(|fp| fp != header.fingerprint)
        {
            return Err(StoreError::Conflict);
        }
        if transition
            .next_status
            .is_some_and(|status| !header.status.can_transition_to(status))
        {
            return Err(StoreError::Terminal);
        }
        Ok(())
    }

    fn apply_records(
        &self,
        tx: &rusqlite::Transaction<'_>,
        lease: &RunLease,
        now: i64,
        limits: &Limits,
    ) -> Result<(), StoreError> {
        let transition = &self.transition;
        for write in &transition.writes {
            check_acceptance_deadline(write, now)?;
        }
        for (write, payload) in transition.writes.iter().zip(&self.writes) {
            artifacts::check_artifact_write(tx, &lease.run_id, write, limits)?;
            let old = tx.query_row("SELECT revision,final FROM records WHERE run_id=?1 AND family=?2 AND record_id=?3",
                params![lease.run_id.as_str(), write.key.family.as_str(), write.key.id], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, bool>(1)?))).optional()?;
            if old.is_some_and(|(_, final_record)| final_record) {
                return Err(StoreError::Terminal);
            }
            if old.map(|(revision, _)| revision) != write.expected_revision {
                return Err(StoreError::Conflict);
            }
            let revision = next_revision(write.expected_revision)?;
            write_checkpoint(tx, &lease.run_id, write, revision, payload)?;
        }
        retention::check_references(tx, &transition.writes, limits)?;
        artifacts::check_output_publication(tx, &lease.run_id, &transition.writes, limits)?;
        Ok(())
    }
}

pub(super) fn checkpoint_payload(
    db: &rusqlite::Connection,
    payload: &[u8],
) -> Result<String, StoreError> {
    let digest = htlk_executable::digest::hash_bytes(payload).to_string();
    if db.execute(
        "INSERT OR IGNORE INTO checkpoint_payloads(payload_digest,payload) VALUES(?1,?2)",
        params![digest, payload],
    )? == 0
    {
        let exact = db.query_row(
            "SELECT payload=?2 FROM checkpoint_payloads WHERE payload_digest=?1",
            params![digest, payload],
            |row| row.get::<_, bool>(0),
        )?;
        if !exact {
            return Err(StoreError::Corrupt);
        }
    }
    Ok(digest)
}

pub(super) fn write_checkpoint(
    tx: &rusqlite::Transaction<'_>,
    run: &RunId,
    write: &RecordWrite,
    revision: i64,
    payload: &[u8],
) -> Result<(), StoreError> {
    let digest = checkpoint_payload(tx, payload)?;
    tx.execute("INSERT INTO record_heads(run_id,family,record_id,revision,final,payload_digest) VALUES(?1,?2,?3,?4,?5,?6)
        ON CONFLICT(run_id,family,record_id) DO UPDATE SET revision=excluded.revision,final=excluded.final,payload_digest=excluded.payload_digest",
        params![run.as_str(), write.key.family.as_str(), write.key.id, revision, write.final_record, digest])?;
    Ok(())
}

pub(super) fn next_revision(previous: Option<i64>) -> Result<i64, StoreError> {
    match previous {
        None => Ok(1),
        Some(n) if n >= 1 => n.checked_add(1).ok_or(StoreError::Invalid),
        _ => Err(StoreError::Invalid),
    }
}
pub(super) fn transition_body(
    transition: &mut Transition,
    limits: &Limits,
) -> Result<Value, StoreError> {
    transition.writes.sort_by(|a, b| a.key.cmp(&b.key));
    let mut unique = BTreeSet::new();
    let mut accounting = RecordAccounting::new(limits)?;
    accounting
        .collection(4, 0)
        .map_err(|_| StoreError::Invalid)?;
    accounting
        .collection(transition.writes.len(), 1)
        .map_err(|_| StoreError::Invalid)?;
    let mut writes = Vec::new();
    for write in &transition.writes {
        next_revision(write.expected_revision)?;
        if !unique.insert(&write.key) {
            return Err(StoreError::Conflict);
        }
        accounting
            .collection(5, 2)
            .map_err(|_| StoreError::Invalid)?;
        accounting
            .text(write.key.family.as_str(), 3)
            .map_err(|_| StoreError::Invalid)?;
        accounting
            .text(&write.key.id, 3)
            .map_err(|_| StoreError::Invalid)?;
        accounting
            .value(
                &write.expected_revision.map_or(Value::Null, Value::Integer),
                3,
            )
            .map_err(|_| StoreError::Invalid)?;
        accounting
            .value(&Value::Bool(write.final_record), 3)
            .map_err(|_| StoreError::Invalid)?;
        accounting
            .value(&write.payload, 3)
            .map_err(|_| StoreError::Invalid)?;
        writes.push(Value::Array(vec![
            Value::Text(write.key.family.as_str().into()),
            Value::Text(write.key.id.clone()),
            write.expected_revision.map_or(Value::Null, Value::Integer),
            Value::Bool(write.final_record),
            write.payload.clone(),
        ]));
    }
    let status = transition
        .next_status
        .map_or(Value::Null, |s| Value::Text(s.as_str().into()));
    let root = transition.root.as_ref().map_or(Value::Null, |r| {
        Value::Array(vec![
            Value::Text(r.fingerprint.to_string()),
            Value::Text(r.root_scope.to_string()),
        ])
    });
    let expected = transition
        .expected_fingerprint
        .map_or(Value::Null, |fp| Value::Text(fp.to_string()));
    let body = Value::Array(vec![Value::Array(writes), status, root, expected]);
    RecordAccounting::new(limits)?
        .value(&body, 0)
        .map_err(|_| StoreError::Invalid)?;
    Ok(body)
}
pub(super) fn command_digest(
    run: &RunId,
    actor: &str,
    causation: &str,
    body: &Value,
    limits: &Limits,
) -> Result<Digest, StoreError> {
    crate::identity::hash(
        "store_command",
        &[
            &Value::Text(actor.into()),
            &Value::Text(causation.into()),
            &Value::Text(run.as_str().into()),
            body,
        ],
        limits,
    )
    .map_err(|_| StoreError::Invalid)
}
pub(super) fn decode_stored(bytes: &[u8], limits: &Limits) -> Result<Value, StoreError> {
    cbor::decode(bytes, limits).map_err(|e| {
        if matches!(e.kind(), cbor::ErrorKind::LimitExceeded { .. }) {
            StoreError::Invalid
        } else {
            StoreError::Corrupt
        }
    })
}
pub(super) fn text(
    row: &rusqlite::Row<'_>,
    index: usize,
    limits: &Limits,
) -> Result<String, StoreError> {
    let text = row
        .get_ref(index)?
        .as_str()
        .map_err(|_| StoreError::Corrupt)?;
    if text.len() > limits.max_document_bytes {
        return Err(StoreError::Invalid);
    }
    Ok(text.into())
}
