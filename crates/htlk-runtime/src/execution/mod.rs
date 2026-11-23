//! Actix supervision of fenced durable graph execution.
mod approval;
mod artifacts;
mod boundary;
mod checkpoint;
mod composite;
mod control;
mod extension;
pub use extension::{CloseWait, ExtensionReceipt, ExtensionRequest};
mod host;
mod inspection;
mod mcp;
mod mcp_evidence;
mod mcp_recovery;
pub use mcp_evidence::{McpEvidenceKind, McpEvidencePayload, McpEvidenceView};
mod mcp_samples;
pub use mcp_samples::{McpInferenceSelection, ProtectedInferenceRevision};
mod recovery;
mod retry;
pub use inspection::{
    AttemptView, DecisionView, GraphView, HistoryEntry, HistoryPage, NodeView, RunView, ScopeView,
    WaitView,
};
mod waits;
mod working;
pub use host::{CancelReceipt, ReplyReceipt};
pub use waits::WaitPolicy;
mod frame;
mod model;
mod step;
use crate::{
    Runtime, RuntimeError,
    failure::FailureCode,
    identity::RunId,
    store::{Acquisition, DeadlineObservation, RunLease, StoreError},
};
use actix::WeakAddr;
use actix::prelude::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::Arc,
    time::{Duration, Instant},
};

/// Host-selected coordination scope, independent of graph authorization.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionScope {
    /// Coordinate every eligible nonterminal run in the store.
    #[default]
    All,
    /// Start empty and coordinate only runs explicitly attached by the host.
    /// Admission, inspection and host-command notifications never attach a run.
    Attached,
}

/// Trusted, durable accounting for the active work of a native coordinator.
/// A grant is reserved before work; an interrupted settlement never refunds it.
pub trait ProcessingBudget: Send + Sync {
    /// Reserve one grant under the exact native execution fence. `None` means
    /// the known budget is exhausted. Storage ambiguity must return an error.
    fn reserve<'a>(
        &'a self,
        lease: &'a RunLease,
    ) -> crate::mcp::McpFuture<'a, Option<ProcessingGrant>>;
    /// Settle the original grant after work returns. Time is monotonic and may
    /// exceed the grant while a task is being stopped or reclaimed.
    fn settle<'a>(
        &'a self,
        lease: &'a RunLease,
        grant: ProcessingGrant,
        elapsed: Duration,
    ) -> crate::mcp::McpFuture<'a, ()>;
}

/// A durable reservation and the maximum active duration it authorizes.
#[derive(Clone, Copy, Debug)]
pub struct ProcessingGrant {
    /// Host-owned opaque reservation identity.
    pub id: i64,
    /// Positive time allowance reserved by the host.
    pub allowance: Duration,
}

tokio::task_local! { static PROCESSING_DEADLINE: Instant; }

pub(super) fn remaining_processing_ms() -> Result<Option<u64>, RuntimeError> {
    PROCESSING_DEADLINE
        .try_with(|deadline| {
            let remaining = deadline
                .saturating_duration_since(Instant::now())
                .as_millis();
            if remaining == 0 {
                Err(RuntimeError::failure(FailureCode::Budget))
            } else {
                Ok(Some(u64::try_from(remaining).unwrap_or(u64::MAX)))
            }
        })
        .unwrap_or(Ok(None))
}

/// Finite worker, mailbox, ownership and polling settings.
#[derive(Clone, Debug)]
pub struct ExecutionConfig {
    /// Which runs may acquire ownership, recover, enforce deadlines or execute.
    pub scope: ExecutionScope,
    /// Finite lifetime attachment-set bound, separate from concurrent `max_runs`.
    pub max_attached_runs: usize,
    /// Concurrent calculation workers; network attempts never become pure retries.
    pub workers: usize,
    /// Maximum locally coordinated runs.
    pub max_runs: usize,
    /// Ownership lease duration in milliseconds.
    pub lease_ms: i64,
    /// Active scheduling and retry interval. Idle discovery uses hints and the
    /// next known eligibility, with reconciliation at least every 250 ms or this
    /// interval, whichever is longer.
    pub poll_interval: Duration,
    /// Maximum ready local decisions committed together within a scope.
    /// Includes sibling guards, admission, skips and pure eval completion.
    /// One preserves independently pinnable transitions; two through eight opt in.
    pub pure_batch_steps: usize,
}
impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            scope: ExecutionScope::All,
            max_attached_runs: 4096,
            workers: 2,
            max_runs: 64,
            lease_ms: 30000,
            poll_interval: Duration::from_millis(20),
            pure_batch_steps: 1,
        }
    }
}
/// A running execution service. Dropping control Runtime data does not rewrite run
/// outcomes; shutdown stops workers and preserves durable state for recovery.
#[derive(Clone)]
pub struct ExecutionEngine {
    address: Addr<Supervisor>,
}
impl ExecutionEngine {
    /// Starts supervision inside the application's Actix system. Source admission
    /// can be used separately; this service consumes durable work and recovery hints.
    ///
    /// # Errors
    /// Returns missing Actix context, invalid finite configuration, or an already
    /// running engine attached to this runtime.
    pub fn launch(runtime: Arc<Runtime>, config: ExecutionConfig) -> Result<Self, RuntimeError> {
        if System::try_current().is_none()
            || config.workers == 0
            || config.workers > 64
            || config.max_runs == 0
            || config.max_runs > 4096
            || config.max_attached_runs == 0
            || config.max_attached_runs > 4096
            || config.lease_ms < 100
            || config.poll_interval.is_zero()
            || config.poll_interval > Duration::from_secs(10)
            || !(1..=8).contains(&config.pure_batch_steps)
            || runtime.processing.is_some() && config.pure_batch_steps != 1
        {
            return Err(RuntimeError::failure(FailureCode::Profile));
        }
        // Reserve routing before starting any workers. Concurrent launches must
        // never leave two supervisors sharing a single host-command address.
        let mut execution = runtime
            .execution
            .lock()
            .map_err(|_| RuntimeError::failure(FailureCode::Invariant))?;
        if execution
            .as_ref()
            .and_then(WeakAddr::upgrade)
            .is_some_and(|address| address.connected())
        {
            return Err(RuntimeError::failure(FailureCode::Profile));
        }
        let workers = SyncArbiter::start(config.workers, || Worker);
        let address = Supervisor {
            stop: Arc::new(crate::mcp::McpCancellation::new()),
            runtime: runtime.clone(),
            config,
            workers,
            active: BTreeMap::new(),
            attached: BTreeSet::new(),
            polling: false,
            hinted: false,
            wake: None,
            next_eligible: None,
            cursor: None,
        }
        .start();
        *execution = Some(address.downgrade());
        Ok(Self { address })
    }

    /// Select an existing run for this engine's lifetime. Repetition is idempotent;
    /// attaching an already terminal run adds no entry. Prior attachments remain
    /// in the bounded set even after completion.
    /// An `Attached` engine does no background run work before this call. Selection
    /// does not grant policy authority, restart deadlines or create an admission.
    /// The set is additive: use shutdown/new engine to select a different scope.
    ///
    /// # Errors
    /// Returns an unknown run, exhausted attachment capacity or stopped engine.
    pub async fn attach_run(&self, run: &RunId) -> Result<(), RuntimeError> {
        self.address
            .send(AttachRun(run.clone()))
            .await
            .map_err(|_| RuntimeError::failure(FailureCode::Invariant))?
    }
    /// Stops local coordination without changing terminal outcomes or redispatching
    /// effects. Restart reacquires ownership after existing leases expire.
    ///
    /// # Errors
    /// Returns unavailable supervisor.
    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        self.address
            .send(Stop)
            .await
            .map_err(|_| RuntimeError::failure(FailureCode::Invariant))
    }
}
struct Worker;
impl Actor for Worker {
    type Context = SyncContext<Self>;
}
struct Work {
    stop: Arc<crate::mcp::McpCancellation>,
    runtime: Arc<Runtime>,
    lease: RunLease,
    recovering: bool,
    pure_batch_steps: usize,
    working: working::Slot,
}
impl Message for Work {
    type Result = (Result<step::Progress, RuntimeError>, working::Slot);
}
impl Handler<Work> for Worker {
    type Result = MessageResult<Work>;
    fn handle(&mut self, msg: Work, _: &mut Self::Context) -> Self::Result {
        if msg.stop.is_cancelled() {
            return MessageResult((Ok(step::Progress::Blocked), None));
        }
        let cell = std::cell::RefCell::new(msg.working);
        let mut run = Box::pin(working::WORKING.scope(
            cell,
            drive_work(msg.runtime, msg.lease, msg.recovering, msg.pure_batch_steps),
        ));
        let result = System::new().block_on(run.as_mut());
        let cache = run
            .as_mut()
            .take_value()
            .and_then(std::cell::RefCell::into_inner);
        MessageResult((result, cache))
    }
}

async fn drive_work(
    runtime: Arc<Runtime>,
    lease: RunLease,
    recovering: bool,
    batch: usize,
) -> Result<step::Progress, RuntimeError> {
    let Some(meter) = runtime.processing.as_ref() else {
        return recovery::drive(runtime, lease, recovering, batch).await;
    };
    let grant = meter.reserve(&lease).await?;
    let Some(grant) = grant else {
        control::stop(
            &runtime,
            &lease,
            None,
            None,
            FailureCode::Budget,
            false,
            vec![],
        )
        .await?;
        return Ok(step::Progress::Terminal);
    };
    let started = Instant::now();
    let deadline = started
        .checked_add(grant.allowance)
        .filter(|deadline| *deadline > started)
        .ok_or_else(|| RuntimeError::failure(FailureCode::Profile))?;
    let result = PROCESSING_DEADLINE
        .scope(
            deadline,
            recovery::drive(runtime.clone(), lease.clone(), recovering, batch),
        )
        .await;
    meter.settle(&lease, grant, started.elapsed()).await?;
    match result {
        Err(error) if error.code() == FailureCode::Budget => {
            control::stop(
                &runtime,
                &lease,
                None,
                None,
                FailureCode::Budget,
                false,
                vec![],
            )
            .await?;
            Ok(step::Progress::Terminal)
        }
        result => result,
    }
}
pub(crate) struct Supervisor {
    stop: Arc<crate::mcp::McpCancellation>,
    runtime: Arc<Runtime>,
    config: ExecutionConfig,
    workers: Addr<Worker>,
    active: BTreeMap<RunId, Addr<Coordinator>>,
    attached: BTreeSet<RunId>,
    polling: bool,
    hinted: bool,
    wake: Option<SpawnHandle>,
    next_eligible: Option<Instant>,
    cursor: Option<RunId>,
}
impl Actor for Supervisor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(64);
        ctx.notify(Poll);
    }
    fn stopped(&mut self, ctx: &mut Self::Context) {
        self.stop.cancel();
        // A delayed stop from an earlier engine cannot detach its replacement.
        if let Ok(mut execution) = self.runtime.execution.lock()
            && execution
                .as_ref()
                .and_then(WeakAddr::upgrade)
                .is_some_and(|address| address == ctx.address())
        {
            *execution = None;
        }
    }
}
struct Poll;
impl Message for Poll {
    type Result = ();
}

pub(crate) fn hint(runtime: &Runtime) {
    if let Ok(address) = runtime.execution.lock()
        && let Some(address) = address.as_ref().and_then(WeakAddr::upgrade)
    {
        address.do_send(Poll);
    }
}

struct AttachRun(RunId);
impl Message for AttachRun {
    type Result = Result<(), RuntimeError>;
}
impl Handler<AttachRun> for Supervisor {
    type Result = AtomicResponse<Self, Result<(), RuntimeError>>;
    fn handle(&mut self, msg: AttachRun, _: &mut Self::Context) -> Self::Result {
        let runtime = self.runtime.clone();
        AtomicResponse::new(Box::pin(
            async move {
                runtime
                    .store
                    .run_header(&msg.0)
                    .await
                    .map_err(RuntimeError::from)
            }
            .into_actor(self)
            .map(|result, actor, ctx| {
                let header = result?;
                if actor.config.scope == ExecutionScope::Attached
                    && !header.status.is_terminal()
                    && !actor.attached.contains(&header.run_id)
                {
                    if actor.attached.len() >= actor.config.max_attached_runs {
                        return Err(RuntimeError::failure(FailureCode::Budget));
                    }
                    actor.attached.insert(header.run_id);
                }
                ctx.notify(Poll);
                Ok(())
            }),
        ))
    }
}
struct Finished(RunId);
impl Message for Finished {
    type Result = ();
}
impl Handler<Finished> for Supervisor {
    type Result = ();
    fn handle(&mut self, msg: Finished, ctx: &mut Self::Context) {
        self.active.remove(&msg.0);
        ctx.notify(Poll);
    }
}
impl Handler<Poll> for Supervisor {
    type Result = ();
    fn handle(&mut self, _: Poll, ctx: &mut Self::Context) {
        if self.polling {
            self.hinted = true;
            return;
        }
        if let Some(handle) = self.wake.take() {
            ctx.cancel_future(handle);
        }
        if self.active.len() >= self.config.max_runs {
            return;
        }
        self.polling = true;
        let runtime = self.runtime.clone();
        let cursor = self.cursor.clone();
        let room = self
            .config
            .max_runs
            .saturating_sub(self.active.len())
            .min(runtime.store.config().max_page_size);
        // Page the selected IDs themselves: unrelated store runs cannot starve
        // an attached run, acquire leases or trigger deadline/recovery mutations.
        let attached = (self.config.scope == ExecutionScope::Attached).then(|| {
            let after = cursor.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
            self.attached
                .range((after, Bound::Unbounded))
                .filter(|run| !self.active.contains_key(*run))
                .take(room)
                .cloned()
                .collect::<Vec<_>>()
        });
        let active = if attached.is_none() {
            self.active.keys().cloned().collect()
        } else {
            Vec::new()
        };
        ctx.spawn(
            async move {
                let observed = runtime.clock.now_ms()?;
                let sample = Instant::now();
                let due = runtime
                    .store
                    .due_runs(observed, room, attached.clone(), active)
                    .await?;
                let next = due.next_eligible.and_then(|at| {
                    sample.checked_add(Duration::from_millis(at.saturating_sub(due.now) as u64))
                });
                let mut ready = Vec::new();
                for run in due.runs {
                    let owner = runtime.store.reserve_run_id().await?;
                    ready.push((run, owner.to_string(), due.now));
                }
                Ok::<_, RuntimeError>((attached.unwrap_or_default(), ready, next))
            }
            .into_actor(self)
            .map(move |result, actor, ctx| {
                actor.polling = false;
                let mut delay = actor.config.poll_interval;
                if let Ok((runs, ready, next)) = result {
                    actor.cursor = (runs.len() == room).then(|| runs.last().cloned()).flatten();
                    if let Some(next) = next {
                        actor.next_eligible =
                            Some(actor.next_eligible.map_or(next, |old| old.min(next)));
                    }
                    if ready.len() < room {
                        delay = delay.max(Duration::from_millis(250));
                    }
                    for (run, owner, now) in ready {
                        if actor.active.contains_key(&run)
                            || actor.active.len() >= actor.config.max_runs
                        {
                            continue;
                        }
                        let address = Coordinator {
                            stop: Arc::new(crate::mcp::McpCancellation::linked(&actor.stop)),
                            runtime: actor.runtime.clone(),
                            config: actor.config.clone(),
                            run: run.clone(),
                            owner,
                            state: CoordinationState::Acquiring,
                            busy: false,
                            renewing: false,
                            workers: actor.workers.clone(),
                            parent: ctx.address().downgrade(),
                            initial_time: now,
                            renew_after: now.saturating_add(actor.config.lease_ms / 3),
                            checking: false,
                            deadlines: None,
                            deadline_wake: None,
                            deadline_hint: None,
                            working: Some(working::WorkingSet::default()),
                        }
                        .start();
                        actor.active.insert(run, address);
                    }
                } else {
                    actor.cursor = None;
                }
                let hinted = std::mem::take(&mut actor.hinted);
                if actor.active.len() >= actor.config.max_runs {
                    actor.cursor = None;
                    actor.next_eligible = None;
                } else if hinted || actor.cursor.is_some() {
                    ctx.notify(Poll);
                } else if actor.config.scope == ExecutionScope::All || !actor.attached.is_empty() {
                    if let Some(at) = actor.next_eligible.take() {
                        delay = delay.min(at.saturating_duration_since(Instant::now()));
                    }
                    actor.wake = Some(ctx.run_later(delay, |actor, ctx| {
                        actor.wake = None;
                        ctx.notify(Poll);
                    }));
                }
            }),
        );
    }
}
struct Stop;
impl Message for Stop {
    type Result = ();
}
impl Handler<Stop> for Supervisor {
    type Result = ();
    fn handle(&mut self, _: Stop, ctx: &mut Self::Context) {
        self.stop.cancel();
        for (_, actor) in std::mem::take(&mut self.active) {
            actor.do_send(Stop)
        }
        ctx.stop();
    }
}
struct Coordinator {
    stop: Arc<crate::mcp::McpCancellation>,
    runtime: Arc<Runtime>,
    config: ExecutionConfig,
    run: RunId,
    owner: String,
    state: CoordinationState,
    busy: bool,
    renewing: bool,
    workers: Addr<Worker>,
    parent: WeakAddr<Supervisor>,
    initial_time: i64,
    renew_after: i64,
    checking: bool,
    deadlines: Option<DeadlineObservation>,
    deadline_wake: Option<(Instant, SpawnHandle)>,
    deadline_hint: Option<i64>,
    working: working::Slot,
}

impl Coordinator {
    fn wake_deadline(&mut self, ctx: &mut Context<Self>, delay: Duration) {
        let delay = delay.min(self.config.poll_interval.max(Duration::from_millis(250)));
        let at = Instant::now() + delay;
        if let Some((old_at, handle)) = self.deadline_wake {
            if old_at <= at {
                return;
            }
            ctx.cancel_future(handle);
        }
        let handle = ctx.run_later(delay, |actor, ctx| {
            actor.deadline_wake = None;
            ctx.notify(CheckDeadline);
        });
        self.deadline_wake = Some((at, handle));
    }
}

fn deadline_delay(at: i64, now: i64) -> Duration {
    Duration::from_millis(at.saturating_sub(now).max(0) as u64)
}
enum CoordinationState {
    Acquiring,
    Recovering(RunLease),
    Ready(RunLease),
}
impl CoordinationState {
    fn lease(&self) -> Option<&RunLease> {
        match self {
            Self::Acquiring => None,
            Self::Recovering(lease) | Self::Ready(lease) => Some(lease),
        }
    }
    fn is_recovered(&self) -> bool {
        matches!(self, Self::Ready(_))
    }
    fn mark_recovered(&mut self) {
        if let Self::Recovering(lease) = self {
            *self = Self::Ready(lease.clone());
        }
    }
}
impl Actor for Coordinator {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(32);
        let runtime = self.runtime.clone();
        let run = self.run.clone();
        let owner = self.owner.clone();
        let now = self.initial_time;
        let expiry = now.saturating_add(self.config.lease_ms);
        ctx.wait(
            async move {
                runtime
                    .store
                    .acquire_inner(&run, owner, now, expiry, Acquisition::Due)
                    .await
            }
            .into_actor(self)
            .map(|result, actor, ctx| match result {
                Ok(lease) => {
                    if actor
                        .runtime
                        .mcp_control
                        .execution(&lease, &actor.stop)
                        .is_err()
                    {
                        ctx.stop();
                        return;
                    }
                    actor.state = CoordinationState::Recovering(lease);
                    ctx.notify(CheckDeadline);
                    ctx.notify(Pulse)
                }
                Err(_) => ctx.stop(),
            }),
        );
        ctx.run_interval(self.config.poll_interval, |_, ctx| ctx.notify(Pulse));
        let renewal_tick = self
            .config
            .poll_interval
            .min(Duration::from_millis(self.config.lease_ms as u64 / 3));
        ctx.run_interval(renewal_tick, |_, ctx| ctx.notify(RenewLease));
    }
    fn stopped(&mut self, _: &mut Self::Context) {
        self.stop.cancel();
        if let Some(parent) = self.parent.upgrade() {
            parent.do_send(Finished(self.run.clone()))
        }
    }
}
struct CheckDeadline;
impl Message for CheckDeadline {
    type Result = ();
}
impl Handler<CheckDeadline> for Coordinator {
    type Result = ();
    fn handle(&mut self, _: CheckDeadline, ctx: &mut Self::Context) {
        if self.checking {
            return;
        }
        let Some(lease) = self.state.lease().cloned() else {
            return;
        };
        self.checking = true;
        ctx.spawn(
            control::check(self.runtime.clone(), lease, self.deadlines)
                .into_actor(self)
                .map(|result, actor, ctx| {
                    actor.checking = false;
                    let delay = match result {
                        Ok(observed) => {
                            actor.deadlines = Some(observed);
                            let next = observed
                                .next
                                .into_iter()
                                .chain(actor.deadline_hint.take())
                                .min();
                            let now = actor
                                .runtime
                                .clock
                                .now_ms()
                                .unwrap_or(observed.now)
                                .max(observed.now);
                            // The cap reconciles independent writers and clock jumps.
                            let ceiling =
                                actor.config.poll_interval.max(Duration::from_millis(250));
                            next.map_or(ceiling, |at| deadline_delay(at, now))
                        }
                        Err(RuntimeError::Storage(
                            StoreError::Fenced | StoreError::CommitUnknown,
                        )) => {
                            ctx.stop();
                            return;
                        }
                        Err(_) => actor.config.poll_interval,
                    };
                    actor.wake_deadline(ctx, delay);
                }),
        );
    }
}

struct RenewLease;
impl Message for RenewLease {
    type Result = ();
}
impl Handler<RenewLease> for Coordinator {
    type Result = ();
    fn handle(&mut self, _: RenewLease, ctx: &mut Self::Context) {
        let Some(lease) = self.state.lease().cloned() else {
            return;
        };
        if !self.renewing
            && self
                .runtime
                .clock
                .now_ms()
                .is_ok_and(|now| now >= self.renew_after)
        {
            self.renewing = true;
            let runtime = self.runtime.clone();
            let duration = self.config.lease_ms;
            ctx.spawn(
                async move {
                    runtime
                        .store
                        .renew_for(&lease, runtime.clock.now_ms()?, duration)
                        .await
                        .map_err(RuntimeError::from)
                }
                .into_actor(self)
                .map(|result, actor, ctx| {
                    actor.renewing = false;
                    actor.renew_after = actor
                        .runtime
                        .clock
                        .now_ms()
                        .unwrap_or(actor.initial_time)
                        .saturating_add(actor.config.lease_ms / 3);
                    if result.is_err() {
                        ctx.stop()
                    }
                }),
            );
        }
    }
}

struct Pulse;
impl Message for Pulse {
    type Result = ();
}
impl Handler<Pulse> for Coordinator {
    type Result = ();
    fn handle(&mut self, _: Pulse, ctx: &mut Self::Context) {
        if self.busy {
            return;
        }
        let Some(lease) = self.state.lease().cloned() else {
            return;
        };
        self.busy = true;
        ctx.spawn(
            self.workers
                .send(Work {
                    stop: self.stop.clone(),
                    runtime: self.runtime.clone(),
                    lease,
                    recovering: !self.state.is_recovered(),
                    pure_batch_steps: self.config.pure_batch_steps,
                    working: self.working.take(),
                })
                .into_actor(self)
                .map(|result, actor, ctx| {
                    actor.busy = false;
                    let result = result.map(|(result, mut working)| {
                        if let Some(at) = working.as_mut().and_then(|set| set.deadline_hint.take())
                        {
                            if actor.checking {
                                actor.deadline_hint =
                                    actor.deadline_hint.into_iter().chain(Some(at)).min();
                            } else if let Ok(now) = actor.runtime.clock.now_ms() {
                                actor.wake_deadline(ctx, deadline_delay(at, now));
                            }
                        }
                        actor.working = working;
                        result
                    });
                    match result {
                        Ok(Ok(step::Progress::Changed)) => {
                            actor.state.mark_recovered();
                            ctx.notify(Pulse)
                        }
                        Ok(Ok(step::Progress::Blocked)) => {}
                        Ok(Ok(step::Progress::Quiescent { sequence, wake_at })) => {
                            let runtime = actor.runtime.clone();
                            let Some(lease) = actor.state.lease().cloned() else {
                                ctx.stop();
                                return;
                            };
                            ctx.wait(
                                async move {
                                    Ok::<_, RuntimeError>(
                                        runtime
                                            .store
                                            .park_run(
                                                &lease,
                                                runtime.clock.now_ms()?,
                                                sequence,
                                                wake_at,
                                                false,
                                            )
                                            .await?,
                                    )
                                }
                                .into_actor(actor)
                                .map(|result, _, ctx| match result {
                                    Ok(Some(_))
                                    | Err(RuntimeError::Storage(
                                        StoreError::Fenced | StoreError::CommitUnknown,
                                    )) => ctx.stop(),
                                    Err(RuntimeError::Storage(StoreError::Conflict)) => {
                                        ctx.notify(Pulse)
                                    }
                                    _ => {}
                                }),
                            );
                        }
                        Ok(Ok(step::Progress::Terminal)) => ctx.stop(),
                        Ok(Err(RuntimeError::Storage(StoreError::Fenced))) => ctx.stop(),
                        Ok(Err(RuntimeError::Storage(
                            StoreError::Conflict | StoreError::Terminal,
                        ))) => ctx.notify(Pulse),
                        _ => ctx.stop(),
                    }
                }),
        );
    }
}
impl Handler<Stop> for Coordinator {
    type Result = ();
    fn handle(&mut self, _: Stop, ctx: &mut Self::Context) {
        self.stop.cancel();
        ctx.stop();
    }
}
