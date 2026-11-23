//! Real wait/reply/cancellation transactions through the fenced actor owner.
mod support;
use htlk::runtime::{
    Authorization, HostContext, Runtime, RuntimeConfig, RuntimePolicy,
    clock::CoordinatorClock,
    execution::{ExecutionConfig, ExecutionEngine},
    failure::{FailureCode, RuntimeFailure},
    identity::RunId,
    records::{InputValue, RunStatus, SecurityLabels},
    store::StoreConfig,
};
use htlk_executable::{
    Identifier,
    cbor::{Limits, Value},
    digest::Digest,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
use support::store::TestStore as SqliteStore;
use support::{catalogs, native_compiler};
struct Policy;
impl RuntimePolicy for Policy {
    fn authorize(&self, c: &HostContext, _: Authorization<'_>) -> Result<(), RuntimeFailure> {
        if c.principal() == "alice" {
            Ok(())
        } else {
            Err(RuntimeFailure::new(FailureCode::Policy))
        }
    }
    fn input_labels(
        &self,
        _: &HostContext,
        _: Digest,
        _: &Identifier,
        _: Option<&Value>,
    ) -> Result<SecurityLabels, RuntimeFailure> {
        Ok(SecurityLabels::new(vec!["private".into()], &Limits::default()).unwrap())
    }
}
struct Clock(AtomicI64);
impl CoordinatorClock for Clock {
    fn now_ms(&self) -> Result<i64, RuntimeFailure> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}
fn context(name: &str) -> HostContext {
    HostContext::new(name.into(), &Limits::default()).unwrap()
}
async fn setup() -> (
    Arc<Runtime>,
    ExecutionEngine,
    Arc<Clock>,
    RunId,
    SqliteStore,
) {
    setup_with_budget(None).await
}

async fn setup_with_budget(
    budget: Option<Arc<dyn htlk::runtime::execution::ProcessingBudget>>,
) -> (
    Arc<Runtime>,
    ExecutionEngine,
    Arc<Clock>,
    RunId,
    SqliteStore,
) {
    let (compiler, registry) = native_compiler();
    let result=compiler.compile_source(r#"ir_version="0.1" graph test.wait{inputs={request=json} outputs={value=string}nodes{review=wait(string){topic="review" timeout_ms=500}}edges{edge request{from=inputs.request to=review.inputs.request} edge reply{from=review.outputs.value to=outputs.value}}}"#,&catalogs()).unwrap();
    let clock = Arc::new(Clock(AtomicI64::new(1000)));
    let store = SqliteStore::in_memory(StoreConfig::default())
        .await
        .unwrap();
    let runtime = Runtime::new(
        store.clone(),
        Arc::new(registry),
        Arc::new(Policy),
        RuntimeConfig::default(),
    )
    .unwrap()
    .with_clock(clock.clone());
    let runtime = Arc::new(if let Some(budget) = budget {
        runtime.with_processing_budget(budget)
    } else {
        runtime
    });
    runtime
        .register_graph(&context("alice"), &result.executable_bytes, &[])
        .await
        .unwrap();
    let engine = ExecutionEngine::launch(
        runtime.clone(),
        ExecutionConfig {
            poll_interval: Duration::from_millis(2),
            ..Default::default()
        },
    )
    .unwrap();
    let run = runtime
        .start_run(
            &context("alice"),
            result.fingerprint,
            &BTreeMap::from([(
                "request".parse().unwrap(),
                InputValue::Inline(Value::Text("question".into())),
            )]),
            "start",
            None,
        )
        .await
        .unwrap()
        .run_id;
    (runtime, engine, clock, run, store)
}

#[actix::test]
async fn parked_run_holds_no_active_grant_and_budget_stops_the_next_work() {
    use htlk::runtime::{
        execution::{ProcessingBudget, ProcessingGrant},
        mcp::McpFuture,
        store::RunLease,
    };
    use std::sync::atomic::AtomicBool;
    #[derive(Default)]
    struct Meter {
        issued: AtomicI64,
        settled: AtomicI64,
        exhausted: AtomicBool,
    }
    impl ProcessingBudget for Meter {
        fn reserve<'a>(&'a self, _: &'a RunLease) -> McpFuture<'a, Option<ProcessingGrant>> {
            Box::pin(async move {
                Ok(if self.exhausted.load(Ordering::SeqCst) {
                    None
                } else {
                    Some(ProcessingGrant {
                        id: self.issued.fetch_add(1, Ordering::SeqCst),
                        allowance: Duration::from_secs(5),
                    })
                })
            })
        }
        fn settle<'a>(
            &'a self,
            _: &'a RunLease,
            grant: ProcessingGrant,
            _: Duration,
        ) -> McpFuture<'a, ()> {
            Box::pin(async move {
                assert_eq!(self.settled.fetch_add(1, Ordering::SeqCst), grant.id);
                Ok(())
            })
        }
    }
    let meter = Arc::new(Meter::default());
    let (runtime, engine, _, run, store) = setup_with_budget(Some(meter.clone())).await;
    let wait = open(&runtime, &run).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.run_header(&run).await.unwrap().owner.is_some() {
            actix::clock::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let count = meter.issued.load(Ordering::SeqCst);
    assert!(count > 0);
    actix::clock::sleep(Duration::from_millis(300)).await;
    assert_eq!(count, meter.issued.load(Ordering::SeqCst));
    assert_eq!(count, meter.settled.load(Ordering::SeqCst));
    meter.exhausted.store(true, Ordering::SeqCst);
    runtime
        .submit_reply(
            &context("alice"),
            wait,
            Value::Text("answer".into()),
            "meter-resume",
        )
        .await
        .unwrap();
    assert_eq!(terminal(&runtime, &run).await, RunStatus::Failed);
    let view = runtime.inspect_run(&context("alice"), &run).await.unwrap();
    assert_eq!(view.failure.unwrap().code(), FailureCode::Budget);
    engine.shutdown().await.unwrap();
}
async fn open(runtime: &Runtime, run: &RunId) -> htlk::runtime::identity::WaitId {
    for _ in 0..1000 {
        let view = runtime.inspect_run(&context("alice"), run).await.unwrap();
        if let Some(wait) = view.waits.first() {
            return wait.wait_id;
        }
        actix::clock::sleep(Duration::from_millis(2)).await;
    }
    panic!("wait did not open")
}
async fn terminal(runtime: &Runtime, run: &RunId) -> RunStatus {
    for _ in 0..1000 {
        let view = runtime.inspect_run(&context("alice"), run).await.unwrap();
        if view.header.status.is_terminal() {
            return view.header.status;
        }
        actix::clock::sleep(Duration::from_millis(2)).await;
    }
    panic!("run did not terminate")
}
#[test]
fn parked_waits_release_the_only_execution_slot() {
    actix::System::new().block_on(async {
        let (runtime, engine, _, first, store) = setup().await;
        let caller = context("alice");
        let first_wait = open(&runtime, &first).await;
        let config = ExecutionConfig {
            max_runs: 1,
            workers: 1,
            poll_interval: Duration::from_millis(2),
            ..Default::default()
        };
        let wait_for_park = |run: RunId| {
            let store = store.clone();
            async move {
                let until = std::time::Instant::now() + Duration::from_secs(20);
                loop {
                    let header = store.run_header(&run).await.unwrap();
                    if header.owner.is_none() && header.fence > 0 {
                        return header;
                    }
                    assert!(
                        std::time::Instant::now() < until,
                        "wait did not park: {header:?}"
                    );
                    actix::clock::sleep(Duration::from_millis(5)).await;
                }
            }
        };
        let first_header = wait_for_park(first.clone()).await;
        engine.shutdown().await.unwrap();
        let engine = ExecutionEngine::launch(runtime.clone(), config).unwrap();
        let inputs = BTreeMap::from([(
            "request".parse().unwrap(),
            InputValue::Inline(Value::Text("queued".into())),
        )]);
        let mut others = Vec::new();
        for i in 0..6 {
            others.push(
                runtime
                    .start_run(
                        &caller,
                        first_header.fingerprint,
                        &inputs,
                        &format!("queued-{i}"),
                        None,
                    )
                    .await
                    .unwrap()
                    .run_id,
            );
        }
        let mut parked = Vec::new();
        for run in others {
            parked.push(wait_for_park(run).await);
        }
        runtime
            .submit_reply(
                &caller,
                first_wait,
                Value::Text("accepted".into()),
                "resume",
            )
            .await
            .unwrap();
        assert_eq!(terminal(&runtime, &first).await, RunStatus::Succeeded);
        assert!(store.run_header(&first).await.unwrap().fence > first_header.fence);
        for unchanged in parked {
            assert_eq!(
                store.run_header(&unchanged.run_id).await.unwrap(),
                unchanged
            );
        }
        engine.shutdown().await.unwrap();
    });
}

#[test]
fn reply_type_policy_and_idempotency_are_checked_without_holding_workers() {
    actix::System::new().block_on(async {
        let (runtime, engine, clock, run, store) = setup().await;
        let wait = open(&runtime, &run).await;
        assert!(
            runtime
                .reconcile_reply(
                    &context("alice"),
                    wait,
                    Value::Text("answer".into()),
                    "reply"
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(open(&runtime, &run).await, wait);
        assert_eq!(
            runtime
                .submit_reply(&context("bob"), wait, Value::Text("answer".into()), "reply")
                .await
                .err()
                .unwrap()
                .code(),
            FailureCode::Policy
        );
        assert_eq!(
            runtime
                .submit_reply(&context("alice"), wait, Value::Integer(7), "reply")
                .await
                .err()
                .unwrap()
                .code(),
            FailureCode::Input
        );
        assert_eq!(
            runtime
                .submit_reply(
                    &context("alice"),
                    wait,
                    Value::Text("answer".into()),
                    "reply"
                )
                .await
                .unwrap()
                .disposition,
            "accepted"
        );
        assert_eq!(terminal(&runtime, &run).await, RunStatus::Succeeded);
        engine.shutdown().await.unwrap();
        let before = store.run_header(&run).await.unwrap();
        assert_eq!(
            runtime
                .reconcile_reply(
                    &context("alice"),
                    wait,
                    Value::Text("answer".into()),
                    "reply"
                )
                .await
                .unwrap()
                .unwrap()
                .disposition,
            "accepted"
        );
        assert!(
            runtime
                .reconcile_reply(
                    &context("alice"),
                    wait,
                    Value::Text("answer".into()),
                    "other-key"
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            runtime
                .reconcile_reply(
                    &context("alice"),
                    wait,
                    Value::Text("changed".into()),
                    "reply"
                )
                .await
                .unwrap_err()
                .code(),
            FailureCode::IdempotencyConflict
        );
        assert_eq!(
            runtime
                .reconcile_reply(&context("bob"), wait, Value::Text("answer".into()), "reply")
                .await
                .unwrap_err()
                .code(),
            FailureCode::Policy
        );
        assert_eq!(store.run_header(&run).await.unwrap(), before);
        let receipt = runtime
            .cancel_run(&context("alice"), &run, "already complete", "terminal-noop")
            .await
            .unwrap();
        assert_eq!(receipt.status, RunStatus::Succeeded);
        let after = store.run_header(&run).await.unwrap();
        assert_eq!(after.sequence, before.sequence + 1);
        assert_eq!(
            runtime
                .cancel_run(&context("alice"), &run, "already complete", "terminal-noop")
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(
            store.run_header(&run).await.unwrap().sequence,
            after.sequence
        );
        assert_eq!(
            runtime
                .cancel_run(&context("alice"), &run, "changed", "terminal-noop")
                .await
                .unwrap_err()
                .code(),
            FailureCode::IdempotencyConflict
        );
        let entries = store.journal(&run, 0, 100).await.unwrap();
        let mut replay =
            htlk::runtime::store::ReplayState::begin(&entries[0], &Limits::default()).unwrap();
        for entry in &entries[1..] {
            replay.apply(entry, &Limits::default()).unwrap();
        }
        assert_eq!(replay.header(), &after);
        clock.0.store(9000, Ordering::SeqCst);
        assert_eq!(
            runtime
                .submit_reply(
                    &context("alice"),
                    wait,
                    Value::Text("answer".into()),
                    "reply"
                )
                .await
                .unwrap()
                .disposition,
            "accepted"
        );
        assert_eq!(
            runtime
                .submit_reply(
                    &context("alice"),
                    wait,
                    Value::Text("changed".into()),
                    "reply"
                )
                .await
                .err()
                .unwrap()
                .code(),
            FailureCode::IdempotencyConflict
        );
    })
}
#[test]
fn cancellation_closes_waits_and_expiry_is_not_an_approval_or_success() {
    actix::System::new().block_on(async {
        let (runtime, engine, _, run, _) = setup().await;
        let wait = open(&runtime, &run).await;
        assert_eq!(
            runtime
                .cancel_run(&context("alice"), &run, "stop", "cancel")
                .await
                .unwrap()
                .status,
            RunStatus::Cancelled
        );
        assert_eq!(
            runtime
                .submit_reply(&context("alice"), wait, Value::Text("late".into()), "late")
                .await
                .err()
                .unwrap()
                .code(),
            FailureCode::Cancelled
        );
        assert_eq!(
            runtime
                .cancel_run(&context("alice"), &run, "different", "cancel")
                .await
                .err()
                .unwrap()
                .code(),
            FailureCode::IdempotencyConflict
        );
        engine.shutdown().await.unwrap();
        let (runtime, engine, clock, run, _) = setup().await;
        open(&runtime, &run).await;
        clock.0.store(1500, Ordering::SeqCst);
        assert_eq!(terminal(&runtime, &run).await, RunStatus::Failed);
        engine.shutdown().await.unwrap();
    })
}

#[test]
fn accepted_waits_return_the_same_disposition_under_fresh_keys_and_reject_changed_values() {
    actix::System::new().block_on(async {
        let (runtime, engine, clock, run, store) = setup().await;
        let wait = open(&runtime, &run).await;
        assert_eq!(
            runtime
                .submit_approval(
                    &context("alice"),
                    wait,
                    Digest::from_bytes([0; 32]),
                    true,
                    "wrong-kind"
                )
                .await
                .unwrap_err()
                .code(),
            FailureCode::Policy
        );
        let accepted = runtime
            .submit_reply(
                &context("alice"),
                wait,
                Value::Text("answer".into()),
                "first",
            )
            .await
            .unwrap();
        assert_eq!(
            runtime
                .submit_reply(
                    &context("alice"),
                    wait,
                    Value::Text("answer".into()),
                    "fresh-key"
                )
                .await
                .unwrap(),
            accepted
        );
        assert_eq!(
            runtime
                .submit_reply(
                    &context("alice"),
                    wait,
                    Value::Text("changed".into()),
                    "changed-key"
                )
                .await
                .unwrap_err()
                .code(),
            FailureCode::IdempotencyConflict
        );
        assert_eq!(terminal(&runtime, &run).await, RunStatus::Succeeded);
        let terminal_header = store.run_header(&run).await.unwrap();
        clock.0.store(9000, Ordering::SeqCst);
        assert_eq!(
            runtime
                .submit_reply(
                    &context("alice"),
                    wait,
                    Value::Text("answer".into()),
                    "terminal-fresh-key"
                )
                .await
                .unwrap(),
            accepted
        );
        assert_eq!(
            runtime
                .submit_reply(
                    &context("alice"),
                    wait,
                    Value::Text("changed".into()),
                    "terminal-changed-key"
                )
                .await
                .unwrap_err()
                .code(),
            FailureCode::IdempotencyConflict
        );
        let after = store.run_header(&run).await.unwrap();
        assert_eq!(after.status, terminal_header.status);
        assert_eq!(after.root_scope, terminal_header.root_scope);
        assert_eq!(after.fingerprint, terminal_header.fingerprint);
        engine.shutdown().await.unwrap();
        let (runtime, engine, clock, run, _) = setup().await;
        let wait = open(&runtime, &run).await;
        clock.0.store(1500, Ordering::SeqCst);
        assert_eq!(terminal(&runtime, &run).await, RunStatus::Failed);
        assert_eq!(
            runtime
                .submit_reply(
                    &context("alice"),
                    wait,
                    Value::Text("late".into()),
                    "expired-reply"
                )
                .await
                .unwrap_err()
                .code(),
            FailureCode::WaitExpired
        );
        engine.shutdown().await.unwrap();
    });
}
