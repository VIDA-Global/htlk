//! Public host admission and recovery contracts on real PostgreSQL.
#![cfg(feature = "postgres")]

#[path = "../crates/htlk-runtime/tests/support/commit_proxy.rs"]
#[allow(dead_code)]
mod commit_proxy;
#[allow(dead_code)]
#[path = "support/e2e.rs"]
mod e2e;
#[path = "support/graph_recovery.rs"]
mod graph_recovery;
#[allow(dead_code)]
mod support;

use htlk::runtime::{
    Authorization, HostContext, NativeRegistry, Runtime, RuntimeConfig, RuntimeError,
    RuntimePolicy,
    clock::CoordinatorClock,
    failure::{FailureCode, RuntimeFailure},
    identity::RunId,
    records::SecurityLabels,
    store::{
        PostgresConfig, PostgresStore, RecordFamily, RecordKey, ReplayState, StoreConfig,
        StoreError,
    },
};
use htlk_executable::{
    ExecutionLimits, Identifier,
    cbor::{Limits, Value},
    digest::Digest,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn settings() -> PostgresConfig {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mut config = PostgresConfig::new(
        std::env::var("HTLK_TEST_POSTGRES_URL").expect("requires disposable PostgreSQL"),
    );
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    config.schema = format!(
        "htlk_upstream_{}_{nanos}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    config
}

fn admin(settings: &PostgresConfig) -> postgres::Client {
    let mut client = postgres::Client::connect(
        &std::env::var("HTLK_TEST_POSTGRES_URL").unwrap(),
        postgres::NoTls,
    )
    .unwrap();
    client
        .batch_execute(&format!(
            "SET search_path TO \"{}\", pg_catalog",
            settings.schema
        ))
        .unwrap();
    client
}

struct Clock(AtomicI64);
impl CoordinatorClock for Clock {
    fn now_ms(&self) -> Result<i64, RuntimeFailure> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}
struct Policy;
impl RuntimePolicy for Policy {
    fn authorize(&self, _: &HostContext, _: Authorization<'_>) -> Result<(), RuntimeFailure> {
        Ok(())
    }
    fn input_labels(
        &self,
        _: &HostContext,
        _: Digest,
        _: &Identifier,
        _: Option<&Value>,
    ) -> Result<SecurityLabels, RuntimeFailure> {
        Ok(SecurityLabels::default())
    }
}
fn runtime(
    store: PostgresStore,
    registry: &Arc<NativeRegistry>,
    clock: Arc<dyn CoordinatorClock>,
) -> Runtime {
    Runtime::new(
        store,
        registry.clone(),
        Arc::new(Policy),
        RuntimeConfig {
            idempotency_retention_ms: 1_000,
            ..RuntimeConfig::default()
        },
    )
    .unwrap()
    .with_clock(clock)
}
fn caller() -> HostContext {
    HostContext::new("host".into(), &Limits::default()).unwrap()
}

async fn assert_deadline(store: &PostgresStore, run: &RunId, expected: i64) {
    let header = store.run_header(run).await.unwrap();
    let scope = store
        .record(
            run,
            &RecordKey {
                family: RecordFamily::Scope,
                id: header.root_scope.to_string(),
            },
        )
        .await
        .unwrap();
    let Value::Map(scope) = scope.payload else {
        panic!()
    };
    assert_eq!(scope.get("deadline_ms"), Some(&Value::Integer(expected)));
    let timers = store
        .records(run, RecordFamily::Timer, None, 10)
        .await
        .unwrap();
    assert_eq!(timers.len(), 2);
    for timer in timers {
        let Value::Map(fields) = timer.payload else {
            panic!()
        };
        assert_eq!(fields.get("due_at_ms"), Some(&Value::Integer(expected)));
    }
}

const PURE_GRAPH: &str =
    r#"ir_version="0.1" graph test.admission { inputs={} outputs={} nodes{} edges{} }"#;

#[test]
fn absolute_admission_checks_the_locked_clock_and_replays_the_original_bound() {
    actix::System::new().block_on(async {
        let settings = settings();
        let store = PostgresStore::initialize(settings.clone(), StoreConfig::default()).await.unwrap();
        let (compiler, registry) = support::native_compiler();
        let clock = Arc::new(Clock(AtomicI64::new(1_000)));
        let runtime = runtime(store.clone(), &Arc::new(registry), clock.clone());
        let graph = compiler.compile_source(PURE_GRAPH, &support::catalogs()).unwrap();
        let host = caller();
        runtime.register_graph(&host, &graph.executable_bytes, &[]).await.unwrap();
        let fp = graph.fingerprint;
        let input = BTreeMap::new();
        // Warm the ID allocator. The next contended writer is initialization.
        let original = runtime.start_run_not_after(&host, fp, &input, "absolute", None, 1_100).await.unwrap();
        assert_deadline(&store, &original.run_id, 1_100).await;
        let tight = ExecutionLimits::new().with_timeout_ms(50).unwrap();
        let stricter = runtime.start_run_not_after(&host, fp, &input, "stricter", Some(&tight), 1_100).await.unwrap();
        assert_deadline(&store, &stricter.run_id, 1_050).await;
        let relative = runtime.start_run(&host, fp, &input, "relative", Some(&tight)).await.unwrap();
        assert_deadline(&store, &relative.run_id, 1_050).await;

        let pin = store.inspection_position().await.unwrap();
        let (ready, locked) = tokio::sync::oneshot::channel();
        let locker = std::thread::spawn({
            let clock = clock.clone();
            let settings = settings.clone();
            move || {
                let mut db = admin(&settings);
                db.batch_execute("BEGIN; SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema(),1213484107))").unwrap();
                ready.send(()).unwrap();
                let until = Instant::now() + Duration::from_secs(5);
                while !db.query_one("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND pg_backend_pid()=ANY(pg_blocking_pids(pid)))", &[]).unwrap().get::<_, bool>(0) {
                    assert!(Instant::now() < until, "admission never reached the locked write boundary");
                    std::thread::sleep(Duration::from_millis(2));
                }
                clock.0.store(1_100, Ordering::SeqCst);
                db.batch_execute("ROLLBACK").unwrap();
            }
        });
        locked.await.unwrap();
        let denied = runtime.start_run_not_after(&host, fp, &input, "expired-fresh", None, 1_100).await;
        locker.join().unwrap();
        assert!(matches!(denied, Err(RuntimeError::Storage(StoreError::Deadline(FailureCode::Timeout)))));
        assert_eq!(store.inspection_position().await.unwrap(), pin);
        assert_eq!(store.running_runs(None, 10).await.unwrap().len(), 3);
        assert_eq!(runtime.start_run_not_after(&host, fp, &input, "absolute", None, 1_100).await.unwrap(), original);
        for changed in [1_099, 1_101] {
            assert_eq!(runtime.start_run_not_after(&host, fp, &input, "absolute", None, changed).await.unwrap_err().code(), FailureCode::IdempotencyConflict);
        }
        assert_eq!(runtime.start_run(&host, fp, &input, "absolute", None).await.unwrap_err().code(), FailureCode::IdempotencyConflict);
        clock.0.store(2_000, Ordering::SeqCst);
        assert_eq!(runtime.reconcile_run_start(&host, fp, &input, "absolute", None, Some(1_100)).await.unwrap(), None);
        assert_eq!(runtime.start_run_not_after(&host, fp, &input, "absolute", None, 1_100).await.unwrap_err().code(), StoreError::Expired.code());
        assert_eq!(store.inspection_position().await.unwrap(), pin);
        store.close().await.unwrap();
    });
}

#[test]
fn absolute_admission_recovers_the_same_run_after_its_commit_ack_is_lost() {
    struct ArmOnFinalObservation {
        arm: Arc<commit_proxy::CommitProxy>,
        calls: AtomicU64,
    }
    impl CoordinatorClock for ArmOnFinalObservation {
        fn now_ms(&self) -> Result<i64, RuntimeFailure> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                self.arm.arm();
            }
            Ok(1_000)
        }
    }
    actix::System::new().block_on(async {
        let settings = settings();
        let store = PostgresStore::initialize(settings.clone(), StoreConfig::default())
            .await
            .unwrap();
        let (compiler, registry) = support::native_compiler();
        let registry = Arc::new(registry);
        let host = caller();
        let graph = compiler
            .compile_source(PURE_GRAPH, &support::catalogs())
            .unwrap();
        let healthy_clock = Arc::new(Clock(AtomicI64::new(1_100)));
        let healthy = runtime(store.clone(), &registry, healthy_clock.clone());
        healthy
            .register_graph(&host, &graph.executable_bytes, &[])
            .await
            .unwrap();
        let proxy = Arc::new(commit_proxy::CommitProxy::start());
        let mut config = PostgresConfig::new(proxy.connection());
        config.schema.clone_from(&settings.schema);
        let interrupted_store = PostgresStore::open(config, StoreConfig::default())
            .await
            .unwrap();
        let interrupted = runtime(
            interrupted_store.clone(),
            &registry,
            Arc::new(ArmOnFinalObservation {
                arm: proxy.clone(),
                calls: AtomicU64::new(0),
            }),
        );
        let fp = graph.fingerprint;
        let inputs = BTreeMap::new();
        assert!(matches!(
            interrupted
                .start_run_not_after(&host, fp, &inputs, "lost", None, 1_100)
                .await,
            Err(RuntimeError::Storage(StoreError::CommitUnknown))
        ));
        assert!(proxy.observed_commit());
        let runs = store.running_runs(None, 10).await.unwrap();
        assert_eq!(runs.len(), 1);
        let entries = store.journal(&runs[0], 0, 10).await.unwrap();
        assert_eq!(entries.len(), 2);
        let pin = store.inspection_position().await.unwrap();
        let mut replay = ReplayState::begin(&entries[0], &Limits::default()).unwrap();
        replay.apply(&entries[1], &Limits::default()).unwrap();
        assert_eq!(
            replay.start_receipt().unwrap().request_digest,
            healthy
                .start_run_request_digest(&host, fp, &inputs, None, Some(1_100))
                .unwrap()
        );
        let original = healthy
            .reconcile_run_start(&host, fp, &inputs, "lost", None, Some(1_100))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.run_id, runs[0]);
        assert_eq!(
            healthy
                .start_run_not_after(&host, fp, &inputs, "lost", None, 1_100)
                .await
                .unwrap(),
            original
        );
        assert_deadline(&store, &original.run_id, 1_100).await;
        healthy_clock.0.store(2_000, Ordering::SeqCst);
        assert_eq!(
            healthy
                .reconcile_run_start(&host, fp, &inputs, "lost", None, Some(1_100))
                .await
                .unwrap(),
            None
        );
        assert_eq!(store.inspection_position().await.unwrap(), pin);
        interrupted_store.close().await.unwrap();
        store.close().await.unwrap();
    });
}
