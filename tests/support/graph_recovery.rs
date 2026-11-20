//! Real process death between the trusted host's COMMIT and graph response acceptance.
use super::*;
use htlk::runtime::{
    execution::{ExecutionConfig, ExecutionEngine, ExecutionScope},
    identity::{AttemptId, InvocationId},
    mcp::{client::McpOperationOrigin, *},
    records::RunStatus,
    store::{CommitReceipt, RunLease, Transition, TransitionContext},
};
use htlk_executable::{JsonDocument, McpBinding, cbor};
use std::process::{Child, Command, Stdio};

const GRAPH: &str = r#"ir_version="0.1" graph test.recovery {
  inputs={} outputs={value=json}
  nodes { args=eval(json,{value="original saved work"}) echo=call(mcp.tool("fixture","echo")) }
  edges { edge args{from=args.outputs.value to=echo.inputs.arguments} edge result{from=echo.outputs.value to=outputs.value} }
}"#;

struct RecoverPolicy;
impl RuntimePolicy for RecoverPolicy {
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
    fn authorize_graph_mcp_recovery(
        &self,
        context: &HostContext,
        request: McpGraphRecovery<'_>,
    ) -> Result<(), RuntimeFailure> {
        if context.principal() == "recovery-host" && request.original_principal == "host" {
            Ok(())
        } else {
            Err(RuntimeFailure::new(FailureCode::Policy))
        }
    }
}
fn recovery_host() -> HostContext {
    HostContext::new("recovery-host".into(), &Limits::default()).unwrap()
}

struct SaveBeforeReturning {
    native: NativeMcpAdapter,
    schema: PostgresConfig,
}
struct SaveConnection<'a> {
    native: Box<dyn McpConnection + 'a>,
    schema: PostgresConfig,
}
impl McpAdapter for SaveBeforeReturning {
    fn connect<'a>(
        &'a self,
        binding: &'a McpBinding,
        context: &'a McpRunContext,
        limits: &'a Limits,
    ) -> McpFuture<'a, Box<dyn McpConnection + 'a>> {
        Box::pin(async move {
            let native = self.native.connect(binding, context, limits).await?;
            Ok(Box::new(SaveConnection {
                native,
                schema: self.schema.clone(),
            }) as Box<dyn McpConnection + 'a>)
        })
    }
    fn descriptor<'a>(
        &'a self,
        _: &'a McpBinding,
        _: &'a McpRunContext,
        _: &'a Limits,
    ) -> McpFuture<'a, JsonDocument> {
        unreachable!()
    }
    fn invoke<'a>(
        &'a self,
        _: &'a McpBinding,
        _: &'a McpRunContext,
        _: McpRequest,
    ) -> McpFuture<'a, Vec<u8>> {
        unreachable!()
    }
}
impl McpConnection for SaveConnection<'_> {
    fn descriptor(&self) -> &JsonDocument {
        self.native.descriptor()
    }
    fn invoke<'a>(&'a mut self, _: McpRequest) -> McpAttemptFuture<'a> {
        unreachable!()
    }
    fn invoke_with_context<'a>(
        &'a mut self,
        request: McpRequest,
        context: McpAttemptContext,
    ) -> McpAttemptFuture<'a> {
        Box::pin(async move {
            let identity = context.graph_attempt().unwrap().clone();
            let outcome = self.native.invoke_with_context(request, context).await;
            assert_eq!(outcome.delivery, McpDelivery::ResponseReceived);
            let raw = outcome.result.unwrap();
            let fields = Value::Array(vec![
                Value::Text(identity.namespace),
                Value::Text(identity.origin.run_id.to_string()),
                Value::Text(identity.origin.invocation_id.to_string()),
                Value::Text(identity.origin.attempt_id.to_string()),
                Value::Text(identity.executable.to_string()),
                Value::Text(identity.binding.to_string()),
                Value::Text(identity.request_digest.to_string()),
                Value::Integer(identity.dispatch_fence),
                Value::Integer(identity.deadline_ms),
                Value::Bytes(raw),
            ]);
            let bytes = cbor::encode(&fields, &Limits::default()).unwrap();
            let schema = self.schema.clone();
            tokio::task::spawn_blocking(move || {
                let mut db = admin(&schema);
                db.execute("INSERT INTO host_saved VALUES (1, $1)", &[&bytes])
                    .unwrap();
            })
            .await
            .unwrap();
            // The parent waits for the committed host row and kills this worker.
            // No result escapes this barrier into the native graph runtime.
            std::future::pending().await
        })
    }
}

#[test]
fn worker() {
    let Ok(schema) = std::env::var("HTLK_GRAPH_RECOVERY_SCHEMA") else {
        return;
    };
    actix::System::new().block_on(async {
        let mut config = PostgresConfig::new(std::env::var("HTLK_TEST_POSTGRES_URL").unwrap());
        config.schema = schema;
        let store = PostgresStore::open(config.clone(), StoreConfig::default())
            .await
            .unwrap();
        let (compiler, registry) = support::native_compiler_with_timeouts(60_000, 30_000);
        let graph = compiler
            .compile_source(GRAPH, &e2e::catalogs("streamable_http"))
            .unwrap();
        let native = NativeMcpAdapter::new(
            BTreeMap::from([(
                "test".into(),
                McpEndpoint::Http {
                    url: std::env::var("HTLK_GRAPH_RECOVERY_PEER").unwrap(),
                    headers: BTreeMap::new(),
                },
            )]),
            5_000,
        )
        .unwrap();
        let runtime = Arc::new(
            runtime(
                store,
                &Arc::new(registry),
                Arc::new(Clock(AtomicI64::new(1_000))),
            )
            .with_mcp(Arc::new(SaveBeforeReturning {
                native,
                schema: config,
            })),
        );
        runtime
            .register_graph(&caller(), &graph.executable_bytes, &[])
            .await
            .unwrap();
        let engine = ExecutionEngine::launch(
            runtime.clone(),
            ExecutionConfig {
                scope: ExecutionScope::Attached,
                lease_ms: 100,
                ..Default::default()
            },
        )
        .unwrap();
        let run = runtime
            .start_run_not_after(
                &caller(),
                graph.fingerprint,
                &BTreeMap::new(),
                "managed",
                None,
                40_000,
            )
            .await
            .unwrap();
        engine.attach_run(&run.run_id).await.unwrap();
        std::future::pending::<()>().await;
    });
}

struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn killed_worker() -> (
    PostgresConfig,
    PostgresStore,
    e2e::Peer,
    McpGraphAttempt,
    Vec<u8>,
) {
    let config = settings();
    let store = PostgresStore::initialize(config.clone(), StoreConfig::default())
        .await
        .unwrap();
    tokio::task::spawn_blocking({
        let config = config.clone();
        move || {
            admin(&config)
                .batch_execute(
                    "CREATE TABLE host_saved(id integer PRIMARY KEY, evidence bytea NOT NULL)",
                )
                .unwrap()
        }
    })
    .await
    .unwrap();
    let peer = e2e::Peer::new("http", "success");
    let McpEndpoint::Http { url, .. } = &peer.endpoint else {
        unreachable!()
    };
    let mut worker = Worker(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "graph_recovery::worker", "--nocapture"])
            .env("HTLK_GRAPH_RECOVERY_SCHEMA", &config.schema)
            .env("HTLK_GRAPH_RECOVERY_PEER", url)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let bytes = tokio::task::spawn_blocking({
        let config = config.clone();
        move || {
            let mut db = admin(&config);
            let end = Instant::now() + Duration::from_secs(25);
            loop {
                if let Some(row) = db
                    .query_opt("SELECT evidence FROM host_saved WHERE id=1", &[])
                    .unwrap()
                {
                    break row.get::<_, Vec<u8>>(0);
                }
                assert!(
                    Instant::now() < end,
                    "worker never committed external evidence"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    })
    .await
    .unwrap();
    let Value::Array(fields) = cbor::decode(&bytes, &Limits::default()).unwrap() else {
        panic!()
    };
    let [
        Value::Text(namespace),
        Value::Text(run),
        Value::Text(invocation),
        Value::Text(attempt),
        Value::Text(executable),
        Value::Text(binding),
        Value::Text(request_digest),
        Value::Integer(dispatch_fence),
        Value::Integer(deadline_ms),
        Value::Bytes(raw),
    ] = fields.as_slice()
    else {
        panic!()
    };
    let origin = McpOperationOrigin {
        run_id: RunId::new(run.clone(), &Limits::default()).unwrap(),
        invocation_id: invocation.parse().unwrap(),
        attempt_id: attempt.parse().unwrap(),
    };
    assert_eq!(
        store.running_runs(None, 10).await.unwrap(),
        vec![origin.run_id.clone()]
    );
    assert_eq!(
        store
            .records(&origin.run_id, RecordFamily::Attempt, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .records(&origin.run_id, RecordFamily::Response, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(peer.calls().len(), 1);
    worker.0.kill().unwrap();
    assert!(!worker.0.wait().unwrap().success());
    assert!(
        store
            .records(&origin.run_id, RecordFamily::Response, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    (
        config,
        store,
        peer,
        McpGraphAttempt {
            namespace: namespace.clone(),
            origin,
            executable: executable.parse().unwrap(),
            binding: binding.parse().unwrap(),
            request_digest: request_digest.parse().unwrap(),
            dispatch_fence: *dispatch_fence,
            deadline_ms: *deadline_ms,
        },
        raw.clone(),
    )
}

fn recovery_runtime(store: PostgresStore, clock: Arc<dyn CoordinatorClock>) -> Runtime {
    let (_, registry) = support::native_compiler_with_timeouts(60_000, 30_000);
    Runtime::new(
        store,
        Arc::new(registry),
        Arc::new(RecoverPolicy),
        RuntimeConfig::default(),
    )
    .unwrap()
    .with_clock(clock)
}

async fn submit(
    runtime: &Runtime,
    host: &HostContext,
    lease: &RunLease,
    attempt: &McpGraphAttempt,
    raw: &[u8],
) -> Result<CommitReceipt, RuntimeError> {
    runtime
        .recover_graph_mcp_response(
            host,
            lease,
            McpSavedGraphResponse {
                attempt: attempt.clone(),
                outcome: McpOutcome {
                    delivery: McpDelivery::ResponseReceived,
                    result: Ok(raw.to_vec()),
                },
                protocol_error: None,
            },
        )
        .await
}

#[test]
fn new_owner_recovers_original_graph_response_and_its_lost_commit_ack() {
    actix::System::new().block_on(async {
        let (config, store, peer, attempt, raw) = killed_worker().await;
        let run = &attempt.origin.run_id;
        let old = store.run_header(run).await.unwrap();
        assert_eq!(old.fence, attempt.dispatch_fence);
        let takeover = old.expires_at_ms.unwrap();
        let stale = store
            .acquire(run, "expired-recovery".into(), takeover, takeover + 1)
            .await
            .unwrap();
        let now = takeover + 1;
        let lease = store
            .acquire(run, "successor".into(), now, now + 10)
            .await
            .unwrap();
        assert!(lease.fence() > attempt.dispatch_fence);
        let clock = Arc::new(Clock(AtomicI64::new(now)));
        let healthy = Arc::new(recovery_runtime(store.clone(), clock.clone()));
        let host = recovery_host();
        let before = store.run_header(run).await.unwrap();
        assert_eq!(
            submit(&healthy, &caller(), &lease, &attempt, &raw)
                .await
                .unwrap_err()
                .code(),
            FailureCode::Policy
        );
        assert_eq!(
            submit(&healthy, &host, &stale, &attempt, &raw)
                .await
                .unwrap_err()
                .code(),
            FailureCode::Fenced
        );
        for alteration in 0..9 {
            let mut changed = attempt.clone();
            match alteration {
                0 => changed.namespace = "another store".into(),
                1 => {
                    changed.origin.run_id =
                        RunId::new("foreign".into(), &Limits::default()).unwrap()
                }
                2 => {
                    changed.origin.invocation_id = InvocationId::new(
                        old.root_scope,
                        &"args".parse().unwrap(),
                        &Limits::default(),
                    )
                    .unwrap()
                }
                3 => {
                    changed.origin.attempt_id =
                        AttemptId::new(changed.origin.invocation_id, 2, &Limits::default()).unwrap()
                }
                4 => changed.binding = Digest::from_bytes([1; 32]),
                5 => changed.request_digest = Digest::from_bytes([2; 32]),
                6 => changed.executable = Digest::from_bytes([3; 32]),
                7 => changed.dispatch_fence += 1,
                _ => changed.deadline_ms += 1,
            }
            assert!(
                submit(&healthy, &host, &lease, &changed, &raw)
                    .await
                    .is_err(),
                "identity alteration {alteration}"
            );
        }
        for malformed in [b"not json".as_slice(), b"{}"] {
            assert_eq!(
                submit(&healthy, &host, &lease, &attempt, malformed)
                    .await
                    .unwrap_err()
                    .code(),
                FailureCode::McpProtocol
            );
        }
        assert_eq!(store.run_header(run).await.unwrap(), before);

        struct ArmResponse(Arc<commit_proxy::CommitProxy>, i64);
        impl CoordinatorClock for ArmResponse {
            fn now_ms(&self) -> Result<i64, RuntimeFailure> {
                self.0.arm();
                Ok(self.1)
            }
        }
        let proxy = Arc::new(commit_proxy::CommitProxy::start());
        let mut through_proxy = PostgresConfig::new(proxy.connection());
        through_proxy.schema.clone_from(&config.schema);
        let interrupted_store = PostgresStore::open(through_proxy, StoreConfig::default())
            .await
            .unwrap();
        let interrupted = recovery_runtime(
            interrupted_store.clone(),
            Arc::new(ArmResponse(proxy.clone(), now)),
        );
        assert!(matches!(
            submit(&interrupted, &host, &lease, &attempt, &raw).await,
            Err(RuntimeError::Storage(StoreError::CommitUnknown))
        ));
        assert!(proxy.observed_commit());
        let receipt = submit(&healthy, &host, &lease, &attempt, &raw)
            .await
            .unwrap();
        assert_eq!(receipt.sequence, before.sequence + 1);
        assert_eq!(
            submit(&healthy, &host, &lease, &attempt, &raw)
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(
            submit(&healthy, &host, &lease, &attempt, br#"{"content":[]}"#)
                .await
                .unwrap_err()
                .code(),
            FailureCode::IdempotencyConflict
        );
        assert_eq!(
            store.run_header(run).await.unwrap().sequence,
            receipt.sequence
        );

        clock.0.store(now + 10, Ordering::SeqCst);
        assert_eq!(
            submit(&healthy, &host, &lease, &attempt, &raw)
                .await
                .unwrap_err()
                .code(),
            FailureCode::Fenced
        );
        let next = store
            .acquire(run, "next-successor".into(), now + 10, now + 20)
            .await
            .unwrap();
        assert_eq!(
            submit(&healthy, &host, &next, &attempt, &raw)
                .await
                .unwrap(),
            receipt
        );
        clock.0.store(now + 20, Ordering::SeqCst);
        let engine = ExecutionEngine::launch(
            healthy.clone(),
            ExecutionConfig {
                scope: ExecutionScope::Attached,
                ..Default::default()
            },
        )
        .unwrap();
        engine.attach_run(run).await.unwrap();
        let completed = e2e::terminal(&healthy, run).await;
        assert_eq!(
            completed.header.status,
            RunStatus::Succeeded,
            "{:?}",
            completed.failure
        );
        let output = e2e::output(&healthy, &completed, "value").await;
        let Value::Map(output) = output else { panic!() };
        let Value::Map(data) = output.get("data").unwrap() else {
            panic!()
        };
        assert_eq!(
            data.get("value"),
            Some(&Value::Text("original saved work".into()))
        );
        let evidence = healthy
            .inspect_mcp_evidence(
                &host,
                run,
                attempt.origin.invocation_id,
                attempt.origin.attempt_id,
                65_536,
            )
            .await
            .unwrap();
        assert_eq!(evidence.raw_response.as_deref(), Some(raw.as_slice()));
        assert!(evidence.recorded_normalization.is_some());
        let usage = store
            .record(
                run,
                &RecordKey {
                    family: RecordFamily::Usage,
                    id: "run".into(),
                },
            )
            .await
            .unwrap();
        let Value::Map(usage) = usage.payload else {
            panic!()
        };
        assert_eq!(usage.get("calls_spent"), Some(&Value::Integer(1)));
        assert_eq!(usage.get("concurrency_reserved"), Some(&Value::Integer(0)));
        assert_eq!(usage.get("deadline_ms"), Some(&Value::Integer(40_000)));
        assert_eq!(
            store
                .records(run, RecordFamily::Attempt, None, 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .records(run, RecordFamily::Response, None, 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(peer.calls().len(), 1);
        assert_eq!(
            tokio::task::spawn_blocking(move || admin(&config)
                .query_one("SELECT count(*) FROM runs", &[])
                .unwrap()
                .get::<_, i64>(0))
            .await
            .unwrap(),
            1
        );
        e2e::assert_replay(&store, run).await;
        engine.shutdown().await.unwrap();
        interrupted_store.close().await.unwrap();
        store.close().await.unwrap();
    });
}

#[test]
fn late_saved_graph_evidence_does_not_write_or_reopen_the_run() {
    actix::System::new().block_on(async {
        let (_, store, peer, attempt, raw) = killed_worker().await;
        let run = &attempt.origin.run_id;
        let now = attempt.deadline_ms;
        let clock = Arc::new(Clock(AtomicI64::new(now)));
        let runtime = recovery_runtime(store.clone(), clock);
        let lease = store
            .acquire(run, "late-recovery".into(), now, now + 100)
            .await
            .unwrap();
        let before = store.run_header(run).await.unwrap();
        let host = recovery_host();
        assert!(matches!(
            submit(&runtime, &host, &lease, &attempt, &raw).await,
            Err(RuntimeError::Storage(StoreError::Deadline(
                FailureCode::Timeout
            )))
        ));
        assert_eq!(store.run_header(run).await.unwrap(), before);
        store
            .commit(
                &lease,
                Transition {
                    context: TransitionContext {
                        command_id: "terminal".into(),
                        actor: "test".into(),
                        causation: "fixture".into(),
                        timestamp_ms: now,
                    },
                    writes: vec![],
                    next_status: Some(RunStatus::Cancelled),
                    root: None,
                    expected_fingerprint: Some(before.fingerprint),
                },
            )
            .await
            .unwrap();
        let terminal = store.run_header(run).await.unwrap();
        assert!(matches!(
            submit(&runtime, &host, &lease, &attempt, &raw).await,
            Err(RuntimeError::Storage(StoreError::Terminal))
        ));
        assert_eq!(store.run_header(run).await.unwrap(), terminal);
        assert!(
            store
                .records(run, RecordFamily::Response, None, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .records(run, RecordFamily::Attempt, None, 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(peer.calls().len(), 1);
        store.close().await.unwrap();
    });
}
