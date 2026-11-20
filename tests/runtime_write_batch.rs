//! Observable write and inspection contracts of opt-in local batches.
#[allow(dead_code)]
#[path = "support/e2e.rs"]
mod e2e;
#[allow(dead_code)]
mod support;

use htlk::runtime::{
    Runtime, RuntimeConfig,
    execution::{ExecutionConfig, ExecutionEngine},
    records::RunStatus,
    store::{JournalEvent, StoreConfig},
};
use htlk_executable::cbor::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use support::store::TestStore;

#[actix::test]
async fn eval_batches_reduce_writes_and_keep_replayable_decisions() {
    compare_batches(r#"ir_version="0.1" graph test.batch {
      inputs={} outputs={value=integer}
      nodes{ first=eval(integer,7) second=eval(integer,inputs.number){inputs={number=integer}} }
      edges{edge bind{from=first.outputs.value to=second.inputs.number} edge done{from=second.outputs.value to=outputs.value}}
    }"#).await;
}

#[actix::test]
async fn sibling_guard_and_skip_batches_keep_order_and_never_dispatch() {
    compare_batches(
        r#"ir_version="0.1" graph test.siblings {
      inputs={} outputs={value=integer}
      nodes {
        absent=read(mcp.resource("fixture","context://fixed")){when=false}
        never=read(mcp.resource("fixture","context://fixed")){when=status(@absent)=="succeeded"}
        value=eval(integer,7){when=status(@never)=="skipped"}
      }
      edges {edge done{from=value.outputs.value to=outputs.value}}
    }"#,
    )
    .await;
}

async fn compare_batches(source: &str) {
    let (compiler, registry) = support::native_compiler_with_timeouts(60_000, 5_000);
    let registry = Arc::new(registry);
    let graph = compiler
        .compile_source(source, &e2e::catalogs("streamable_http"))
        .unwrap();
    let mut baseline = None;
    for steps in [1, 8] {
        let store = TestStore::in_memory(StoreConfig::default()).await.unwrap();
        let runtime = Arc::new(
            Runtime::new(
                store.clone(),
                registry.clone(),
                Arc::new(e2e::Policy),
                RuntimeConfig::default(),
            )
            .unwrap(),
        );
        let caller = e2e::caller();
        runtime
            .register_graph(&caller, &graph.executable_bytes, &[])
            .await
            .unwrap();
        let run = runtime
            .start_run(&caller, graph.fingerprint, &BTreeMap::new(), "batch", None)
            .await
            .unwrap()
            .run_id;
        let start = store.inspection_position().await.unwrap();
        let before = store.write_metrics().operations;
        let engine = ExecutionEngine::launch(
            runtime.clone(),
            ExecutionConfig {
                pure_batch_steps: steps,
                lease_ms: 120_000,
                poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .unwrap();
        let view = e2e::terminal(&runtime, &run).await;
        assert_eq!(view.header.status, RunStatus::Succeeded);
        assert_eq!(
            e2e::output(&runtime, &view, "value").await,
            Value::Integer(7)
        );
        engine.shutdown().await.unwrap();
        assert!(
            store
                .records(&run, htlk::runtime::store::RecordFamily::Attempt, None, 100)
                .await
                .unwrap()
                .is_empty()
        );
        e2e::assert_replay(&store, &run).await;
        let end = store.inspection_position().await.unwrap();
        let journal = store.journal(&run, 0, 100).await.unwrap();
        let events = journal
            .iter()
            .filter(|entry| entry.event == JournalEvent::RecordsCommitted)
            .map(|entry| {
                entry
                    .command_id
                    .as_deref()
                    .unwrap()
                    .split(':')
                    .next()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let positions = end.number() - start.number();
        let writes = store.write_metrics().operations - before;
        if let Some((ref expected, old_positions, old_writes)) = baseline {
            assert_eq!(&events, expected);
            assert!(positions < old_positions && writes < old_writes);
            let mut previous = 0;
            let mut has_interior = false;
            for number in start.number()..=end.number() {
                let position = store.inspection_position_at(number).await.unwrap();
                let sequence = store
                    .open_inspection_snapshot(&run, Some(&position))
                    .await
                    .unwrap()
                    .header()
                    .sequence;
                has_interior |= previous != 0 && sequence > previous + 1;
                previous = sequence;
            }
            assert!(
                has_interior,
                "logical journal events share a physical position"
            );
        } else {
            if source.contains("test.siblings") {
                assert_eq!(events.iter().filter(|event| *event == "skipped").count(), 2);
            }
            baseline = Some((events, positions, writes));
        }
    }
}
