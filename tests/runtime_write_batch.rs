//! Observable write and inspection contracts of opt-in eval batches.
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
    let (compiler, registry) = support::native_compiler_with_timeouts(60_000, 5_000);
    let registry = Arc::new(registry);
    let graph = compiler.compile_source(r#"ir_version="0.1" graph test.batch {
      inputs={} outputs={value=integer}
      nodes{ first=eval(integer,7) second=eval(integer,inputs.number){inputs={number=integer}} }
      edges{edge bind{from=first.outputs.value to=second.inputs.number} edge done{from=second.outputs.value to=outputs.value}}
    }"#, &support::catalogs()).unwrap();
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
        e2e::assert_replay(&store, &run).await;
        let end = store.inspection_position().await.unwrap();
        let journal = store.journal(&run, 0, 100).await.unwrap();
        let events = journal
            .iter()
            .filter(|entry| entry.event == JournalEvent::RecordsCommitted)
            .count();
        let positions = end.number() - start.number();
        let writes = store.write_metrics().operations - before;
        if let Some((expected, old_positions, old_writes)) = baseline {
            assert_eq!(events, expected);
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
            baseline = Some((events, positions, writes));
        }
    }
}
