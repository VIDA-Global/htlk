//! Independent stores retain local idempotency and independent history positions.
#[allow(dead_code)]
#[path = "support/e2e.rs"]
mod e2e;
#[allow(dead_code)]
mod support;
use htlk::runtime::{
    Runtime, RuntimeConfig,
    store::{InspectionRunFilter, StoreConfig, StoreError, StorePartitions},
};
use htlk_executable::Identifier;
use std::{collections::BTreeMap, sync::Arc};
use support::store::TestStore;

#[actix::test]
async fn home_routing_retries_and_pins_are_isolated_across_durable_stores() {
    let directory = e2e::TempDir::new();
    let left = TestStore::open(directory.path().join("left.sqlite"), StoreConfig::default())
        .await
        .unwrap();
    let right = TestStore::open(
        directory.path().join("right.sqlite"),
        StoreConfig::default(),
    )
    .await
    .unwrap();
    let a: Identifier = "left".parse().unwrap();
    let b: Identifier = "right".parse().unwrap();
    assert!(matches!(
        StorePartitions::new(BTreeMap::from([
            (a.clone(), left.clone().into()),
            (b.clone(), left.clone().into())
        ]))
        .await,
        Err(StoreError::Invalid)
    ));
    let partitions = StorePartitions::new(BTreeMap::from([
        (a.clone(), left.clone().into()),
        (b.clone(), right.clone().into()),
    ]))
    .await
    .unwrap();
    let (compiler, registry) = support::native_compiler();
    let registry = Arc::new(registry);
    let graph = compiler
        .compile_source(
            r#"ir_version="0.1" graph test.partition{inputs={} outputs={} nodes{} edges{}}"#,
            &support::catalogs(),
        )
        .unwrap();
    let caller = e2e::caller();
    let mut runs = Vec::new();
    for partition in [&a, &b] {
        let runtime = Runtime::new(
            partitions.store(partition).unwrap().clone(),
            registry.clone(),
            Arc::new(e2e::Policy),
            RuntimeConfig::default(),
        )
        .unwrap();
        runtime
            .register_graph(&caller, &graph.executable_bytes, &[])
            .await
            .unwrap();
        let started = runtime
            .start_run(
                &caller,
                graph.fingerprint,
                &BTreeMap::new(),
                "same-key",
                None,
            )
            .await
            .unwrap();
        let retry = runtime
            .start_run(
                &caller,
                graph.fingerprint,
                &BTreeMap::new(),
                "same-key",
                None,
            )
            .await
            .unwrap();
        assert_eq!(started.run_id, retry.run_id);
        assert_eq!(
            partitions.partition_for_run(&started.run_id).unwrap(),
            partition
        );
        runs.push(started.run_id);
    }
    assert_ne!(runs[0], runs[1]);
    let positions = partitions.positions().await.unwrap();
    assert_ne!(positions[&a].namespace(), positions[&b].namespace());
    assert!(
        left.inspection_runs(Some(&positions[&b]), InspectionRunFilter::default(), 0, 1)
            .await
            .is_err()
    );
    for (partition, run) in [&a, &b].into_iter().zip(runs) {
        let store = partitions.store(partition).unwrap();
        let pin = store
            .open_inspection_snapshot(&run, Some(&positions[partition]))
            .await
            .unwrap();
        assert_eq!(pin.header().run_id, run);
        assert_eq!(pin.position(), &positions[partition]);
    }
    left.close().await.unwrap();
    right.close().await.unwrap();
}
