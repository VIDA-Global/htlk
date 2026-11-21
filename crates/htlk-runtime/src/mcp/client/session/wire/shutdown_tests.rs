use super::*;

#[actix::test]
async fn owner_shutdown_waits_for_an_evicted_wire() {
    let limits = Limits::default();
    let config =
        Arc::new(McpClientConfig::new(McpClientLimits::default(), BTreeMap::new(), None).unwrap());
    let stop = Arc::new(McpCancellation::new());
    let lifetime = Arc::new(McpCancellation::new());
    let registry = Registry::default();
    let executors = workers::Executors::launch(&config, stop.clone()).unwrap();
    let (dispatcher, _commands) = mpsc::channel(1);
    let owner = McpOwner::new(
        HostContext::new("host".into(), &limits).unwrap(),
        McpOwnerScope::Host("shutdown-test".into()),
        1,
        &limits,
    )
    .unwrap();
    let server = ServerIdentity::new(
        "test".into(),
        McpTransport::Stdio,
        "fixture".into(),
        "1".into(),
        &limits,
    )
    .unwrap();
    let wire = open(registry.clone(), config, executors, stop.clone(), Arc::new(AtomicU64::new(1)), owner, server, McpEndpoint::Stdio {
        program: "node".into(),
        arguments: vec!["-e".into(), r#"require('node:readline').createInterface({input:process.stdin}).on('line', line => {
            const {id, method} = JSON.parse(line);
            if (method === 'initialize') console.log(JSON.stringify({jsonrpc:'2.0', id, result:{protocolVersion:'2025-11-25',capabilities:{},serverInfo:{name:'fixture',version:'1'}}}));
        });"#.into()],
        environment: BTreeMap::new(),
    }, limits, lifetime.clone(), dispatcher).await.unwrap();

    // Force the lookup watcher to win the race while the wire is still open.
    let hold = wire.cleanup.lock().await;
    lifetime.cancel();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !registry.lock().unwrap().sessions.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("lookup watcher did not retire the session");
    stop.cancel();
    let mut jobs = tokio::task::JoinSet::new();
    let mut drain = std::pin::pin!(drain_shutdown(&registry, &mut jobs));
    std::future::poll_fn(|context| {
        use std::future::Future as _;
        assert!(
            drain.as_mut().poll(context).is_pending(),
            "owner acknowledged shutdown before wire cleanup"
        );
        std::task::Poll::Ready(())
    })
    .await;
    drop(hold);
    drain.await.unwrap();
    assert!(*wire.closed.borrow());
}
