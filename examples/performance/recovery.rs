//! Restart a fresh runtime against a durable, observed host wait checkpoint.
use crate::{Config, Database, Result, e2e, micros};
use htlk::runtime::{
    NativeRegistry, Runtime, RuntimeConfig,
    execution::{ExecutionConfig, ExecutionEngine},
    failure::FailureCode,
    identity::{RunId, WaitId},
    records::RunStatus,
    store::{RecordFamily, Store},
};
use htlk_executable::digest::Digest;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::{sleep, timeout};

const POLL: Duration = Duration::from_millis(10);
const DEADLINE: Duration = Duration::from_secs(20);

async fn checkpoint(store: &Store, run: &RunId) -> Result<WaitId> {
    loop {
        if let Some(record) = store
            .records(run, RecordFamily::Wait, None, 1)
            .await?
            .first()
        {
            return Ok(record.key.id.parse()?);
        }
        if store.run_header(run).await?.status.is_terminal() {
            return Err("run terminated before durable wait opened".into());
        }
        sleep(POLL).await;
    }
}

pub async fn samples(
    db: &Database,
    reader: &Store,
    registry: Arc<NativeRegistry>,
    config: &Config,
    fingerprint: Digest,
) -> Result<()> {
    let engine_config = ExecutionConfig {
        max_runs: 1,
        workers: 1,
        lease_ms: 2_000,
        poll_interval: Duration::from_millis(config.poll_ms),
        ..Default::default()
    };
    let runtime = |store: &Store| -> Result<Arc<Runtime>> {
        Ok(Arc::new(Runtime::new(
            store.clone(),
            registry.clone(),
            Arc::new(e2e::Policy),
            RuntimeConfig::default(),
        )?))
    };
    let caller = e2e::caller();
    for index in 0..config.recovery_samples {
        let store = db.open(false).await?;
        let previous = runtime(&store)?;
        let engine = ExecutionEngine::launch(previous.clone(), engine_config.clone())?;
        let run = previous
            .start_run(
                &caller,
                fingerprint,
                &config.workload.input(index),
                &format!("recovery-{index}"),
                None,
            )
            .await?
            .run_id;
        let wait = timeout(DEADLINE, checkpoint(reader, &run)).await??;
        let downtime = Instant::now();
        engine.shutdown().await?;
        store.close().await?;
        let header = reader.run_header(&run).await?;
        let due = header.expires_at_ms.ok_or("missing recovery lease")?;
        let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        sleep(Duration::from_millis(due.saturating_sub(now).max(0) as u64)).await;
        println!(
            "sample,recovery_lease_wait,{},1",
            micros(downtime.elapsed())
        );

        let resume = Instant::now();
        let store = db.open(false).await?;
        let resumed = runtime(&store)?;
        let engine = ExecutionEngine::launch(resumed.clone(), engine_config.clone())?;
        let takeover = timeout(DEADLINE, async {
            loop {
                let current = reader.run_header(&run).await?;
                if current.status.is_terminal() {
                    return Err("recovery terminated before host reply".into());
                }
                if current.fence > header.fence {
                    return Ok::<_, Box<dyn std::error::Error>>(Instant::now());
                }
                sleep(POLL).await;
            }
        })
        .await??;
        println!(
            "sample,recovery_reopen_to_fence,{},1",
            micros(takeover.duration_since(resume))
        );
        if checkpoint(reader, &run).await? != wait {
            return Err("recovery changed wait identity".into());
        }
        timeout(DEADLINE, async {
            loop {
                match resumed
                    .submit_reply(
                        &caller,
                        wait,
                        config.workload.expected(index),
                        "recovery-reply",
                    )
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(error) if error.code() == FailureCode::Fenced => sleep(POLL).await,
                    Err(error) => return Err(error),
                }
            }
        })
        .await??;
        let terminal = timeout(DEADLINE, async {
            loop {
                if reader.run_header(&run).await?.status.is_terminal() {
                    return Ok::<_, Box<dyn std::error::Error>>(Instant::now());
                }
                sleep(POLL).await;
            }
        })
        .await??;
        println!(
            "sample,recovery_reopen_to_terminal,{},1",
            micros(terminal.duration_since(resume))
        );
        let view = resumed.inspect_run(&caller, &run).await?;
        if view.header.status != RunStatus::Succeeded
            || e2e::output(&resumed, &view, "value").await != config.workload.expected(index)
        {
            return Err("recovered output mismatch".into());
        }
        engine.shutdown().await?;
        store.close().await?;
    }
    Ok(())
}
