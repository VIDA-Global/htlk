//! Local process barriers for the PostgreSQL capacity campaign.
use crate::{Config, Database, Result, e2e, support};
use htlk::runtime::{Runtime, RuntimeConfig, execution::ExecutionEngine};
use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::{sleep, timeout};

pub struct Capacity {
    pub schema: String,
    pub dir: PathBuf,
    pub engine_only: bool,
}

impl Capacity {
    pub fn new(schema: &str, driver: &str, engine: &str) -> Result<Option<Self>> {
        if schema.is_empty() && driver.is_empty() && engine.is_empty() {
            return Ok(None);
        }
        if !schema.starts_with("htlk_perf_cap_")
            || schema.len() > 63
            || !schema
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || driver.is_empty() == engine.is_empty()
        {
            return Err(
                "capacity control requires a generated schema and one process directory".into(),
            );
        }
        Ok(Some(Self {
            schema: schema.into(),
            dir: if driver.is_empty() { engine } else { driver }.into(),
            engine_only: driver.is_empty(),
        }))
    }

    fn mark(&self, file: &str) -> Result<()> {
        fs::File::create_new(self.dir.join(file))?;
        Ok(())
    }

    async fn wait(&self, file: &str) -> Result<PathBuf> {
        let path = self.dir.join(file);
        timeout(Duration::from_secs(3600), async {
            while !path.try_exists()? {
                sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, std::io::Error>(path)
        })
        .await
        .map_err(|_| format!("capacity barrier timed out: {file}"))?
        .map_err(Into::into)
    }

    pub async fn synchronize(&self, count: usize) -> Result<(Instant, Vec<usize>)> {
        let arrivals = fs::read_to_string(self.dir.join("arrivals"))?
            .lines()
            .map(str::parse::<usize>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if arrivals.len() != count
            || arrivals.last().is_none_or(|i| *i >= 100_000)
            || !arrivals.windows(2).all(|w| w[0] < w[1])
        {
            return Err("invalid capacity arrival schedule".into());
        }
        self.mark("ready")?;
        let start = fs::read_to_string(self.wait("start").await?)?
            .trim()
            .parse::<u64>()?;
        let instant = Instant::now();
        let delay = (UNIX_EPOCH + Duration::from_nanos(start))
            .duration_since(SystemTime::now())
            .map_err(|_| "capacity start time already passed")?;
        if delay > Duration::from_secs(10) {
            return Err("invalid capacity start time".into());
        }
        Ok((instant + delay, arrivals))
    }

    pub async fn await_verification(&self) -> Result<()> {
        self.mark("measured")?;
        self.wait("verify").await?;
        Ok(())
    }

    pub async fn serve(&self, config: &Config) -> Result<()> {
        let database = Database::new(config)?;
        let store = database.open(false).await?;
        let (_, registry) =
            support::native_compiler_with_timeouts(config.graph_timeout_ms(), 5_000);
        let runtime = Runtime::new(
            store.clone(),
            Arc::new(registry),
            Arc::new(e2e::Policy),
            RuntimeConfig {
                max_cached_executables: config.cache,
                ..Default::default()
            },
        )?;
        let engine = ExecutionEngine::launch(Arc::new(runtime), config.engine_config())?;
        let before = store.publication_metrics();
        self.mark("ready")?;
        self.wait("measure_done").await?;
        let measured = store.publication_metrics();
        crate::report_publication(measured, before);
        self.mark("captured")?;
        self.wait("stop").await?;
        engine.shutdown().await?;
        store.clone().close().await?;
        let metrics = store.write_metrics();
        println!("metadata,write_operations,{}", metrics.operations);
        println!("metadata,affected_rows,{}", metrics.rows);
        Ok(())
    }
}
