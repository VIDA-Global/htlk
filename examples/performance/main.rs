//! Release-mode workload driver for disposable SQLite files and PostgreSQL schemas.
#[allow(dead_code)]
#[path = "../../tests/support/e2e.rs"]
mod e2e;
#[allow(dead_code)]
#[path = "../../tests/support/mod.rs"]
mod support;

mod mixed;
mod recovery;
mod workload;

use htlk::runtime::{
    Runtime, RuntimeConfig,
    execution::{ExecutionConfig, ExecutionEngine},
    identity::RunId,
    records::RunStatus,
    store::{
        InspectionRunFilter, JournalEvent, PostgresConfig, PostgresStore, RecordFamily,
        ReplayState, Store, StoreConfig, StorePosition,
    },
};
use htlk_executable::{
    cbor::{Limits, Value},
    digest::Digest,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::Semaphore,
    task::JoinSet,
    time::{sleep, sleep_until, timeout},
};
use workload::Workload;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const PAGE: usize = 256;
const HELP: &str =
    "performance [--backend sqlite|postgres] [--shape chain|wide|loop|wait|stdio|http|sse]
  [--nodes 1] [--payload-bytes 16] [--peer-delay-ms 0] [--runs 128] [--warmup 8]
  [--workers 2] [--max-runs 64] [--coordinators 1] [--poll-ms 20] [--observe-ms 10]
  [--concurrency 16 | --rate 0] [--timeout-s 300] [--micro-samples 30]
  [--history-samples 20] [--readers 0] [--recovery-samples 0] [--cache 16] [--work-dir target]
  [--mix-nodes 0] [--parked-runs 0] adds deep/wide runs and a parked population to a one-node chain.
  [--runs 0 --idle-ms 5000] measures an empty engine with no completion reader.
  PostgreSQL requires HTLK_PERF_POSTGRES_URL for a disposable database.";

struct Config {
    backend: String,
    workload: Workload,
    runs: usize,
    warmup: usize,
    workers: usize,
    max_runs: usize,
    coordinators: usize,
    poll_ms: u64,
    observe_ms: u64,
    concurrency: usize,
    rate: u64,
    timeout_s: u64,
    micro_samples: usize,
    history_samples: usize,
    readers: usize,
    recovery_samples: usize,
    mix_nodes: usize,
    parked_runs: usize,
    idle_ms: u64,
    cache: usize,
    work_dir: PathBuf,
}

impl Config {
    fn parse(args: Vec<String>) -> Result<Self> {
        let mut options = BTreeMap::new();
        for pair in args.chunks(2) {
            if pair.len() != 2
                || !pair[0].starts_with("--")
                || options.insert(pair[0].as_str(), pair[1].as_str()).is_some()
            {
                return Err(format!("expected distinct --option value pairs\n{HELP}").into());
            }
        }
        let mut get = |key, default| options.remove(key).unwrap_or(default);
        let backend = get("--backend", "sqlite").to_owned();
        if !matches!(backend.as_str(), "sqlite" | "postgres") {
            return Err("unknown backend".into());
        }
        let shape = get("--shape", "chain");
        let workload = Workload::new(
            shape,
            get("--nodes", "1").parse()?,
            get("--payload-bytes", "16").parse()?,
            get("--peer-delay-ms", "0").parse()?,
        )?;
        let rate = get("--rate", "0").parse()?;
        let concurrency = get("--concurrency", if rate == 0 { "16" } else { "0" }).parse()?;
        let config = Self {
            backend,
            workload,
            rate,
            concurrency,
            runs: get("--runs", "128").parse()?,
            warmup: get("--warmup", "8").parse()?,
            workers: get("--workers", "2").parse()?,
            max_runs: get("--max-runs", "64").parse()?,
            coordinators: get("--coordinators", "1").parse()?,
            poll_ms: get("--poll-ms", "20").parse()?,
            observe_ms: get("--observe-ms", "10").parse()?,
            timeout_s: get("--timeout-s", "300").parse()?,
            micro_samples: get("--micro-samples", "30").parse()?,
            history_samples: get("--history-samples", "20").parse()?,
            readers: get("--readers", "0").parse()?,
            recovery_samples: get("--recovery-samples", "0").parse()?,
            mix_nodes: get("--mix-nodes", "0").parse()?,
            parked_runs: get("--parked-runs", "0").parse()?,
            idle_ms: get("--idle-ms", "0").parse()?,
            cache: get("--cache", "16").parse()?,
            work_dir: get("--work-dir", "target").into(),
        };
        if let Some(key) = options.keys().next() {
            return Err(format!("unknown option: {key}").into());
        }
        if (rate == 0) == (concurrency == 0)
            || rate > 100_000
            || concurrency > 4096
            || config.runs > 100_000
            || (config.runs == 0) != (config.idle_ms > 0)
            || config.idle_ms > 60_000
            || config.warmup > 4096
            || !(1..=64).contains(&config.workers)
            || !(1..=4096).contains(&config.max_runs)
            || !(1..=8).contains(&config.coordinators)
            || !(1..=10_000).contains(&config.poll_ms)
            || !(1..=10_000).contains(&config.observe_ms)
            || !(1..=3600).contains(&config.timeout_s)
            || config.micro_samples > 1000
            || config.history_samples > 1000
            || config.readers > 8
            || config.cache > 1024
            || config.recovery_samples > 30
            || config.mix_nodes > 64
            || config.parked_runs > 4096
            || (config.parked_runs > 0 && config.mix_nodes == 0)
            || (config.mix_nodes > 0 && (!config.workload.is_short_chain() || config.runs == 0))
            || (config.recovery_samples > 0 && !config.workload.is_wait())
        {
            return Err(
                "invalid workload bounds; specify exactly one positive arrival rate or concurrency"
                    .into(),
            );
        }
        Ok(config)
    }
}

struct Database {
    path: PathBuf,
    settings: Option<PostgresConfig>,
}
impl Database {
    fn new(config: &Config) -> Result<Self> {
        let id = format!(
            "htlk_perf_{}_{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let path = config.work_dir.join(&id);
        let settings = if config.backend == "postgres" {
            let mut settings = PostgresConfig::new(
                std::env::var("HTLK_PERF_POSTGRES_URL")
                    .map_err(|_| "set HTLK_PERF_POSTGRES_URL to a disposable database")?,
            );
            settings.schema = id;
            println!("metadata,postgres_schema,{}", settings.schema);
            Some(settings)
        } else {
            std::fs::create_dir(&path)?;
            None
        };
        Ok(Self { path, settings })
    }

    async fn open(&self, initialize: bool) -> Result<Store> {
        let config = StoreConfig::default();
        Ok(match &self.settings {
            Some(settings) if initialize => PostgresStore::initialize(settings.clone(), config)
                .await?
                .into(),
            Some(settings) => PostgresStore::open(settings.clone(), config).await?.into(),
            None => Store::open(self.path.join("runtime.sqlite"), config).await?,
        })
    }

    fn sqlite_bytes(&self) -> Result<Option<u64>> {
        if self.settings.is_some() {
            return Ok(None);
        }
        Ok(Some(
            std::fs::metadata(self.path.join("runtime.sqlite"))?.len(),
        ))
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        if self.settings.is_none() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

struct Submission {
    index: usize,
    planned: Instant,
    sent: Instant,
    ack: Instant,
    run: RunId,
}

struct Observer {
    cursor: i64,
    reads: usize,
    header_reads: usize,
    roots: BTreeMap<RunId, String>,
}
impl Observer {
    async fn observe(
        &mut self,
        store: &Store,
        runtimes: &[Arc<Runtime>],
        config: &Config,
        slots: &Semaphore,
        count: usize,
    ) -> Result<BTreeMap<RunId, Instant>> {
        let mut finished = BTreeMap::new();
        let mut replied = BTreeSet::new();
        while finished.len() < count {
            let read = store.inspection_changes(self.cursor, None, PAGE).await?;
            self.reads += 1;
            let full = read.len() == PAGE;
            let mut candidates = BTreeSet::new();
            for change in read {
                self.cursor = change.cursor;
                if finished.contains_key(&change.run_id) {
                    continue;
                }
                let Some(key) = change.key else {
                    continue;
                };
                if key.family == RecordFamily::Scope {
                    let root = self
                        .roots
                        .entry(change.run_id.clone())
                        .or_insert_with(|| key.id.clone());
                    if *root == key.id {
                        candidates.insert(change.run_id);
                    }
                } else if config.workload.is_wait()
                    && key.family == RecordFamily::Wait
                    && replied.insert(key.id.clone())
                {
                    runtimes[0]
                        .submit_reply(
                            &e2e::caller(),
                            key.id.parse()?,
                            Value::Text("host reply".into()),
                            "perf-reply",
                        )
                        .await?;
                }
            }
            for run in candidates {
                self.header_reads += 1;
                let header = store.run_header(&run).await?;
                if header.status.is_terminal() {
                    if header.status != RunStatus::Succeeded {
                        let view = runtimes[0].inspect_run(&e2e::caller(), &run).await?;
                        return Err(format!("workload run failed: {:?}", view.failure).into());
                    }
                    finished.insert(run, Instant::now());
                    slots.add_permits(1);
                }
            }
            if !full && finished.len() < count {
                sleep(Duration::from_millis(config.observe_ms)).await;
            }
        }
        Ok(finished)
    }
}

async fn submit(
    runtimes: &[Arc<Runtime>],
    graphs: [Digest; 3],
    config: &Config,
    slots: &Semaphore,
    count: usize,
    offset: usize,
    begin: Instant,
) -> Result<Vec<Submission>> {
    let mut tasks = JoinSet::new();
    let mut submissions = Vec::with_capacity(count);
    for i in 0..count {
        let planned = if config.rate == 0 || offset == 0 {
            slots.acquire().await?.forget();
            Instant::now()
        } else {
            let at = begin + Duration::from_nanos((i as u64 * 1_000_000_000) / config.rate);
            sleep_until(at.into()).await;
            at
        };
        let index = offset + i;
        let fingerprint = graphs[mixed::class(index)];
        let runtime = runtimes[i % runtimes.len()].clone();
        let input = config.workload.input(index);
        tasks.spawn_local(async move {
            let sent = Instant::now();
            let started = runtime
                .start_run(
                    &e2e::caller(),
                    fingerprint,
                    &input,
                    &format!("performance-{index}"),
                    None,
                )
                .await?;
            Ok::<_, htlk::runtime::RuntimeError>(Submission {
                index,
                planned,
                sent,
                ack: Instant::now(),
                run: started.run_id,
            })
        });
        while let Some(result) = tasks.try_join_next() {
            submissions.push(result??);
        }
    }
    while let Some(result) = tasks.join_next().await {
        submissions.push(result??);
    }
    submissions.sort_unstable_by_key(|s| s.index);
    Ok(submissions)
}

struct Campaign<'a> {
    config: &'a Config,
    runtimes: &'a [Arc<Runtime>],
    reader: &'a Store,
    graphs: [Digest; 3],
}

impl Campaign<'_> {
    async fn run(
        &self,
        observer: &mut Observer,
        count: usize,
        offset: usize,
    ) -> Result<(Vec<Submission>, BTreeMap<RunId, Instant>, Instant)> {
        let begin = Instant::now();
        let slots = Semaphore::new(self.config.concurrency.max(1));
        let (submitted, completed) = timeout(Duration::from_secs(self.config.timeout_s), async {
            tokio::try_join!(
                submit(
                    self.runtimes,
                    self.graphs,
                    self.config,
                    &slots,
                    count,
                    offset,
                    begin
                ),
                observer.observe(self.reader, self.runtimes, self.config, &slots, count),
            )
        })
        .await
        .map_err(|_| {
            format!(
                "campaign timed out after {} seconds (at feed cursor {})",
                self.config.timeout_s, observer.cursor
            )
        })??;
        Ok((submitted, completed, begin))
    }
}

fn micros(t: Duration) -> u128 {
    t.as_micros()
}

async fn verify(
    runtime: &Runtime,
    store: &Store,
    config: &Config,
    submissions: &[Submission],
    replay_samples: usize,
) -> Result<()> {
    let caller = e2e::caller();
    let mut seen = BTreeSet::new();
    for (position, submitted) in submissions.iter().enumerate() {
        let view = runtime.inspect_run(&caller, &submitted.run).await?;
        if !seen.insert(&submitted.run)
            || view.header.status != RunStatus::Succeeded
            || e2e::output(runtime, &view, "value").await
                != config.workload.expected(submitted.index)
        {
            return Err(
                format!("incorrect or duplicate result at index {}", submitted.index).into(),
            );
        }
        if position >= replay_samples {
            continue;
        }
        let begin = Instant::now();
        let mut after = 0;
        let mut replay = None;
        let limits = Limits::default();
        while after < view.header.sequence {
            let page = store.journal(&submitted.run, after, 32).await?;
            if page.is_empty() {
                return Err("missing journal during replay".into());
            }
            for entry in page {
                after = entry.sequence;
                if let Some(state) = &mut replay {
                    ReplayState::apply(state, &entry, &limits)?;
                } else {
                    replay = Some(ReplayState::begin(&entry, &limits)?);
                }
            }
        }
        let replay = replay.ok_or("empty replay")?;
        if replay.header() != &view.header {
            return Err("replay diverged".into());
        }
        println!("sample,replay,{},{}", micros(begin.elapsed()), after);
        let retry = runtime
            .start_run(
                &caller,
                view.header.fingerprint,
                &config.workload.input(submitted.index),
                &format!("performance-{}", submitted.index),
                None,
            )
            .await?;
        if retry.run_id != submitted.run || store.run_header(&submitted.run).await? != view.header {
            return Err("idempotent start changed the authoritative run".into());
        }
    }
    Ok(())
}

async fn read_history(
    store: &Store,
    pin: &StorePosition,
    samples: usize,
    label: &str,
) -> Result<()> {
    println!("metadata,{label}_position,{}", pin.number());
    for _ in 0..samples {
        let begin = Instant::now();
        let page = store
            .inspection_runs(Some(pin), InspectionRunFilter::default(), 0, 32)
            .await?;
        println!(
            "sample,{label}_first_page,{},{}",
            micros(begin.elapsed()),
            page.runs.len()
        );
        if let Some(header) = page.runs.first() {
            let begin = Instant::now();
            let snapshot = store
                .open_inspection_snapshot(&header.run_id, Some(pin))
                .await?;
            if snapshot.header() != header {
                return Err("pinned header diverged".into());
            }
            println!("sample,{label}_pin_open,{},1", micros(begin.elapsed()));
        }
    }
    Ok(())
}

async fn measure(config: Config) -> Result<()> {
    let database = Database::new(&config)?;
    let primary = database.open(true).await?;
    // Measure initial persisted SQLite bytes with the WAL checkpointed.
    primary.close().await?;
    let initial_bytes = database.sqlite_bytes()?;
    let primary = database.open(false).await?;
    let mut stores = vec![primary];
    for _ in 1..config.coordinators {
        stores.push(database.open(false).await?);
    }
    let timeout_ms = if config.mix_nodes > 0 {
        300_000
    } else {
        60_000
    };
    let (compiler, registry) = support::native_compiler_with_timeouts(timeout_ms, 5_000);
    let registry = Arc::new(registry);
    let engine_config = ExecutionConfig {
        workers: config.workers,
        max_runs: config.max_runs,
        poll_interval: Duration::from_millis(config.poll_ms),
        ..Default::default()
    };
    if config.runs == 0 {
        let mut engines = Vec::new();
        for store in &stores {
            let runtime = Runtime::new(
                store.clone(),
                registry.clone(),
                Arc::new(e2e::Policy),
                RuntimeConfig::default(),
            )?;
            engines.push(ExecutionEngine::launch(
                Arc::new(runtime),
                engine_config.clone(),
            )?);
        }
        let begin = Instant::now();
        sleep(Duration::from_millis(config.idle_ms)).await;
        println!("metadata,elapsed_us,{}", micros(begin.elapsed()));
        for engine in engines {
            engine.shutdown().await?;
        }
        for store in stores {
            store.close().await?;
        }
        return Ok(());
    }
    let reader = database.open(false).await?;
    let peer = config.workload.peer();
    let catalogs = peer
        .as_ref()
        .map_or_else(support::catalogs, |peer| e2e::catalogs(peer.transport));
    let source = config.workload.source();
    let begin = Instant::now();
    let graph = compiler.compile_source(&source, &catalogs)?;
    println!(
        "sample,compile_first,{},{}",
        micros(begin.elapsed()),
        source.len()
    );
    println!("metadata,executable_bytes,{}", graph.executable_bytes.len());
    let mut runtimes = Vec::new();
    for store in &stores {
        let runtime = Runtime::new(
            store.clone(),
            registry.clone(),
            Arc::new(e2e::Policy),
            RuntimeConfig {
                max_cached_executables: config.cache,
                ..Default::default()
            },
        )?;
        runtimes.push(Arc::new(if let Some(peer) = &peer {
            runtime.with_mcp(peer.adapter())
        } else {
            runtime
        }));
    }
    let caller = e2e::caller();
    let begin = Instant::now();
    runtimes[0]
        .register_graph(&caller, &graph.executable_bytes, &[])
        .await?;
    println!(
        "sample,register_new,{},{}",
        micros(begin.elapsed()),
        graph.executable_bytes.len()
    );
    workload::compiler_samples(
        &compiler,
        &registry,
        &source,
        &catalogs,
        &graph,
        config.micro_samples,
    )?;
    let mut graphs = [graph.fingerprint; 3];
    let parked_graph = if config.mix_nodes > 0 {
        for (slot, shape) in [(1, "chain"), (2, "wide")] {
            let work = Workload::new(shape, config.mix_nodes, 16, 0)?;
            let compiled = compiler.compile_source(&work.source(), &catalogs)?;
            runtimes[0]
                .register_graph(&caller, &compiled.executable_bytes, &[])
                .await?;
            graphs[slot] = compiled.fingerprint;
        }
        let work = Workload::new("wait", 1, 16, 0)?;
        let source = work
            .source()
            .replace("timeout_ms=50000", "timeout_ms=240000");
        let compiled = compiler.compile_source(&source, &catalogs)?;
        runtimes[0]
            .register_graph(&caller, &compiled.executable_bytes, &[])
            .await?;
        Some((work, compiled.fingerprint))
    } else {
        None
    };
    let mut engines = Vec::new();
    for runtime in &runtimes {
        engines.push(ExecutionEngine::launch(
            runtime.clone(),
            engine_config.clone(),
        )?);
    }
    let parked = if let Some((work, fingerprint)) = parked_graph {
        mixed::park(
            &runtimes[0],
            &reader,
            &work,
            fingerprint,
            config.parked_runs,
        )
        .await?
    } else {
        Vec::new()
    };

    let campaign = Campaign {
        config: &config,
        runtimes: &runtimes,
        reader: &reader,
        graphs,
    };
    let mut observer = Observer {
        cursor: 0,
        reads: 0,
        header_reads: 0,
        roots: BTreeMap::new(),
    };
    let early_pin = if config.warmup > 0 {
        campaign.run(&mut observer, config.warmup, 0).await?;
        let pin = reader.inspection_position().await?;
        read_history(&reader, &pin, config.history_samples, "history_small").await?;
        Some(pin)
    } else {
        None
    };
    observer.reads = 0;
    observer.header_reads = 0;

    let mut readers = JoinSet::new();
    let (stop, active) = tokio::sync::watch::channel(false);
    for _ in 0..config.readers {
        let reader = database.open(false).await?;
        let mut active = active.clone();
        readers.spawn_local(async move {
            let mut count = 0;
            while !*active.borrow() {
                reader.inspection_runs(None, InspectionRunFilter::default(), 0, 32).await?;
                count += 1;
                tokio::select! { _ = active.changed() => {}, _ = sleep(Duration::from_millis(20)) => {} }
            }
            reader.close().await?;
            Ok::<_, htlk::runtime::store::StoreError>(count)
        });
    }
    let (submissions, completed, begin) = campaign
        .run(&mut observer, config.runs, config.warmup.max(1))
        .await?;
    let end = completed.values().copied().max().ok_or("no completions")?;
    let _ = stop.send(true);
    let mut reader_pages = 0;
    while let Some(result) = readers.join_next().await {
        reader_pages += result??;
    }
    mixed::verify(&runtimes[0], &reader, parked).await?;
    for engine in engines {
        engine.shutdown().await?;
    }
    println!("metadata,elapsed_us,{}", micros(end.duration_since(begin)));
    println!("metadata,feed_reads,{}", observer.reads);
    println!("metadata,header_reads,{}", observer.header_reads);
    println!("metadata,mixed_reader_pages,{reader_pages}");
    // Keep journal reads and output assertions outside the timed interval.
    for s in &submissions {
        let done = completed.get(&s.run).ok_or("missing completion")?;
        let first = reader.journal(&s.run, 0, 3).await?;
        let acquired = first
            .iter()
            .find(|entry| entry.event == JournalEvent::OwnershipAcquired)
            .ok_or("missing ownership")?;
        let header = reader.run_header(&s.run).await?;
        if config.mix_nodes > 0 {
            if header.fingerprint != graphs[mixed::class(s.index)] {
                return Err("wrong mixed graph executed".into());
            }
            println!(
                "sample,mixed_{}_latency,{},1",
                ["short", "chain", "wide"][mixed::class(s.index)],
                micros(done.duration_since(s.planned))
            );
        }
        println!(
            "run,{},{},{},{},{},{},{},{}",
            s.index,
            micros(s.planned.duration_since(begin)),
            micros(s.sent.duration_since(begin)),
            micros(s.ack.duration_since(begin)),
            micros(done.duration_since(begin)),
            acquired.timestamp_ms - first[0].timestamp_ms,
            header.last_time_ms - acquired.timestamp_ms,
            header.sequence
        );
    }
    verify(
        &runtimes[0],
        &reader,
        &config,
        &submissions,
        config.history_samples,
    )
    .await?;
    if let Some(peer) = peer {
        let calls = peer.calls().len();
        if calls != config.runs + config.warmup {
            return Err(format!("unexpected MCP dispatch count: {calls}").into());
        }
        println!("metadata,mcp_calls,{calls}");
    }
    let late_pin = reader.inspection_position().await?;
    read_history(&reader, &late_pin, config.history_samples, "history_large").await?;
    if let Some(early_pin) = early_pin {
        read_history(
            &reader,
            &early_pin,
            config.history_samples,
            "history_old_pin",
        )
        .await?;
    }
    if config.recovery_samples > 0 {
        recovery::samples(&database, &reader, registry, &config, graph.fingerprint).await?;
    }
    reader.close().await?;
    for store in stores {
        store.close().await?;
    }
    if let (Some(before), Some(after)) = (initial_bytes, database.sqlite_bytes()?) {
        println!(
            "metadata,sqlite_growth_bytes,{}",
            after
                .checked_sub(before)
                .ok_or("unexpected database shrink")?
        );
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!("{HELP}");
        return Ok(());
    }
    let config = Config::parse(args)?;
    actix::System::new().block_on(measure(config))
}
