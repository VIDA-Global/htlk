//! Release qualification with a retained parked population and several graph shapes.
use crate::{Result, Workload, e2e, micros};
use htlk::runtime::{
    Runtime,
    identity::WaitId,
    records::RunStatus,
    store::{RecordFamily, RunHeader, Store},
};
use htlk_executable::digest::Digest;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

pub fn class(index: usize) -> usize {
    match index % 8 {
        2 => 1,
        6 => 2,
        _ => 0,
    }
}

pub struct Parked {
    header: RunHeader,
    wait: WaitId,
}

pub async fn park(
    runtime: &Runtime,
    store: &Store,
    workload: &Workload,
    fingerprint: Digest,
    count: usize,
) -> Result<Vec<Parked>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let begin = Instant::now();
    let caller = e2e::caller();
    let mut runs = Vec::with_capacity(count);
    for index in 0..count {
        runs.push(
            runtime
                .start_run(
                    &caller,
                    fingerprint,
                    &workload.input(index),
                    &format!("parked-{index}"),
                    None,
                )
                .await?
                .run_id,
        );
    }
    let parked = timeout(Duration::from_secs(180), async {
        let mut parked = Vec::with_capacity(count);
        for run in runs {
            loop {
                let header = store.run_header(&run).await?;
                if header.status.is_terminal() {
                    return Err("run terminated before parking".into());
                }
                if header.fence > 0 && header.owner.is_none() {
                    let records = store.records(&run, RecordFamily::Wait, None, 2).await?;
                    let [wait] = records.as_slice() else {
                        return Err("parked run does not have one wait".into());
                    };
                    if wait.final_record {
                        return Err("parked run's wait is already closed".into());
                    }
                    parked.push(Parked {
                        header,
                        wait: wait.key.id.parse()?,
                    });
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(parked)
    })
    .await??;
    println!("metadata,parked_runs,{count}");
    println!(
        "sample,park_population,{},{}",
        micros(begin.elapsed()),
        count
    );
    Ok(parked)
}

pub async fn verify(runtime: &Runtime, store: &Store, parked: Vec<Parked>) -> Result<()> {
    for entry in &parked {
        if store.run_header(&entry.header.run_id).await? != entry.header {
            return Err("parked run changed authority or sequence during unrelated load".into());
        }
    }
    let caller = e2e::caller();
    for entry in parked.iter().take(12) {
        let begin = Instant::now();
        let value = htlk_executable::cbor::Value::Text("resume under load".into());
        runtime
            .submit_reply(&caller, entry.wait, value.clone(), "resume-mixed")
            .await?;
        timeout(Duration::from_secs(20), async {
            loop {
                let header = store.run_header(&entry.header.run_id).await?;
                if header.status.is_terminal() {
                    if header.status != RunStatus::Succeeded || header.fence <= entry.header.fence {
                        return Err("parked reply did not succeed under a higher fence".into());
                    }
                    println!(
                        "sample,parked_reply_to_terminal,{},1",
                        micros(begin.elapsed())
                    );
                    let view = runtime.inspect_run(&caller, &header.run_id).await?;
                    if e2e::output(runtime, &view, "value").await != value {
                        return Err("parked reply output mismatch".into());
                    }
                    return Ok::<_, Box<dyn std::error::Error>>(());
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await??;
    }
    Ok(())
}
