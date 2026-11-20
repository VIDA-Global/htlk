//! Bounded polling for a coherent snapshot while an execution engine is active.
use htlk::runtime::{
    HostContext, Runtime, execution::RunView, failure::FailureCode, identity::RunId,
};
use std::time::{Duration, Instant};

pub async fn inspect_when_stable(
    runtime: &Runtime,
    caller: &HostContext,
    run: &RunId,
    deadline: Instant,
) -> RunView {
    loop {
        match runtime.inspect_run(caller, run).await {
            Ok(view) => return view,
            // A coordinator can commit between the snapshot's reads.
            Err(error) if error.code() == FailureCode::Fenced => {
                assert!(
                    Instant::now() < deadline,
                    "inspection never settled: {error}"
                );
            }
            Err(error) => panic!("run inspection failed: {error:?}"),
        }
        actix::clock::sleep(Duration::from_millis(2)).await;
    }
}
