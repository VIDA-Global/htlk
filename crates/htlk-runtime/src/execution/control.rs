//! Cancellation/deadline transitions remain responsive while calculations run.
use super::{model::*, step};
use crate::{
    Runtime, RuntimeError,
    failure::{FailureCode, RuntimeFailure},
    records::{RunStatus, SecurityLabels},
    store::{DeadlineObservation, DeadlineTarget, RecordFamily, RunLease, StoredRecord},
};
use htlk_executable::cbor::Value;
use std::{collections::BTreeSet, sync::Arc};

pub(super) fn earliest(writes: &[crate::store::RecordWrite]) -> Option<i64> {
    writes
        .iter()
        .filter_map(|write| {
            if write.final_record
                || !matches!(
                    write.key.family,
                    RecordFamily::Scope | RecordFamily::Invocation | RecordFamily::Usage
                )
            {
                return None;
            }
            match map(&write.payload).ok()?.get("deadline_ms")? {
                Value::Integer(at) => Some(*at),
                _ => None,
            }
        })
        .min()
}

pub(super) async fn all(
    runtime: &Runtime,
    lease: &RunLease,
    family: RecordFamily,
) -> Result<Vec<StoredRecord>, RuntimeError> {
    let mut rows = Vec::new();
    let mut cursor = None;
    loop {
        let page = runtime
            .store
            .records(
                lease.run_id(),
                family,
                cursor.as_deref(),
                runtime.store.config().max_page_size,
            )
            .await?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|r| r.key.id.clone());
        rows.extend(page);
    }
    Ok(rows)
}
pub(super) async fn enforce(runtime: Arc<Runtime>, lease: RunLease) -> Result<(), RuntimeError> {
    check(runtime, lease, None).await.map(|_| ())
}

pub(super) async fn check(
    runtime: Arc<Runtime>,
    lease: RunLease,
    cached: Option<DeadlineObservation>,
) -> Result<DeadlineObservation, RuntimeError> {
    let (observation, target) = runtime
        .store
        .deadlines(lease.run_id(), runtime.clock.now_ms()?, cached)
        .await?;
    let (scope, node, code) = match target {
        None => return Ok(observation),
        Some(DeadlineTarget::Run) => (None, None, FailureCode::Timeout),
        Some(DeadlineTarget::Scope(id)) => (Some(id), None, FailureCode::Timeout),
        Some(DeadlineTarget::Invocation(record)) => {
            let code = node_timeout_code(&runtime, &lease, &record).await?;
            (None, Some(record.key.id), code)
        }
    };
    stop(
        &runtime,
        &lease,
        scope.as_deref(),
        node.as_deref(),
        code,
        false,
        vec![],
    )
    .await?;
    Ok(observation)
}
pub(super) async fn node_timeout_code(
    runtime: &Runtime,
    lease: &RunLease,
    node: &StoredRecord,
) -> Result<FailureCode, RuntimeError> {
    if let Some(Value::Text(id)) = map(&node.payload)?.get("approval_wait") {
        let wait = runtime
            .store
            .record(lease.run_id(), &key(RecordFamily::Wait, id))
            .await?;
        let state = string(&wait.payload, "state")?;
        let waiting = string(&node.payload, "reason")
            .is_ok_and(|r| matches!(r.as_str(), "awaiting_approval" | "approval_resolved"));
        if waiting
            && (state == "open"
                || (state == "accepted" && string(&wait.payload, "decision")? == "deny"))
        {
            return Ok(FailureCode::ApprovalDenied);
        }
    }
    Ok(FailureCode::Timeout)
}
pub(super) async fn stop(
    runtime: &Runtime,
    lease: &RunLease,
    scope_target: Option<&str>,
    node_target: Option<&str>,
    code: FailureCode,
    cancel: bool,
    extra: Vec<crate::store::RecordWrite>,
) -> Result<(), RuntimeError> {
    let header = runtime.store.run_header(lease.run_id()).await?;
    if header.status.is_terminal() {
        return Ok(());
    }
    let scopes = all(runtime, lease, RecordFamily::Scope).await?;
    let nodes = all(runtime, lease, RecordFamily::Invocation).await?;
    let whole = scope_target.is_none() && node_target.is_none();
    let mut affected = BTreeSet::new();
    if whole {
        affected.extend(scopes.iter().map(|r| r.key.id.clone()));
    } else if let Some(scope) = scope_target {
        affected.insert(scope.to_string());
    }
    loop {
        let mut changed = false;
        for scope in &scopes {
            if affected.contains(&scope.key.id) {
                continue;
            }
            if let Some(Value::Text(parent)) = map(&scope.payload)?.get("parent_invocation") {
                let inside = node_target.is_some_and(|n| n == parent)
                    || nodes.iter().any(|node| {
                        node.key.id == *parent
                            && string(&node.payload, "scope_instance_id")
                                .is_ok_and(|id| affected.contains(&id))
                    });
                if inside {
                    affected.insert(scope.key.id.clone());
                    changed = true
                }
            }
        }
        if !changed {
            break;
        }
    }
    let mut writes = extra;
    let mut protection = Vec::new();
    for node in &nodes {
        if node.final_record {
            continue;
        }
        if whole
            || node_target == Some(node.key.id.as_str())
            || affected.contains(&string(&node.payload, "scope_instance_id")?)
        {
            protection.push(labels(&node.payload, &runtime.config.limits)?);
            let direct = node_target == Some(node.key.id.as_str());
            let status = if direct && !cancel {
                "failed"
            } else {
                "cancelled"
            };
            let error = RuntimeFailure::new(if direct { code } else { FailureCode::Cancelled });
            writes.push(update(
                node,
                replace(
                    &node.payload,
                    [
                        ("status", Value::Text(status.into())),
                        (
                            "failure",
                            error.to_value(&runtime.config.limits).map_err(|_| err())?,
                        ),
                        ("reason", Value::Null),
                    ],
                )?,
                true,
            ));
        }
    }
    for scope in &scopes {
        if scope.final_record || !affected.contains(&scope.key.id) {
            continue;
        }
        protection.push(labels(&scope.payload, &runtime.config.limits)?);
    }
    let influence = SecurityLabels::combine(protection.iter(), &runtime.config.limits)?;
    for scope in &scopes {
        if scope.final_record || !affected.contains(&scope.key.id) {
            continue;
        }
        let primary = scope_target == Some(scope.key.id.as_str())
            || (whole && scope.key.id == header.root_scope.to_string());
        let status = if primary && !cancel {
            "failed"
        } else {
            "cancelled"
        };
        let error = RuntimeFailure::new(if primary {
            code
        } else {
            FailureCode::Cancelled
        });
        writes.push(update(
            scope,
            replace(
                &scope.payload,
                [
                    ("status", Value::Text(status.into())),
                    (
                        "failure",
                        error.to_value(&runtime.config.limits).map_err(|_| err())?,
                    ),
                    (
                        "influence_labels",
                        influence.to_value(&runtime.config.limits)?,
                    ),
                ],
            )?,
            true,
        ));
    }
    for wait in all(runtime, lease, RecordFamily::Wait).await? {
        if wait.final_record {
            continue;
        }
        let invocation = string(&wait.payload, "invocation_id")?;
        if whole
            || node_target == Some(invocation.as_str())
            || nodes.iter().any(|n| {
                n.key.id == invocation
                    && string(&n.payload, "scope_instance_id")
                        .is_ok_and(|id| affected.contains(&id))
            })
        {
            writes.push(update(
                &wait,
                replace(
                    &wait.payload,
                    [(
                        "state",
                        Value::Text(
                            if code == FailureCode::ApprovalDenied
                                && string(&wait.payload, "purpose")? == "approval"
                            {
                                "expired"
                            } else {
                                "cancelled"
                            }
                            .into(),
                        ),
                    )],
                )?,
                true,
            ));
        }
    }
    let root_affected = whole || affected.contains(&header.root_scope.to_string());
    let status = if root_affected {
        Some(if cancel {
            RunStatus::Cancelled
        } else {
            RunStatus::Failed
        })
    } else {
        None
    };
    if !writes.is_empty() || status.is_some() {
        let stopped = if whole {
            None
        } else {
            Some(
                writes
                    .iter()
                    .filter(|w| w.key.family == RecordFamily::Invocation && w.final_record)
                    .map(|w| w.key.id.clone())
                    .collect::<BTreeSet<_>>(),
            )
        };
        step::commit(
            runtime,
            lease,
            format!(
                "stop:{}:{}:{}",
                scope_target.unwrap_or("run"),
                node_target.unwrap_or("none"),
                header.sequence
            ),
            writes,
            status,
        )
        .await?;
        runtime.mcp_control.cancel(lease.run_id(), stopped.as_ref());
    }
    Ok(())
}
