//! Authorized adoption of an already saved graph response, without provider I/O.
use super::{mcp, model::*, step::ScopeState};
use crate::{
    HostContext, Runtime, RuntimeError,
    failure::FailureCode,
    identity::{self, AttemptId, InvocationId, ScopeInstanceId},
    mcp::{
        McpDelivery, McpGraphAttempt, McpGraphRecovery, McpSavedGraphResponse,
        client::McpOperationOrigin,
    },
    records::SecurityLabels,
    store::{
        CommitReceipt, GraphResponseCommit, RecordFamily, RunHeader, RunLease, StoreError,
        StoredRecord, Transition, TransitionContext,
    },
};
use htlk_executable::{
    JsonDocument, McpBindingKind, Operation,
    cbor::{self, Limits, Value},
    digest::{Digest, hash_bytes},
};
use std::collections::{BTreeMap, BTreeSet};

fn mismatch() -> RuntimeError {
    RuntimeError::failure(FailureCode::Digest)
}

pub(super) fn attempt_reference(
    origin: McpOperationOrigin,
    executable: Digest,
    binding: Digest,
    dispatch_fence: i64,
    deadline_ms: i64,
    attempt: &Value,
    limits: &Limits,
) -> Result<McpGraphAttempt, RuntimeError> {
    let (namespace, _) = origin
        .run_id
        .as_str()
        .strip_prefix("run:")
        .and_then(|id| id.split_once(':'))
        .ok_or_else(mismatch)?;
    let request_digest = identity::hash(
        "graph_mcp_request",
        &[
            field(attempt, "method")?,
            field(attempt, "encoded_request")?,
            field(attempt, "protocol_key")?,
        ],
        limits,
    )
    .map_err(|_| mismatch())?;
    Ok(McpGraphAttempt {
        namespace: namespace.into(),
        origin,
        executable,
        binding,
        request_digest,
        dispatch_fence,
        deadline_ms,
    })
}

impl Runtime {
    /// Submits the exact correlated outcome saved by a trusted host before a
    /// previous graph owner was lost. Acquire a higher [`RunLease`] via
    /// [`crate::store::Store::acquire`] before attaching the run to the scheduler.
    /// Use a short lease so the engine can take over promptly when it expires.
    /// This only persists the response; normal graph processing validates and
    /// publishes outputs. It cannot send a request or create another attempt.
    ///
    /// Exact retries under current authority return the original commit receipt.
    /// A first late submission is rejected without writing to the run or moving
    /// evidence out of host custody. Keep the saved evidence for reconciliation.
    ///
    /// # Errors
    /// Returns denied authority, stale fence, mismatched identity, malformed
    /// response, conflicting evidence, an expired/ineligible attempt, or a store
    /// error. On [`StoreError::CommitUnknown`], reopen the store and resubmit the
    /// same saved response under a current successor lease; never call the provider.
    pub async fn recover_graph_mcp_response(
        &self,
        context: &HostContext,
        lease: &RunLease,
        saved: McpSavedGraphResponse,
    ) -> Result<CommitReceipt, RuntimeError> {
        let McpSavedGraphResponse {
            attempt: claimed,
            outcome,
            protocol_error,
        } = saved;
        let origin = &claimed.origin;
        let run = lease.run_id();
        let limits = &self.config.limits;
        if &origin.run_id != run
            || self.store.inspection_position().await?.namespace() != claimed.namespace
        {
            return Err(mismatch());
        }
        let header = self.store.run_header(run).await?;
        if header.fence != lease.fence() || header.owner.as_deref() != Some(lease.owner()) {
            return Err(StoreError::Fenced.into());
        }
        let invocation = self
            .store
            .record(run, &key(RecordFamily::Invocation, origin.invocation_id))
            .await?;
        let attempt = self
            .store
            .record(run, &key(RecordFamily::Attempt, origin.attempt_id))
            .await?;
        let scope = self
            .store
            .record(
                run,
                &key(
                    RecordFamily::Scope,
                    string(&invocation.payload, "scope_instance_id")?,
                ),
            )
            .await?;
        let scope_id: ScopeInstanceId = scope.key.id.parse().map_err(|_| mismatch())?;
        let executable = self
            .admitted(digest(&scope.payload, "origin_executable")?)
            .await?;
        let scope_definition = executable
            .document()
            .fields()
            .scopes
            .get(&digest(&scope.payload, "definition_digest")?)
            .ok_or_else(mismatch)?;
        let local_id = string(&invocation.payload, "local_id")?;
        let node = scope_definition
            .fields()
            .nodes
            .iter()
            .find(|node| node.id().as_str() == local_id)
            .ok_or_else(mismatch)?;
        let Operation::Mcp {
            binding: binding_id,
            ..
        } = &node.fields().operation
        else {
            return Err(mismatch());
        };
        let binding = executable
            .document()
            .fields()
            .bindings
            .get(binding_id)
            .ok_or_else(mismatch)?;
        let dispatch_fence = number(&attempt.payload, "owner_fence")?;
        let deadline_ms = number(&attempt.payload, "dispatch_deadline_ms")?;
        if InvocationId::new(scope_id, node.id(), limits).map_err(|_| mismatch())?
            != origin.invocation_id
            || digest(&invocation.payload, "origin_executable")? != executable.fingerprint()
            || digest(&invocation.payload, "definition_digest")?
                != node.digest(limits).map_err(|_| mismatch())?
            || string(&attempt.payload, "invocation_id")? != origin.invocation_id.to_string()
            || AttemptId::new(
                origin.invocation_id,
                u64::try_from(number(&attempt.payload, "number")?).map_err(|_| mismatch())?,
                limits,
            )
            .map_err(|_| mismatch())?
                != origin.attempt_id
            || digest(&attempt.payload, "server_digest")?
                != binding.server().digest(limits).map_err(|_| mismatch())?
            || digest(&attempt.payload, "input_hash")? != digest(&invocation.payload, "input_hash")?
            || claimed
                != attempt_reference(
                    origin.clone(),
                    executable.fingerprint(),
                    *binding_id,
                    dispatch_fence,
                    deadline_ms,
                    &attempt.payload,
                    limits,
                )?
        {
            return Err(mismatch());
        }
        if dispatch_fence <= 0 || lease.fence() <= dispatch_fence {
            return Err(StoreError::Fenced.into());
        }
        let principal = string(&scope.payload, "principal")?;
        let execution_context = HostContext::new(principal.clone(), limits)?;
        let params = field(&attempt.payload, "params")?;
        let Value::Bytes(encoded) = field(&attempt.payload, "encoded_request")? else {
            return Err(mismatch());
        };
        if JsonDocument::from_value(params, limits)
            .map_err(|_| mismatch())?
            .as_bytes()
            != encoded
        {
            return Err(mismatch());
        }
        let client_labels = self
            .store
            .run_client_metadata(run, None, Some(origin.attempt_id))
            .await?
            .1;
        let policy = self
            .policy
            .mcp_policy_for_request(&execution_context, binding, params)?;
        mcp::require_result_policy(binding, &policy)?;
        let mut protection = labels(&attempt.payload, limits)?
            .union(&labels(&invocation.payload, limits)?, limits)?
            .union(&labels(&scope.payload, limits)?, limits)?
            .union(
                &SecurityLabels::from_value(field(&attempt.payload, "result_labels")?, limits)?,
                limits,
            )?
            .union(&client_labels, limits)?
            .union(&policy.result_labels, limits)?;
        if attempt.final_record {
            let response = self
                .store
                .record(run, &key(RecordFamily::Response, origin.attempt_id))
                .await?;
            protection = protection.union(&labels(&response.payload, limits)?, limits)?;
        }
        self.policy.authorize_graph_mcp_recovery(
            context,
            McpGraphRecovery {
                attempt: &claimed,
                original_principal: &principal,
                binding,
                labels: &protection,
            },
        )?;
        let malformed = || RuntimeError::failure(FailureCode::McpProtocol);
        if outcome.delivery != McpDelivery::ResponseReceived
            || outcome
                .result
                .as_ref()
                .is_ok_and(|raw| raw.len() > mcp::response_limit(self))
            || protocol_error.as_ref().is_some_and(|error| {
                outcome.result.is_ok()
                    || error.as_bytes().len() > mcp::response_limit(self)
                    || JsonDocument::new(error.as_bytes(), limits).is_err()
            })
        {
            return Err(malformed());
        }
        let response = mcp::response_payload(self, outcome, protocol_error, false)?;
        let response_digest = hash_bytes(&cbor::encode(&response, limits)?);
        let first = if attempt.final_record || header.status.is_terminal() {
            None
        } else {
            if string(&attempt.payload, "dispatch_state")? != "possibly_sent"
                || map(&invocation.payload)?.get("attempt_id")
                    != Some(&Value::Text(origin.attempt_id.to_string()))
                || field(&attempt.payload, "protocol_key")?
                    != field(&invocation.payload, "dispatch_protocol_key")?
                || field(&attempt.payload, "deduplication")?
                    != field(&invocation.payload, "dispatch_deduplication")?
            {
                return Err(StoreError::Conflict.into());
            }
            let reservation = self
                .store
                .record(run, &key(RecordFamily::Reservation, origin.attempt_id))
                .await?;
            if reservation.final_record
                || string(&reservation.payload, "state")? != "outstanding"
                || string(&reservation.payload, "invocation_id")?
                    != origin.invocation_id.to_string()
            {
                return Err(StoreError::Conflict.into());
            }
            let deadline = deadline_ms
                .min(active_deadline(self, &header, invocation.clone(), scope.clone()).await?);
            let scope = ScopeState {
                site: use_site(&scope.payload)?,
                record: scope,
                definition: scope_definition.clone(),
                executable: executable.clone(),
                id: scope_id,
                context: execution_context,
                nodes: BTreeMap::new(),
            };
            let (method, expected_params, uri) =
                mcp::request(self, &scope, binding, &invocation).await?;
            if method != string(&attempt.payload, "method")?
                || &expected_params != params
                || uri.map_or(Value::Null, Value::Text)
                    != *field(&attempt.payload, "requested_uri")?
            {
                return Err(mismatch());
            }
            if let Some(Value::Bytes(raw)) = map(&response)?.get("raw") {
                if matches!(binding.kind(), McpBindingKind::Tool { .. }) {
                    let document = JsonDocument::new(raw, limits).map_err(|_| malformed())?;
                    crate::validate_mcp_tool_result(&document, limits).map_err(|_| malformed())?;
                } else {
                    mcp::normalize(self, &scope, binding, raw, &attempt.payload).await?;
                }
            }
            let response = replace(
                &response,
                [
                    ("influence_labels", protection.to_value(limits)?),
                    (
                        "host_recovery_digest",
                        Value::Text(response_digest.to_string()),
                    ),
                ],
            )?;
            Some((
                deadline,
                Transition {
                    context: TransitionContext {
                        command_id: format!("recover-response:{}", origin.attempt_id),
                        actor: context.principal().into(),
                        causation: "recover_graph_mcp_response".into(),
                        timestamp_ms: 0, // Sampled under the transaction's write authority.
                    },
                    writes: mcp::response_writes(&attempt, response, true)?,
                    next_status: None,
                    root: None,
                    expected_fingerprint: Some(header.fingerprint),
                },
            ))
        };
        Ok(self
            .store
            .commit_graph_response(
                lease,
                GraphResponseCommit {
                    namespace: claimed.namespace,
                    attempt: origin.attempt_id,
                    response_digest,
                    sequence: header.sequence,
                    first,
                },
                self.clock.clone(),
            )
            .await?)
    }
}

async fn active_deadline(
    runtime: &Runtime,
    header: &RunHeader,
    mut invocation: StoredRecord,
    mut scope: StoredRecord,
) -> Result<i64, RuntimeError> {
    let run = &header.run_id;
    let usage = runtime
        .store
        .record(run, &key(RecordFamily::Usage, "run"))
        .await?;
    let mut deadline = number(&usage.payload, "deadline_ms")?;
    let mut seen = BTreeSet::new();
    loop {
        if invocation.final_record
            || scope.final_record
            || string(&invocation.payload, "status")? != "active"
            || string(&scope.payload, "status")? != "active"
        {
            return Err(StoreError::Terminal.into());
        }
        if seen.len() >= 1024 || !seen.insert(scope.key.id.clone()) {
            return Err(mismatch());
        }
        deadline = deadline
            .min(number(&invocation.payload, "deadline_ms")?)
            .min(number(&scope.payload, "deadline_ms")?);
        match map(&scope.payload)?.get("parent_invocation") {
            Some(Value::Text(parent)) => {
                invocation = runtime
                    .store
                    .record(run, &key(RecordFamily::Invocation, parent))
                    .await?;
                scope = runtime
                    .store
                    .record(
                        run,
                        &key(
                            RecordFamily::Scope,
                            string(&invocation.payload, "scope_instance_id")?,
                        ),
                    )
                    .await?;
            }
            None | Some(Value::Null) if scope.key.id == header.root_scope.to_string() => {
                return Ok(deadline);
            }
            _ => return Err(mismatch()),
        }
    }
}
