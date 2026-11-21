//! Host run admission and immutable initial scope checkpoints.
use crate::influence::{InfluenceContext, InfluenceEnvironment, ReferenceInfluence};
use crate::{
    Authorization, EvaluationFrame, EvaluationValue, HostContext, Runtime, RuntimeError,
    artifact::{ArtifactInput, ArtifactMetadata, ArtifactProducer},
    failure::{FailureBoundary, FailureCode},
    identity::{ArtifactId, host_request_digest},
    records::{InputValue, ManifestEntry, SecurityLabels},
    store::runs::{AdmissionTime, HostRequest},
    validate_typed_value,
};
use crate::{
    artifact::PreparedArtifact,
    failure::RuntimeFailure,
    identity::{RunId, ScopeInstanceId},
    records::InputManifest,
    store::{RecordFamily, RecordKey, RecordWrite, StoreError},
};
use htlk_analyzer::{ExpressionSite, ScopeUse};
use htlk_executable::{
    ExecutionLimits,
    cbor::{Limits, Map, Value},
    digest::Digest,
};
use htlk_executable::{Identifier, ValueReference, record_accounting::RecordAccounting};
use std::{collections::BTreeMap, sync::Arc};

/// Stable logical result of an idempotent start request. Root contract failure is
/// recorded on the created run and does not turn this receipt into a host error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunStarted {
    /// Unique durable host run identity.
    pub run_id: RunId,
}

struct BoundInput {
    name: Identifier,
    value: Option<Arc<Value>>,
    source: Option<Arc<ArtifactMetadata>>,
    boundary_labels: SecurityLabels,
}
impl Runtime {
    /// Validates complete typed host inputs and supported budgets, freezes the root
    /// manifest and atomically creates its input artifacts, root, timers, journal,
    /// outbox and principal/method/target receipt. Valid root contract failures create
    /// failed runs; invalid host values create no run or executable graph work.
    ///
    /// Idempotency retention is advertised by RuntimeConfig. A historical identical
    /// receipt is returned before requiring old native code or old input bytes.
    ///
    /// # Errors
    /// Returns current authorization, conflicting key, invalid ingress, unsupported
    /// enforcement, native admission or atomic storage failure.
    pub async fn start_run(
        &self,
        context: &HostContext,
        fingerprint: Digest,
        inputs: &BTreeMap<Identifier, InputValue>,
        idempotency_key: &str,
        limits: Option<&ExecutionLimits>,
    ) -> Result<RunStarted, RuntimeError> {
        self.start_run_inner(context, fingerprint, inputs, idempotency_key, limits, None)
            .await
    }

    /// Starts a run subject to a caller-frozen, exclusive epoch-millisecond deadline.
    /// The coordinator checks it after acquiring write authority. Both the run and
    /// root scope retain the earlier of this bound and their existing limits.
    /// A retained identical receipt wins even after the deadline; the bound is part
    /// of the original request identity. Reuse of expired indexed evidence is refused.
    ///
    /// # Errors
    /// Returns the same admission errors as [`Self::start_run`], plus an elapsed
    /// deadline or expired receipt. After an uncertain commit, use
    /// [`Self::reconcile_run_start`] to check for positive evidence without admission.
    pub async fn start_run_not_after(
        &self,
        context: &HostContext,
        fingerprint: Digest,
        inputs: &BTreeMap<Identifier, InputValue>,
        idempotency_key: &str,
        limits: Option<&ExecutionLimits>,
        not_after_ms: i64,
    ) -> Result<RunStarted, RuntimeError> {
        self.start_run_inner(
            context,
            fingerprint,
            inputs,
            idempotency_key,
            limits,
            Some(not_after_ms),
        )
        .await
    }

    /// Computes the canonical run admission identity before a host submits work.
    /// `None` identifies the relative API; `Some` identifies the frozen absolute API.
    ///
    /// # Errors
    /// Returns invalid or out-of-bounds arguments.
    pub fn start_run_request_digest(
        &self,
        context: &HostContext,
        fingerprint: Digest,
        inputs: &BTreeMap<Identifier, InputValue>,
        limits: Option<&ExecutionLimits>,
        not_after_ms: Option<i64>,
    ) -> Result<Digest, RuntimeError> {
        if not_after_ms.is_some_and(|deadline| deadline < 0) {
            return Err(RuntimeError::failure(FailureCode::Input));
        }
        let requested = limits.cloned().unwrap_or_default();
        let arguments = request_arguments(inputs, &requested, not_after_ms, &self.config.limits)?;
        host_request_digest(
            context.principal(),
            "start_run",
            &fingerprint.to_string(),
            &arguments,
            &self.config.limits,
        )
        .map_err(|_| RuntimeError::failure(FailureCode::Input))
    }

    /// Checks a retained receipt for the exact original request without admitting work.
    /// `None` means no retained receipt was found, not that admission did not commit.
    /// A known run's initialization journal can still provide positive historical evidence.
    ///
    /// # Errors
    /// Returns current authorization, conflicting identity, or lookup failure.
    pub async fn reconcile_run_start(
        &self,
        context: &HostContext,
        fingerprint: Digest,
        inputs: &BTreeMap<Identifier, InputValue>,
        idempotency_key: &str,
        limits: Option<&ExecutionLimits>,
        not_after_ms: Option<i64>,
    ) -> Result<Option<RunStarted>, RuntimeError> {
        let requested = limits.cloned().unwrap_or_default();
        let request = self.start_request(
            context,
            fingerprint,
            inputs,
            idempotency_key,
            &requested,
            not_after_ms,
        )?;
        Ok(self
            .store
            .host_receipt(&request, self.clock.now_ms()?)
            .await?
            .map(|run_id| RunStarted { run_id }))
    }

    fn start_request(
        &self,
        context: &HostContext,
        fingerprint: Digest,
        inputs: &BTreeMap<Identifier, InputValue>,
        idempotency_key: &str,
        limits: &ExecutionLimits,
        not_after_ms: Option<i64>,
    ) -> Result<HostRequest, RuntimeError> {
        self.policy.authorize(
            context,
            Authorization::StartRun {
                fingerprint,
                limits,
            },
        )?;
        if idempotency_key.is_empty() {
            return Err(RuntimeError::failure(FailureCode::Input));
        }
        let request = HostRequest {
            principal: context.principal().into(),
            method: "start_run".into(),
            target: fingerprint.to_string(),
            key: idempotency_key.into(),
            digest: self.start_run_request_digest(
                context,
                fingerprint,
                inputs,
                Some(limits),
                not_after_ms,
            )?,
        };
        request.check(&self.config.limits)?;
        Ok(request)
    }

    async fn start_run_inner(
        &self,
        context: &HostContext,
        fingerprint: Digest,
        inputs: &BTreeMap<Identifier, InputValue>,
        idempotency_key: &str,
        limits: Option<&ExecutionLimits>,
        not_after_ms: Option<i64>,
    ) -> Result<RunStarted, RuntimeError> {
        let requested = limits.cloned().unwrap_or_default();
        let request = self.start_request(
            context,
            fingerprint,
            inputs,
            idempotency_key,
            &requested,
            not_after_ms,
        )?;
        let now = self.clock.now_ms()?;
        if let Some(run_id) = self.store.host_receipt(&request, now).await? {
            return Ok(RunStarted { run_id });
        }
        let executable = self.admitted(fingerprint).await?;
        let fields = executable.document().fields();
        let root = &fields.scopes[&fields.root_scope];
        if inputs
            .keys()
            .any(|name| root.fields().inputs.get(name.as_str()).is_none())
        {
            return Err(RuntimeError::failure(FailureCode::Input));
        }
        let principal_limits = self.policy.execution_limits(context)?;
        for settings in [
            &requested,
            &self.config.execution_limits,
            &principal_limits,
            &executable.policy().fields().defaults,
        ] {
            reject_unsupported(settings)?;
        }
        for scope in fields.scopes.values() {
            reject_unsupported(&scope.fields().limits)?;
            for node in &scope.fields().nodes {
                reject_unsupported(&node.fields().limits)?;
            }
        }
        let run_limits = min_limits(&[
            &executable.policy().fields().defaults,
            &self.config.execution_limits,
            &principal_limits,
            &requested,
        ])?;
        let scope_limits = min_limits(&[&run_limits, &root.fields().limits])?;
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                run_limits
                    .timeout_ms()
                    .ok_or_else(|| RuntimeError::failure(FailureCode::Profile))?,
            ))
            .is_none()
        {
            return Err(RuntimeError::failure(FailureCode::Profile));
        }
        let mut bound = Vec::new();
        let mut cached: BTreeMap<ArtifactId, (Arc<ArtifactMetadata>, Arc<Value>)> = BTreeMap::new();
        let mut values_budget = RecordAccounting::new(&self.config.limits)?;
        values_budget
            .collection(root.fields().inputs.len(), 0)
            .map_err(|_| RuntimeError::failure(FailureCode::Input))?;
        let mut labels_budget = RecordAccounting::new(&self.config.limits)?;
        for (name, port) in root.fields().inputs.iter() {
            let (value, source) = match inputs.get(name) {
                None if port.required() => return Err(RuntimeError::failure(FailureCode::Input)),
                None => (None, None),
                Some(InputValue::Inline(value)) => {
                    charge_input(&mut values_budget, name, value)?;
                    validate_typed_value(
                        value,
                        port.value_type(),
                        Some(executable.schemas()),
                        &self.config.limits,
                        &executable.policy().fields().evaluator_limits,
                    )
                    .map_err(|e| {
                        RuntimeError::failure(FailureCode::for_evaluation(
                            &e,
                            FailureBoundary::Input,
                        ))
                    })?;
                    (Some(Arc::new(value.clone())), None)
                }
                Some(InputValue::Artifact(id)) => {
                    let (metadata, value) = if let Some(value) = cached.get(id) {
                        value.clone()
                    } else {
                        let accepted = self.read_artifact(context, *id).await?;
                        let pair = (Arc::new(accepted.metadata), Arc::new(accepted.value));
                        cached.insert(*id, pair.clone());
                        pair
                    };
                    charge_input(&mut values_budget, name, &value)?;
                    validate_typed_value(
                        &value,
                        port.value_type(),
                        Some(executable.schemas()),
                        &self.config.limits,
                        &executable.policy().fields().evaluator_limits,
                    )
                    .map_err(|e| {
                        RuntimeError::failure(FailureCode::for_evaluation(
                            &e,
                            FailureBoundary::Input,
                        ))
                    })?;
                    (Some(value), Some(metadata))
                }
            };
            let labels = self
                .policy
                .input_labels(context, fingerprint, name, value.as_deref())?;
            labels_budget
                .value(&labels.to_value(&self.config.limits)?, 0)
                .map_err(|_| RuntimeError::failure(FailureCode::Input))?;
            bound.push(BoundInput {
                name: name.clone(),
                value,
                source,
                boundary_labels: labels,
            });
        }
        let run_id = self.store.reserve_run_id().await?;
        let mut entries = Vec::new();
        let mut artifacts = Vec::new();
        let mut frame = EvaluationFrame::default();
        let preconditions = executable
            .graphs()
            .plan(&ScopeUse::Root)
            .and_then(|plan| plan.environment(&ExpressionSite::Preconditions))
            .ok_or_else(|| RuntimeError::failure(FailureCode::Invariant))?;
        for input in &bound {
            let entry = match (&input.value, &input.source) {
                (None, _) => ManifestEntry::Absent,
                (Some(_), Some(metadata)) => ManifestEntry::Value {
                    artifact_id: metadata.artifact_id(),
                    content_digest: metadata.content_digest(),
                    security_labels: metadata.security_labels().clone(),
                },
                (Some(value), None) => {
                    let ty = root
                        .fields()
                        .inputs
                        .get(input.name.as_str())
                        .expect("validated declared input")
                        .value_type();
                    let prepared = PreparedArtifact::new(
                        &executable,
                        ArtifactInput {
                            producer: ArtifactProducer::RunInput {
                                run_id: run_id.clone(),
                                extension_number: 0,
                                port: input.name.clone(),
                            },
                            value_type: ty,
                            value,
                            input_artifact_ids: vec![],
                            security_labels: input.boundary_labels.clone(),
                            accepted_at: now,
                        },
                        &self.config.limits,
                    )?;
                    let metadata = prepared.metadata();
                    let entry = ManifestEntry::Value {
                        artifact_id: metadata.artifact_id(),
                        content_digest: metadata.content_digest(),
                        security_labels: metadata.security_labels().clone(),
                    };
                    artifacts.push(prepared);
                    entry
                }
            };
            if preconditions
                .references
                .contains_key(&ValueReference::Input(input.name.clone()))
            {
                frame
                    .bind(
                        ValueReference::Input(input.name.clone()),
                        vec![],
                        Ok(input.value.as_ref().map_or(EvaluationValue::Absent, |v| {
                            EvaluationValue::Present(v.as_ref().clone())
                        })),
                        &self.config.limits,
                    )
                    .map_err(|e| {
                        RuntimeError::failure(FailureCode::for_evaluation(
                            &e,
                            FailureBoundary::Input,
                        ))
                    })?;
            }
            entries.push((input.name.clone(), entry));
        }
        let influence = SecurityLabels::combine(
            bound.iter().map(|input| &input.boundary_labels),
            &self.config.limits,
        )?;
        let manifest = InputManifest::freeze(
            &root.fields().inputs,
            entries,
            influence,
            &self.config.limits,
        )?;
        let environment = InfluenceEnvironment {
            base: manifest.influence_labels().clone(),
            references: preconditions
                .references
                .keys()
                .map(|reference| (reference.clone(), ReferenceInfluence::default()))
                .collect(),
            outcomes: BTreeMap::new(),
        };
        let scope_id = ScopeInstanceId::root(&run_id, &self.config.limits)
            .map_err(|_| RuntimeError::failure(FailureCode::Invariant))?;
        let authorize = |labels: &SecurityLabels| {
            self.policy.authorize(
                context,
                Authorization::ReadBoundary {
                    run_id: &run_id,
                    scope_instance_id: scope_id,
                    labels,
                },
            )
        };
        let guarded = InfluenceContext::new(&frame, &environment, &authorize, &self.config.limits)?;
        let evaluation =
            guarded.evaluate(&executable, &ScopeUse::Root, &ExpressionSite::Preconditions)?;
        let failure = match evaluation.result {
            Ok(result) => match result.value {
                EvaluationValue::Present(Value::Bool(true)) => None,
                EvaluationValue::Present(Value::Bool(false)) => {
                    Some(RuntimeFailure::new(FailureCode::Preconditions))
                }
                _ => Some(RuntimeFailure::new(FailureCode::Invariant)),
            },
            Err(error) => Some(RuntimeFailure::new(error.code())),
        };
        let prepared = PreparedRun {
            run_id,
            fingerprint,
            definition: fields.root_scope,
            principal: context.principal().into(),
            inputs: manifest,
            artifacts,
            failure,
            run_limits,
            scope_limits,
            requested_limits: requested,
            audit_retention_ms: self.config.audit_retention_ms as i64,
            limits: self.config.limits.clone(),
        };
        let time = match not_after_ms {
            Some(not_after_ms) => AdmissionTime::Absolute {
                not_after_ms,
                observed: now,
                clock: self.clock.clone(),
            },
            None => AdmissionTime::Relative(self.clock.now_ms()?.max(now)),
        };
        let run_id = self
            .store
            .initialize_run(
                prepared,
                request,
                time,
                self.config.idempotency_retention_ms as i64,
            )
            .await?;
        crate::execution::hint(self);
        Ok(RunStarted { run_id })
    }
}
fn request_arguments(
    inputs: &BTreeMap<Identifier, InputValue>,
    limits: &ExecutionLimits,
    not_after_ms: Option<i64>,
    codec: &Limits,
) -> Result<Value, RuntimeError> {
    let mut accounting = RecordAccounting::new(codec)?;
    accounting
        .collection(inputs.len(), 0)
        .map_err(|_| RuntimeError::failure(FailureCode::Input))?;
    let mut values = Vec::new();
    for (name, input) in inputs {
        accounting
            .text(name.as_str(), 1)
            .map_err(|_| RuntimeError::failure(FailureCode::Input))?;
        let value = input.to_value(codec)?;
        accounting
            .value(&value, 1)
            .map_err(|_| RuntimeError::failure(FailureCode::Input))?;
        values.push((name.to_string(), value));
    }
    let mut arguments = vec![
        ("inputs".into(), Value::Map(Map::try_from_entries(values)?)),
        (
            "limits".into(),
            limits
                .to_value(codec)
                .map_err(|_| RuntimeError::failure(FailureCode::Input))?,
        ),
    ];
    if let Some(deadline) = not_after_ms {
        arguments.push(("not_after_ms".into(), Value::Integer(deadline)));
    }
    Ok(Value::Map(Map::try_from_entries(arguments)?))
}
fn charge_input(
    accounting: &mut RecordAccounting<'_>,
    name: &Identifier,
    value: &Value,
) -> Result<(), RuntimeError> {
    accounting
        .collection(2, 1)
        .and_then(|()| accounting.text(name.as_str(), 2))
        .and_then(|()| accounting.value(value, 2))
        .map_err(|_| RuntimeError::failure(FailureCode::Input))
}
pub(crate) fn reject_unsupported(limits: &ExecutionLimits) -> Result<(), RuntimeError> {
    if limits.max_tokens().is_some() || limits.max_cost_units().is_some() {
        return Err(RuntimeError::failure(FailureCode::BudgetUnsupported));
    }
    Ok(())
}
pub(crate) fn min_limits(layers: &[&ExecutionLimits]) -> Result<ExecutionLimits, RuntimeError> {
    let mut result = ExecutionLimits::new();
    macro_rules! field {
        ($get:ident,$set:ident) => {
            if let Some(value) = layers.iter().filter_map(|l| l.$get()).min() {
                result = result
                    .$set(value)
                    .map_err(|_| RuntimeError::failure(FailureCode::Profile))?;
            }
        };
    }
    field!(timeout_ms, with_timeout_ms);
    field!(attempt_timeout_ms, with_attempt_timeout_ms);
    field!(max_concurrency, with_max_concurrency);
    field!(max_mcp_calls, with_max_mcp_calls);
    field!(max_tokens, with_max_tokens);
    field!(max_cost_units, with_max_cost_units);
    Ok(result)
}

pub(crate) struct PreparedRun {
    pub(crate) audit_retention_ms: i64,
    pub run_id: RunId,
    pub fingerprint: Digest,
    pub definition: Digest,
    pub principal: String,
    pub inputs: InputManifest,
    pub artifacts: Vec<PreparedArtifact>,
    pub failure: Option<RuntimeFailure>,
    pub run_limits: ExecutionLimits,
    pub scope_limits: ExecutionLimits,
    pub requested_limits: ExecutionLimits,
    pub limits: Limits,
}
impl PreparedRun {
    pub(crate) fn root(&self) -> Result<ScopeInstanceId, StoreError> {
        ScopeInstanceId::root(&self.run_id, &self.limits).map_err(|_| StoreError::Invalid)
    }
    pub(crate) fn initial_records(
        &self,
        now: i64,
        not_after_ms: Option<i64>,
    ) -> Result<Vec<RecordWrite>, StoreError> {
        let root = self.root()?;
        let failed = self.failure.is_some();
        let deadline = if failed {
            None
        } else {
            Some(deadline_value(
                now,
                self.scope_limits.timeout_ms(),
                not_after_ms,
            )?)
        };
        let run_deadline = if failed {
            None
        } else {
            Some(deadline_value(
                now,
                self.run_limits.timeout_ms(),
                not_after_ms,
            )?)
        };
        let error = self.failure.as_ref().map_or(Ok(Value::Null), |f| {
            f.to_value(&self.limits).map_err(|_| StoreError::Invalid)
        })?;
        let precondition = match self.failure.as_ref() {
            None => Value::Bool(true),
            Some(f) if f.code() == FailureCode::Preconditions => Value::Bool(false),
            Some(_) => error.clone(),
        };
        let scope = Value::Map(Map::try_from_entries([
            ("record_version".into(), Value::Text("0.1".into())),
            (
                "definition_digest".into(),
                Value::Text(self.definition.to_string()),
            ),
            (
                "origin_executable".into(),
                Value::Text(self.fingerprint.to_string()),
            ),
            (
                "scope_use".into(),
                Value::Array(vec![Value::Text("root".into())]),
            ),
            ("principal".into(), Value::Text(self.principal.clone())),
            (
                "inputs".into(),
                self.inputs
                    .to_value(&self.limits)
                    .map_err(|_| StoreError::Invalid)?,
            ),
            (
                "input_checkpoint".into(),
                Value::Bytes(
                    self.inputs
                        .encode(&self.limits)
                        .map_err(|_| StoreError::Invalid)?,
                ),
            ),
            (
                "influence_labels".into(),
                self.inputs
                    .influence_labels()
                    .to_value(&self.limits)
                    .map_err(|_| StoreError::Invalid)?,
            ),
            (
                "status".into(),
                Value::Text(if failed { "failed" } else { "active" }.into()),
            ),
            ("precondition".into(), precondition),
            ("failure".into(), error),
            (
                "failure_site".into(),
                if failed {
                    Value::Text(format!("/scopes/{}/preconditions", self.definition))
                } else {
                    Value::Null
                },
            ),
            (
                "admitted_at_ms".into(),
                if failed {
                    Value::Null
                } else {
                    Value::Integer(now)
                },
            ),
            (
                "deadline_ms".into(),
                deadline.map_or(Value::Null, Value::Integer),
            ),
            (
                "effective_limits".into(),
                self.scope_limits
                    .to_value(&self.limits)
                    .map_err(|_| StoreError::Invalid)?,
            ),
            (
                "requested_limits".into(),
                self.requested_limits
                    .to_value(&self.limits)
                    .map_err(|_| StoreError::Invalid)?,
            ),
            ("run_context".into(), Value::Text(root.to_string())),
            ("outputs".into(), Value::Null),
        ])?);
        cbor_check(&scope, &self.limits)?;
        let mut records = vec![RecordWrite {
            key: RecordKey {
                family: RecordFamily::Scope,
                id: root.to_string(),
            },
            expected_revision: None,
            final_record: failed,
            payload: scope,
        }];
        for artifact in &self.artifacts {
            let metadata = artifact.metadata_at(now).map_err(|_| StoreError::Invalid)?;
            let payload = metadata
                .to_value(&self.limits)
                .map_err(|_| StoreError::Invalid)?;
            records.push(RecordWrite {
                key: RecordKey {
                    family: RecordFamily::Artifact,
                    id: metadata.artifact_id().to_string(),
                },
                expected_revision: None,
                final_record: true,
                payload,
            });
        }
        let usage = Value::Map(Map::try_from_entries([
            (
                "limits".into(),
                self.run_limits
                    .to_value(&self.limits)
                    .map_err(|_| StoreError::Invalid)?,
            ),
            (
                "deadline_ms".into(),
                run_deadline.map_or(Value::Null, Value::Integer),
            ),
            ("calls_spent".into(), Value::Integer(0)),
            (
                "audit_retention_ms".into(),
                Value::Integer(self.audit_retention_ms),
            ),
            ("tokens_spent".into(), Value::Integer(0)),
            ("cost_spent".into(), Value::Integer(0)),
            ("concurrency_reserved".into(), Value::Integer(0)),
            ("principal".into(), Value::Text(self.principal.clone())),
        ])?);
        records.push(RecordWrite {
            key: RecordKey {
                family: RecordFamily::Usage,
                id: "run".into(),
            },
            expected_revision: None,
            final_record: false,
            payload: usage,
        });
        if let Some(due) = deadline {
            records.push(timer(
                format!("scope:{root}"),
                "scope_deadline",
                due,
                root,
                &self.limits,
            )?);
        }
        if let Some(due) = run_deadline {
            records.push(timer(
                "run".into(),
                "run_deadline",
                due,
                root,
                &self.limits,
            )?);
        }
        Ok(records)
    }
}
fn deadline_value(
    now: i64,
    timeout: Option<u64>,
    not_after_ms: Option<i64>,
) -> Result<i64, StoreError> {
    let timeout = timeout.ok_or(StoreError::Invalid)?;
    let timeout = if let Some(bound) = not_after_ms {
        timeout.min(
            u64::try_from(bound.checked_sub(now).ok_or(StoreError::Invalid)?)
                .map_err(|_| StoreError::Invalid)?,
        )
    } else {
        timeout
    };
    let timeout = i64::try_from(timeout).map_err(|_| StoreError::Invalid)?;
    if timeout <= 0 {
        return Err(StoreError::Invalid);
    }
    now.checked_add(timeout).ok_or(StoreError::Invalid)
}
fn timer(
    id: String,
    kind: &str,
    due: i64,
    scope: ScopeInstanceId,
    limits: &Limits,
) -> Result<RecordWrite, StoreError> {
    let payload = Value::Map(Map::try_from_entries([
        ("kind".into(), Value::Text(kind.into())),
        ("scope_instance_id".into(), Value::Text(scope.to_string())),
        ("due_at_ms".into(), Value::Integer(due)),
        ("generation".into(), Value::Integer(0)),
        ("state".into(), Value::Text("open".into())),
    ])?);
    cbor_check(&payload, limits)?;
    Ok(RecordWrite {
        key: RecordKey {
            family: RecordFamily::Timer,
            id,
        },
        expected_revision: None,
        final_record: false,
        payload,
    })
}
fn cbor_check(value: &Value, limits: &Limits) -> Result<(), StoreError> {
    RecordAccounting::new(limits)?
        .value(value, 0)
        .map_err(|_| StoreError::Invalid)
}
