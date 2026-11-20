# 04: Simplify runtime test boundaries

**Scope.** Simplify the test-only backend helper and remove misleading SQLite
names from backend-neutral tests. Add a focused guard-selection regression test.

**Acceptance.** Preserve database selection and schema identity across independent
opens. Verify that guard callbacks run canonically and that a later guard exceeding
the metadata budget prevents all source access.

**Verification.** Run focused binding, reopen/replay, native identity and fixture
checks, including shared runtime scenarios on both backends.

## Results

The test store shares PostgreSQL connection setup and worker handling for
independent inspection and cleanup. It derives the same schema from the same
path. The shared E2E fixture and content-library test use `TestStore` directly.
The latter now runs against the selected backend and shares bounded snapshot
polling with the E2E fixture: only `Fenced` is retried, and all result and access
assertions are retained. This addresses the polling failure seen on baseline CI.

The guard test supplies edges out of order, observes both callbacks in canonical
order, and checks the typed aggregate byte-limit error with a source callback
that must never run. Binding, identity, admission, interaction store, recovery
and content-library tests passed on SQLite and PostgreSQL 16. Historical
inspection lifecycle also passed on PostgreSQL 16. Formatting, all four strict
Rust check cases, and independent native fixture validation passed.
