# 04: Simplify runtime test boundaries

**Scope.** Simplify the test-only backend helper and remove misleading SQLite
names from backend-neutral tests. Add a focused guard-selection regression test.

**Acceptance.** Preserve database selection and schema identity across independent
opens. Verify that guard callbacks run canonically and that a later guard exceeding
the metadata budget prevents all source access.

**Verification.** Run focused binding, reopen/replay, native identity and fixture
checks, including shared runtime scenarios on both backends.
