# 03: Run SQLite-only tests in CI

**Scope.** Close the gap between strict compilation with only SQLite and ordinary
CI execution with both backends. Exercise workspace libraries and host examples
with only the SQLite feature in the PR and release validation jobs.

**Acceptance.** Both workflows run the same locked SQLite-only test command.
Existing all-feature, PostgreSQL, and instrumented campaigns still run.

**Verification.** Run the exact new CI command, `actionlint`, and the documentation
and specification validators.
