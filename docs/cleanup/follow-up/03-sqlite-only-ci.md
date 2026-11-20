# 03: Run SQLite-only tests in CI

**Scope.** Close the gap between strict compilation with only SQLite and ordinary
CI execution with both backends. Exercise workspace libraries and host examples
with only the SQLite feature in the PR and release validation jobs.

**Acceptance.** Both workflows run the same locked SQLite-only test command.
Existing all-feature, PostgreSQL, and instrumented campaigns still run.

**Verification.** Run the exact new CI command, `actionlint`, and the documentation
and specification validators.

## Results

PR quality and release validation now run the same focused SQLite-only command
alongside the existing all-feature tests. The contributor checklist and testing
guide describe the gate.

- `cargo test --workspace --no-default-features --features sqlite --lib --examples --locked`:
  85 passed (72 library tests and 13 example tests).
- `actionlint`, `node tools/validate-docs.mjs`, and `node specs/validate-specs.mjs`:
  passed.
