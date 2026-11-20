# 04: Isolate example test logs

**Scope.** Fix an `AlreadyExists` panic at `Logs::new` during the final all-feature
example run. SQLite's startup-budget test and PostgreSQL's catalog-browsing test
can create log directories in parallel in the same process and read the same clock
tick. Both used just the process ID and timestamp as the name.

**Acceptance.** Each log owner gets a unique directory even within the same clock
tick. Keep the existing scoped cleanup and the example behavior and CLI output.

**Verification.** Run the all-feature workspace examples, repeat the affected
example with both tests enabled and normal parallel execution, and run the
SQLite-only and PostgreSQL-only gates, formatting, and strict feature checks.

## Results

A process-local atomic sequence distinguishes log allocations independently of
clock precision. The timestamp still separates allocations across process
lifetimes; each owner still removes its own directory on drop.

- `cargo test --workspace --all-features --examples --locked`: 14 passed.
- `cargo test -p htlk-runtime --all-features --example mcp_client --locked`:
  all 5 passed on repeat with the usual parallel test runner.
- SQLite-only workspace libraries and examples: 85 passed. PostgreSQL-only
  runtime library and `mcp_client` example: 10 passed on PostgreSQL 18.6.
- The strict Rust feature matrix and the documentation, specification, and
  independent fixture validators passed.
