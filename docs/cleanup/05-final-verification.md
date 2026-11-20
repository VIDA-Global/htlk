# 05: Verify the finished branch

**Scope and acceptance.** Run the complete applicable matrix and document any
remaining issues with their actual check outcomes. Review the final diff for API,
behavior and fixture changes.

**Verification.** Repeat formatting, workspace check and strict Clippy, SQLite
and PostgreSQL tests, warnings-as-errors rustdoc, dependency policy and notices,
verified fresh workspace packaging, Node checks and workflow lint. Report hosted
coverage status separately from ordinary test results.

## Local results

Rust 1.98.0 and Node 24.22.3 on arm64 macOS; disposable PostgreSQL 16.15 and 18.6
services on loopback. The local Docker engine also ran the separate server crash
and synchronous replication tests where enabled.

| Check | Result |
| --- | --- |
| `bash tools/check-rust.sh` | Passed: formatting, locked workspace check and `-D warnings` Clippy across all four feature cases. |
| `cargo test --workspace --locked --no-fail-fast` | 861 passed with default SQLite, including doctests. |
| `cargo test --workspace --all-features --locked --no-fail-fast` (SQLite selected) | 879 passed, including PostgreSQL-specific store and Docker campaigns plus doctests. |
| Same all-feature command with `HTLK_TEST_BACKEND=postgres`, PostgreSQL 16 | 879 passed. |
| Same all-feature command with `HTLK_TEST_BACKEND=postgres`, PostgreSQL 18 | 878 passed; one native stdio classification assertion failed and passed on focused rerun. |
| `cargo test -p htlk-runtime --no-default-features --features postgres --lib --example mcp_client --locked --no-fail-fast` | 10 passed on each PostgreSQL version. |
| `cargo test --workspace --all-features --examples --locked` | 12 passed with the disposable PostgreSQL URL set. |
| `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked` | All five crates built. |
| `cargo deny check` and pinned `cargo about generate` with notice diff | Audit and license policy passed, with the same ten allowed duplicate-version warnings; notices matched. |
| `cargo package --workspace --all-features --locked --target-dir <fresh directory>` | All five crates packaged and verified. |
| Node documentation, specification and independent fixture checks; Node helper tests; `actionlint` | Passed, including all 20 helper tests. |

The final diff leaves public signatures, dependency pins, native implementation
inputs, and golden artifacts unchanged. See the [catalog benchmark](03-compiler-catalog-selection.md#release-benchmark)
for the before/after timings. Hosted PR runs provide the line and branch coverage
reports; local ordinary tests do not claim those measurements.

### PostgreSQL 18 rerun

In the concurrent local campaigns, `deep_e2e` reported `McpTransport` instead
of `McpDescriptor` for `stdio/missing`. The same PostgreSQL 18 service passed
the complete classification test when rerun by itself with
`cargo test -p htlk --all-features --locked --test deep_e2e native_protocol_failures_have_durable_classifications_and_never_publish_outputs -- --exact`.
The full first-run result remains recorded above; the test's expectation and
runtime behavior were not changed.
