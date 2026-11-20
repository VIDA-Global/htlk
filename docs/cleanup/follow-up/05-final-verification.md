# 05: Verify the completed cleanup

**Scope and acceptance.** Run the workspace verification suite and review the
committed diff for changes to public APIs, behavior, dependencies, or native
executable identity. Record failures and limitations with the observed evidence.

**Verification.** Run formatting, workspace check and strict Clippy across the
backend matrix, ordinary workspace tests and doctests with default and all
features, supported example checks, both PostgreSQL campaigns and the PostgreSQL-only
embedding and host example, strict rustdoc, dependency policy and notices, fresh
workspace packaging, Node validation and tests, and workflow lint. Report hosted
coverage status separately.

## Local results

arm64 macOS, Rust 1.98.0, Node 24.22.3, and disposable PostgreSQL 16.15 and 18.6
services. The all-feature workspace runs included the Docker-backed crash and
synchronous-replication tests. Workspace counts include doctests.

| Check | Result |
| --- | --- |
| `bash tools/check-rust.sh` | Passed formatting, locked workspace compilation, and Clippy with warnings denied across SQLite-only, PostgreSQL-only, both, and backend-free library embedding. |
| `cargo test --workspace --locked --no-fail-fast` | 864 passed with default SQLite. |
| `cargo test --workspace --all-features --locked --no-fail-fast` | 882 passed with SQLite selected, and 882 passed with each of PostgreSQL 16 and 18 selected. |
| `cargo test --workspace --no-default-features --features sqlite --lib --examples --locked` | 85 passed. |
| `cargo test -p htlk-runtime --no-default-features --features postgres --lib --example mcp_client --locked` | 10 passed on each PostgreSQL version. |
| `cargo test --workspace --all-features --examples --locked` | 14 passed after the [example log isolation fix](04-example-log-isolation.md). |
| `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked` | All five crates built. |
| `cargo deny check` and pinned `cargo about generate` with notice comparison | Audit and license policy passed, with the same ten policy-allowed duplicate-version warnings. Notices matched. |
| `cargo package --workspace --all-features --locked --target-dir <fresh directory>` | All five crates packaged and verified after the example fix. |
| Documentation, specification, and independent executable fixture validators; Node helper tests; `actionlint` | Passed, including all 20 Node helper tests. |

The complete workspace runs were on `6762f8c`. The separate all-feature examples
run exposed a temporary-directory collision. After the test-only fix in `90a2a7d`,
the all-feature examples, both single-backend gates, strict Rust matrix, validators,
and fresh packaging were rerun successfully.

The diff preserves public APIs, emitted diagnostic text, CLI output, dependency
pins, and the native profile and golden artifacts. The compiler
[benchmark](01-compiler-diagnostic-locations.md#release-measurement) found no
clear timing change on the warning-producing bundle.

## Hosted status

The [baseline line-coverage failure](00-baseline.md#results-at-a1b34ff) in HTTP
session cleanup did not reproduce in the local ordinary suites. Its cause is
unresolved. Hosted line and branch coverage have not run on these local commits.
