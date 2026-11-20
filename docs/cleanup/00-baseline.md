# 00: Establish the baseline

**Scope and acceptance.** Record the workspace boundaries, existing CI gates, and
checks on clean `origin/main` (`214bb5d`). Distinguish toolchain or service issues
from project failures before editing code.

The executable crate owns the canonical model and codecs. The analyzer provides
shared linkage, type, graph and schema analysis; the compiler adds source, bundle,
catalog and composition frontends. Runtime independently admits executables and
owns Actix execution, MCP transport and fenced durable storage. The facade
reexports compiler and runtime and hosts the cross-crate campaigns. Implementation
hashes include executable, analyzer and runtime sources; see [fixture maintenance](../testing.md#native-identity-and-fixtures).

PR CI already gates workflow syntax, Node validation, formatting, all-feature
Clippy, backend-specific compilation, all-feature tests and rustdoc, package
verification and dependency policy. Reusable jobs run line and branch coverage
and complete PostgreSQL 16/18 campaigns, also called by release validation.
Release lacks the independent backend build checks; neither workflow has strict
Clippy on standalone backend builds.

## Commands and results

Local arm64 macOS; Rust/Cargo 1.98.0, Node 26.13.1 (CI uses Node 24),
Docker Engine 29.7.2, actionlint 1.7.12, cargo-deny 0.20.2 and cargo-about 0.9.2.

- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed for default features,
  `--all-features`, and `--no-default-features --features sqlite` / `postgres`.
  `cargo check --workspace --no-default-features --lib --locked`: passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo deny check`: passed; reports ten allowed duplicate-version warnings in
  the resolved graph. Pinned `cargo about generate` matched `THIRD_PARTY_LICENSES`.
- Documentation, specification and independent fixture validators, all 20 Node
  helper tests and `actionlint`: passed.
- `cargo package --workspace --all-features --locked --target-dir <fresh directory>`:
  all five crates packaged and verified.
- `cargo test --workspace --locked` on SQLite: 858 passed, including doctests.
- [Hosted CI at the baseline revision](https://github.com/thomasquintana/htlk/actions/runs/44669389803):
  Quality (including all-feature tests), line and branch coverage, dependency
  policy, workflow syntax and PostgreSQL 16 qualification passed. The PostgreSQL
  18 job hit a transient `Fenced` result
  while polling `inspect_run` in `mcp_content_library`. The polling test unwraps
  a snapshot result that the shared E2E poller already treats as retryable.
  Check this test harness boundary in chunk 04; repeat both PostgreSQL versions
  on the final tree.
