# 00: Establish the baseline

**Scope and acceptance.** Record the crate boundaries and CI gates. Run the
required local checks on clean sources and inspect hosted results for the same
revision. Record failures separately from environmental limitations.

**Verification.** Run the shared Rust feature matrix, the default-feature
workspace tests, strict all-feature rustdoc, dependency policy and license-notice
comparison, Node validators and helper tests, and workflow lint. Review the
all-feature, PostgreSQL, packaging, and instrumented hosted jobs.

## Architecture and CI

The executable crate owns the canonical model and codecs. The analyzer owns
shared linkage, graph, type, and schema checks. The compiler lowers source and
bundles and handles composition and provenance. The runtime independently admits
executables and runs them against native services and fenced durable stores. The
facade reexports the compiler/runtime and runs the cross-crate qualification tests.
Only the runtime, executable, and analyzer contribute Rust source to the native
implementation identity; see [fixture maintenance](../../testing.md#native-identity-and-fixtures).

PR and release jobs share compile/Clippy checks for SQLite-only, PostgreSQL-only,
both backends, and backend-free library embedding. They run all-feature tests,
but they do not execute the SQLite-only library and example configuration. The
PostgreSQL workflow already executes a focused PostgreSQL-only gate.

## Results at `a1b34ff`

Local arm64 macOS, Rust 1.98.0 and Node 24.22.3, with CI's line-table debug profiles.

- `bash tools/check-rust.sh`: passed the format, workspace check, and strict Clippy matrix.
- `cargo test --workspace --locked --no-fail-fast`: passed, including doctests.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo deny check`: passed with ten policy-allowed duplicate-version warnings;
  notices generated with `cargo-about` 0.9.2 matched `THIRD_PARTY_LICENSES`.
- Documentation, specification, and executable fixture validators, 20 Node helper
  tests, and `actionlint`: passed.
- [Hosted baseline](https://github.com/thomasquintana/htlk/actions/runs/44737555259):
  Quality (including all-feature tests and packaging), workflow syntax, dependency
  policy, branch coverage, and PostgreSQL 16 and 18 passed. The line-coverage job
  failed in `cancelling_one_session_keeps_other_runs_live_and_shutdown_is_acknowledged`:
  the peer logged one of the two expected session closures before the deadline.
  This differs from the earlier stdio discovery classification failure. The local
  default run passed that test; no cause was established. The local all-feature
  replay was stopped after the default pass in favor of the same-revision hosted
  baseline; it did not produce a result.
