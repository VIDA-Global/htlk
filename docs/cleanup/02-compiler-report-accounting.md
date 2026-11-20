# 02: Consolidate compiler report accounting

**Scope.** Share size calculations across canonical compilation, source lowering
and module inspection without merging distinct budget enforcement points.

**Acceptance.** Preserve diagnostic ordering, stage-specific error messages, and
early ceilings. Cover exact-limit acceptance and one-byte-over rejection with
warnings and retained provenance; confirm successful executable bytes match.

**Verification.** Run compiler and documentation tests, strict compiler Clippy,
formatting and the independent native fixture check.

## Results

Canonical compilation, source lowering, and inspection now share diagnostic and
provenance size calculations. Lowering still enforces its incremental budget and
the final authored report has its own check. The new public-API test covers the
exact final limit and one byte below it, including warning locations, authored
descriptions, and executable-byte equality at the limit.

- `cargo test -p htlk-compiler --locked`: 58 integration tests and 3 doctests passed.
- `cargo test -p htlk --locked --test compiler --test documentation`: 8 passed.
- Strict Clippy across compiler targets, `cargo fmt --all -- --check`, and the
  independent executable fixture validator passed. The fixture fingerprint is unchanged.
