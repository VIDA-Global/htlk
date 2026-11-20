# 01: Consolidate compiler diagnostic location mapping

**Scope.** Share one private operation for mapping a diagnostic's primary and
related canonical locations back to authored spans. Leave analysis, diagnostic
sorting and deduplication, and report budgets at their existing boundaries.

**Acceptance.** Module inspection and bundle compilation agree on the exact
warning and both source spans, including module identity. Canonical readmission
keeps canonical locations and returns the same executable bytes and fingerprint.

**Verification.** Run the compiler and facade compiler suites, strict compiler
Clippy, formatting, and independent fixture validation. Compare repeated
release-mode bundle compilation and inspection on the same input before and after.

## Results

One in-place operation now remaps the primary and related locations at the four
existing call sites. It preserves the longest matching record path and no longer
rebuilds the related-location vector or reclones an unchanged location. The
regression asserts exact authored UTF-8 byte ranges and module identity for both
edges, canonical pointers on readmission, and exact executable equality.

- The strengthened regression passed before and after the refactor.
- `cargo test -p htlk-compiler --locked`: 59 integration tests and 3 doctests passed.
- `cargo test -p htlk --test compiler --locked`: 5 passed.
- `cargo clippy -p htlk-compiler --all-targets --locked -- -D warnings` and
  `cargo fmt --all -- --check`: passed. An initial lint flagged cloned test
  expectations; the test now compares borrowed slices.
- `node tools/validate-executable-fixtures.mjs`: passed with unchanged identity.

### Release measurement

On arm64 macOS with Rust 1.98.0, a temporary release test used the same bundle,
catalogs, and requested export as the regression. Each of seven alternating
before/after runs warmed up both calls 20 times, then timed 3,000 calls each to
`compile_bundle` and `inspect_module`, consuming the results with `black_box`.
Numbers are per-call means, summarized by their median and min–max across runs.

| Operation | Before (µs) | After (µs) |
| --- | ---: | ---: |
| Compile bundle | 528.0 (523.6–536.6) | 534.8 (524.7–541.7) |
| Inspect selected export | 141.0 (140.2–146.1) | 141.9 (140.4–143.5) |

The ranges overlap; the timings do not establish a material change. All runs
produced fingerprint `sha256:a2fa532c9711426bd89cb1f045bffded7e1527d8596065fef4a429cff4c24d99`.
