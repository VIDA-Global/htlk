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
