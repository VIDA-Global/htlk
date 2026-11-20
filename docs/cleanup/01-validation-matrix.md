# 01: Align the validation matrix

**Scope.** Share the statically checked workspace feature matrix between local
development, PR CI, and release validation. Keep the existing service-backed
and instrumented campaigns.

**Acceptance.** Exercise both standalone backends and the all-feature workspace
with all applicable targets, and pure embedding with library-only checks. Apply
strict Clippy to each compilation case. Fix any new lint at its source.

**Verification.** Run the shared matrix, `actionlint`, and the documentation
validator. Record results before starting compiler work.
