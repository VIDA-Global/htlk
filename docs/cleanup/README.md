# Iterative cleanup: scope and verification

This record follows the work from `214bb5d` in dependency order. The compiler
changes preserve public APIs, diagnostic wording and order, and executable bytes.
Runtime work is confined to tests so the exact native implementation identity and
storage behavior do not change.

| Order | Chunk | Depends on |
| --- | --- | --- |
| 00 | [Baseline](00-baseline.md) | — |
| 01 | [Validation matrix](01-validation-matrix.md) | 00 |
| 02 | [Compiler report accounting](02-compiler-report-accounting.md) | 01 |
| 03 | [Compiler catalog selection](03-compiler-catalog-selection.md) | 02 |
| 04 | [Runtime test boundaries](04-runtime-test-boundaries.md) | 03 |
| 05 | [Final verification](05-final-verification.md) | 01–04 |
