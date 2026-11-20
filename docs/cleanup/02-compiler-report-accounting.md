# 02: Consolidate compiler report accounting

**Scope.** Share size calculations across canonical compilation, source lowering
and module inspection without merging distinct budget enforcement points.

**Acceptance.** Preserve diagnostic ordering, stage-specific error messages, and
early ceilings. Cover exact-limit acceptance and one-byte-over rejection with
warnings and retained provenance; confirm successful executable bytes match.

**Verification.** Run compiler and documentation tests, strict compiler Clippy,
formatting and the independent native fixture check.
