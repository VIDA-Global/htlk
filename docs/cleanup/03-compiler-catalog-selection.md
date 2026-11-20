# 03: Simplify compiler catalog selection

**Scope.** Use one private typed lookup for the four operation kinds. Share it
across ingestion, lowering, and canonical revalidation. Remove redundant key
construction if measurements support it.

**Acceptance.** Preserve per-catalog alias membership, compound server identity,
conflict detection, exact selection/digest checks, and diagnostic precedence.
Exercise same-name operations across kinds and servers and verify unchanged bytes.

**Verification.** Run compiler tests and strict Clippy, facade compiler tests and
the independent fixture validator. Compare repeated release-mode measurements for
catalog ingestion and compile from the same inputs before and after.
