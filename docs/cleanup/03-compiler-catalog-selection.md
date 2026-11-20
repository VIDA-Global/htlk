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

## Results

Source syntax carries a private operation kind through lowering. Ingestion and
both selection paths use that kind, and lookups borrow the selection string.
Tests cover a tool and prompt sharing a name, two deployments sharing a tool
name, and canonical rejection when the right descriptor is on the wrong server.
Existing conflict, source-pin, admission and unused-inventory tests still pass.

- Compiler: 59 integration tests and 3 doctests passed; strict Clippy passed.
- Facade compiler: 5 passed; independent native fixture validation passed.

### Release benchmark

Apple M4 Max, Rust 1.98.0, `cargo build --release --example catalog_cleanup_bench
--locked` with the same temporary driver on `6bf4432` and the candidate. The
driver parses the checked-in decision-brief catalogs and compiles the checked-in
standalone decision brief from the parsed catalogs. Seven alternating runs per
binary, each with 20 warmups, 500 catalog parses and 100 compiles:

| Mean per operation, median across runs | Before | After |
| --- | ---: | ---: |
| Catalog parse | 62.17 µs | 61.72 µs |
| Source compile | 7.539 ms | 7.510 ms |

The ranges overlap; this workload shows no meaningful speed change. Every run
produced the same executable fingerprint. Raw samples and the driver are in the
local `.opencode/.tmp/` directory.
