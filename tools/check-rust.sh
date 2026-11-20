#!/usr/bin/env bash
# Keep local, PR, and release validation on the same feature matrix.
set -euo pipefail

check() {
    cargo check --workspace --locked "$@"
    cargo clippy --workspace --locked "$@" -- -D warnings
}

cargo fmt --all -- --check
check --all-targets --all-features
check --all-targets --no-default-features --features sqlite
check --all-targets --no-default-features --features postgres
# Without a backend, only the library embedding surface is supported.
check --lib --no-default-features
