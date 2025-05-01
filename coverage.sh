#!/usr/bin/env sh
./scripts/install_cargo-tarpaulin.sh || exit 1

echo "Resetting sccache stats"
sccache --zero-stats || exit 1

export CARGO_TARGET_DIR=target-coverage
export RUST_BACKTRACE=1
# https://crates.io/crates/cargo-tarpaulin
echo "Running cargo tarpaulin"
cargo tarpaulin --verbose --out lcov --out html --include-tests --ignore-panics --count --output-dir target-coverage --skip-clean --engine llvm --exclude-files 'target/*' || exit 1

echo "Showing sccache stats"
sccache --show-stats || exit 1
