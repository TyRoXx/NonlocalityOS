#!/usr/bin/env sh
./scripts/install_cargo-tarpaulin.sh || exit 1

echo "Resetting sccache stats"
sccache --zero-stats || exit 1

export CARGO_TARGET_DIR=target-coverage
export RUST_BACKTRACE=1
# https://crates.io/crates/cargo-tarpaulin
# https://github.com/xd009642/tarpaulin
# "Don't supply an explicit `--test-threads` argument to test executable. By default tarpaulin will infer the default rustc would pick if not ran via tarpaulin and set it"
# This flag makes the tests run faster (18 s instead of 21 s on my machine, 2026-01-17).
CT_MULTI_THREAD_ARGUMENTS="--implicit-test-threads"
CT_COMMAND="/usr/bin/time -v cargo tarpaulin --verbose --out lcov --out html --include-tests --ignore-panics --count --output-dir target-coverage --skip-clean --engine llvm --exclude-files 'target/*' $CT_MULTI_THREAD_ARGUMENTS"

echo "Building tests for cargo tarpaulin"
eval "$CT_COMMAND --no-run" || exit 1
echo "Running tests with cargo tarpaulin"
eval "$CT_COMMAND" || exit 1

echo "Showing sccache stats"
sccache --show-stats || exit 1
