#!/usr/bin/env sh
./install_sccache.sh || exit 1

export RUSTC_WRAPPER=sccache
cargo install --version 0.31.2 --locked cargo-tarpaulin || exit 1

sccache --zero-stats || exit 1

export CARGO_TARGET_DIR=target-coverage
export RUST_BACKTRACE=1
# https://crates.io/crates/cargo-tarpaulin
cargo tarpaulin --verbose --out lcov --out html --include-tests --ignore-panics --count --output-dir target-coverage --skip-clean --engine llvm --exclude-files 'target/*' || exit 1

sccache --show-stats || exit 1
