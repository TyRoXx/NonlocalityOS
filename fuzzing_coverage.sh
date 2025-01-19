#!/usr/bin/env sh
set -e

./scripts/install_cargo-fuzz.sh
cargo install --version 0.3.6 --locked cargo-binutils

BINARY=target/x86_64-unknown-linux-gnu/coverage/x86_64-unknown-linux-gnu/release/write-read-large-files
COVERAGE_DATA=fuzz/coverage/write-read-large-files/coverage.profdata

rm -f "$BINARY"
rm -f "$COVERAGE_DATA"
cargo fuzz coverage write-read-large-files

cargo-cov --verbose -- \
    report "$BINARY" \
    "-instr-profile=$COVERAGE_DATA" \
    "-ignore-filename-regex=\.cargo/.*" \
    "-ignore-filename-regex=\.rustup/.*"
