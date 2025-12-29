#!/usr/bin/env sh
set -e
# cargo test will run all tests that contain any of these identifiers in their names:
TEST="$1"

./scripts/install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache

cargo install --version 0.13.1 --locked samply || exit 1

# just compiling first so that the compiler won't be profiled
cargo test --no-run -- "$TEST" || exit 1

samply record cargo test -- "$TEST" || exit 1
