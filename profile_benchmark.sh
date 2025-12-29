#!/usr/bin/env sh
set -e
# cargo bench will run all benchmarks that contain any of these identifiers in their names:
BENCHMARKS="$1"

./scripts/install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache

cargo install --version 0.13.1 --locked samply || exit 1

# just compiling first so that the compiler won't be profiled
cargo bench --no-run -- $BENCHMARKS || exit 1

samply record cargo bench -- $BENCHMARKS || exit 1
