#!/usr/bin/env sh
set -e
PACKAGE=dogbox_tree_editor
BENCHMARKS="read_large_file_sqlite_in_memory_storage_cold_realistic_read_size"

./install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache

cargo install --version 0.12.0 --locked samply

# just compiling first so that the compiler won't be profiled
cargo bench --no-run --package $PACKAGE -- $BENCHMARKS

samply record cargo bench --package $PACKAGE -- $BENCHMARKS
