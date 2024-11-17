#!/usr/bin/env sh
set -e
# limit to one package to avoid unnecessary builds of other packages:
PACKAGE="dogbox_tree_editor"
#PACKAGE="astraea"
# cargo bench will run all benchmarks that contain any of these identifiers in their names:
#BENCHMARKS="read_large_file_sqlite_in_memory_storage"
BENCHMARKS=""

./install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache

cargo install --version 0.12.0 --locked samply

# just compiling first so that the compiler won't be profiled
cargo bench --no-run --package $PACKAGE -- $BENCHMARKS

samply record cargo bench --package $PACKAGE -- $BENCHMARKS
