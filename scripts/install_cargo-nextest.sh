#!/usr/bin/env sh
scripts_dir=$(dirname "$(realpath "$0")")
$scripts_dir/install_sccache.sh || exit 1

export RUSTC_WRAPPER=sccache
cargo install --locked --version 0.9.86 cargo-nextest || exit 1
