#!/usr/bin/env sh
scripts_dir=$(dirname "$(realpath "$0")")
$scripts_dir/install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache
cargo install --version 3.5.0 --locked bacon || exit 1
