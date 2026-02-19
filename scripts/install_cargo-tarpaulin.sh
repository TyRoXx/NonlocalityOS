#!/usr/bin/env sh
scripts_dir=$(dirname "$(realpath "$0")")
$scripts_dir/install_sccache.sh || exit 1

export RUSTC_WRAPPER=sccache
echo "Installing cargo-tarpaulin"
cargo install --version 0.35.2 --locked cargo-tarpaulin || exit 1
