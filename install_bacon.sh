#!/usr/bin/env sh
./install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache
cargo install --version 3.5.0 --locked bacon || exit 1
