#!/usr/bin/env sh
./install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache
cargo install --version 3.1.1 --locked bacon || exit 1
