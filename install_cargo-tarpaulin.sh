#!/usr/bin/env sh
./install_sccache.sh || exit 1

export RUSTC_WRAPPER=sccache
cargo install --version 0.31.2 --locked cargo-tarpaulin || exit 1
