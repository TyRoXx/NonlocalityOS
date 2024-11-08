#!/usr/bin/env sh
./install_sccache.sh || exit 1

export RUSTC_WRAPPER=sccache
cargo install --version 0.12.0 --locked cargo-fuzz || exit 1
