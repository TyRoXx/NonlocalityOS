#!/usr/bin/env sh
./install_sccache.sh || exit 1

export RUSTC_WRAPPER=sccache
cargo install --locked --version 0.9.86 cargo-nextest || exit 1
