#!/usr/bin/env sh
./install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache
cargo install --version 1.7.3 --locked cargo-run-bin || exit 1
