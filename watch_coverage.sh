#!/usr/bin/env sh
./install_bacon.sh || exit 1
cargo install --version 0.31.2 --locked cargo-tarpaulin || exit 1
export RUST_LOG=info
bacon coverage || exit 1
