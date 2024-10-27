#!/usr/bin/env sh
./install_cargo-run-bin.sh || exit 1
cargo install --version 0.31.2 cargo-tarpaulin || exit 1
export RUST_LOG=info
cargo bin bacon coverage || exit 1
