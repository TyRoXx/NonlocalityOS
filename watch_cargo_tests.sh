#!/usr/bin/env sh
./install_cargo-run-bin.sh || exit 1
export RUST_LOG=info
cargo bin bacon nextest || exit 1
