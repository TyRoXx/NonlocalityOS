#!/usr/bin/env sh
cargo install --locked --version 3.1.1 bacon || exit 1
export RUST_LOG=info
bacon nextest || exit 1
