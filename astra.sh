#!/usr/bin/env sh
repository=$(dirname "$0")
command=$1
cargo run --bin astra --release -- $repository $command || exit 1
echo Success!
