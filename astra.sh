#!/usr/bin/env sh
./scripts/install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache

repository=$(dirname "$0")
command=$1
RUST_BACKTRACE=full

cargo run --bin astra --release -- $repository $command || exit 1

echo Success!
