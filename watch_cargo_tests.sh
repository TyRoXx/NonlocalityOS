#!/usr/bin/env sh
./scripts/install_bacon.sh || exit 1
./scripts/install_cargo-nextest.sh || exit 1
export RUST_LOG=info
bacon nextest || exit 1
