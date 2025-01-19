#!/usr/bin/env sh
set -e

./scripts/install_cargo-nextest.sh
cargo nextest run
