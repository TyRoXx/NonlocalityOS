#!/usr/bin/env sh
set -e

./install_cargo-nextest.sh
cargo nextest run
