#!/usr/bin/env sh
set -e

cargo install cargo-nextest
cargo nextest run
