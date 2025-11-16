#!/usr/bin/env sh
set -e

cargo clippy --all-targets --all-features "$@" -- -D warnings
