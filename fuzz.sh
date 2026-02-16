#!/usr/bin/env sh
set -e

TARGET_NAME="$1"
./scripts/install_cargo-fuzz.sh
JOBS=`nproc`
TIME_PER_TARGET=25

cargo fuzz run --release --jobs "$JOBS" "$TARGET_NAME" -- -max_total_time=$TIME_PER_TARGET
