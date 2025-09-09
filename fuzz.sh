#!/usr/bin/env sh
set -e

./scripts/install_cargo-fuzz.sh
JOBS=`nproc`
FUZZ_TARGET_DIR=./fuzz/fuzz_targets
TIME_PER_TARGET=10

for file in $(ls "$FUZZ_TARGET_DIR"/*.rs | sort); do
    target_name=$(basename "$file" .rs)
    echo "Running fuzz target: $target_name"
    cargo fuzz run --release --jobs $JOBS "$target_name" -- -max_total_time=$TIME_PER_TARGET || exit 1
done
