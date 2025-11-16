#!/usr/bin/env sh
set -e

FUZZ_TARGET_DIR=./fuzz/fuzz_targets

for file in $(ls "$FUZZ_TARGET_DIR"/*.rs | sort); do
    target_name=$(basename "$file" .rs)
    echo "Running fuzz target: $target_name"
    ./fuzz.sh "$target_name" || exit 1
done
