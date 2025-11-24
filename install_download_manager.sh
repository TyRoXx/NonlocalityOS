#!/usr/bin/env bash
set -e
CONFIG_DIRECTORY="$1"
if [ -z "$CONFIG_DIRECTORY" ]; then
    echo "Usage: $0 <config_directory>"
    exit 1
fi

TARGET_BINARY=`pwd`/target/release/download_manager
rm -rf "$TARGET_BINARY"
cargo build --release --bin download_manager || exit 1
cp "$TARGET_BINARY" "$CONFIG_DIRECTORY/" || exit 1
