#!/usr/bin/env sh
set -e

./install_cargo-fuzz.sh
JOBS=`nproc`
cargo fuzz run --release --jobs $JOBS write-read-large-files