#!/usr/bin/env sh
set -e
export RUST_BACKTRACE=1

if cargo run --bin system_tests; then
  echo "✓ System tests passed"
else
  echo "✗ System tests failed"
  exit 1
fi
