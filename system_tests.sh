#!/usr/bin/env sh
set -e

cargo run --bin system_tests && echo "✓ System tests passed" || echo "✗ System tests failed"
