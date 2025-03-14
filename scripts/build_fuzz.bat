@echo off
cargo build --release --package nonlocality-fuzz || exit /B 1
