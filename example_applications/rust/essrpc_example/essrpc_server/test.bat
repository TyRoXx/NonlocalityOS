@echo off
cargo test || exit /B 1
cargo +nightly build --target wasm32-wasip1-threads || exit /B 1
