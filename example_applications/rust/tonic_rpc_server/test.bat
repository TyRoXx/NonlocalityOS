@echo off
rem cargo test || exit /B 1
cargo build --target wasm32-wasi --no-default-features || exit /B 1
rem cargo run || exit /B 1
