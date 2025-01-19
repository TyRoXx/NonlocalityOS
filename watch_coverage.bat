@echo off
call .\scripts\install_bacon.bat || exit /B 1
cargo install --version 0.31.2 --locked cargo-tarpaulin || exit /B 1
set RUST_LOG=info
bacon coverage || exit /B 1
