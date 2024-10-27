@echo off
call .\install_cargo-run-bin.bat || exit /B 1
set RUST_LOG=info
cargo bin bacon nextest || exit /B 1
