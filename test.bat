@echo off
call .\install_cargo-nextest.bat || exit /B 1
cargo nextest run || exit /B 1
