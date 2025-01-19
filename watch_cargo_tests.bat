@echo off
call .\scripts\install_bacon.bat || exit /B 1
call .\scripts\install_cargo-nextest.bat || exit /B 1
set RUST_LOG=info
bacon nextest || exit /B 1
