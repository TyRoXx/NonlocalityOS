@echo off
set scripts_dir=%~dp0
call %scripts_dir%\install_sccache.bat || exit /B 1

set RUSTC_WRAPPER=sccache
cargo install --locked cargo-nextest --version 0.9.86 || exit /B 1
