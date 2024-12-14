@echo off
call install_sccache.bat || exit /B 1
set RUSTC_WRAPPER=sccache
cargo install --version 3.5.0 --locked bacon || exit /B 1
