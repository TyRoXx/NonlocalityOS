@echo off
call install_sccache.bat || exit /B 1
set RUSTC_WRAPPER=sccache
cargo install --version 1.7.3 --locked cargo-run-bin || exit /B 1
