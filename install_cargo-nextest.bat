@echo off
call install_sccache.bat || exit /B 1

set RUSTC_WRAPPER=sccache
cargo install --locked cargo-nextest || exit /B 1
