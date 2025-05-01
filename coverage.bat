@echo off
call .\scripts\install_sccache.bat || exit /B 1

set RUSTC_WRAPPER=sccache
cargo install --version 0.32.3 --locked cargo-tarpaulin || exit /B 1

sccache --zero-stats || exit /B 1

set CARGO_TARGET_DIR=target-coverage
set RUST_BACKTRACE=1
rem https://crates.io/crates/cargo-tarpaulin
cargo tarpaulin --verbose --out lcov --out html --include-tests --ignore-panics --count --output-dir target-coverage --skip-clean --engine llvm --exclude-files "target/*" || exit /B 1

sccache --show-stats || exit /B 1
