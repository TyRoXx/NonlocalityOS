@echo off

rem Set LIBCLANG_PATH for any crates that need it during build/coverage
if not defined LIBCLANG_PATH (
    set "LIBCLANG_PATH=%~dp0tools\clang+llvm-22.1.0-x86_64-pc-windows-msvc\bin"
)

call .\scripts\install_sccache.bat || exit /B 1

set RUSTC_WRAPPER=sccache
cargo install --version 0.32.5 --locked cargo-tarpaulin || exit /B 1

sccache --zero-stats || exit /B 1

set CARGO_TARGET_DIR=target-coverage
set RUST_BACKTRACE=1
rem https://crates.io/crates/cargo-tarpaulin
cargo tarpaulin --verbose --out lcov --out html --include-tests --ignore-panics --count --output-dir target-coverage --skip-clean --engine llvm --exclude-files "target/*" || exit /B 1

sccache --show-stats || exit /B 1
