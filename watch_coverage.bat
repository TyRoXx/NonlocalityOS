@echo off

rem Set LIBCLANG_PATH for any crates that need it during build/coverage
if not defined LIBCLANG_PATH (
    set "LIBCLANG_PATH=%~dp0tools\clang+llvm-22.1.0-x86_64-pc-windows-msvc\bin"
)

call .\scripts\install_bacon.bat || exit /B 1
cargo install --version 0.32.5 --locked cargo-tarpaulin || exit /B 1
set RUST_LOG=info
bacon coverage || exit /B 1
