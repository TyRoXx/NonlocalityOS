@echo off
cls || exit /B 1

rem Set LIBCLANG_PATH for any crates that need it during build
if not defined LIBCLANG_PATH (
    set "LIBCLANG_PATH=%~dp0tools\clang+llvm-22.1.0-x86_64-pc-windows-msvc\bin"
)

call .\scripts\install_sccache.bat || exit /B 1

setlocal
set RUSTC_WRAPPER=sccache
set repository=%~dp0
set command=%1
set RUST_BACKTRACE=1

pushd %repository% || exit /B 1
cargo run --bin astra -- %command% || exit /B 1
popd

echo Success!
endlocal
