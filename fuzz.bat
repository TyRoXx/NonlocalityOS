@echo off

set "TARGET_NAME=%1"
set "TIME_PER_TARGET=5"

call .\scripts\install_cargo-fuzz.bat || exit /B 1

rem Set LIBCLANG_PATH for any crates that need it during build/coverage
if not defined LIBCLANG_PATH (
    set "LIBCLANG_PATH=%~dp0tools\clang+llvm-22.1.0-x86_64-pc-windows-msvc\bin"
)

cargo fuzz run --release --jobs 8 "%TARGET_NAME%" -- -max_total_time=%TIME_PER_TARGET%
