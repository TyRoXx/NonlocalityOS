@echo off

rem Set LIBCLANG_PATH for any crates that need it during build/coverage
if not defined LIBCLANG_PATH (
    set "LIBCLANG_PATH=%~dp0tools\clang+llvm-22.1.0-x86_64-pc-windows-msvc\bin"
)

rem https://doc.rust-lang.org/nightly/unstable-book/compiler-flags/report-time.html#examples
cargo test --tests -- -Zunstable-options --report-time --test-threads=1 || exit /B 1
