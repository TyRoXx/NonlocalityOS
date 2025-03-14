@echo off
call .\scripts\install_cargo-nextest.bat || exit /B 1
cargo nextest run || exit /B 1

echo Building fuzzers
call .\scripts\build_fuzz.bat || exit /B 1
