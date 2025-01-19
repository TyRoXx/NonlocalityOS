@echo off
cls || exit /B 1
call .\scripts\install_sccache.bat || exit /B 1

setlocal
set RUSTC_WRAPPER=sccache
set repository=%~dp0
set command=%1
set RUST_BACKTRACE=full

cargo run --bin astra -- %repository% %command% || exit /B 1

echo Success!
endlocal
