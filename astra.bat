@echo off
cls || exit /B 1
setlocal
set command=%1
set RUST_BACKTRACE=full
set repository=%~dp0

cargo run --bin astra -- %repository% %command% || exit /B 1

echo Success!
endlocal
