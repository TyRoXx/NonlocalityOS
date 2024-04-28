@echo off
cls || exit /B 1
setlocal

set repository=%~dp0

cargo run --bin astra --release %repository% || exit /B 1

echo Success!
endlocal
