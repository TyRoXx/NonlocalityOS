@echo off
cls || exit /B 1
setlocal
set repository=%~dp0
set command=%1
set RUST_BACKTRACE=full

cargo run --bin example_cluster --release -- %repository% %command% || exit /B 1

echo Success!
endlocal
