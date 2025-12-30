@echo off
setlocal enabledelayedexpansion

set CONFIG_DIRECTORY=%1
if "%CONFIG_DIRECTORY%"=="" (
    echo Usage: %0 ^<config_directory^>
    exit /b 1
)

set TARGET_BINARY=%cd%\target\release\download_manager.exe
if exist "%TARGET_BINARY%" del "%TARGET_BINARY%"

echo Building download_manager...
cargo build --release --bin download_manager --timings
if errorlevel 1 exit /b 1

echo Copying binary to %CONFIG_DIRECTORY%...
copy "%TARGET_BINARY%" "%CONFIG_DIRECTORY%\" >nul
if errorlevel 1 exit /b 1

echo Download manager installed successfully to %CONFIG_DIRECTORY%
