@echo off
rem https://github.com/mozilla/sccache
rem https://github.com/mozilla/sccache/blob/main/docs/Configuration.md
rem TODO: solve issue of cargo always rebuilding sccache for no reason

setlocal
set "executable=sccache.exe"
set "found=false"

for %%D in ("%PATH:;=" "%") do (
    if exist "%%~D\%executable%" (
        set "found=true"
        echo %executable% found in %%~D
        goto :end
    )
)

if "%found%" == "false" (
    echo %executable% not found in PATH, trying to install it.
    cargo --verbose install --locked sccache --version 0.9.1 || exit /B 1
)

:end
endlocal
