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
        echo sccache is in the PATH, so we assume that the correct version is already installed.
        echo The reason for avoiding the cargo install command is that cargo likes to rebuild sccache unnecessarily which takes several minutes.
        echo If you want to update sccache, run: cargo uninstall sccache
        goto :end
    )
)

if "%found%" == "false" (
    echo %executable% not found in PATH, trying to install it.
    cargo --verbose install --locked sccache --version 0.14.0 || exit /B 1
)

:end
endlocal
