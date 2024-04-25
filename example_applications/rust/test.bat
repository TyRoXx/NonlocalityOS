@echo off
pushd tonic_rpc_server || exit /B 1
call .\test.bat || exit /B 1
popd

pushd hello_rust || exit /B 1
call .\test.bat || exit /B 1
popd

pushd idle_service || exit /B 1
call .\test.bat || exit /B 1
popd

pushd provide_api || exit /B 1
call .\test.bat || exit /B 1
popd

pushd call_api || exit /B 1
call .\test.bat || exit /B 1
popd
