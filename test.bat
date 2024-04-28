cls || exit /B 1
call .\astra.bat || exit /B 1

setlocal
set repository=%CD%

set rpi_compiler_name=gcc-arm-10.3-2021.07-mingw-w64-i686-aarch64-none-linux-gnu
set rpi_compiler_unpack_dir=%repository%\tools\raspberry_pi_compiler

set wasi_compiler_name=wasi-sdk-22
set wasi_compiler_unpack_dir=%repository%\tools\%wasi_compiler_name%.0.m-mingw
set CC_wasm32-wasi=%wasi_compiler_unpack_dir%\%wasi_compiler_name%.0+m\bin\clang.exe
set CC_wasm32-wasip1-threads=%CC_wasm32-wasi%

set raspberry_pi_target=aarch64-unknown-linux-gnu
set CC_aarch64-unknown-linux-gnu=%rpi_compiler_unpack_dir%\%rpi_compiler_name%\bin\aarch64-none-linux-gnu-gcc.exe
set AR_aarch64-unknown-linux-gnu=%rpi_compiler_unpack_dir%\%rpi_compiler_name%\bin\aarch64-none-linux-gnu-ar.exe
set LD_LIBRARY_PATH=%rpi_compiler_unpack_dir%\%rpi_compiler_name%\aarch64-none-linux-gnu\libc\lib64

pushd management_service || exit /B 1
cargo build --target %raspberry_pi_target% --config target.aarch64-unknown-linux-gnu.linker='%CC_aarch64-unknown-linux-gnu%' --release || exit /B 1
call .\test.bat || exit /B 1
popd

rustup toolchain install nightly-x86_64-pc-windows-msvc || exit /B 1

pushd example_applications || exit /B 1
call .\test.bat || exit /B 1
popd

echo Success!
