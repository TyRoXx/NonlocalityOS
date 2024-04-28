@echo off
cls || exit /B 1
call .\format.bat || exit /B 1

setlocal
set repository=%CD%

pushd downloader || exit /B 1

rem found this compiler on https://developer.arm.com/downloads/-/gnu-a
set rpi_compiler_name=gcc-arm-10.3-2021.07-mingw-w64-i686-aarch64-none-linux-gnu
set rpi_compiler_download_url=https://developer.arm.com/-/media/Files/downloads/gnu-a/10.3-2021.07/binrel/%rpi_compiler_name%.tar.xz?rev=06b6c36e428c48fda4b6d907f17308be^&hash=B36CC5C9544DCFCB2DB06FB46C8B8262
set rpi_compiler_archive_path=%repository%\tools\%rpi_compiler_name%.tar.xz
set rpi_compiler_unpack_dir=%repository%\tools\raspberry_pi_compiler
cargo run --release "%rpi_compiler_download_url%" "%rpi_compiler_archive_path%" "%rpi_compiler_unpack_dir%" "tar.xz" || exit /B 1

set wasi_compiler_name=wasi-sdk-22
set wasi_compiler_download_url=https://github.com/WebAssembly/wasi-sdk/releases/download/%wasi_compiler_name%/%wasi_compiler_name%.0.m-mingw.tar.gz
set wasi_compiler_archive_path=%repository%\tools\%wasi_compiler_name%.0.m-mingw.tar.gz
set wasi_compiler_unpack_dir=%repository%\tools\%wasi_compiler_name%.0.m-mingw
cargo run --release "%wasi_compiler_download_url%" "%wasi_compiler_archive_path%" "%wasi_compiler_unpack_dir%" "tar.gz" || exit /B 1
set CC_wasm32-wasi=%wasi_compiler_unpack_dir%\%wasi_compiler_name%.0+m\bin\clang.exe
set CC_wasm32-wasip1-threads=%CC_wasm32-wasi%

popd

pushd management_interface || exit /B 1
cargo test || exit /B 1
popd

pushd admin_tool || exit /B 1
cargo test || exit /B 1
popd

pushd management_service || exit /B 1
cargo test || exit /B 1

rem build for the Raspberry Pi because compiling stuff on the device itself is very slow

rem this target is duplicated in management_service/.cargo/config.toml
set raspberry_pi_target=aarch64-unknown-linux-gnu

rustup target add %raspberry_pi_target% || exit /B 1
set CC_aarch64-unknown-linux-gnu=%rpi_compiler_unpack_dir%\%rpi_compiler_name%\bin\aarch64-none-linux-gnu-gcc.exe
set AR_aarch64-unknown-linux-gnu=%rpi_compiler_unpack_dir%\%rpi_compiler_name%\bin\aarch64-none-linux-gnu-ar.exe
set LD_LIBRARY_PATH=%rpi_compiler_unpack_dir%\%rpi_compiler_name%\aarch64-none-linux-gnu\libc\lib64
cargo build --target %raspberry_pi_target% --config target.aarch64-unknown-linux-gnu.linker='%CC_aarch64-unknown-linux-gnu%' --release || exit /B 1

rustup toolchain install nightly-x86_64-pc-windows-msvc || exit /B 1

rustup target add wasm32-wasi || exit /B 1
rustup target add wasm32-wasip1-threads --toolchain nightly || exit /B 1

call .\test.bat || exit /B 1
popd

pushd nonlocality_env || exit /B 1
call .\test.bat || exit /B 1
popd

pushd example_applications || exit /B 1
call .\test.bat || exit /B 1
popd

echo Success!
