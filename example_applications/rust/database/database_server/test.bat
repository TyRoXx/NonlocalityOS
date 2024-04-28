@echo off
setlocal
cargo test || exit /B 1

rem With default compiler options, wasmtime fails to run an application using SQLite:
rem "unknown import: `env::__extenddftf2` has not been defined"
rem This has something to do with long double (?).
rem Solution from: https://github.com/nmandery/h3ron/blob/9d80a2bf9fd5c4f311e64ffd40087dfb41fa55a5/h3ron/examples/compile_to_wasi/Makefile
rem https://docs.rs/cc/latest/cc/
set CFLAGS=-pthread
set RUSTFLAGS=-C target-feature=-crt-static -C link-arg=-L%wasi_compiler_unpack_dir%\%wasi_compiler_name%.0+m\lib\clang\18\lib\wasip1 -C link-arg=-lclang_rt.builtins-wasm32

cargo +nightly build --target wasm32-wasip1-threads || exit /B 1
endlocal
