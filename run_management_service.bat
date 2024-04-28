call .\test.bat || exit /B 1

cargo run --bin management_service --release "%CD%" || exit /B 1
