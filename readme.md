[![Coverage Status](https://coveralls.io/repos/github/TyRoXx/NonlocalityOS/badge.svg)](https://coveralls.io/github/TyRoXx/NonlocalityOS)

# Setup

* install Rust: <https://www.rust-lang.org/learn/get-started> (tested with Rust 1.84.0)
* restart your terminal

## Visual Studio Code

* install the `rust-analyzer` VS Code extension

## Windows

* install Visual Studio 2022 including the C++ toolchain
  * possibly, the [Build Tools](https://aka.ms/vs/17/release/vs_BuildTools.exe) are sufficient
* install <https://strawberryperl.com/> (tested with *5.40.0.1 (2024-08-10)*)
* restart your terminal

## Linux Mint / Ubuntu / Debian

* `sudo apt install build-essential libssl-dev podman`
* restart your terminal

# Tests

## Run tests once

* `cargo nextest run`

### Linux

* `./test.sh`

### Windows

* `test.bat`

## Watch code and keep running tests

### Linux

* `./watch_cargo_tests.sh`

### Windows

* `watch_cargo_tests.bat`

# Test coverage

## One time

### Linux

* `./coverage.sh`

### Windows

* `coverage.bat`

## Watch code and keep running tests to measure test coverage

### Windows

* `watch_coverage.bat`

## Visual Studio Code extension

* install the `coverage-gutters` VS Code extension <https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters> to visualize coverage in the code editor.

## HTML report

After measuring the test coverage, you can find [an HTML report under `target-coverage/tarpaulin-report.html`](target-coverage/tarpaulin-report.html).

# Formatting code

* `cargo fmt`

# Dogbox DAV server

## Run locally

### Release mode for better performance

Caution: The server binds to `0.0.0.0` which means it will be accessible from the local network. It doesn't support auth nor SSL yet, so be careful!

* `cargo run --bin nonlocality_host --release -- run [database directory]`

### Mount the DAV drive on Linux via fstab

Configure your system once:

* `./setup_dav_fstab.sh`

You will have to explicitly mount the filesystem after starting your DAV server. When it asks for username and password, you can just hit return. The server requires no username or password at the moment.

* `sudo mount /mnt/dogbox_localhost`

## Deploy to Raspberry Pi 4

1. copy `.env.template` as `.env`
2. edit `.env` for your setup
3. run `astra.bat install` or `./astra.sh install`

# Fuzzing (Linux only)

* `./quick_fuzzing.sh`
* or `./fuzz.sh prolly-tree-insert`

# Benchmarks

Example:

* `cargo bench --package dogbox_tree_editor`

# Profiling (Linux only)

* `echo '1' | sudo tee /proc/sys/kernel/perf_event_paranoid`
* `./profile_benchmark.sh [benchmark name]`
* or `./profile_test.sh [test name]`
