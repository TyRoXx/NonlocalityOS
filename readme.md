# Setup

* install Rust: <https://www.rust-lang.org/learn/get-started> (tested with Rust 1.84.0)
* restart your terminal
* install `sccache` by running `cargo --verbose install sccache`

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
