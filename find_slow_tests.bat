@echo off
rem https://doc.rust-lang.org/nightly/unstable-book/compiler-flags/report-time.html#examples
cargo test --tests -- -Zunstable-options --report-time --test-threads=1 || exit /B 1
