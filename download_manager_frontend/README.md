Leptos Docs: https://book.leptos.dev/

## How to run
Start the web server with hot reloading:

```bash
rustup target add wasm32-unknown-unknown
# Installing trunk (if not already installed)
# Please take the current version from the GitHub `.github/workflows/rust.yml` workflow; otherwise it might not work.
cargo install trunk --version 0.21.13

trunk serve --port 3000 --open
```
