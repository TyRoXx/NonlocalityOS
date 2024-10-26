#!/usr/bin/env sh
# https://github.com/mozilla/sccache
# https://github.com/mozilla/sccache/blob/main/docs/Configuration.md
if which sccache >/dev/null 2>&1; then
    echo "sccache is in the PATH, so we assume that the correct version is already installed."
    echo "The reason for avoiding the cargo install command is that cargo likes to rebuild sccache unnecessarily which takes several minutes."
    sccache --version || exit 1
else
    echo "sccache is not in the PATH, trying to install it."
    set CARGO_LOG=info
    cargo --verbose install sccache || exit 1
fi
