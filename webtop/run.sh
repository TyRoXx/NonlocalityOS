#!/usr/bin/env bash
set -e
set -x

# Prerequisites: Install docker and compose: https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository

docker compose -f compose.yml up
