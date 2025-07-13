#!/bin/bash

TIMESTAMP=$(date +%s)
NODE_NAME="disttest_${TIMESTAMP}@127.0.0.1"
COOKIE="test_cookie_${TIMESTAMP}"

echo "Starting distributed test with node: $NODE_NAME"
elixir --name "$NODE_NAME" --cookie "$COOKIE" -S mix run ./examples/distributed_test.exs