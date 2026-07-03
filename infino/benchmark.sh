#!/bin/bash
# Thin shim — actual flow is in lib/benchmark-common.sh.
export BENCH_DOWNLOAD_SCRIPT="download-hits-parquet-single"
# Embedded engine: data persisted to ./data, no daemon to restart.
export BENCH_RESTARTABLE=no
export BENCH_DURABLE=yes
# Single-process: each query forks a fresh process, so the concurrent-QPS test
# only oversubscribes RAM. Skip by default.
export BENCH_CONCURRENT_DURATION="${BENCH_CONCURRENT_DURATION:-0}"
exec ../lib/benchmark-common.sh
