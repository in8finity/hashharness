#!/usr/bin/env bash
set -euo pipefail

: "${HASHHARNESS_HTTP_HOST:=127.0.0.1}"
: "${HASHHARNESS_HTTP_PORT:=38417}"

curl -fsS "http://${HASHHARNESS_HTTP_HOST}:${HASHHARNESS_HTTP_PORT}/health"
