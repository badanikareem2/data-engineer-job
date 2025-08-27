#!/usr/bin/env bash
set -euo pipefail
docker compose pull
docker compose build --no-cache external-mock
docker compose up -d
docker compose ps
