#!/usr/bin/env bash
set +e
mkdir -p runtime/out
( docker compose ps || true ) >runtime/out/compose_ps.txt 2>&1
( docker compose logs --timestamps --no-color --tail=2000 || true ) >runtime/out/compose_logs.txt 2>&1
( curl -fsS http://localhost:8081/jobs/overview || true ) >runtime/out/flink_jobs.json
( docker compose images || true ) >runtime/out/images.txt
( docker compose config || true ) >runtime/out/resolved_compose.txt 2>&1
( uname -a || true ) >runtime/out/kernel.txt
find runtime/out -type f -maxdepth 1 -print0 | xargs -0 sha256sum > runtime/out/output_sha256.txt 2>/dev/null || true
