#!/usr/bin/env bash
set -Eeuo pipefail
mkdir -p runtime/cdc/lib runtime/out
CDC_BASE='https://dlcdn.apache.org/flink/flink-cdc-3.7.0'
CDC_ARCH='flink-cdc-3.7.0-1.20-bin.tar.gz'
curl -fsSL --retry 3 "${CDC_BASE}/${CDC_ARCH}" -o "runtime/${CDC_ARCH}"
curl -fsSL --retry 3 "${CDC_BASE}/${CDC_ARCH}.sha512" -o "runtime/${CDC_ARCH}.sha512"
python3 - <<'PY'
from pathlib import Path
import hashlib,re
p=Path('runtime/flink-cdc-3.7.0-1.20-bin.tar.gz')
t=Path('runtime/flink-cdc-3.7.0-1.20-bin.tar.gz.sha512').read_text()
m=re.search(r'\b[a-fA-F0-9]{128}\b',t)
if not m:raise SystemExit('Cannot parse Apache SHA512 checksum — ABORT')
actual=hashlib.sha512(p.read_bytes()).hexdigest()
if actual!=m.group().lower():raise SystemExit('Apache release checksum mismatch — ABORT')
print('Apache CDC SHA512 VERIFIED:',actual)
Path('runtime/out/release_checksums.txt').write_text(f'CDC archive SHA512 {actual}\n')
PY
mkdir -p runtime/extracted
tar -xzf "runtime/${CDC_ARCH}" -C runtime/extracted
SRC="$(find runtime/extracted -name flink-cdc.sh -type f | head -1)"
if [ -z "$SRC" ]; then echo 'CDC executable missing' >&2; exit 1; fi
cp -a "$(dirname "$(dirname "$SRC")")"/. runtime/cdc/
chmod +x runtime/cdc/bin/flink-cdc.sh
MAVEN='https://repo.maven.apache.org/maven2'
for name in mysql starrocks; do
    artifact="flink-cdc-pipeline-connector-${name}"
    curl -fsSL --retry 3 "${MAVEN}/org/apache/flink/${artifact}/3.7.0-1.20/${artifact}-3.7.0-1.20.jar" -o "runtime/cdc/lib/${artifact}-3.7.0-1.20.jar"
done
# Connector GPL-licensed driver is fetched at test runtime and NOT included in this ZIP.
curl -fsSL --retry 3 "${MAVEN}/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar" -o runtime/cdc/lib/mysql-connector-java-8.0.27.jar
sha256sum runtime/cdc/lib/* >> runtime/out/release_checksums.txt
cat > runtime/cdc/pipeline.yaml <<'YAML'
source:
  type: mysql
  hostname: mysql
  port: 3306
  username: root
  password: p7d_local_only_123
  tables: flink.orders_.*
  server-id: 5100-5104
  scan.startup.mode: initial
   server-id: 5100-5104
  jdbc.properties.allowPublicKeyRetrieval: "true"
  scan.startup.mode: initial
  scan.newly-added-table.enabled: false
  scan.binlog.newly-added-table.enabled: true
  server-time-zone: UTC
sink:
  type: starrocks
  name: StarRocks Sink
  jdbc-url: jdbc:mysql://starrocks:9030
  load-url: starrocks:8030
  username: root
  password: ""
  table.create.properties.replication_num: 1
pipeline:
  name: P7D-FLINK-39965-reproduction
  schema.change.behavior: evolve
  parallelism: 2
YAML
printf 'Downloaded Apache CDC 3.7.0-1.20 and connectors; wrote runtime config.\n'
