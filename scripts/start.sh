#!/usr/bin/env bash
set -Eeuo pipefail
mkdir -p runtime/out
docker compose up -d
python3 - <<'PY'
import subprocess,time,urllib.request,json

def run(args):
 return subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True).returncode==0
for name,cmd in [
 ('MySQL', ['docker','compose','exec','-T','mysql','mysql','-uroot','-pp7d_local_only_123','-N','-e','SELECT 1']),
 ('StarRocks', ['docker','compose','exec','-T','mysql','mysql','-hstarrocks','-P9030','-uroot','-N','-e','SELECT 1'])]:
 for n in range(60):
  if run(cmd):break
  time.sleep(5)
 else:raise SystemExit(f'{name} did not become ready; infrastructure blocked')
for n in range(60):
 try:
  with urllib.request.urlopen('http://localhost:8081/overview',timeout=3) as f:
   if json.load(f).get('taskmanagers',0)>0:break
 except Exception:pass
 time.sleep(3)
else:raise SystemExit('Flink not ready; infrastructure blocked')
print('All three services ready.')
PY
# Pipeline must be launched after services are ready. If submission fails, do not claim a bug.
docker compose exec -T mysql mysql -uroot -pp7d_local_only_123 -e "CREATE DATABASE IF NOT EXISTS flink; CREATE TABLE flink.orders_0 (id BIGINT NOT NULL, order_no VARCHAR(64) NOT NULL, user_name VARCHAR(64), status VARCHAR(32) NOT NULL, total_amount DECIMAL(18,2) NOT NULL, create_at TIMESTAMP NOT NULL, PRIMARY KEY(id)); INSERT INTO flink.orders_0 VALUES (1,'before','Alice','NEW',10.00,CURRENT_TIMESTAMP); CREATE TABLE flink.orders_1 LIKE flink.orders_0; INSERT INTO flink.orders_1 VALUES (1,'control','Alice','NEW',10.00,CURRENT_TIMESTAMP);"
docker compose exec -T jobmanager bash -lc 'export FLINK_HOME=/opt/flink; bash /opt/flink-cdc/bin/flink-cdc.sh /opt/flink-cdc/pipeline.yaml' | tee runtime/out/submit.log
python3 - <<'PY'
import json,time,urllib.request
for _ in range(40):
 try:
  with urllib.request.urlopen('http://localhost:8081/jobs/overview',timeout=5) as f: jobs=json.load(f)['jobs']
  if any(j['state']=='RUNNING' and 'P7D-' in j['name'] for j in jobs):
   print('CDC job RUNNING');break
 except Exception:pass
 time.sleep(3)
else:raise SystemExit('CDC not RUNNING; setup failure, not reproduced bug')
PY
