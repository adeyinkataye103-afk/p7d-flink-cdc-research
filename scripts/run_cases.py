#!/usr/bin/env python3
"""P7D: real MySQL->Flink CDC->StarRocks lifecycle test, not an abstract simulation.
Reports setup failures separately and saves raw per-stage observations.
"""
import argparse,csv,datetime,json,os,pathlib,subprocess,time,urllib.request
OUT=pathlib.Path('runtime/out'); OUT.mkdir(parents=True,exist_ok=True)

def dc(*args,check=True):
 p=subprocess.run(['docker','compose',*args],capture_output=True,text=True,timeout=90)
 if check and p.returncode:raise RuntimeError('Docker error '+repr(args)+': '+p.stderr[-1000:])
 return p

def query(sql,starrocks=False):
 args=['exec','-T','mysql','mysql']
 if starrocks:args+=['-hstarrocks','-P9030','-uroot']
 else:args+=['-uroot','-pp7d_local_only_123']
 p=dc(*(args+['-N','-s','-e',sql]),check=False)
 return (p.returncode,p.stdout.strip(),p.stderr.strip())

def table_sql(tbl):
 return f"CREATE TABLE flink.{tbl} (id BIGINT NOT NULL, order_no VARCHAR(64) NOT NULL, user_name VARCHAR(64), status VARCHAR(32) NOT NULL, total_amount DECIMAL(18,2) NOT NULL, create_at TIMESTAMP NOT NULL, PRIMARY KEY(id))"

def exists(tbl):
 rc,out,err=query(f"SHOW TABLES FROM flink LIKE '{tbl}'",starrocks=True)
 if rc: return None
 return tbl in out.splitlines()

def row_present(tbl,pk):
 rc,out,err=query(f'SELECT id FROM flink.{tbl} WHERE id={pk}',starrocks=True)
 if rc: return None
 return str(pk) in out.splitlines()

def wait_until(fn,expected,timeout=90):
 deadline=time.monotonic()+timeout; last=None
 while time.monotonic()<deadline:
  last=fn()
  if last is expected:return True
  time.sleep(2)
 return False

def job_info():
 try:
  with urllib.request.urlopen('http://localhost:8081/jobs/overview',timeout=5) as f:
   jobs=json.load(f).get('jobs',[])
  return next((x for x in jobs if 'P7D-' in x.get('name','')),None)
 except Exception:return None

def checkpoint_count(jobid):
 try:
  with urllib.request.urlopen(f'http://localhost:8081/jobs/{jobid}/checkpoints',timeout=5) as f:
   return json.load(f).get('counts',{}).get('completed',0)
 except Exception:return None

def stage(rec,name,ok,comment=''):
 rec['stages'].append({'name':name,'pass':ok,'comment':comment,'UTC':datetime.datetime.now(datetime.timezone.utc).isoformat()})
 print(f'{rec["case"]}: {name}: {"PASS" if ok else "BLOCKED/FAIL"} {comment}',flush=True)
 if not ok:raise RuntimeError(name+' '+comment)

def row_present_source(tbl,pk):
 rc,out,err=query(f'SELECT id FROM flink.{tbl} WHERE id={pk}')
 return rc==0 and str(pk) in out.splitlines()

def run_case(mode):
 tbl='orders_0'
 rec={'case':mode,'table':tbl,'mode':mode,'stages':[],'status':'UNKNOWN'}
 try:
  stage(rec,'initial_source_row',row_present_source(tbl,1))
  stage(rec,'initial_sink_row',wait_until(lambda:row_present(tbl,1),True,100),'real sink observation')
  rc,o,e=query(f'DROP TABLE flink.{tbl};')
  stage(rec,'source_drop',rc==0,e[-400:])
  stage(rec,'sink_drop_visible',wait_until(lambda:exists(tbl),False,100),'real sink observation')
  if mode=='restart':
   ji=job_info();cnt=checkpoint_count(ji['jid']) if ji else None
   stage(rec,'checkpoint_completed_before_restart',cnt is not None and cnt>=1,f'completed={cnt}')
   p=dc('restart','taskmanager',check=False)
   stage(rec,'taskmanager_restart_command',p.returncode==0,p.stderr[-300:])
   stage(rec,'job_returned_running',wait_until(lambda:(job_info() or {}).get('state')=='RUNNING',True,100))
  rc,o,e=query(table_sql(tbl)+';'+f"INSERT INTO flink.{tbl} VALUES (2,'after','Bob','NEW',20.00,CURRENT_TIMESTAMP);")
  stage(rec,'recreate_and_second_insert',rc==0,e[-300:])
  recovered=wait_until(lambda:row_present(tbl,2),True,120)
  stage(rec,'recreated_row_visible',recovered,'end-to-end source-sink outcome')
  rec['status']='CORRECT_RECREATION'
 except Exception as ex:
  rec['failure_detail']=str(ex)
  passed={x['name'] for x in rec['stages'] if x['pass']}
  if {'initial_sink_row','sink_drop_visible','recreate_and_second_insert'} <= passed:
   rec['status']='SUSPECT_CDC_LIFECYCLE_FAILURE_REQUIRES_LOG_TRIAGE'
  else:rec['status']='INCONCLUSIVE_OR_SETUP_FAILURE'
 rec['job_info']=job_info();return rec

def run_control():
 rec={'case':'control','table':'orders_1','mode':'control','stages':[],'status':'UNKNOWN'}
 try:
  stage(rec,'source_row',row_present_source('orders_1',1))
  stage(rec,'sink_initial',wait_until(lambda:row_present('orders_1',1),True,100))
  rc,o,e=query("INSERT INTO flink.orders_1 VALUES (2,'control-update','Bob','NEW',20.00,CURRENT_TIMESTAMP);")
  stage(rec,'source_update',rc==0)
  stage(rec,'sink_update',wait_until(lambda:row_present('orders_1',2),True,100))
  rec['status']='CONTROL_PASSED'
 except Exception as ex:rec.update(status='CONTROL_FAILED_OR_SETUP',failure_detail=str(ex))
 rec['job_info']=job_info();return rec

if __name__=='__main__':
 pa=argparse.ArgumentParser();pa.add_argument('--mode',choices=['normal','restart'],default='normal');args=pa.parse_args()
 rows=[run_control(),run_case(args.mode)]
 (OUT/'P7D_trial_evidence.json').write_text(json.dumps(rows,indent=2))
 with (OUT/'P7D_trial_summary.csv').open('w',newline='') as f:
  w=csv.DictWriter(f,fieldnames=['case','mode','table','status','failure_detail']);w.writeheader()
  for x in rows:w.writerow({k:x.get(k,'') for k in w.fieldnames})
 print(json.dumps({s:sum(r['status']==s for r in rows) for s in set(r['status'] for r in rows)},indent=2))
