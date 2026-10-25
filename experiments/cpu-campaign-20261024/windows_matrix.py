#!/usr/bin/env python3
import json, pathlib, time, datetime
from windows_bench import ROOT,run,save
cases=[
 ('cache96-w1',[0],1),('cache32-w1',[16],1),
 ('cache96-w4',[0,2,4,6],4),('cache32-w4',[16,18,20,22],4),
 ('spread-w4',[0,2,16,18],4),('smt-w4',[0,1,16,17],4),
 ('cache96-w8',list(range(0,16,2)),8),('cache32-w8',list(range(16,32,2)),8),
 ('spread-w8',[0,2,4,6,16,18,20,22],8),('smt-w8',[0,1,2,3,16,17,18,19],8)]
paths=['avx2','avx512','avx512-256'];results=json.loads((ROOT/'windows-screen-results.json').read_text()) if (ROOT/'windows-screen-results.json').exists() else [];started=time.monotonic()
completed={r['label'] for r in results}
protocol=ROOT/'windows-screen-order-protocol.json'
if results:
 assert protocol.exists() and json.loads(protocol.read_text()).get('version')==2,'archive old-protocol results before resuming'
else:save(protocol,{'version':2,'order':'stable configuration rotation; reverse every pass'})
def check(r,threads,kernel):
 assert r['schema_version']==14 and r['cpu_threads']==threads
 assert r['config_fingerprint']['cpu_kernel']==kernel
 assert not r['config_fingerprint']['cpu_autotune_threads']
 for sample in r['runs']:
  for b in sample['backend_runs']:
   for field in ['dropped_events','memory_allocation_failures','assignment_enqueue_timeouts','assignment_execution_timeouts','control_enqueue_timeouts','control_execution_timeouts']:
    assert b[field]==0,(field,b[field])
   assert b['memory_regular_workers']==threads and b['memory_regular_bytes']==threads*2*1024**3
 return sum(x['hashes'] for x in r['runs'])/sum(x['actual_elapsed_secs'] for x in r['runs'])
try:
 for repeat in range(2):
  for case,cpus,threads in (cases if repeat==0 else list(reversed(cases))):
   i=cases.index((case,cpus,threads))
   order=paths[i%3:]+paths[:i%3]
   if repeat:order=list(reversed(order))
   for kernel in order:
    assert time.monotonic()-started<5400,'screening deadline'
    label=f'win-screen-r{repeat+1}-{case}-{kernel}'
    if label in completed:continue
    while (ROOT/'PAUSE-WINDOWS-SCREEN').exists():time.sleep(2)
    r=run(label,cpus,threads,kernel=kernel,secs=10,rounds=2,warmup=1)
    rate=check(r,threads,kernel)
    results.append({'label':label,'repeat':repeat+1,'case':case,'threads':threads,'cpus':cpus,'kernel':kernel,'completed_hps':rate})
    save(ROOT/'windows-screen-results.json',results)
    print(datetime.datetime.now(datetime.timezone.utc).isoformat(),label,f'{rate:.5f} H/s',flush=True)
    time.sleep(3)
 for case,_,_ in cases:
  passes=[[r['kernel'] for r in results if r['case']==case and r['repeat']==repeat] for repeat in (1,2)]
  assert len(passes[0])==len(paths) and passes[1]==list(reversed(passes[0]))
 (ROOT/'WINDOWS-SCREEN-COMPLETE').write_text('Screening only; actual per-configuration reversal verified.\n')
finally:
 save(ROOT/'windows-screen-results.json',results)
