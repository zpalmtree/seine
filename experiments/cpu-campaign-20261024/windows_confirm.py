#!/usr/bin/env python3
import sys,json,time,datetime,pathlib
from windows_bench import ROOT,run,save
from screen import validate
comparisons=[
 ('w1-ccd',('cache32',[16],1,'avx512'),('cache96',[0],1,'avx512')),
 ('w4-spread',('cache96',[0,2,4,6],4,'avx512'),('spread',[0,2,16,18],4,'avx512')),
 ('w8-spread',('cache96',list(range(0,16,2)),8,'avx512'),('spread',[0,2,4,6,16,18,20,22],8,'avx512')),
 ('w4-smt',('physical',[0,2,16,18],4,'avx512'),('smt',[0,1,16,17],4,'avx512')),
 ('w1-isa',('avx2',[0],1,'avx2'),('avx512',[0],1,'avx512')),
 ('w8-isa',('avx2',[0,2,4,6,16,18,20,22],8,'avx2'),('avx512',[0,2,4,6,16,18,20,22],8,'avx512')),
]
path=ROOT/'windows-confirm-results.json'
results=json.loads(path.read_text()) if path.exists() else []
complete={r['label'] for r in results}
started=time.monotonic()
try:
 for comparison,a,b in comparisons:
  for pair in range(4):
   order=[('baseline',a),('candidate',b)]
   if pair%2:order.reverse()
   for role,(case,cpus,workers,kernel) in order:
    label=f'win-confirm-{comparison}-pair{pair+1}-{role}'
    if label in complete:continue
    assert time.monotonic()-started<7200,'confirmation session deadline'
    while (ROOT/'PAUSE-WINDOWS-CONFIRM').exists():time.sleep(2)
    r=run(label,cpus,workers,kernel=kernel,secs=30,rounds=2,warmup=1)
    rate=validate(r,workers,kernel,'regular')
    results.append({'label':label,'comparison':comparison,'pair':pair+1,'role':role,'case':case,'cpus':cpus,'workers':workers,'kernel':kernel,'completed_hps':rate})
    save(path,results)
    print(datetime.datetime.now(datetime.timezone.utc).isoformat(),label,f'{rate:.6f} H/s',flush=True)
    time.sleep(3)
 (ROOT/'WINDOWS-CONFIRM-COMPLETE').write_text('Four alternating pairs for each comparison.\n')
finally:save(path,results)
