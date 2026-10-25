#!/usr/bin/env python3
"""Bounded native-Windows benchmark runner. Only task-owned processes and files."""
import datetime, hashlib, json, os, pathlib, re, shutil, subprocess, time, zipfile
ROOT=pathlib.Path(__file__).resolve().parents[2]/'data/cpu-campaign-20261024'
WIN=pathlib.Path(os.environ['SEINE_WINDOWS_BENCH_DIR'])
ARCHIVE=pathlib.Path(os.environ['SEINE_WINDOWS_RELEASE_ZIP'])
WIN_STR=subprocess.check_output(['wslpath','-w',str(WIN)],text=True).strip()
GPUQ_STATE=os.environ['SEINE_WINDOWS_GPUQ_STATE']
# Keep these explicit benchmark paths in the task-owned Windows temp directory.
assert "'" not in WIN_STR and "'" not in GPUQ_STATE
def save(path,obj):path.write_text(json.dumps(obj,indent=2)+'\n')
def setup():
 ROOT.mkdir(parents=True,exist_ok=True)
 WIN.mkdir(exist_ok=True)
 with zipfile.ZipFile(ARCHIVE) as z:
  data=z.read('seine.exe');(WIN/'seine.exe').write_bytes(data)
 save(ROOT/'windows-binary.json',{'source':str(ARCHIVE),'sha256':hashlib.sha256(data).hexdigest()})
def run(label, cpus, threads, kernel='avx512', page='regular', secs=12, rounds=2, warmup=1):
 gate=subprocess.run(['gpuq','gaming','check','--quiet'])
 if gate.returncode:raise RuntimeError(f'local timing deferred: gaming gate {gate.returncode}')
 assert re.fullmatch(r"[a-zA-Z0-9_-]+",label)
 assert 1 <= threads <= 8 and len(cpus)>=threads
 assert len(set(cpus))==len(cpus) and all(0 <= c < 32 for c in cpus)
 name=label;mask=sum(1<<n for n in cpus);output=WIN/(name+'.json');log=WIN/(name+'.log')
 args=['--bench','--backend','cpu','--threads',str(threads),'--disable-cpu-autotune-threads','--bench-kind','backend','--cpu-page-mode',page,'--cpu-affinity','os','--bench-cpu-kernel',kernel,'--bench-secs',str(secs),'--bench-rounds',str(rounds),'--bench-warmup-rounds',str(warmup),'--bench-output',WIN_STR+'\\'+output.name,'--ui','plain']
 # The dedicated shell's affinity is inherited before miner initialization.
 script=f'''$ErrorActionPreference = 'Stop'
$mine = $null
try {{
 $os = Get-CimInstance Win32_OperatingSystem
 if (($os.FreePhysicalMemory * 1024) -lt ({threads} * 2GB + 10GB)) {{ throw 'Insufficient free RAM for bounded benchmark' }}
 [System.Diagnostics.Process]::GetCurrentProcess().ProcessorAffinity = [IntPtr]::new({mask})
 $arguments = @({','.join("'"+a.replace("'","''")+"'" for a in args)})
 $mine = Start-Process -FilePath '{WIN_STR}\\seine.exe' -ArgumentList $arguments -WorkingDirectory '{WIN_STR}' -NoNewWindow -PassThru -RedirectStandardOutput '{WIN_STR}\\{name}.log' -RedirectStandardError '{WIN_STR}\\{name}.err'
 $nativeHandle = $mine.Handle
 $actualMask = $mine.ProcessorAffinity.ToInt64()
 if ($actualMask -ne {mask}) {{ throw "Affinity mismatch $actualMask" }}
 $mine.PriorityClass = 'BelowNormal'
 $watch = [Diagnostics.Stopwatch]::StartNew()
 while (!$mine.WaitForExit(3000)) {{
  if ($watch.Elapsed.TotalSeconds -gt 180) {{ throw 'Benchmark deadline exceeded' }}
  $state = Get-Content -Raw '{GPUQ_STATE}' | ConvertFrom-Json
  $age = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() - $state.updatedAtUnixMs
  if ($state.schemaVersion -ne 3 -or $age -gt 60000 -or $age -lt -5000 -or $state.gamingActive) {{ throw 'Gaming Mode active or state unavailable; discard this sample' }}
 }}
 $mine.WaitForExit()
 Write-Output ("affinity_mask=" + $actualMask + "; elapsed=" + $watch.Elapsed.TotalSeconds)
 if ($mine.ExitCode -ne 0) {{ throw "Miner exit $($mine.ExitCode)" }}
}} finally {{
 if ($mine -and !$mine.HasExited) {{ $mine.Kill(); $mine.WaitForExit() }}
}}
'''
 ps=WIN/(name+'.ps1');ps.write_text(script)
 began=time.time();result=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',WIN_STR+'\\'+ps.name],capture_output=True,text=True,timeout=210)
 for suffix in ['.json','.log','.err']:
  src=WIN/(name+suffix)
  if src.exists():shutil.copy2(src,ROOT/(name+suffix))
 meta={'label':label,'cpus':cpus,'mask':mask,'threads':threads,'kernel':kernel,'page':page,'seconds':secs,'rounds':rounds,'warmup':warmup,'exit_code':result.returncode,'wall_seconds':time.time()-began,'stdout':result.stdout,'stderr':result.stderr,'timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 save(ROOT/(name+'.meta.json'),meta)
 if result.returncode:raise RuntimeError(result.stderr)
 report=json.loads((ROOT/(name+'.json')).read_text());return report
if __name__=='__main__':
 setup()
 r=run('pilot-regular-w1',[0],1,page='regular',secs=3,rounds=1,warmup=1)
 print(json.dumps(r.get('summary',{}),indent=2))
