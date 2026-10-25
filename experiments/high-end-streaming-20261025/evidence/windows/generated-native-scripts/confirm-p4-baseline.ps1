$ErrorActionPreference = 'Stop'
$mine = $null
try {
 if (Get-Process mod_tf_win64 -ErrorAction SilentlyContinue) { throw 'Competing desktop benchmark is running' }
 $os = Get-CimInstance Win32_OperatingSystem
 if (($os.FreePhysicalMemory * 1024) -lt (16 * 2GB + 10GB)) { throw 'Insufficient free RAM for bounded benchmark' }
 [System.Diagnostics.Process]::GetCurrentProcess().ProcessorAffinity = [IntPtr]::new(1431655765)
 $arguments = @('--bench','--backend','cpu','--threads','16','--disable-cpu-autotune-threads','--bench-kind','backend','--cpu-page-mode','regular','--cpu-affinity','os','--bench-cpu-kernel','avx512-256','--bench-secs','20','--bench-rounds','2','--bench-warmup-rounds','1','--bench-output','C:\Users\zp\AppData\Local\Temp\seine-high-end-20261025-archdispatch\reserved-run\confirm-p4-baseline.json','--ui','plain','--data-dir','C:\Users\zp\AppData\Local\Temp\seine-high-end-20261025-archdispatch\reserved-run\config')
 $mine = Start-Process -FilePath 'C:\Users\zp\AppData\Local\Temp\seine-high-end-20261025-archdispatch\reserved-run\seine.exe' -ArgumentList $arguments -WorkingDirectory 'C:\Users\zp\AppData\Local\Temp\seine-high-end-20261025-archdispatch\reserved-run' -NoNewWindow -PassThru -RedirectStandardOutput 'C:\Users\zp\AppData\Local\Temp\seine-high-end-20261025-archdispatch\reserved-run\confirm-p4-baseline.log' -RedirectStandardError 'C:\Users\zp\AppData\Local\Temp\seine-high-end-20261025-archdispatch\reserved-run\confirm-p4-baseline.err'
 $nativeHandle = $mine.Handle
 $actualMask = $mine.ProcessorAffinity.ToInt64()
 if ($actualMask -ne 1431655765) { throw "Affinity mismatch $actualMask" }
 $mine.PriorityClass = 'BelowNormal'
 $watch = [Diagnostics.Stopwatch]::StartNew()
 while (!$mine.WaitForExit(3000)) {
  $reservation = & 'D:\AI\gpuq\gpuq.exe' show 'd209288d65554e6997e2c7c6fe0cf9f3' | ConvertFrom-Json; if ($reservation.state -ne 'running') { throw 'Exclusive reservation ended; discard sample' }
  if ($watch.Elapsed.TotalSeconds -gt 180) { throw 'Benchmark deadline exceeded' }
  $state = Get-Content -Raw 'C:\Users\zp\AppData\Local\GpuQ\operator-state.json' | ConvertFrom-Json
  $age = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() - $state.updatedAtUnixMs
  if ($state.schemaVersion -ne 3 -or $age -gt 60000 -or $age -lt -5000 -or $state.gamingActive) { throw 'Gaming Mode active or state unavailable; discard this sample' }
  if (Get-Process mod_tf_win64 -ErrorAction SilentlyContinue) { throw 'Competing desktop benchmark returned; discard sample' }
 }
 $mine.WaitForExit()
 Write-Output ("affinity_mask=" + $actualMask + "; elapsed=" + $watch.Elapsed.TotalSeconds)
 if ($mine.ExitCode -ne 0) { throw "Miner exit $($mine.ExitCode)" }
} finally {
 if ($mine -and !$mine.HasExited) { $mine.Kill(); $mine.WaitForExit() }
}
