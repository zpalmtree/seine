param([Parameter(Mandatory=$true)][string]$Root)
$ErrorActionPreference = 'Stop'
$statePath = Join-Path $env:LOCALAPPDATA 'GpuQ\operator-state.json'

function Check-Admission {
    $state = Get-Content -Raw $statePath | ConvertFrom-Json
    $age = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() - $state.updatedAtUnixMs
    if ($state.schemaVersion -ne 3 -or $age -gt 60000 -or $age -lt -5000 -or $state.gamingActive) {
        throw 'Gaming Mode active or coordinator state stale; stop owned native work'
    }
}

Check-Admission
$os = Get-CimInstance Win32_OperatingSystem
if ($os.FreePhysicalMemory * 1024 -lt 12GB) { throw 'Insufficient free memory for 2 GiB parity plus 10 GiB headroom' }
@{
    utc = [DateTime]::UtcNow.ToString('o')
    cpu = @(Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors)
    memory = $os | Select-Object TotalVisibleMemorySize,FreePhysicalMemory
    process_affinity = [System.Diagnostics.Process]::GetCurrentProcess().ProcessorAffinity.ToInt64()
} | ConvertTo-Json -Depth 5 | Set-Content (Join-Path $Root 'native-hardware.json')

& (Join-Path $Root 'seine.exe') --version | Set-Content (Join-Path $Root 'native-version.txt')
if ($LASTEXITCODE -ne 0) { throw 'Published miner version check failed' }
& "$env:USERPROFILE\.cargo\bin\rustc.exe" +1.93.0 -vV | Set-Content (Join-Path $Root 'native-toolchain.txt')
if ($LASTEXITCODE -ne 0) { throw 'Native toolchain identification failed' }

foreach ($name in @('verify_cpu_kernels', 'verify_streaming_dispatch')) {
    Check-Admission
    $exe = Join-Path $Root "source\target\release\examples\$name.exe"
    Get-FileHash -Algorithm SHA256 $exe | Select-Object Algorithm,Hash | ConvertTo-Json |
        Set-Content (Join-Path $Root "$name.sha256.json")
    $owned = $null
    try {
        $owned = Start-Process -FilePath $exe -WorkingDirectory $Root -NoNewWindow -PassThru `
            -RedirectStandardOutput (Join-Path $Root "$name.log") `
            -RedirectStandardError (Join-Path $Root "$name.err")
        $nativeHandle = $owned.Handle
        $owned.PriorityClass = 'BelowNormal'
        $watch = [Diagnostics.Stopwatch]::StartNew()
        while (!$owned.WaitForExit(1000)) {
            Check-Admission
            if ($watch.Elapsed.TotalSeconds -gt 240) { throw "$name exceeded its 240-second deadline" }
        }
        $owned.WaitForExit()
        if ($owned.ExitCode -ne 0) { throw "$name failed with exit $($owned.ExitCode)" }
        Write-Output "$name PASS"
    } finally {
        if ($owned -and !$owned.HasExited) { $owned.Kill(); $owned.WaitForExit() }
    }
}
'Native MSVC full-size parity and kernel-switch checks passed.' |
    Set-Content (Join-Path $Root 'NATIVE-PARITY-COMPLETE')
