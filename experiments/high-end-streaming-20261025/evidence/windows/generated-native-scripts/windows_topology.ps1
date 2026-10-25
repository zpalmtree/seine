param([Parameter(Mandatory=$true)][string]$Output)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $Output)
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class SeineTopology {
    [DllImport("kernel32.dll", SetLastError=true)]
    static extern bool GetLogicalProcessorInformation(IntPtr buffer, ref uint length);
    public static byte[] Read() {
        if (IntPtr.Size != 8) throw new Exception("Requires native x64 process");
        uint length=0;
        GetLogicalProcessorInformation(IntPtr.Zero, ref length);
        if (length==0 || length>1048576) throw new Exception("Invalid topology size");
        IntPtr p=Marshal.AllocHGlobal((int)length);
        try {
            if (!GetLogicalProcessorInformation(p, ref length)) throw new Exception("Topology query failed: " + Marshal.GetLastWin32Error());
            byte[] bytes=new byte[length];
            Marshal.Copy(p,bytes,0,(int)length);
            return bytes;
        } finally { Marshal.FreeHGlobal(p); }
    }
}
'@
@{
    utc=[DateTime]::UtcNow.ToString('o')
    raw=[Convert]::ToBase64String([SeineTopology]::Read())
    process_affinity=[Diagnostics.Process]::GetCurrentProcess().ProcessorAffinity.ToInt64()
    logical_processors=[Environment]::ProcessorCount
} | ConvertTo-Json | Set-Content $Output
