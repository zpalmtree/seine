$ErrorActionPreference = 'Stop'
$check = Join-Path $PSScriptRoot 'check_windows_signature.ps1'
$signedFile = Join-Path $PSHOME 'powershell.exe'
if (!(Test-Path -LiteralPath $signedFile)) { $signedFile = Join-Path $PSHOME 'pwsh.exe' }
$publisher = (Get-AuthenticodeSignature -LiteralPath $signedFile).SignerCertificate.Subject
$temp = Join-Path ([IO.Path]::GetTempPath()) ('seine-signature-test-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temp | Out-Null

function Assert-Rejected([string]$File, [string]$Publisher, [string]$Reason) {
    $rejected = $false
    try { & $check -Path $File -ExpectedPublisher $Publisher | Out-Null }
    catch {
        if ($_.Exception.Message -notlike "*$Reason*") { throw }
        $rejected = $true
    }
    if (!$rejected) { throw "Signature check unexpectedly accepted: $File ($Reason)" }
}

try {
    & $check -Path $signedFile -ExpectedPublisher $publisher | Out-Null
    Assert-Rejected $signedFile 'CN=Wrong Publisher' 'Unexpected Windows publisher'

    $unsigned = Join-Path $temp 'unsigned.exe'
    [IO.File]::WriteAllText($unsigned, 'unsigned test file')
    Assert-Rejected $unsigned $publisher 'Invalid Windows signature'

    $tampered = Join-Path $temp 'tampered.exe'
    Copy-Item -LiteralPath $signedFile -Destination $tampered
    $bytes = [IO.File]::ReadAllBytes($tampered)
    # Change the DOS stub without modifying the PE signature table.
    $bytes[0x40] = $bytes[0x40] -bxor 1
    [IO.File]::WriteAllBytes($tampered, $bytes)
    Assert-Rejected $tampered $publisher 'Invalid Windows signature'
    Write-Output 'Windows signature checks passed: valid, wrong publisher, unsigned, tampered.'
} finally {
    Remove-Item -LiteralPath $temp -Recurse -Force
}
