param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$ExpectedPublisher
)
$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($ExpectedPublisher)) {
    throw 'ExpectedPublisher must be the full certificate subject from the signing profile.'
}
$file = Get-Item -LiteralPath $Path
if ($file.PSIsContainer) { throw 'Path must identify an executable file.' }
$signature = Get-AuthenticodeSignature -LiteralPath $file.FullName
if ($signature.Status -ne 'Valid') {
    throw "Invalid Windows signature for ${Path}: $($signature.Status) - $($signature.StatusMessage)"
}
if ($signature.SignerCertificate.Subject -cne $ExpectedPublisher) {
    throw "Unexpected Windows publisher: $($signature.SignerCertificate.Subject)"
}
if ($null -eq $signature.TimeStamperCertificate) {
    throw "Windows signature has no timestamp: $Path"
}

[pscustomobject]@{
    file = $file.Name
    sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash.ToLowerInvariant()
    publisher = $signature.SignerCertificate.Subject
    timestampAuthority = $signature.TimeStamperCertificate.Subject
} | ConvertTo-Json
