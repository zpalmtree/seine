# Windows release signing

Windows releases can carry an Authenticode publisher signature through Azure
Artifact Signing. This is separate from the Ed25519 signature on the archive
checksums, which the updater continues to require.

## One-time setup

1. Create an Azure Artifact Signing account and complete identity validation for
   the publisher. Create a **Public Trust** certificate profile. Private Trust
   and Public Trust Test profiles are not suitable for public releases.
2. Create a Microsoft Entra application/service principal or a user-assigned Azure
   managed identity for the release job. A managed identity can be created through
   Azure Resource Manager without permission to create Entra applications.
   Grant only the **Artifact Signing Certificate Profile Signer** role at the
   certificate profile scope.
3. Add a GitHub OIDC federated credential with issuer
   `https://token.actions.githubusercontent.com`, audience
   `api://AzureADTokenExchange`, and subject
   `repo:zpalmtree/seine:environment:windows-signing`.
4. Create the GitHub environment `windows-signing`. Restrict its deployment
   branches/tags to `master` and release tags matching `v*`. Store the following
   environment variables (the IDs are identifiers, not client secrets):

   | Variable | Value |
   | --- | --- |
   | `AZURE_CLIENT_ID` | Application or managed identity client ID |
   | `AZURE_TENANT_ID` | Directory/tenant ID |
   | `AZURE_SUBSCRIPTION_ID` | Subscription containing the signing account |
   | `SEINE_SIGNING_ENDPOINT` | Regional signing URL, e.g. `https://eus.codesigning.azure.net` |
   | `SEINE_SIGNING_ACCOUNT` | Artifact Signing account name |
   | `SEINE_SIGNING_PROFILE` | Public Trust certificate profile name |
   | `SEINE_SIGNING_PUBLISHER` | Exact full certificate subject returned by `Get-AuthenticodeSignature`, including `CN`, `O`, etc. |
   | `SEINE_WINDOWS_SIGNING_ENABLED` | `true` once the profile and identity are ready |

No long-lived Azure client secret or exportable signing key is needed. Keep a
consistent publisher identity across releases. The certificate subject is checked
instead of a thumbprint because the service rotates its short-lived certificates.

Before activation, an unset or `false` enable variable leaves existing Windows
packaging unchanged and emits a workflow warning. With signing enabled, missing
configuration, signing failures, an unexpected publisher, an invalid signature,
or a missing timestamp block release publication. Other enable values also fail.

## Release order and validation

The build job tests and packages the Windows binary. The `windows-sign` job then
signs only `seine.exe`, applies a SHA-256 RFC3161 timestamp, verifies the publisher
and signature, regenerates the inner checksums, and replaces the Windows archive
artifact. NVIDIA DLLs retain their original bytes and signatures. The final
release job waits for signing before hashing and signing the finished archives.

Run **Build Release** manually on `master` after setup. A manual branch run creates
workflow artifacts and checks Windows signing without requesting release approval.
Only version tags matching `v*` enter the protected `release` environment to sign
archive checksums and publish a release. Download the manual run's Windows archive,
extract it, and verify it on Windows:

```powershell
./scripts/check_windows_signature.ps1 -Path ./seine.exe -ExpectedPublisher 'FULL CERTIFICATE SUBJECT'
```

The **Windows signing checks** workflow verifies that the check accepts a trusted,
timestamped executable and rejects an unsigned file, a modified executable, and
the wrong publisher. It does not exercise the Azure service; that requires the
configured manual release build.

For end-user validation, download the final archive through a browser on a clean
Windows 11 machine and check startup and updating. A valid signature establishes
publisher identity; it does not guarantee SmartScreen reputation or a clean
Defender verdict. Initial SmartScreen warnings may persist while reputation builds.

If a specific file is incorrectly detected, record the exact warning/detection,
release URL and SHA-256, then submit the exact executable as a software developer
to [Microsoft Security Intelligence](https://www.microsoft.com/en-us/wdsi/filesubmission).
Select the product that produced the warning (SmartScreen or Defender Antivirus).

References: [Artifact Signing setup](https://learn.microsoft.com/en-us/azure/artifact-signing/quickstart),
[GitHub OIDC integration](https://github.com/Azure/artifact-signing-action/blob/main/docs/OIDC.md),
[SmartScreen reputation](https://learn.microsoft.com/windows/apps/package-and-deploy/smartscreen-reputation).
