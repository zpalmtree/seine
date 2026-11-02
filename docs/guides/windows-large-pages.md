# Windows large pages

Native Windows CPU workers first try `VirtualAlloc` with large pages, then safely
fall back to regular pages. No setup is required to mine; large pages are an
optional extra.

## Enable large pages

Large-page use requires the mining account to hold **Lock pages in memory**
(`SeLockMemoryPrivilege`):

1. Open `secpol.msc` → *Local Policies* → *User Rights Assignment*.
2. Add your user to **Lock pages in memory**.
3. Sign out and back in (the right only applies to new logon sessions).

`secpol.msc` ships with Windows Pro, Enterprise and Education. On Home editions,
grant the right with an equivalent tool such as `ntrights`.

Seine never grants the right or triggers a sign-out itself.

## If Seine says Windows doesn't have enough free memory

Your account already has the right, but Windows can't find enough free memory for
large pages. This is common once a PC has been running for a while. Restart your
PC and start Seine before other apps.

## Page modes

- `--cpu-page-mode auto` (default): try large pages, fall back to regular pages.
- `--cpu-page-mode large`: require large pages for every worker; startup fails with
  the Win32 allocation error if the privilege or enough large-page memory is
  unavailable.
- `--cpu-page-mode regular`: a matched ordinary-page control for benchmarks.

The Linux-only `large-1g` mode is rejected on Windows. macOS supports `auto` and
`regular`; explicit `large` and `large-1g` modes are rejected there because there is
no equivalent allocator contract.

## CPU placement

Native Windows `--cpu-affinity auto` uses complete physical cores before SMT
siblings. If Windows returns incomplete or contradictory topology data, Seine keeps
the original logical-CPU order. Cached tuning is refreshed when the permitted CPU
mask or physical-core grouping changes.

## GPU

The Windows release archive bundles the CUDA 12.8.61 NVRTC runtime DLLs needed by
the NVIDIA backend; see [NVIDIA GPUs](nvidia.md).
