# Privacy policy

Last updated: November 3, 2026.

This policy describes hardware, performance and reliability telemetry in the Seine miner.
Telemetry helps the project understand which hardware is in use, identify
performance differences, diagnose failures and prioritize fixes and optimization work.

## Collection and controls

Hardware telemetry is enabled by default in releases configured with a telemetry
collector, including unattended mining. It runs in the background without a
startup prompt. Builds with no collector configured do not send telemetry.
Benchmark mode does not send reports.

To disable it, run Seine with `--telemetry off`, alongside your normal mining
arguments. This preference is saved in `seine.telemetry.json` in your Seine data
directory and applies to later launches. You can also set `enabled` to `false`
in that file. Running instances check the preference before each upload; a
request already in progress may finish. Use `--telemetry on` to enable it again.
Disabling future collection does not immediately delete earlier reports, which
expire under the retention policy below.

## Information reported

Reports contain the CPU model, vendor and architecture identifiers, supported
CPU kernels, core counts, memory capacity, operating system and version, and
whether the miner runs natively or under WSL. Report-time resource snapshots include
the allowed CPU count, OS-estimated available parallelism, effective memory limit
and available memory, including container limits where supported. They include
the miner version, build target and features, and whether it is a debug build.
Where the firmware's memory tables can be read, reports also describe the installed
memory modules: how many there are, the memory type (such as DDR5), whether they are
registered, and the lowest rated and configured speeds. Module serial and part
numbers are not read.

For active mining backends, reports include GPU model and memory where available,
the number of enabled workers or lanes, CPU kernel, requested page and affinity
policies, allocator-observed memory backing by page type, available GPU tuning
settings, hash counts and measurement durations. Reports
identify measurement windows in which settings changed or telemetry samples
were missed so those windows can be excluded from performance comparisons.
GPU backends can also report the GPU architecture (such as `gfx1100` or `sm_86`),
whether the hash kernel was built into Seine or compiled on the machine, the AMD
GPU performance level found at startup and which proof of work the backend was
mining. Reports count the backends of each type a run expected and record how long
each took to produce its first hash. They also count the AMD GPUs found, selected
and left out (integrated or unsupported), whether the AMD GPU runtime could be
loaded, and how many AMD display devices the operating system lists.

Paired CPU tuning evidence can include baseline and candidate worker/kernel
settings, per-window hashes and durations, resource snapshots, outcome and fixed
reason codes, acceptance policy, and measured switching, settling and idle costs.
Unknown costs remain unknown. An optional digest describes CPU topology and
placement without transmitting hostnames or configuration secrets. A random run
ID and run-local trial numbers prevent duplicate comparisons. Inconclusive and
aborted trials are retained too; explicit counts identify omitted evidence.
When automatic CPU tuning settles, reports can include the chosen worker count and
CPU kernel, its hash rate, how long tuning took and how many comparisons it made,
whether available memory limited it, where it started from, whether it was checking
a new release, and the rate measured at each worker count it tried.

Reliability summaries include bounded counts of warnings, errors, backend failures
and quarantines, failed mining runs and Rust panics. Each summary includes a
repository-relative source file and line (or a fixed, allowlisted dependency
source identifier), a fixed failure category, backend type
where known, occurrence counts and elapsed times within the mining run. Categories
are hints derived locally; free-form error messages, panic contents and stack
traces are never uploaded. Reports include the source revision and fingerprint,
pool-or-daemon mode, requested backend types, requested CPU threads, page and
affinity policies and whether CPU thread tuning is enabled. They also identify
truncated summaries and an unclean previous run, which may reflect a forced stop
or power loss rather than a crash. Where available, structured failure details
include a fixed reason and operation, a numeric operating-system error code and
the requested allocation size. CPU startup timeouts can include ready and expected
worker counts. Reports also include the current run phase and,
for an interrupted previous run, its last observed phase, elapsed runtime and
build revision and release. Checkpoints preserve up to eight diagnostic groups
from that run, prioritizing panics/errors and structured causes, with an indication
when groups were omitted. These checkpoints do not identify the termination cause.
They can also retain a fixed code for an observed interrupt, console close, logoff
or system shutdown request, API stop/restart, or TUI quit/thread-count restart.
Current-run summaries and interrupted-run checkpoints can include the last entered
cleanup step. This records the request and progress, not whether cleanup completed.
NVIDIA runs can include the loaded driver's CUDA compatibility version and loaded
NVRTC compiler version as bounded numbers. These are not the installed NVIDIA
display-driver release or toolkit version. Missing version queries remain absent.
Loader failures can identify an unavailable NVIDIA driver or CUDA compiler library
without uploading library search paths or error messages.
On Linux, reports can include the kernel's out-of-memory kill count and whether it
rose since an unclean previous run, which suggests but does not prove that the
miner was stopped for lack of memory. Reports count the seconds mining was paused
on work this version cannot mine.

In pool mode, reports describe the pool connection with flags and counts only:
whether the primary pool uses TLS, whether pools are reached through a SOCKS5
proxy, whether a payout memo is set, how many backup pools are configured, and how
many failovers, reconnects and nonce-range refills happened and how long was spent
on a backup pool. Update status covers whether the miner installs new releases by
itself, how many release checks succeeded or failed, how many installs succeeded or
failed, and the stage at which the latest install failed.

Seine creates a random installation ID in its data directory to distinguish
installations and avoid counting frequent reports as additional users. This ID
is not derived from a wallet, CPU, GPU or other hardware identifier. Each report
also has a random ID to prevent duplicate storage. The collector records when
reports arrive. Installation IDs make reports pseudonymous; they do not make
them anonymous.

Telemetry does not include wallet addresses or balances, payout memos, private
keys, API tokens, passwords, worker names, pool or proxy addresses, hostnames, MAC
addresses, hardware serial numbers, command lines or raw logs.

## Transmission, access and retention

Regular reports are attempted after five minutes of a mining run, followed by
hourly reports. A mining run that ends with diagnostics or paired trial evidence
can attempt a final report, including stop/cleanup context and failures before the first regular report. Reports use HTTPS, except
when explicitly configured for a local loopback test server. Collection failures
do not stop mining.

Seine keeps at most one pending diagnostic or paired-trial report (up to 32 KiB)
and a small run-state marker and checkpoint (up to 8 KiB) in its data directory.
A failed evidence upload can be retried on a later reporting cycle or launch, with the same report ID to avoid duplicate
storage. New evidence may replace an older pending report. Reports older
than 24 hours are discarded when next checked, rather than uploaded. The local
upload cooldown limits repeated attempts across launches. Ordinary rate-only
reports are not queued. Opting out suppresses pending uploads
and removes pending state when the miner can acquire it; an active reporting
worker also checks opt-out before sending. A forced termination may leave the
marker for the next launch. No crash dump is collected.

The receiving server necessarily sees the connection's IP address. IP addresses
are not part of report payloads or stored telemetry records. The supplied
deployment configuration disables request and proxy error logging that could
retain IP addresses. Operators who add a CDN or another proxy must configure
its logging consistently with this policy.

Raw reports are retained for 30 days and removed by an hourly cleanup task.
Analysis uses the retained reports to summarize hardware adoption and performance.
The collector exposes no public endpoint for individual reports. Access to the
database is limited to its operators; aggregate hardware and performance results
may be shared to explain optimization work. Reports are used for miner development,
not advertising or wallet profiling. Backups must follow the same retention limit.

The collector URL is recorded in your telemetry settings. Operators of a custom
collector control its hosting and data handling and should provide their own
policy if they change these practices.

## Scope and questions

This telemetry is separate from the connections needed to mine with a pool or
daemon, wallet operations and the miner's update checks. This policy does not
describe those services' data handling or website hosting logs.

Questions about Seine's telemetry can be directed to the
[project maintainers](https://github.com/zpalmtree/seine).

## Website analytics

The public landing pages, pool setup page and blogs on **seineminer.com** and
**bntpool.com** count page loads and clicks toward Seine downloads, platform
setup guides, pool setup or worker statistics. These are aggregate activity counts,
not unique visitors, completed downloads or confirmed miners.

Each request contains a fixed page name or article slug, its language, one action
(`view`, `download`, `pool_setup`, `setup_guide` or `worker_stats`), and a broad
referral category (`search`, `direct`, `internal` or `referral`). The browser derives
this category locally; it never sends the referring URL or search query. A missing
referrer is counted as direct. These categories do not establish an individual
visitor journey or distinguish paid from organic search. The collector identifies the source website
from the browser's Origin header. It does not store individual events, IP
addresses, wallet addresses, names, cookies, browser identifiers, full page URLs,
query strings or referrers in these counters. The script uses no browser storage
and sends requests without credentials or a referrer. Like other web requests,
the connection is visible to our hosting infrastructure and its normal access logs.

We keep daily aggregate counters for 30 days, with no person-level history or
cross-site visitor matching. Routine database backups can retain an older snapshot
for up to seven additional days. Reports require the pool operator's API key.
Counts are best effort: reloads and bots can increase them, blockers can prevent
them, and an interrupted server can lose the latest unflushed batch.

The script respects **Do Not Track** and **Global Privacy Control** when enabled
in your browser. Blocking `https://bntpool.com/api/site-events` prevents these
website counts without affecting reading or mining. On static pages, the
`site-events.js` and `site-metrics.mjs` scripts (or the legacy `article-metrics.js`)
can also be blocked; the pool dashboard includes its collector in the app bundle.
These website counters are separate from the miner's hardware telemetry described
above.
