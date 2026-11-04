# Changelog

Short release notes for miners deciding whether to upgrade. Lead with the biggest
user benefit, name the hardware for measured gains, and include required upgrade
steps. Keep test results and implementation details in the engineering docs.
Each `## vX.Y.Z` section becomes the corresponding GitHub release body.
Never mention competitors in release notes; describe Seine improvements and
supported hardware only.

## Unreleased

## v0.3.1

- **Up to 2.5x faster on NVIDIA GPUs** (GTX 1660 Ti, RTX 2070, RTX 2080 Ti, RTX 3060, RTX 4060, RTX 4090, RTX 5060 Ti)
- **Up to 33% faster on CPUs** (12th Gen Intel Core, 3rd Gen Xeon, Ryzen 5000, 7000 and 9000)
- **Much faster with fewer than 8 threads**: about 90% higher hashrate on Ryzen 5 5600, 40% on Skylake Xeon servers and 25% on Ice Lake Xeons and EPYC 9754 when `--threads` is set, and full speed from the first minutes without it
- **Up to 13% higher CPU hashrate in containers with a CPU limit** (Ryzen 7 9800X3D; about 4% on Ryzen 5 7600X)
- **Up to 3% higher CPU hashrate with default settings, often on less RAM** (Ryzen 9 5900X, Ryzen 9 9900X, Core i5-12400F, Core i7-14700K)
- **AMD GPU support** (RX 6000, RX 7000, RX 9000, Radeon PRO): [setup](https://github.com/zpalmtree/seine/blob/v0.3.1/docs/guides/amd.md)
- **Exchange mining support** (mine straight to your exchange deposit address; `--payout-memo` is required): [setup](https://github.com/zpalmtree/seine/blob/master/docs/EXCHANGE_PAYOUTS.md)
- **Solo mining through pools that support it**: put `solo:` in front of your address (`-u solo:ADDRESS`) and the pool pays you the full reward of every block you find, less its fee
- **Ready for the Faraday fork** (block 103,680): pool mining moves to the new proof of work by itself, and with automatic updates on, Seine pauses and installs a newer version whenever the pool asks for one
- **Tunes faster, then stops**: finds your best thread count and kernel in minutes, keeps the lowest thread count that gets the same hashrate, and stops testing until something changes
- **A cleaner first run**: the dashboard opens right away, setup happens on screen, and the log only shows what you need, with room to read it and scrollback (↑↓, PgUp, PgDn)
- **Mining OS packages**
  - HiveOS: [seine-hiveos-0.3.1.tar.gz](https://github.com/zpalmtree/seine/releases/download/v0.3.1/seine-hiveos-0.3.1.tar.gz) ([guide](https://github.com/zpalmtree/seine/blob/v0.3.1/docs/guides/hiveos.md))
  - mmpOS: [seine-mmpos-0.3.1.tar.gz](https://github.com/zpalmtree/seine/releases/download/v0.3.1/seine-mmpos-0.3.1.tar.gz) ([guide](https://github.com/zpalmtree/seine/blob/v0.3.1/docs/guides/mmpos.md))
- **[Farm proxy](https://github.com/zpalmtree/seine/blob/v0.3.1/docs/guides/farm-proxy.md)** (run a whole farm through one pool connection)
- **Automatic updates that keep your tuning**: a new version starts on your saved settings and only tests what it changed (turn off with `--no-auto-update`)
- **Shorter `--help`** (lists the flags most miners need; every other flag still works and is in the [flag reference](https://github.com/zpalmtree/seine/blob/master/docs/MINER_FLAGS.md))
- **Pool Pending balance includes estimated rewards** from blocks that have not matured yet

**Upgrading from v0.2.45 with `--auto-update`:**
- **Linux:** v0.2.45 installs this version but then exits instead of restarting. Start seine again once; setups that restart seine automatically, such as systemd, need nothing.
- **Windows:** v0.2.45 starts this version and then exits. If a script or service restarts seine when it exits, stop it once after the update so only one seine keeps running.

## v0.3.0

- **Up to 2.5x faster on NVIDIA GPUs** (GTX 1660 Ti, RTX 2070, RTX 2080 Ti, RTX 3060, RTX 4060, RTX 4090, RTX 5060 Ti)
- **Up to 33% faster on CPUs** (12th Gen Intel Core, 3rd Gen Xeon, Ryzen 5000, 7000 and 9000)
- **AMD GPU support** (RX 6000, RX 7000, RX 9000, Radeon PRO): [setup](https://github.com/zpalmtree/seine/blob/v0.3.0/docs/guides/amd.md)
- **Exchange mining support** (mine straight to your exchange deposit address; `--payout-memo` is required): [setup](https://github.com/zpalmtree/seine/blob/master/docs/EXCHANGE_PAYOUTS.md)
- **Mining OS packages**
  - HiveOS: [seine-hiveos-0.3.0.tar.gz](https://github.com/zpalmtree/seine/releases/download/v0.3.0/seine-hiveos-0.3.0.tar.gz) ([guide](https://github.com/zpalmtree/seine/blob/v0.3.0/docs/guides/hiveos.md))
  - mmpOS: [seine-mmpos-0.3.0.tar.gz](https://github.com/zpalmtree/seine/releases/download/v0.3.0/seine-mmpos-0.3.0.tar.gz) ([guide](https://github.com/zpalmtree/seine/blob/v0.3.0/docs/guides/mmpos.md))
- **[Farm proxy](https://github.com/zpalmtree/seine/blob/v0.3.0/docs/guides/farm-proxy.md)** (run a whole farm through one pool connection)
- **Automatic updates** (turn off with `--no-auto-update`)

## v0.2.45

- **Up to about 49% faster CPU hashing on Windows when fewer workers than cores run on AMD Zen 5 EPYC** (measured on an AWS instance with 16 Zen 5 EPYC cores: +14% at 4 workers, +49% at 8): Windows now spreads workers across the processor's cache domains the way Linux already did.
- **About 8% faster CPU hashing on 4th and 5th Gen Intel Xeon servers** (measured on Xeon Platinum 8488C).
- **About 4% faster CPU hashing on AMD Zen 4 EPYC servers** (EPYC 9004 series, measured on EPYC 9124P and 9R14), and up to 12% faster at lower thread counts.
- **About 1-3% faster CPU hashing on AMD Zen 5 EPYC Linux servers** (measured on EPYC 9355 and an AWS Zen 5 EPYC instance) **and about 2% more on 4th and 5th Gen Intel Xeon Linux servers at their full thread count** (measured on Xeon Platinum 8488C).
- **More accepted shares on every machine:** work in progress when the pool sends a new job, or when a dev-fee round starts or ends, is now submitted instead of discarded.
- **Seine can update itself:** `seine --self-update` installs the newest release for your platform, and `--auto-update` keeps a running miner current and restarts it with the same settings.
- **WSL2 users are told how to unlock more workers** when the Linux VM's memory cap, not the CPU, limits the worker count: the exact `.wslconfig` setting and the native Windows build are named at startup.
- Linux no longer warns about reserving huge pages when the miner is already running on transparent huge pages.
- Miners that set `--threads` on CPUs without a validated kernel default (most Intel desktops and older Xeons) now keep their count but also try the memory-streaming kernel while mining, instead of staying on the plain kernel forever.
- Optional telemetry now records whether the worker count was set manually and how many NUMA nodes the host has, so defaults can be tuned for how miners are actually run. Self-hosted collectors need the schema 9 update before this release.

## v0.2.44

- **About 2-3% more hashes in the first 15 minutes on AMD Zen 4 and Zen 5 EPYC servers** (measured on EPYC 9R14 and 9R45): automatic tuning no longer drops workers on measurement noise.

## v0.2.43

- **Mining starts within a second on large Linux servers** (measured on a 64-core Zen 4 EPYC 9554P: 11 s to 0.5 s), instead of waiting for all worker memory to be prepared first.
- **About 5% faster on 32 GiB Windows machines with 16 or more CPU threads** (measured on a Zen 5 EPYC 9R45): automatic sizing now keeps at most 1 GiB of RAM in reserve, fitting one more 2 GiB worker.

## v0.2.42

- **About 45% faster CPU hashing on cloud VMs that report "AMD EPYC" without a specific CPU model** (AVX2 only), including with `--threads`: they now use the memory-streaming kernel.

## v0.2.41

- **About 10% more hashes in the first 15 minutes on 4th and 5th Gen Intel Xeon servers** (measured on Xeon Platinum 8488C): mining now starts on the memory-streaming kernel at every logical CPU. Other x86 CPUs can now discover it while mining.
- **Mining starts hashing in seconds instead of up to half a minute on Linux** (measured on a 32-core Zen 5 EPYC 9355: 21 s to 1.5 s), adding about 2% more hashes in the first 15 minutes.
- Linux no longer asks you to reserve HugeTLB pages when transparent huge pages are enabled; they made no measurable difference to hashrate.

## v0.2.40

- **About 40-50% faster CPU hashing on Windows with AMD Zen 2 and Zen 3 processors** (measured on EPYC 7R13): the memory-streaming kernel is now used on Windows too.
- **About 7% faster CPU hashing on AMD Zen 4 servers** (measured on EPYC 9124P), and up to about 4% on Ryzen 7000 (measured on Ryzen 7 7700X).
- **About 5% faster CPU hashing on AMD Zen 5 servers** (measured on EPYC 9355P and EPYC 9R45).
- **About 5% more hashes in the first 15 minutes on AMD Zen 4 and Zen 5 servers with SMT** (measured on EPYC 9355P): automatic tuning now starts at every logical CPU that fits in memory.
- **About 2-3% more hashes in the first 15 minutes on AMD Zen 3 to Zen 5 CPUs** (measured on EPYC 7313P and Ryzen 7 9700X): automatic tuning no longer spends its first minutes re-testing slower kernels.
- `seine --bench` thread autotuning now checks a few widely spaced counts first, so it settles on a good thread count in fewer steps.
- Faster restarts with saved CPU tuning on AMD Zen 5 EPYC 9375F servers on Linux: the fast kernel resumes promptly after memory initialization.

## v0.2.39

- **About 23% more hashes in the first 20 minutes of mining on a 32-core AMD Zen 5 server** (measured on EPYC 9R45): automatic tuning now starts AMD Zen 2 to Zen 5 CPUs at their physical core count, settles on higher worker counts when they are faster, and stops re-testing slower kernels. Steady mining there rose about 17%.
- **Up to about 6% faster CPU hashing on Linux with AMD Zen 3 processors** (measured on EPYC 7R13).
- Closing a Windows console now allows brief cleanup, and slow telemetry uploads no longer leave completed runs marked as interrupted.
- NVIDIA startup avoids panics from unused driver functions and reports missing driver or CUDA compiler libraries clearly.
- Reliability reports now include stop origins, cleanup progress and loaded CUDA software versions to help diagnose interruptions.
- Stopping during backend initialization now exits cleanly instead of reporting a startup failure.
- Fixed shutdown hangs when a dashboard keeps the control API event stream open.

## v0.2.38

- **Up to about 16% faster CPU hashing on AMD Zen 4 servers** (measured on 32-core EPYC 9R14) for systems using the default memory-streaming kernel, and up to about 3% on Ryzen 9000 (measured on Ryzen 7 9700X).
- **Up to about 8% faster CPU hashing on Linux with AMD Zen 2 and Zen 3 processors** (measured on EPYC 7R32 and EPYC 7R13).
- `seine --bench` now measures the same CPU kernel that mining uses, so benchmark results match real mining speed on AMD Zen 2 to Zen 5.

## v0.2.37

- **About 67% faster CPU hashing on Linux with AMD Zen 2 processors (measured on EPYC 7R32) and about 48% faster with Zen 3 (measured on EPYC 7443P)**, using a new memory-streaming kernel.
- **About 31% faster for miners who set `--threads` on AMD Ryzen 9000** (measured on Ryzen 7 9700X): fixed thread counts on AMD Zen 4 and Zen 5 CPUs now use the streaming kernel, and automatic tuning now starts from it on every Zen 4 and Zen 5 model.
- Seine now suggests removing `--threads` when the setting leaves more than half of the machine's cores and free memory unused.
- Slightly faster CPU hashing on 32-core AWS instances with AMD Zen 5 EPYC (EPYC 9R45, 128 GiB RAM, Linux with transparent huge pages), with saved tuning preserved.
- Better diagnostics for interrupted mining runs and startup failures, helping identify reliability issues while keeping raw logs and panic contents private.

## v0.2.36

- About 1% faster CPU hashing on 32-core AWS instances with AMD Zen 5 EPYC (EPYC 9R45, 128 GiB RAM, Linux with transparent huge pages), while retaining automatic tuning and saved settings.

## v0.2.35

- Faster mining startup on 32-core AWS instances with AMD Zen 5 EPYC (EPYC 9R45, 128 GiB RAM, Linux with transparent huge pages). Seine initializes memory faster and starts from validated worker and kernel settings while continuing to tune automatically.

## v0.2.34

- Faster mining startup on 32-thread AWS instances with 4th Gen Intel Xeon (Xeon Platinum 8488C, 128 GiB RAM, Linux with transparent huge pages). Seine starts from validated worker and kernel settings while continuing to tune automatically.

## v0.2.33

- Faster CPU hashing on 32-core AWS instances with AMD Zen 4 EPYC (EPYC 9R14, Windows with ordinary memory pages).

## v0.2.32

- **Faster CPU hashing on 32-core AWS instances with AMD Zen 4 EPYC** (EPYC 9R14, Linux with transparent huge pages).

## v0.2.31

- **Improved NVIDIA driver compatibility:** avoid a startup failure caused by a missing driver symbol on some older CUDA 12 drivers.

## v0.2.30

- **Faster startup tuning on 32-core AWS instances with AMD Zen 4 EPYC** (EPYC 9R14, Windows with ordinary memory pages). Seine spends more time on proven fast settings while continuing to tune automatically and preserving saved tuning.

## v0.2.29

- **Faster startup tuning on 32-core AWS instances with AMD Zen 4 EPYC** (EPYC 9R14, Linux with transparent huge pages). Seine spends more time on proven fast settings while continuing to tune automatically and preserving saved tuning.

## v0.2.28

- **Faster tuning on 32-core AWS instances with AMD Zen 4 EPYC** (EPYC 9R14, Linux with transparent huge pages). Shorter comparisons spend less time on slower settings while preserving confirmed saved tuning.

- **Improved CPU hashing on 32-core AWS instances with AMD Zen 4 EPYC** (EPYC 9R14, Windows with ordinary memory pages). Saved tuning remains usable, and Seine continues comparing settings while mining.

## v0.2.27

- **Faster CPU mining on 32-core AWS instances with AMD Zen 4 EPYC** (EPYC 9R14, Linux with transparent huge pages). Seine starts from the improved hashing path, preserves confirmed local tuning, and continues checking for faster settings.

- **Faster mining startup on 32-core AWS instances with AMD Zen 4 EPYC** (EPYC 9R14): Linux and Windows start from validated worker and kernel settings while continuing to tune. Linux also initializes supported huge-page memory faster.

- **One miner fee everywhere:** a flat 2% dev fee on every pool and in solo mode replaces the previous 1% bntpool.com / 2.5% elsewhere rates. Upgrade to use the new rate.

- **More useful failure reports:** optional telemetry preserves specific memory and pool connection failure reasons, Windows code locations, and the last observed stage of interrupted runs.

**For self-hosted telemetry collectors:** update the collector before upgrading miners to this version. The official collector is already updated; see the [deployment guide](telemetry/README.md#deploy).

- **Clearer tuning startup:** CPU and NVIDIA messages explain when saved tuning is reused and why tuning runs again.

- **Peak hashrate by default:** CPU tuning targets the highest confirmed hashrate; choosing fewer workers at a lower rate is opt-in with `--cpu-profile efficiency`.
- **More accurate CPU kernel benchmarks:** hashing speed is measured separately from memory setup and cleanup. Regenerate saved CPU kernel benchmark baselines before comparing runs.

## v0.2.26

- **Help diagnose mining failures:** privacy-preserving warning and error summaries now identify affected hardware and code paths, including early failures and unclean restarts, while honoring your telemetry opt-out.
- **Help improve mining on your hardware:** fixes missing CPU and GPU performance reports during pool mining and includes memory and CPU settings so future optimizations can target comparable machines. Your telemetry opt-out preference is preserved.

**For self-hosted telemetry collectors:** update the collector before upgrading
miners to this release. The official collector is already updated. See the
[deployment guide](https://github.com/zpalmtree/seine/blob/v0.2.26/telemetry/deploy/PRODUCTION.md).

## v0.2.25

- **Faster startup and restarts on AMD Zen 5 EPYC 9355P servers.**
- **About 8% faster mining after a restart on AWS instances with AMD Zen 4 EPYC** (EPYC 9R14) with saved tuning settings.
- **Fewer lost shares when the pool is busy:** Seine retries temporary rejections automatically.
- **Fewer duplicate-share rejections** when returning from a dev-fee round.
- **Safer thread controls:** the dashboard checks available RAM before adding CPU threads and explains when an increase is blocked.
- AMD-only source builds now work without NVIDIA support.

## v0.2.24

- **About 33% higher CPU hashrate on Ryzen 9 9950X3D at 8 threads**, automatically.
- **Cleaner dashboard logs**, with less routine share and job-switch noise.
- **Clearer tuning updates** show which settings are faster.

## v0.2.23

- **Up to 57% higher CPU hashrate on AWS Graviton4**, 49% on Graviton3 and about 4% on Graviton2.
- **Up to 20% higher hashrate after tuning on AWS instances with AMD Zen 5 EPYC** (EPYC 9R45), and about 5% with 2nd Gen Intel Xeon (Xeon Platinum 8275CL).
- **CPU mining recovers automatically when RAM becomes available.** Seine also starts more reliably on Linux systems with tight thread limits.
- **Protected miner controls:** websites you visit can no longer change your mining settings through an unauthenticated local control API.
- **Update notices** tell you when a newer release is available. Use `--disable-update-check` to turn them off.

**For control API users:** send `Authorization: Bearer <token>` on requests other
than `/v1/health`. Read the token from `<data-dir>/control-api.cookie` after each
start, or set a fixed token with `--api-token`. Browser integrations also need
`--api-cors <origin>`. Plain CLI and dashboard mining need no changes.

## v0.2.22

- **About 30% higher CPU hashrate on AWS instances with AMD Zen 5 EPYC at 24 threads** (EPYC 9R45, huge pages). Seine selects the faster settings automatically.
- **More reliable automatic tuning**, including when you change which CPU cores Seine can use on Windows.
- **Ready-to-run Linux ARM64 downloads**, with no source build needed.

## v0.2.21

- **Start mining sooner.** CPU tuning now runs while you mine.
- **Keep mining as conditions change.** Seine adjusts CPU threads to available RAM and restores them when memory returns.
- **Automatic CPU speed tuning** finds faster settings for your hardware without manual configuration.
- **Steadier efficiency mode** keeps a lower thread count when it delivers close to peak hashrate.

## v0.2.20

- **More reliable CPU tuning** finds suitable thread counts and avoids getting stuck during startup.
- **Compare CPU mining options on your own hardware** with expanded benchmark controls.

**For benchmark users:** capture a new CPU baseline before comparing results
with this release. See the [benchmark guide](https://github.com/zpalmtree/seine/blob/v0.2.20/docs/benchmarks.md) for options.

## v0.2.19

- **Adjust CPU threads from the dashboard** with `+` and `-`, while keeping your session totals. Mining reconnects briefly; changes last for the current run and turn off automatic thread tuning.
- **Use Seine in smaller terminals** with a compact dashboard that keeps key controls and hashrate visible.
- **Read Windows startup errors** before the window closes when launching `seine.exe` by double-clicking.

## v0.2.18

- **Faster CPU mining on AMD Zen 3 and other AVX2 processors.**
- **Better use of CPU cores on Linux**, especially on large servers.
- **More RAM left for other apps:** automatic CPU sizing reserves 5% of system RAM. Use `--cpu-memory-reserve-percent 0` to remove the reserve.

## v0.2.17

- **More reliable pool statistics when the pool is busy**, especially for several rigs sharing one internet connection.
- **Less network traffic** from routine dashboard updates.

## v0.2.16

- **Mine with your Blocknet ID**, using `--address '@name'` or `--address '$name'` instead of a full wallet address. Keep the quotes in shell commands.
- **Use every NVIDIA GPU automatically.** AMD builds also use all supported GPUs when started with `--backend amd`.
- **Faster repeat starts on multi-GPU rigs:** each GPU's tuning settings are saved reliably.
- **See available GPUs through the control API** for easier dashboard and script setup.

**For multi-GPU scripts:** use `--nvidia-devices` or `--amd-devices` to select
specific GPUs. Match `--backend-*-per-instance` lists to the number of selected
GPUs. In control API `backend_specs`, set `device_index` to select one GPU;
omitting it now selects all detected GPUs.

## v0.2.15

- **Start NVIDIA mining on Windows without installing the CUDA toolkit.** Unzip and mine.
- **About half the first-start tuning time on RTX 5090**, with settings saved for later launches.
- **More CPU performance options on Linux and WSL** through huge-page support. See the [setup guide](https://github.com/zpalmtree/seine/blob/v0.2.15/README.md#wsl-linux-hugetlb-provisioning) before using `--cpu-page-mode large` or `large-1g`.
- **Clear warnings if a CPU or GPU stops mining**, so reduced performance is easier to spot.
- **More reliable solo block submission after a daemon restart.**
- Improved automatic CPU tuning on Apple Silicon and CPU core selection on Windows.

## v0.2.14

- **Keep the blocks your GPU finds.** Fixes a case where a block solution could be lost when found alongside a pool share.
- Pools can now identify your Seine version and mining hardware type to help with support and updates.

## v0.2.13

- **Catch invalid payout addresses before mining starts**, with a clear error instead of repeated pool connection failures.

## v0.2.12

- **More reliable pool statistics** when the pool limits requests.
- Improved CPU hashing performance on processors with AVX-512 support.

## v0.2.11

- **Keep NVIDIA GPUs mining when the pool adjusts share difficulty**, reducing interruptions and wasted work.

## v0.2.10

- **More reliable share handling** when pool difficulty changes or share confirmations arrive late.

## v0.2.9

- **Keep mining after finding a share.** Fixes CPU and GPU mining getting stuck after a submission.
- Clearer hardware names in mining logs.

## v0.2.8

- **More reliable NVIDIA pool mining** when new jobs arrive.
- Better recovery from pool connection problems and steadier dashboard hashrate readings.

## v0.2.7

- **Steadier NVIDIA hashrate readings** make the dashboard easier to follow.

## v0.2.6

- **Automatic AVX-512 support** lets compatible CPUs use the faster hashing option without a custom build.
- **More reliable pool statistics** through automatic pool API discovery.

## v0.2.5

- **Faster NVIDIA mining**, with improved automatic tuning for RTX 50-series and other Blackwell GPUs.
- **See your pool and local wallet balances together** while pool mining.
- **Easier solo setup:** Seine finds wrapper-managed Blocknet daemons and waits for the wallet to become ready.

## v0.2.4

- **Keep pool mining moving after a share submission**, with more reliable continuation of the current job.
- See pool block notifications while mining.

## v0.2.3

- **Resume mining immediately after finding a share**, without repeating work already done.
- **Keep your own pool account details on screen** during dev-fee rounds.

## v0.2.2

- **Mine on more ARM devices**, including Raspberry Pi 4.
- **More reliable pool submissions and reconnects.**
- **A lower 1% dev fee on bntpool.com.**
- A better-fitting dashboard in narrow terminals.

Thanks to @donuts-are-good for the ARM compatibility fix.

## v0.2.0

- **Pool mining is here.** Connect to bntpool.com without running a local daemon.
- **Guided first-run setup** saves your pool and payout settings for next time.
- **Follow pool mining from the dashboard**, including network hashrate and accepted shares.
- Keep solo mining available when you want to use your own daemon.

## v0.1.12

- **More accurate hashrate readings** during startup and pauses while the daemon is unavailable.
- **Better automatic CPU thread sizing** when huge pages are reserved.
- Clearer handling of incorrect wallet passwords.

## v0.1.11

- **Network hashrate matches the explorer**, with clearer dashboard number formatting.

## v0.1.10

- **Choose your payout address** with `--address`, with validation before mining starts.
- **Spend less time on CPU tuning** at startup.
- **Clearer mining status**, including network hashrate, startup progress and version information.
- **Simpler source builds** with a Makefile that places the finished binary in the project root.

Thanks to @donuts-are-good for the build improvements.

## v0.1.9

- **Support for more older NVIDIA GPUs** through a more compatible startup path.
- A quieter dashboard shows dev-fee status without extra log messages.

## v0.1.8

- **Easier-to-read CPU memory setup warnings** in the dashboard.

## v0.1.7

- **Verify your full payout address at startup** in the logs.

## v0.1.6

- **See wallet balances and stale-share statistics** in the dashboard.
- **Read complete log messages** with wrapping instead of truncation.
- **Control Seine from other apps and scripts** through the new control API.
- Improved automatic CPU tuning on macOS.

## v0.1.5

- **Clearer huge-page setup guidance** helps you configure CPU mining memory.
- **Easier source builds** with stable Rust support.

## v0.1.4

- **Smoother CPU and NVIDIA mining** with reduced overhead.
- **More useful GPU hashrate readings** between batches of work.
- **Clearer daemon connection and sync status**, with a dashboard that fits your terminal width.

## v0.1.3

- **Automatic CPU tuning stays closer to peak hashrate.**
- **Clearer stale-block reporting** when the chain advances before submission.

## v0.1.2

- **Faster CPU mining on Apple Silicon and x86-64**, with improved automatic hardware selection.
- **Start CPU mining while NVIDIA GPUs initialize**, and reuse saved GPU setup on later launches.
- **Easier daemon connection setup** through automatic discovery and clearer guidance.
- **Stop startup or tuning with one Ctrl+C.**
- **More accurate average hashrate and stale-block reporting.**

**For existing configurations:** use `--daemon-dir` for the daemon's cookie
location. `--data-dir` now stores Seine's own settings.

## v0.1.1

- **Stop automatic tuning cleanly with Ctrl+C.**
- **Read long log messages more easily** in the terminal.
