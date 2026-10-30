# Changelog

Short release notes for miners deciding whether to upgrade. Lead with the biggest
user benefit, name the hardware for measured gains, and include required upgrade
steps. Keep test results and implementation details in the engineering docs.
Each `## vX.Y.Z` section becomes the corresponding GitHub release body.
Never mention competitors in release notes; describe Seine improvements and
supported hardware only.

## Unreleased

- **Faster CPU mining on AMD and Intel:** about 16% on Ryzen 9000 (Ryzen 7 9700X) on Linux and 14% on Windows, 7% on Ryzen 7000 (Ryzen 7 7700X, and Ryzen 9 7950X with 32 GB) and 5% on Ryzen 5000 (Ryzen 9 5900X) on Linux; about 7% on Zen 4c and Zen 3 EPYC (EPYC 9754 and 7443) and 5% on Zen 2 EPYC, on Linux; and about 6% on 12th Gen Intel Core desktops (Core i5-12400F) on Linux, which now reach full speed in the first minute instead of after about eight.
- **AMD Radeon GPUs mine on Linux**, with RDNA3 and RDNA4 cards about twice as fast as the first HIP kernel and RDNA2 cards about 25% faster (measured on a Radeon PRO V710, a Radeon AI PRO R9700 and a Radeon PRO V620). With the ROCm HIP runtime installed, Seine mines on every discrete AMD GPU alongside the CPU with no extra flags; each hash lane uses 2 GiB of VRAM. On RDNA3 cards, run Seine as root or apply the one-line command it prints at startup to get the full speed. See [AMD GPUs](docs/guides/amd.md).
- **No pause between jobs:** NVIDIA and AMD GPUs switch to a new job in milliseconds instead of up to 1.9 s, and CPU threads start on a new block about 0.3 s sooner.
- **Runs on more rigs:** the Linux release now starts on Ubuntu 20.04 and 22.04, Debian 11 and 12 and other systems with glibc 2.31 or newer, and each release includes packages for [HiveOS](docs/guides/hiveos.md) and [mmpOS](docs/guides/mmpos.md) with hashrate and shares on their dashboards.
- **Every Blocknet pool, your way:** encrypted connections (`stratum+ssl://`), backup pools that Seine switches to and back from, a SOCKS5 proxy for networks that block pool ports, and the `-o`/`-u ADDRESS.WORKER`/`-p` flags that mining OS templates use. For farms, `--proxy-listen` lets every rig share a few pool connections and pay its own address. See [pool connections](docs/guides/configuration.md#pool-connections) and [farm proxy](docs/guides/farm-proxy.md).
- **Signed releases that keep themselves current:** Seine checks each release's signature before installing it, and a running miner installs new releases and restarts with the same settings. Pass `--no-auto-update` to only be told about new releases.
- Also: fast rigs get fresh work without reconnecting on pools that support it; Seine backs off when a pool keeps failing instead of reconnecting in a tight loop; dev-fee rounds no longer sit idle when bntpool.com is unreachable; Seine shuts down cleanly when stopped by systemd, Docker or a closed terminal; PCs with one memory module or slow RAM get a hint at startup; and the local API reports live hashrate, shares and per-GPU rates at `GET /v1/miner/stats` (see [docs/API.md](docs/API.md)).

Upgrade note: self-hosted telemetry collectors need the schema 12 update before this release.

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
