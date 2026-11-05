# Keyword ownership map

Shared priorities and cross-site ownership: [publishing plan](cross-site-publishing-plan.md).
Broader Blocknet coverage is open to both sites when the reader job is distinct.

Which seineminer.com URL owns which search intent. It records editorial intent, not current
rankings. The build fails if a published post has no row here.

## Rules

1. Each URL owns one primary keyword cluster and one distinct search job.
2. Read this map before proposing, drafting, renaming or retargeting a post.
3. A supporting phrase may appear naturally on related pages, but only its owner optimizes its
   title, H1, description and core answer around it.
4. Broad hubs summarize and route; narrow spokes answer one task in depth. Their jobs differ.
5. Don't retarget a URL away from its cluster to chase a broader term. Add a new URL only when
   the intent is genuinely different; otherwise strengthen the owner.
6. An ownership change updates this file in the same commit, with the old owner, new owner,
   overlap risk and redirect plan.
7. Translations inherit their English row. A Chinese-only target needs its own row naming the
   Chinese phrase (for example 挖矿 / 挖礦 variants).

## Name collisions

"Blocknet" also names an older project (BLOCK token) and "BNT" is Bancor's ticker, so titles
pair "Blocknet" with "mining", "miner" or "Argon2id" rather than standing alone. Posts don't
explain the collision to readers (owner decision 2026-10-25: assume the reader knows Blocknet).

## Main site and blog boundaries

The homepage `/` owns **Seine download / Blocknet miner software**: supported operating
systems, release downloads, a short setup summary and measured performance highlights.
`/#download`, `/#start` and `/#performance` are sections of that same canonical page,
not separate landing pages. The guides own detailed installation, wallet setup,
hardware sizing and tuning. Homepage links route readers to those existing owners
in their selected language; do not create parallel download or tuning articles.

## Current ownership

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| mine blocknet on linux, linux blocknet miner | `/blog/mine-blocknet-on-linux/` | Install the native Linux release: choose x86_64/ARM64, extract, launch, enter a receiving address and confirm accepted shares in My Stats | seine linux setup, glibc 2.39, Linux ARM64 CPU mining, Linux 下挖 Blocknet / Linux 下挖礦 |
| how to mine blocknet | `/blog/how-to-mine-blocknet/` | Get a newcomer from zero to accepted shares on any OS, and set expectations for payouts and hashrate | blocknet mining guide, blocknet miner, mine Blocknet BNT, bntpool setup, Blocknet Argon2id miner, 如何挖 Blocknet |
| what is blocknet | `/blog/what-is-blocknet/` | Explain what Blocknet is and does for someone new, then route to names, privacy and mining | Blocknet privacy coin, Blocknet cryptocurrency, Blocknet fair launch, Blocknet Faraday upgrade, Blocknet 是什么 |
| blocknet privacy | `/blog/blocknet-privacy-features/` | Explain each layer of Blocknet's mandatory privacy and what it means when you pay | blocknet ring signatures, blocknet stealth address, blocknet ring size, Blocknet 隐私 |
| blocknet id | `/blog/blocknet-id-pay-by-name/` | Claim a Blocknet ID shortname and get paid to `@name` (wallet, payment links, pool payouts) | blocknet.id, blocknet shortname, pay by name blocknet, mine to @name, Blocknet ID 短名称 |
| argon2id proof of work | `/blog/argon2id-proof-of-work-explained/` | Explain how Blocknet's Argon2id PoW works and why RAM and bandwidth decide hashrate | argon2id mining, argon2id parameters, memory-hard proof of work, blocknet 2 GB per thread, Argon2id 工作量证明 |
| best hardware for blocknet mining | `/blog/blocknet-mining-hardware-hashrate/` | Pick or size a mining machine: fastest hardware, threads per GB of RAM, expected H/s, CPU and GPU together | blocknet hashrate, blocknet mining RAM per thread, best CPU for Blocknet, Argon2id mining hardware, 挖 Blocknet 用什么硬件 |
| mine blocknet on mac | `/blog/mine-blocknet-on-mac/` | Download and launch on Apple Silicon or Intel, confirm accepted shares, then explore Mac performance | blocknet apple silicon, blocknet m4 max hashrate, mine blocknet macbook, seine macos, 在 Mac 上挖 Blocknet |
| blocknet memo | `/blog/blocknet-encrypted-memos/` | Add an encrypted memo (invoice, order number, note) to a Blocknet payment, know who can read it, and find received memos | blocknet encrypted memo, blocknet payment memo, blocknet send memo, blocknet memo length, Blocknet 加密备注 / 加密備註 |
| blocknet payment link | `/blog/blocknet-payment-links/` | Write a `blocknet://` or bntpay.com link that requests an amount (with memo and `$name`) and know what pasting it does | bntpay link, blocknet uri, blocknet://, request BNT payment, blocknet invoice link, Blocknet 付款链接 / 付款連結 |
| huge pages blocknet mining | `/blog/huge-pages-blocknet-mining/` | Reserve huge pages (Linux/WSL 2 MiB and 1 GiB) or Windows large pages for seine: why they help Argon2id, pages per thread, exact commands, page modes | blocknet hugepages, seine hugepages, vm.nr_hugepages mining, 1GB huge pages argon2id, windows large pages mining, lock pages in memory seine, Blocknet 大页内存 / 大分頁 |
| mine blocknet on servers | `/blog/mine-blocknet-on-servers/` | Run seine on many-core servers and rigs: NUMA/L3 placement and its measured gains, headless runs, `--service`, the control API, live events and Prometheus | blocknet mining server, blocknet NUMA, EPYC blocknet mining, seine headless, seine control API, seine Prometheus metrics, 在服务器上挖 Blocknet / 在伺服器上挖 Blocknet |
| get a blocknet wallet for mining | `/blog/blocknet-wallet-for-mining/` | Create an official Blocknet wallet, back up the seed, get the address to mine to and watch pool payouts arrive | blocknet address, blocknet recovery seed, blocknet wallet for mining, 获取 Blocknet 钱包地址 / 取得 Blocknet 錢包地址 |
| seine dashboard | `/blog/seine-dashboard-explained/` | Read seine's terminal dashboard: every panel and label, live auto-tuning in the log, keyboard thread controls | seine hashrate current vs average, seine accepted rejected shares, seine change threads while mining, seine tui, seine --ui plain, seine 仪表盘 / 儀表板 |
| what is a privacy coin | `/blog/what-is-a-privacy-coin/` | Explain what a privacy coin is for any coin: what a transparent chain reveals, how privacy coins hide sender, receiver, amount and network origin, and why privacy by default matters, with Blocknet as the worked example | privacy coin meaning, privacy cryptocurrency, how privacy coins work, private by default crypto, privacy coin vs bitcoin, 什么是隐私币 / 什麼是隱私幣 |
| ring signatures explained | `/blog/ring-signatures-explained/` | Explain how ring signatures work for any coin (signer hidden among public keys, key images, LSAG to CLSAG, ring size), with Blocknet's ring of 16 as the example | what is a ring signature, how do ring signatures work, linkable ring signature, key image, CLSAG ring signature, ring size, 环签名 / 環簽章 |
| stealth addresses explained | `/blog/stealth-addresses-explained/` | Explain how stealth (one-time) addresses work for any coin: view and spend keys, the sender/receiver math, view-only wallets and wallet scanning, with Blocknet as the example | stealth address, one-time address, what is a view key, private view key vs spend key, view-only wallet, how wallets scan for payments, 隐身地址 / 隱身地址, 查看密钥 / 檢視金鑰 |
| bulletproofs explained | `/blog/bulletproofs-explained/` | Explain Pedersen commitments and Bulletproofs range proofs for anyone, then show how Blocknet verifies hidden amounts | pedersen commitment, confidential transactions, range proof, bulletproofs size, hidden transaction amounts, Bulletproofs 详解 / 詳解, Pedersen 承诺 / 承諾, 范围证明 / 範圍證明 |
| dandelion++ explained | `/blog/dandelion-plus-plus-explained/` | Explain Dandelion++ stem and fluff relay for any coin, why it hides the sending node, what the 2018 paper changed, and Blocknet's relay settings | dandelion++, dandelion protocol crypto, stem and fluff, transaction relay privacy, blocknet dandelion++, Dandelion++ 交易中继 / 交易中繼 |
| proof of work vs proof of stake | `/blog/proof-of-work-vs-proof-of-stake/` | Explain neutrally how PoW and PoS choose blocks, stay secure and issue coins, and why Blocknet uses memory-hard PoW | pow vs pos, difference between proof of work and proof of stake, proof of stake explained, is blocknet proof of work, 51% attack, 工作量证明和权益证明的区别 / 工作量證明與權益證明的差異 |
| how crypto mining works | `/blog/how-crypto-mining-works/` | Explain proof-of-work mining in plain words (blocks, hashes, nonce, difficulty, rewards, pools and shares) with Blocknet as the worked example, then route to the mining guide | how does cryptocurrency mining work, what is crypto mining, mining difficulty explained, what is a mining pool share, block reward explained, 加密货币挖矿原理 / 加密貨幣挖礦原理 |
| cpu mining 2026 | `/blog/cpu-mining-2026/` | Explain CPU mining in 2026: why memory-hard coins favor ordinary computers, what to check in a CPU coin, what makes a CPU fast, and how to start with seine | what to mine with a cpu, cpu mineable coins, memory-hard coin cpu, is cpu mining dead, CPU 挖矿 / CPU 挖礦, 用 CPU 挖什么币 |
| mine crypto on old pc or laptop | `/blog/mine-crypto-on-old-pc-or-laptop/` | Decide whether a spare old desktop or laptop can mine, size threads to its RAM, keep it cool, and start safely with seine on bntpool.com | mining on an old laptop, mine crypto on laptop, old computer crypto mining, laptop mining 8GB RAM, seine efficiency profile, 用旧电脑挖矿 / 用舊電腦挖礦, 笔记本电脑挖矿 / 筆記型電腦挖礦 |
| seed phrase explained | `/blog/seed-phrase-explained/` | Explain what a wallet seed (recovery) phrase is, how it becomes keys, how to store it and why never to share it, with Blocknet's 12-word seed as the example | what is a seed phrase, recovery phrase, mnemonic phrase, 12-word seed, how to store seed phrase, BIP39 explained, blocknet recovery seed, 助记词是什么 / 助記詞是什麼 |

| mine blocknet on windows | `/blog/mine-blocknet-on-windows/` | Download and extract the Windows ZIP, open seine.exe, enter a receiving address and find accepted shares in My Stats | Windows Blocknet miner, seine Windows setup, Blocknet NVIDIA Windows, 在 Windows 上挖 Blocknet |
| blocknet gpu mining; blocknet NVIDIA mining; mine blocknet RTX 5090 | `/blog/mine-blocknet-nvidia-gpu/` | Prepare the NVIDIA runtime, choose GPU-only or CPU-plus-GPU mining, verify accepted GPU shares, and compare the measured RTX 5090 modes | NVIDIA Blocknet setup, Blocknet GPU VRAM, CPU and GPU mining together |
| blocknet AMD mining; mine blocknet Radeon; blocknet RX 7900 / RX 9070 mining | `/blog/mine-blocknet-amd-gpu/` | Install the AMD driver (Windows) or amdgpu + ROCm HIP runtime (Linux), choose AMD cards and lanes, confirm each `amd#` row is hashing, fix a missing card | AMD GPU Blocknet setup, ROCm Blocknet, Radeon PRO mining, HIP runtime |
| blocknet HiveOS; blocknet mmpOS; seine HiveOS flight sheet | `/blog/mine-blocknet-hiveos-mmpos/` | Add seine to HiveOS (flight sheet, custom miner URL) or mmpOS (imported profile), what the package mines on, adding CPU mining and other arguments, upgrading by URL | HiveOS custom miner Blocknet, mmpOS BNT profile, mining OS Blocknet |
| blocknet mining farm; mining proxy Blocknet; seine farm proxy | `/blog/blocknet-mining-farm-proxy/` | Run many rigs through one seine `--proxy-listen` proxy: start it, point rigs at it, payouts per rig address (16 max), per-rig pool stats, solo rigs, security | stratum proxy Blocknet, one pool connection for a farm, mining farm setup |

| windows defender crypto miner; antivirus blocks miner; verify seine download | `/blog/antivirus-flags-crypto-miner/` | Identify a blocked-download warning, verify the exact official Windows archive and gather evidence for an unresolved detection | SmartScreen miner warning, seine SHA-256, miner quarantine |

## Boundaries

| Broad owner | Narrow owner | Boundary |
|---|---|---|
| `/blog/how-to-mine-blocknet/` | `/blog/blocknet-mining-hardware-hashrate/` | The guide shows one best-per-generation table and links out; the hardware post owns buying advice and full per-chip results. |
| `/blog/what-is-blocknet/` | `/blog/blocknet-privacy-features/`, `/blog/blocknet-id-pay-by-name/` | The overview summarizes privacy and names in a paragraph each and links out; the narrow posts own the mechanics. |
| `/blog/blocknet-mining-hardware-hashrate/` | `/blog/mine-blocknet-on-mac/` | The hardware post has one Mac row; the Mac post owns macOS setup and thread sizing by Mac memory. |
| `/blog/argon2id-proof-of-work-explained/` | `/blog/blocknet-mining-hardware-hashrate/` | The explainer owns why memory decides hashrate; the hardware post owns which machine to buy or use. |
| `/blog/blocknet-privacy-features/` | `/blog/blocknet-encrypted-memos/` | The privacy post explains in one section who can read a memo and why it is padded; the memos post owns adding, sending and reading memos. |
| `/blog/blocknet-id-pay-by-name/` | `/blog/blocknet-payment-links/` | The Blocknet ID post shows the two link forms with a name in one section; the payment-links post owns the full link format, encoding and the paste-to-confirm flow. |
| `/blog/blocknet-payment-links/` | `/blog/blocknet-encrypted-memos/` | The payment-links post says a link can carry a memo; the memos post owns memo encryption, padding and the `send` memo argument. |
| `/blog/how-to-mine-blocknet/` | `/blog/blocknet-wallet-for-mining/`, `/blog/seine-dashboard-explained/` | The guide has one bullet on getting an address and one line on the dashboard; the narrow posts own the wallet walkthrough and the panel-by-panel dashboard tour. |
| `/blog/blocknet-wallet-for-mining/` | `/blog/blocknet-id-pay-by-name/` | The wallet post mentions shortnames in one sentence; the Blocknet ID post owns names. |
| `/blog/argon2id-proof-of-work-explained/`, `/blog/blocknet-mining-hardware-hashrate/`, `/blog/how-to-mine-blocknet/` | `/blog/huge-pages-blocknet-mining/` | The broader posts keep a one-line huge-pages tip and link out; the huge-pages post owns sizing, commands and page modes. |
| `/blog/blocknet-mining-hardware-hashrate/` | `/blog/mine-blocknet-on-servers/` | The hardware post owns which machine to buy; the servers post owns running seine on big machines and rigs (placement, headless, control API). |
| `/blog/mine-blocknet-on-servers/` | `/blog/huge-pages-blocknet-mining/` | The servers post notes that placement and huge pages stack and that pools must be node-local; the huge-pages post owns reserving pages. |
| `/blog/what-is-a-privacy-coin/` | `/blog/blocknet-privacy-features/` | The privacy-coin explainer owns the coin-agnostic concept and gives Blocknet one bulleted summary; the privacy post owns Blocknet's layers in depth and "blocknet privacy". |
| `/blog/what-is-blocknet/` | `/blog/what-is-a-privacy-coin/` | The overview answers "is Blocknet a privacy coin?" in a paragraph and links out; the explainer owns what a privacy coin is in general. |
| `/blog/what-is-a-privacy-coin/` | `/blog/ring-signatures-explained/`, `/blog/stealth-addresses-explained/`, `/blog/bulletproofs-explained/`, `/blog/dandelion-plus-plus-explained/` | The privacy-coin explainer gives each technique one paragraph and links out; each explainer owns its mechanism. |
| `/blog/blocknet-privacy-features/` | `/blog/ring-signatures-explained/` | The privacy post keeps one section on Blocknet's ring of 16 and "blocknet ring signatures"; the explainer owns how ring signatures, key images and CLSAG work for any coin. |
| `/blog/blocknet-privacy-features/` | `/blog/stealth-addresses-explained/` | The privacy post keeps "blocknet stealth address" in two paragraphs; the explainer owns the key math, view keys, view-only wallets and scanning. |
| `/blog/blocknet-privacy-features/` | `/blog/bulletproofs-explained/` | The privacy post keeps one section on hidden amounts; the explainer owns Pedersen commitments, range proofs and the Bulletproofs construction. |
| `/blog/blocknet-privacy-features/` | `/blog/dandelion-plus-plus-explained/` | The privacy post keeps a short Dandelion++ section; the explainer owns stem and fluff relay, the 2018 paper and Blocknet's full relay settings. |
| `/blog/stealth-addresses-explained/` | `/blog/blocknet-wallet-for-mining/`, `/blog/blocknet-id-pay-by-name/` | The explainer covers `viewkeys` and seed recovery in a paragraph each and mentions names; the wallet post owns wallet setup and seed backup, the Blocknet ID post owns names. |
| `/blog/proof-of-work-vs-proof-of-stake/` | `/blog/argon2id-proof-of-work-explained/`, `/blog/how-crypto-mining-works/` | The comparison summarizes PoW in one section with Blocknet's four consensus facts; the Argon2id explainer owns the hash internals and memory arithmetic, and the mining explainer owns the general mining walkthrough. |
| `/blog/how-crypto-mining-works/` | `/blog/how-to-mine-blocknet/`, `/blog/argon2id-proof-of-work-explained/` | The explainer owns the coin-agnostic concepts (blocks, nonce, difficulty, rewards, pools and shares) and ends with a three-step summary; the guide owns the setup steps and the Argon2id post owns how the hash works. |
| `/blog/cpu-mining-2026/` | `/blog/blocknet-mining-hardware-hashrate/`, `/blog/mine-crypto-on-old-pc-or-laptop/` | The CPU post owns "what to mine with a CPU" and keeps one best-per-generation table and a RAM rule of thumb; the hardware post owns buying advice and full per-chip results, and the old-PC post owns small-RAM sizing, cooling and power. |
| `/blog/blocknet-mining-hardware-hashrate/` | `/blog/mine-crypto-on-old-pc-or-laptop/` | The hardware post's RAM table starts at 16 GB and shows hashrates; the old-PC post owns 4-12 GB machines, laptop cooling, the efficiency profile and electricity arithmetic, with no hashrate table. |
| `/blog/seed-phrase-explained/` | `/blog/blocknet-wallet-for-mining/` | The explainer owns BIP39, seed-to-key derivation, storage habits and scams, and summarizes view/restore in one section; the wallet post owns creating the wallet, the `seed`/`import` walkthrough and finding the address. |

bntpool.com's blog (blocknet-pool repo, `frontend/blog/`) owns pool-side jobs (joining the pool,
PPLNS payouts, miner stats, the reward schedule, the fair launch). Don't target those here.

The Windows guide owns the native Windows ZIP/checksum/PowerShell walkthrough. The general mining guide remains the cross-platform overview; the huge-pages guide owns detailed page setup.

The NVIDIA guide owns card selection, NVIDIA runtime diagnosis, accepted GPU-share verification and the dated RTX 5090 comparison. The Windows guide owns ZIP/checksum/PowerShell setup, the dashboard guide owns the panel tour, and bntpool.com owns pool-credit tracking. No RTX 4090 performance or other unmeasured card result is claimed.

The hardware comparison guide also presents the dated RTX 5090 results with the original method and evidence, as part of its per-chip coverage. Its shared measurement section has 8.2% wording overlap with the NVIDIA guide; runtime setup, commands and troubleshooting remain in the NVIDIA guide, linked from the hardware article.

The download-verification guide owns warning identification, archive integrity checks and detection reports. The Windows guide retains the normal installation flow and its brief checksum step; the NVIDIA guide owns runtime and device setup. Neither file integrity nor the software category is presented as an antivirus verdict. Both setup guides link to the new troubleshooting owner.

Added 2026-10-26: the Linux guide owns native archive installation and first-run errors. The general guide remains the cross-platform overview; huge pages and server guides own detailed tuning, NUMA and headless operation. No URL is retargeted or redirected.

## RAM planning (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| how much RAM to mine Blocknet; RAM per mining thread | `/blog/how-much-ram-to-mine-blocknet/` | Budget available memory, choose automatic or fixed CPU threads, and diagnose allocation/page-pool errors | 2 GiB per thread, seine memory reserve, container memory limit |

The hardware article owns chip selection and measured hashrates; Argon2id owns algorithm mechanics; the huge-pages guide owns page configuration. This article owns the cross-platform memory budget and allocation decision process. Examples are arithmetic, not new benchmarks.

## Miner updates (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| update seine miner; update Blocknet miner | `/blog/update-seine-miner/` | Replace an existing installation while preserving its working directory, saved settings and launch options, then verify the replacement | seine update notification, seine-data, 更新 Blocknet 矿工软件 |

Installation guides own first setup; the antivirus guide owns archive verification and warnings; the server guide owns service configuration. This guide owns maintaining an existing installation and diagnosing a changed configuration path after replacement. No URL is retargeted.

## Tuning and Stratum (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| seine automatic CPU tuning; mining autotune | `/blog/seine-automatic-cpu-tuning/` | Understand productive CPU count/kernel comparisons, cache reuse and memory responses | Blocknet mining, seine, bntpool |

Dashboard owns panel interpretation; RAM owns capacity planning; huge pages owns OS reservation. This article owns automatic tuning decisions and manual overrides. No URL is retargeted.

## Benchmarks and confirmations (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| benchmark Blocknet miner; seine benchmark | `/blog/benchmark-seine-miner/` | Run repeatable local measurements, save JSON reports and compare matched workloads | Blocknet mining, seine, bntpool |

Hardware owns chip selection and measured rankings; autotuning owns runtime decisions; hashrate on bntpool owns units and pool estimation. This article owns the benchmark procedure and comparison record. No URL is retargeted.

## CPU usage and mining fees (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| miner not using 100% CPU; Blocknet CPU usage | `/blog/miner-cpu-usage/` | Interpret OS utilization alongside active mining threads, local work and jobs before changing settings | Blocknet mining, seine, bntpool |

Autotuning owns the search algorithm, RAM owns capacity and benchmarks own repeatable testing. This guide owns the diagnostic sequence for a CPU-utilization observation. No URL is retargeted.

## Windows startup and multiple miners (October 26, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| start miner automatically Windows; Blocknet Task Scheduler | `/blog/start-miner-automatically-windows/` | Configure and verify sign-in startup for an already working Windows miner |

Windows setup owns installation; server setup owns Linux systemd; updating owns release replacement. No existing URL is retargeted.

## Configuration and address changes (October 26, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| seine settings; disable CPU or GPU mining; change seine threads; seine configuration file | `/blog/configure-seine-miner/` | Make the common changes to seine: turn CPU or GPU mining off, set threads, choose a card, keep settings for every start, change address or pool |

Updating owns binary replacement, Windows startup owns Task Scheduler, tuning owns performance decisions. This article owns the common settings and how they persist. The pool guide owns address-transition accounting. Retargeted November 2, 2026 from configuration file mechanics to the wider settings job, same URL; the setup guides carry a short shared settings section and link here. `mine-while-using-pc` keeps the desktop-sharing decision.

## Miner stops and wallet receipt (October 26, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| miner keeps stopping; Blocknet miner crash troubleshooting | `/blog/miner-keeps-stopping/` | Distinguish process exit, backend fault, startup failure and computer sleep using logs, then verify a targeted fix |

Configuration owns saved settings; RAM and huge pages own resource setup; NVIDIA owns GPU installation. Pool tracking owns connection diagnosis. This guide owns the cross-stage stopping decision process. No existing URL is retargeted.

## Desktop use and network quality (October 26, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| mine while using PC; mining desktop responsiveness | `/blog/mine-while-using-pc/` | Choose a repeatable mining configuration around a foreground desktop workload |

CPU usage owns utilization interpretation; tuning owns automatic selection; RAM owns capacity planning. This guide owns the foreground-task comparison and daily-use launch routine. No existing URL is retargeted.

## Electricity and part-time mining (October 26, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| Blocknet mining electricity use; mining energy cost | `/blog/mining-electricity-use/` | Define measurement scope, convert energy and calculate total versus incremental electricity cost |

Old-PC guidance owns suitability and brief cost arithmetic; desktop use owns responsiveness; benchmarks own hashrate testing. This article owns whole-PC versus component measurement, full versus incremental consumption and tariff interval budgeting. No existing URL is retargeted.

## Falling hashrate and wallet fees (October 26, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| mining hashrate dropping; miner slower during session | `/blog/mining-hashrate-dropping/` | Locate a sustained change in a running session using matched readings, device scope and resource evidence |
| Blocknet transaction fees; check wallet fee before sending | `/blog/blocknet-transaction-fees/` | Understand size-based fee estimation and review recipient amount, fee and change before confirming a payment |

Hashrate units and pool estimation belong to bntpool; CPU usage owns utilization; stopping owns process/backend failure; benchmarks own the measurement harness. The new diagnostic guide owns the running-session before/after decision path. Wallet setup owns creating/restoring a wallet, payment links own URI fields, mining fees belong to bntpool. The fee article owns transaction-size estimation, input/change accounting and the CLI preview. The user explicitly selected this fee topic. Neither article retargets an existing URL.

## Faraday and node operation (October 26, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| Blocknet Faraday upgrade; Blocknet network upgrade | `/blog/blocknet-faraday-upgrade/` | Understand the announced transition and distinguish future wallet migration, node updates and miner preparation |
| run Blocknet node; Blocknet node setup | `/blog/run-blocknet-node/` | Install a managed mainnet core and inspect peers, sync progress, logs and update policy |

The coin overview and privacy articles own current fundamentals; Faraday owns the dated upgrade plan and role-specific preparation. Wallet setup owns keys and addresses; checkpoints owns sync acceleration details on bntpool. Node operation owns the background process, network health and lifecycle. Neither article retargets an existing URL.

## Payments and explorer (October 27, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| send and receive Blocknet; how to send BNT | `/blog/send-receive-blocknet/` | Complete a fixed-amount wallet payment and follow its ID into the receiving wallet's history |
| Blocknet explorer; look up Blocknet transaction | `/blog/blocknet-explorer-guide/` | Search public identifiers and interpret a transaction, containing block and network readings |

Wallet setup owns creation and recovery, fees owns calculation, payment links owns request construction, and memos owns memo syntax. Sending owns the end-to-end payment procedure. Confirmations on bntpool owns maturity; privacy owns cryptographic mechanisms; node operation owns a local core. The explorer guide owns public-page navigation with dated real screenshots and the boundary between public inclusion and wallet receipt. No existing URL is retargeted.

## Wallet diagnosis and viewing access (October 27, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| Blocknet wallet not syncing; wrong Blocknet wallet balance | `/blog/blocknet-wallet-not-syncing/` | Locate a balance discrepancy in wallet selection, node download, output scanning or payment state and verify progress |
| Blocknet view-only wallet; watch incoming Blocknet payments | `/blog/blocknet-view-only-wallet/` | Export, transfer and load an encrypted viewing file on a second computer, verify its identity and understand the access granted |

Node operation owns installation and lifecycle; checkpoints on bntpool owns accelerated chain sync; wallet setup owns creation and recovery. The diagnostic guide owns the sequence from symptom to the stage needing attention. Stealth addresses owns the key/scan explanation, while the viewing guide owns the exported-file workflow and private access handling. Explorer owns public lookup, and pool tracking owns pool records. Existing URLs retain their intent.

## Payment proofs and signed messages (October 27, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| Blocknet payment proof; verify Blocknet payment | `/blog/blocknet-payment-proof/` | Generate a transaction key and verify recipient outputs and amounts while keeping chain status separate |
| sign Blocknet message; verify Blocknet signature | `/blog/blocknet-sign-verify-message/` | Sign an agreed challenge and check the address, exact message and signature with application-level freshness checks |

Sending owns payment creation; explorer owns public lookup; privacy and stealth addresses own the mechanisms. Payment proofs own deliberate transaction-specific disclosure and output verification. Message signatures own address-control challenges, not transaction receipts. Existing wallet creation, recovery and viewing guides retain their intent.

## Wallet backup, recovery and migration (October 27, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| Blocknet wallet backup; restore Blocknet wallet; move Blocknet wallet | `/blog/blocknet-wallet-backup-restore/` | Make a separate backup, choose file loading or seed import, and verify the same wallet on a destination computer |

Wallet setup owns initial creation and seed phrases owns recovery-material concepts. Sync troubleshooting owns diagnosing scan progress. This guide owns the complete migration/recovery procedure and its verification before retiring the original. Existing articles retain their intent.

## Faraday explanation refresh (October 27, 2026)

The existing `/blog/blocknet-faraday-upgrade/` owns Faraday migration, recovery-word changes, post-quantum addresses and proposed timing. Preserve its original publication date and URL; record the refresh with `updated`. The backup guide owns recovery on currently released wallets; the reward schedule owns current-chain issuance. Link to these rather than duplicating operational instructions. Update all 20 language versions together from the official October 27 post.

## Wallet, node and miner roles (October 27, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| Blocknet wallet vs node vs miner; do I need a node to mine Blocknet; keep wallet open for mining | `/blog/blocknet-wallet-node-miner/` | Choose the applications needed for receiving, pool mining or direct mining; understand what must stay running |

This guide owns the software-role decision before setup. `blocknet-wallet-for-mining` owns initial wallet creation, `run-blocknet-node` owns node operation, and `how-to-mine-blocknet` owns the mining setup procedure. Link to those procedures rather than reproduce commands. Demand is inferred from the documented user journey, not measured Search Console volume.

## Desktop wallet walkthrough (October 27, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| Blocknet desktop wallet; how to use Blocknet wallet; Blocknet wallet screenshots | `/blog/blocknet-desktop-wallet/` | Follow the desktop interface from create/open and recovery backup through synchronization, receiving, send review and transaction history |

This page owns the visual desktop procedure, using the released interface with labeled demo data. `blocknet-wallet-for-mining` owns initial wallet/payout setup and CLI commands, `send-receive-blocknet` owns general payment mechanics and its CLI procedure, and `blocknet-wallet-node-miner` owns software selection. Keep detailed recovery, fee calculations and synchronization troubleshooting at their existing URLs. Demand is inferred from the normal newcomer journey, not measured search volume.

## First Blocknet receipt (October 27, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| how to get Blocknet; get first BNT; receive or mine Blocknet | `/blog/get-first-blocknet/` | Choose receiving a transfer or mining with an existing computer, then identify the first incoming wallet payment and spendable balance |

This is the acquisition starting-point guide. Desktop wallet owns interface setup, sending/receiving owns the payment procedure, and mining owns installation and accepted shares. The new article routes to those owners, adds a simple first-receipt checklist and follows a single labeled demo payment through History and Dashboard. Pool payout rules and troubleshooting remain on bntpool.com. No existing URL is retargeted. Demand is inferred from the newcomer journey; no measured query-volume claim is made.

## Desktop wallet updates (October 27, 2026)

| Primary search intent | Owner | Distinct reader job |
|---|---|---|
| update Blocknet wallet; Blocknet wallet latest version | `/blog/update-blocknet-wallet/` | Replace the released desktop application while preserving the existing wallet, then verify application version, receiving address and synchronized history |

Desktop setup owns creating and opening wallets. Backup/restore owns recovery and moving keys. Synchronization owns diagnosing an incomplete balance. This article owns the routine application replacement sequence, with release-specific version-label and full-exit behavior and actual released-interface screenshots. Faraday owns the future network migration; this article does not supply migration instructions. Demand is inferred from ordinary wallet maintenance, not measured search volume.

## Ordinary payment timing (October 27, 2026)

| Primary search intent | Owner | Distinct reader job |
|---|---|---|
| how long does a Blocknet transfer take; Blocknet transaction pending; when can I spend received BNT | `/blog/blocknet-transfer-time/` | Follow an ordinary payment from mempool to block inclusion to spendable wallet balance, including the ten-block depth rule and differing confirmation counters |

Sending/receiving owns payment construction and approval. Explorer owns public lookup. Wallet synchronization owns diagnosing a stale local view. The pool mining-reward confirmations article owns coinbase maturity and pool accounting, not ordinary recipient wallet timing. This article owns the ordinary-payment timeline, with released-interface screenshots of the same labeled demo receipt in three stages. Search demand is inferred from an ordinary user question, not measured volume.

## Payment visibility and Faraday mechanisms (October 27, 2026)

| Primary search intent | Owner | Distinct reader job |
|---|---|---|
| what can someone see about a Blocknet payment; Blocknet transaction privacy; sharing payment information | `/blog/blocknet-payment-visibility/` | Compare public explorer, sender, recipient and granted viewing/proof access; choose what to share for a particular payment question |
| how Faraday privacy works; Faraday spend proofs; Faraday ML-KEM; Faraday proof aggregation | `/blog/blocknet-faraday-privacy/` | Understand proposed full-set membership, separate spending/receiving mechanisms, proof lifecycle and consequences for wallet use |

Privacy overview remains the mechanism hub for today's released format. Payment-proof and view-only guides own their actual procedures; the new visibility article compares their disclosure scope. Existing Faraday upgrade guide owns activation, recovery material and migration. The new mechanism article explains the eligible Faraday coin set and the proposal's distinct cryptographic jobs. No measured search volume is claimed.

## Transaction anatomy and change (October 27, 2026)

| Primary search intent | Owner | Distinct reader job |
|---|---|---|
| Blocknet transaction inputs and outputs; why two outputs; where transaction change goes | `/blog/blocknet-transaction-explained/` | Follow one payment's selected value into recipient outputs, change and fee; distinguish outputs from recipients and ring decoys from funding |

The fee guide retains fee estimation and preview steps. Send/receive owns wallet procedure; transfer-time owns confirmation/maturity timing; payment-visibility owns observer/disclosure scope. This walkthrough owns the structure and change accounting of one payment. Closest existing fee coverage includes a short version of the same arithmetic; the new page expands output ownership and lifecycle without repeating rate calculations or CLI steps. Search demand is inferred from the user’s requested practical coverage, not measured analytics.

## Currency, supply and privacy by default (October 28, 2026)

| Primary search intent | Owner | Distinct reader job |
|---|---|---|
| Blocknet supply; maximum supply; issuance inflation and tail emission | `/blog/blocknet-supply/` | Understand the approximate target versus a cap, ongoing issuance, why rewards continue, and issuance versus transfers/spendability |
| Blocknet fungibility; interchangeable privacy-coin units | `/blog/blocknet-fungibility/` | Understand fungibility as a currency property and the role of payment privacy, separate from acceptance, price and available balance |
| Faraday currency continuity; block reward versus issuance rate | `/blog/blocknet-faraday-monetary-policy/` | Compare same-chain monetary continuity and proposed interval/reward pairing without treating proposal figures as live rules |

The existing `/blog/blocknet-privacy-features/` remains the owner of privacy by default and its mechanisms. It receives a substantial update rather than a competing URL. The pool's `/blog/blocknet-block-reward-schedule/` retains the full monthly reward table and exact pre-tail arithmetic. The Faraday upgrade guide owns migration and preparation; the Faraday privacy guide owns proof mechanisms. Payment-visibility owns observer/disclosure scope, transaction anatomy owns inputs/change, and the generic privacy-coin article owns the category definition. The fungibility page develops the brief existing mention into a currency-focused explanation. No measured search-volume or ranking claim is made; these topics respond to the user's explicit currency/privacy direction.

## Currency trust and validation (October 28, 2026)

| Primary cluster | Owner | Reader job |
|---|---|---|
| Who controls Blocknet; who enforces Blocknet rules | `/blog/who-controls-blocknet/` | Distinguish developers, miners, validating nodes and wallet owners; understand software adoption versus block acceptance |

The software-role guide owns choosing applications and what must stay running. This new guide owns authority and rule enforcement, without claiming a measured distribution of control or a formal governance vote. The existing `/blog/bulletproofs-explained/` remains the owner of hidden-amount verification and is expanded with the separate authorization, conservation, repeated-spend and issuance checks. Supply owns the emission schedule; Faraday upgrade owns preparation. Demand is inferred from newcomer questions; no measured search volume is claimed.

## Atomic-swap concepts and the public VEIL announcement (October 28, 2026)

| Primary cluster | Owner | Reader job |
|---|---|---|
| Blocknet atomic swaps; what is VEIL; atomic swap claims and refunds | `/blog/blocknet-atomic-swaps/` | Understand generic conditional exchange, claim/refund paths and disclosure, then identify the limited scope of Blocknet’s public VEIL announcement |

The user approved a general explainer with a short public-announcement section after research could not verify a public VEIL implementation. The generic HTLC diagram is explicitly not a VEIL specification or supported trading pair. Faraday upgrade retains migration, ordinary sending retains payment steps, and payment visibility retains ordinary BNT disclosure. No private specification is quoted or used to describe the protocol. Demand is inferred from the approved currency/privacy direction, not measured search volume.

## Choosing a wallet application (November 5, 2026)

| Primary cluster | Owner | Reader job |
|---|---|---|
| blocknet wallet; bnt wallet; best blocknet wallet; blocknet gui wallet; blocknet cli wallet; gyroid wallet | `/blog/blocknet-wallets/` | Choose between the official desktop wallet, the official command-line wallet and Gyroid by platform and interface, verify the download, and move a wallet between them |

Search Console (Oct 24–Nov 3) showed "blocknet wallet" landing on our wallet posts at an average position of 6.3, with the top Google results belonging to an unrelated project's documentation. The hub owns the choice of application and routes to the task owners: `blocknet-desktop-wallet` (desktop walkthrough), `blocknet-wallet-for-mining` (create a wallet and copy a mining address, keeping its short which-wallet list for miners), `blocknet-wallet-backup-restore` (the full migration procedure) and `antivirus-flags-crypto-miner` (checksum verification for seine). Research: `batches/2026-11-wallets-hub/research.md`.
