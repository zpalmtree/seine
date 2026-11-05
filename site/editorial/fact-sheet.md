# seineminer.com blog fact sheet


## Current published performance — November 1, 2026

Use `site/src/data/benchmarks.json`, `docs/assets/charts/benchmarks.json` and
`docs/PERFORMANCE.md` for current figures. Dated measurements below are historical.

- EPYC 9355P: **97.1 H/s**, steady mining, default settings, 64 threads, 192 GiB RAM.
- EPYC 9554P: **81.4 H/s**, automatic settings, 128 threads, 384 GiB RAM;
  local completed hashes over elapsed time, including startup.
- Cloud EPYC 9R45 / 9R14: **50.5 / 45.3 H/s**, 32 threads, transparent huge pages;
  hashing time, excluding startup. These are VM allocations.
- RTX 5090: **13.8 H/s**, GPU only, native Windows 11, driver 610.88,
  steady mining after the miner fee; public development build measured October 31.
  Do not add a separately measured CPU rate to invent a combined result.
- Graviton4 **39.3 H/s**, M4 Max **30.9 H/s**, Xeon 8488C **26.0 H/s** remain current.

Public articles link to the performance guide, not private measurement records.
The older 68.3/75.0 CPU and 7.3/12.9 GPU figures below describe earlier settings
and builds; they are not the current headline results.

Initial research checked 2026-10-25; release, platform and pool facts rechecked 2026-10-26. Writers may state only what is listed here. Every bullet ends with its source.

**Released behavior:** `release-facts.json` pins the reviewed public release. Source on `master` may be newer and must not be described as released. Historical benchmark versions and screenshots retain their original labels.

**How to read the sources**
- `repo@ref:path:line` points at a file in one of the Blocknet repos. Original research snapshots (these are historical commit pins, not today's branch heads):
  - `blocknet-core@pool` is commit c646196 (2026-10-15), the production daemon branch.
  - `blocknet-core@upstream/master` is commit 514a8cf, the public `blocknetprivacy/core` master.
  - `blocknet@master` is commit 973e56e (2026-03-11).
  - `blocknet-pool@master` is commit 9bf4eb6.
  - Original `seine@master` research used d20f253, the v0.2.22 line. The initial platform review used `seine@v0.2.26` (`899ff7ea8bd547163ae48d636f569a0a8c7e25bb`).
  - Seine and blocknet-pool were read from `master` with `git show`, because their working checkouts sit on feature branches.
- **"Live"** means fetched from a public endpoint on 2026-10-25 at about 00:26 EDT. Live values can change.
- **"Derived"** means computed by this sheet from the cited code. The math is shown so it can be checked.

---

## 0. Read this first: the Faraday hard fork (~2027-02-16) will change mining

- Blocknet plans a hard fork called **Faraday**. The site lists these dates:
  - "~2027-02-16 faraday activates", planned "near the first anniversary of block 1"
  - "~2028-02-16 old format closes"
  - The site adds: "Dates are planned, not final."
  - Source: https://blocknetcrypto.com/#faraday (checked 2026-10-25)
- The public site says Faraday brings:
  - **"A lighter proof of work. New nodes catch up with the chain faster."** Its comparison table shows proof of work moving from "Argon2id (2GB)" to "lighter, faster sync".
  - Post-quantum spend proofs ("hash-based proofs, not elliptic curves"; "Payment detection uses ML-KEM").
  - Full-chain anonymity sets ("Today a spend hides among 16 outputs. After Faraday, it hides among every output on the chain.").
  - "No new coin. No relaunch. Your history stays."
  - Status: "Design, testing and adversarial review in progress."
  - Source: https://blocknetcrypto.com/#faraday (checked 2026-10-25)
- The public core repo sets the fork height and the upgrade notice in code:
  - Fork height is **103,680**, "the start of month 12".
  - Notice text: "Blocknet plans a network upgrade around 2027-02-16. After it, you will have 1 year to move your coins to the new format, until about 2028-02-16. Coins not moved by then cannot be spent."
  - This is on upstream master only. It is **not** on the production `pool` branch.
  - Source: `blocknet-core@upstream/master:network_upgrade.go:4-9` (checked 2026-10-25)
- Seine has no Faraday support in its current release (checked 2026-10-25).
- **Impact on writing:** everything below about Argon2id, 2 GB per thread, the H/s benchmarks, memory-hardness and the CPU vs GPU balance describes the network **before Faraday**.
  - The PoW is planned to change about 4 months after this sheet's date.
  - Undated "evergreen" mining articles will go stale. Date posts, or add a "before the Faraday upgrade" qualifier.
  - Only publish Faraday details that blocknetcrypto.com states publicly. Unannounced parameters stay out of posts. The October 27 post now publicly proposes block-time and reward changes; mining details are reserved for a separate future post.

---

## 1. Blocknet protocol (current chain, before Faraday)

### Proof of work
- PoW is **Argon2id** with these parameters:
  - memory 2 GiB (2,097,152 KiB)
  - iterations 1
  - parallelism 1
  - 32-byte output
  - Source: `blocknet-core@pool:crypto-rs/src/pow.rs:11-17` (checked 2026-10-25)
- The Argon2 version is **v0x13 (1.3)**. The reference check is `Params::new(memory_kib, 1, 1, Some(32))` with `Algorithm::Argon2id, Version::V0x13`.
  - Source: `blocknet-core@pool:crypto-rs/src/pow.rs:204-206` (checked 2026-10-25)
- Hash inputs:
  - password = the 8-byte little-endian nonce
  - salt = the serialized block header without the nonce
  - Source: `blocknet-core@pool:crypto-rs/src/pow.rs:96-106`, `block.go:135-137` (checked 2026-10-25)
- The code's design intent says Argon2id is "Memory-hard PoW to resist ASICs" and that 2GB makes "specialized hardware impractical". This is a design claim, not a proof. See §6.
  - Source: `blocknet-core@pool:crypto-rs/src/pow.rs:1-4` (checked 2026-10-25)
- The block header hash (block ID) is SHA3-256. It is separate from the PoW hash.
  - Source: `blocknet-core@pool:block.go:92-96` (checked 2026-10-25)
- Fun fact: the daemon's PoW kernel is a crate from the seine repository (`blocknet-pow-kernel`, git `zpalmtree/seine`, pinned rev 8f9b5a9).
  - Source: `blocknet-core@pool:crypto-rs/Cargo.toml:19` (checked 2026-10-25)
  - Check with the owner before using this publicly.

### Block time, confirmations, limits
- Target block time is **5 minutes (300 s)**.
  - Source: `blocknet-core@pool:block.go:53-54` (checked 2026-10-25); the site also says "block time 5 min" (https://blocknetcrypto.com, checked 2026-10-25)
- Coinbase maturity is **60 blocks** ("~5 hours before mined coins spendable").
  - Source: `blocknet-core@pool:block.go:56` (checked 2026-10-25)
- "Safe" confirmations: 10 (~50 min).
  - Source: `blocknet-core@pool:block.go:55` (checked 2026-10-25)
- Hard finality (maximum reorg depth): 100 blocks (~8 h).
  - Source: `blocknet-core@pool:block.go:57` (checked 2026-10-25)
- Maximum block size: 1 MB (1<<20 bytes).
  - Source: `blocknet-core@pool:block.go:61` (checked 2026-10-25)

### Difficulty adjustment
- The algorithm is **LWMA** (Linear Weighted Moving Average, after Zawy) with these settings:
  - window: 60 blocks
  - minimum difficulty: 4
  - each solvetime clamped to the range 1 s to 1,800 s (6× target)
  - Source: `blocknet-core@pool:block.go:63-68` (checked 2026-10-25)
- The formula is `newDiff = avgDifficulty × (300 × Σweights) / Σ(solvetime_i × i)`, with a floor at MinDifficulty.
  - Source: `blocknet-core@pool:block.go:1018-1046` (checked 2026-10-25)
- The difficulty target is `floor((2^256 − 1) / difficulty)`. This means **difficulty ≈ expected hashes per block**.
  - Source: `blocknet-core@pool:crypto-rs/src/pow.rs:163-164` (checked 2026-10-25)
- A block's timestamp must be later than the median of the last 11 blocks and at most 2 h in the future.
  - Source: `blocknet-core@pool:block.go:58`, `block.go:513` (checked 2026-10-25)

### Reward, emission, supply
- 1 BNT = 100,000,000 atomic units.
  - Source: `blocknet-core@pool:cli.go:1010` (checked 2026-10-25)
- Emission is a smooth exponential decay. The reward is constant within each 30-day "month" of 8,640 blocks.
  - `reward(month) = (InitialReward − Tail) × e^(−0.75 × month/12) + Tail`
  - InitialReward = 723.25093035 BNT
  - Tail = 2.0 BNT
  - Source: `blocknet-core@pool:miner.go:14-32`, `miner.go:383-397` (checked 2026-10-25)
- The tail starts at month 48. From height 414,720 the reward is **2.0 BNT per block forever**.
  - Source: `blocknet-core@pool:miner.go:16-20`, `miner.go:385-386` (checked 2026-10-25)
- Code and site describe supply as "~100M coins over 4 years" plus the tail. The site says "supply ~100M + 2.0/block tail".
  - `TargetSupply` = 100,000,000 BNT is "the approximate cumulative emission before tail… actual supply exceeds it over time".
  - Source: `blocknet-core@pool:miner.go:15`, `miner.go:23-27`; https://blocknetcrypto.com (checked 2026-10-25)
- **Derived:** exact emission before the tail (heights 1–414,719) = **98,561,905.08 BNT**. This is slightly under the "~100M" label.
  - Formula: sum of `GetBlockReward` per month (`miner.go:405-419`)
  - (checked 2026-10-25)
- **Derived:** "4 years" means 48 × 30-day months = 1,440 days, not 4 calendar years. At exactly 300 s per block, the tail would start around 2030-01-26.
  - Actual timing drifts with real block times.
  - Faraday may change block timing and rewards, so this projection is unreliable.
  - (checked 2026-10-25)
- **Derived:** tail emission = 288 blocks/day × 2 BNT = 576 BNT/day ≈ 210,240 BNT/year.
  - Source: `blocknet-core@pool:miner.go:19` (checked 2026-10-25)
- **Derived reward table** (BNT per block) from `miner.go:383-397` (checked 2026-10-25):

  | Month | Reward (BNT) |
  |---|---|
  | 0 | 723.25 |
  | 1 | 679.55 |
  | 12 | 342.69 |
  | 24 | 162.93 |
  | 36 | 78.02 |
  | 47 | 40.22 |
  | 48+ | 2.0 |

- **Surprise:** at month 48 the reward drops sharply, from ~40.2 to 2.0 BNT. The decay does not reach the tail smoothly.
  - Source: `blocknet-core@pool:miner.go:385-391` (checked 2026-10-25)
- Fees: the coinbase must have exactly one output, worth exactly `GetBlockReward(height)`. Its memo must be empty.
  - Source: `blocknet-core@pool:block.go:663-695` (checked 2026-10-25)
  - **Derived:** transaction fees are not paid to the miner through the coinbase. Fees appear to be unclaimed (effectively burned).
  - This is inferred from code and not confirmed in docs. Treat it as unverified for publication.
- Wallet fee policy: minimum fee 0.00001 BNT, plus 0.0000001 BNT per byte.
  - Source: `blocknet-core@pool:api_handlers.go:25-26` (checked 2026-10-25)

### Fair launch, genesis, premine
- The genesis block has **no transactions**: "no coinbase, no burned coins". The first coinbase is at height 1.
  - Source: `blocknet-core@pool:block.go:3001-3002`, `block.go:3012` (checked 2026-10-25)
- No premine, treasury or dev-fund output exists in consensus. Each coinbase is exactly one output equal to the block reward.
  - Source: `blocknet-core@pool:block.go:663-680` (checked 2026-10-25)
  - A search for treasury/devfund/premine/founder strings found nothing.
- Genesis timestamp is 1771207118, which is **2026-02-15 20:58:38 EST** (2026-02-16 01:58:38 UTC).
  - Source: `blocknet-core@pool:block.go:2974` (checked 2026-10-25)
- The genesis message is "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything". The genesis prev-hash is SHA3-256 of that message.
  - Source: `blocknet-core@pool:block.go:2967`, `block.go:2995-2997` (checked 2026-10-25)
- The current chain is a **relaunch**:
  - `ChainID 0x20260215` is called "a fixed relaunch epoch identifier".
  - The genesis comment says "Derived from the relaunch time".
  - Repo history starts 2026-02-05, and "relaunch params" was committed 2026-02-15.
  - Source: `blocknet-core@pool:protocol/params/network.go:10-13`, `block.go:2973` (checked 2026-10-25)
  - Details of the earlier chain are unknown. See §7.

### Addresses and @shortnames (Blocknet ID)
- An address is base58(spend_pub ‖ view_pub ‖ 4-byte checksum), 68 bytes in total.
  - Source: `blocknet-core@pool:wallet/wallet.go:244-256` (checked 2026-10-25)
  - Legacy 64-byte addresses without a checksum are still accepted (`wallet/wallet.go:1822`).
- **Derived:** checksummed addresses are **92–93 characters** long (about 97% are 93) in a 20,000-sample simulation of the encoding.
  - See the contradiction in §6 about "95-character".
  - (checked 2026-10-25)
- Shortnames use the prefix `@` or `$`:
  - case-insensitive
  - allowed characters a–z, 0–9, `_` `-` `.`
  - up to 64 characters
  - resolved through **https://blocknet.id** with a signed mapping
  - Source: `blocknet-core@pool:handle_resolver.go:16`, `handle_resolver.go:186-208` (checked 2026-10-25)
- The CLI docs say: "Get a short name like @name or $name at https://blocknet.id" and "Short names can be used as `@name` or `$name`."
  - Source: `blocknet@master:docs/reference-core.md:178`, `:224` (checked 2026-10-25)
- Payment links: "Pasting a `blocknet://` URI or `bntpay.com/` link auto-parses as a send."
  - Source: `blocknet@master:docs/reference-core.md:226` (checked 2026-10-25)
- Terminology: the products call this "**Blocknet ID shortnames**" (pool, seine) or "short names" (CLI docs).
  - Source: `blocknet-pool@master:README.md:280`, `seine@master:docs/guides/configuration.md:78` (checked 2026-10-25)

### Privacy features (current)
- **Ring signatures:** CLSAG with RingCT. The ring size is **exactly 16** and enforced by consensus. The site says "ring of 16 (15 decoys)".
  - Source: `blocknet-core@pool:crypto.go:660`, `transaction.go:1000-1002`, `crypto-rs/src/ring.rs:1`; https://blocknetcrypto.com (checked 2026-10-25)
- **Receiver privacy:** one-time stealth addresses.
  - Source: `blocknet-core@pool:crypto-rs/src/lib.rs:10`; site "one-time stealth addresses" (checked 2026-10-25)
- **Amounts:** Pedersen commitments with 64-bit Bulletproofs range proofs.
  - Source: `blocknet-core@pool:crypto-rs/src/lib.rs:8-9`, `crypto-rs/src/rangeproof.rs:8` (checked 2026-10-25)
- **Keys and curve:** Ed25519 / Ristretto (curve25519-dalek). Addresses are validated as Ristretto points.
  - Source: `blocknet-core@pool:crypto-rs/Cargo.toml:11-12`, `address_validation.go:14-18` (checked 2026-10-25)
- **Network:** Dandelion++ transaction relay with stem probability 0.9.
  - Source: `blocknet-core@pool:p2p/dandelion.go:21-26` (checked 2026-10-25)
- **Memos:** a fixed 128-byte encrypted memo on every output, with at most 124 bytes of plaintext.
  - Source: `blocknet-core@pool:protocol/params/memo.go:9`, `:17` (checked 2026-10-25)
- Privacy is mandatory. There is no transparent mode in the transaction format; every input carries a 16-member ring (`transaction.go:1000`).
  - The marketing line "No transparent mode" relies on this.
  - Source: `marketing/brand/taglines.md:36` (checked 2026-10-25)

### Ticker and naming
- The unit is **BNT** in code, CLI and pool UI, for example "0.00001 BNT minimum fee" and "Send 100 BNT".
  - Source: `blocknet-core@pool:api_handlers.go:25`, `blocknet@master:docs/reference-core.md:212`, `blocknet-pool@master:frontend/src/lib/format.ts:114` (checked 2026-10-25)
- The official site does not state a ticker.
  - Source: https://blocknetcrypto.com (checked 2026-10-25)
- **Never use the `$BNT` cashtag.** It belongs to Bancor.
  - Source: `marketing/README.md:81`, `marketing/brand/taglines.md:56` (checked 2026-10-25)
- **Name collision:** an unrelated older project, "Blocknet" at blocknet.org (an interoperability protocol, token **BLOCK**, XBridge/XRouter), ranks for the same name.
  - Our project brands as "Blocknet Privacy" (@blocknetprivacy, blocknetcrypto.com).
  - Never link blocknet.org, and never conflate it with this project.
  - Source: https://docs.blocknet.org/ ; `marketing/README.md:3-4` (checked 2026-10-25)
- Official links:
  - site https://blocknetcrypto.com
  - explorer https://explorer.blocknetcrypto.com
  - CLI downloads https://github.com/blocknetprivacy/blocknet/releases/latest
  - GUI downloads https://github.com/blocknetprivacy/blocknet-gui/releases/latest
  - community https://uproar.chat/invite/blocknet
  - Source: https://blocknetcrypto.com (checked 2026-10-25)
- The official site carries the warning "**experimental: do not use for significant value**". Do not contradict it.
  - Source: https://blocknetcrypto.com (checked 2026-10-25)

---

## 2. Mining economics primitives (no price claims)
- Blocks per day = 86,400 / 300 = **288** (derived from `blocknet-core@pool:block.go:54`). Blocks per 30-day month = 8,640 (`miner.go:32`).
  - (checked 2026-10-25)
- Network hashrate ≈ difficulty / 300 H/s. This follows because the target is (2^256−1)/difficulty (`crypto-rs/src/pow.rs:164`).
  - The daemon's own estimate is Σdifficulty / Σsolvetime over recent blocks (`block.go:1601-1630`).
  - (checked 2026-10-25)
- Expected solo blocks per day = 288 × (your H/s ÷ network H/s).
  - Expected gross BNT per day = that × `reward(height)`.
  - Pool income is PPLNS-smoothed, minus the pool fee and the seine dev fee (§3, §4).
  - Derived. (checked 2026-10-25)
- The reward steps down every 8,640 blocks. **Do not hard-code "current reward"** in posts.
  - At height ~69,003 (live 2026-10-25) the reward is 467.67 BNT (month 7).
  - It drops to 439.46 BNT at height 69,120.
  - Derived from `miner.go:383-397`; height from https://bntpool.com/api/stats (checked 2026-10-25)
- Each CPU thread needs a full 2 GiB arena. RAM, not core count, often caps the thread count.
  - Source: `seine@master:README.md:57-58`, `docs/PERFORMANCE.md:3-5`, `blocknet@master:docs/reference-core.md:702` ("Roughly 2GB RAM per thread") (checked 2026-10-25)
- **Live snapshot, volatile, for context only.** Do not publish as current figures.
  - chain height 69,003
  - network hashrate ≈ 50,057 H/s
  - bntpool.com hashrate ≈ 5,627 H/s
  - 249 miners
  - Source: https://bntpool.com/api/stats (2026-10-25 ~00:26 EDT)

---

## 3. bntpool.com
- Live pool parameters:
  - **pool fee 0%**
  - minimum payout **1 BNT** (0.1 BNT until the 2026-11-02 payout memo rollout; exchange addresses with a memo: **10 BNT**, `memo_min_payout_amount`)
  - **60 blocks** before payout
  - **PPLNS** with a **6h** window
  - Stratum port **3333**
  - Source: https://bntpool.com/api/info (live 2026-10-25 ~00:26 EDT; minimum payouts rechecked 2026-11-04 20:55 EST)
  - Production and the example config use `pool_fee_pct: 0` (verified live 2026-10-25, 11:59 PM EDT). Always check the live value.
  - Source: `blocknet-pool@master:config.example.json:40` (checked 2026-10-25)
- The pool fee is taken as `reward × pool_fee_pct / 100` per block.
  - Source: `blocknet-pool@master:crates/pool-runtime/src/config.rs:231-234` (checked 2026-10-25)
- The Stratum URL is **stratum+tcp://bntpool.com:3333**. This is seine's default pool.
  - Source: `seine@master:src/config.rs:134`; `blocknet-pool@master:frontend/src/lib/format.ts:173` (checked 2026-10-25)
- The pool's start page shows Fee, Payout Scheme "PPLNS", Min Payout and "Block Confirmations". It explains: "Rewards are based on shares submitted before a block is found. If your hashrate increases right after a block, that work helps with later blocks, not the one that was just found."
  - Source: `blocknet-pool@master:frontend/src/pages/StartPage.tsx:147-191` (checked 2026-10-25)
- The pool UI shows tentative rewards from unconfirmed blocks separately ("can still move until the block confirms or is orphaned").
  - Source: `blocknet-pool@master:frontend/src/pages/StartPage.tsx:194-195` (checked 2026-10-25)
- Login accepts a full address or a Blocknet ID shortname (`@name`/`$name`). The resolved address stays fixed for that connection. "A name change never redirects an existing balance."
  - Source: `blocknet-pool@master:README.md:280` (checked 2026-10-25)
- The pool asks for "Seine v0.2.16 or newer for shortname support". Stratum login requires protocol v2 with `submit_claimed_hash`.
  - Source: `blocknet-pool@master:frontend/src/pages/StartPage.tsx:48`, `README.md:279-282` (checked 2026-10-25)
  - Whether any miner other than seine works is not verified. See §7.
- Vardiff is per connection. The default aims for ~10 shares per 5 minutes, starting at difficulty 60.
  - Source: `blocknet-pool@master:README.md:285-287` (checked 2026-10-25)
- Public pool pages: dashboard, miner lookup/stats, blocks, payouts, luck, status. Public API: `/api/info`, `/api/stats`, `/api/blocks`, `/api/payouts/recent`, `/api/miner/{address}`, and others.
  - Source: `blocknet-pool@master:README.md:218-232`, `:264-275` (checked 2026-10-25)
- The payout sweep interval is not exposed publicly. The example config says `payout_interval: "1h"`, but the production value is unverified.
  - Source: `blocknet-pool@master:config.example.json:51` (checked 2026-10-25)

---

## 4. seine (the miner)
- Seine is the external miner for Blocknet. License BSD-3-Clause. Repo: github.com/zpalmtree/seine.
  - Latest reviewed release **v0.3.1**, published November 4, 2026 at 5:25:42 PM EST.
  - Source: GitHub latest-release API and `seine@v0.3.1:Cargo.toml`; rechecked November 4, 2026. `release-facts.json` records the immutable source and release assets.
- Name styling: the wordmark is always lowercase, `seine.` with a lime full stop. README prose capitalizes "Seine" at sentence start.
  - Source: `seine@master:docs/BRAND.md:43-44`, `README.md:24` (checked 2026-10-25)
- **Historical released miner fee (v0.2.26):** 1% of mining time on bntpool.com and its subdomains; 2.5% on other pools and in solo mode. The current calculator now uses v0.2.36's 2% fee and names that version.
  - Source: `seine@v0.2.26:src/dev_fee.rs`, `dev_fee_percent_for_pool_url`, and `src/miner/mining_pool.rs`.
- **Released dev fee (v0.2.36):** flat **2% of mining time** on every pool and in solo mode. There is no pool-host discount. Older releases retain their previous rates.
  - Charged as time-sliced dev rounds to a hard-coded dev address, after a **10-minute** fee-free grace period.
  - Source: `src/dev_fee.rs`, `src/miner/mining_pool.rs`, `docs/guides/configuration.md`.
  - Solo/daemon mode uses the same tracker. If the daemon rejects the dev-fee reward address, the existing fallback disables the dev fee for that run.
- **Pool fee transition:** bntpool.com now charges **0%**, confirmed via live `/api/info` after deployment on 2026-10-25 at 11:59 PM EDT. Rechecked 2026-10-26: `/api/info` still reports 0%. The released v0.2.26 miner uses 1% on bntpool.com; v0.2.36 uses a flat 2%; retain v0.2.26 fee values only in version-labelled historical examples.
- **Release archives:**

  | Platform | Archive | GPU support |
  |---|---|---|
  | Windows x86_64 | `seine-windows-x86_64.zip` | NVIDIA (drivers only, CUDA runtime bundled) |
  | Linux x86_64 | `seine-linux-x86_64.tar.gz` | NVIDIA (driver + NVRTC); glibc 2.39+ |
  | Linux ARM64 | `seine-linux-aarch64.tar.gz` | CPU only (glibc 2.39+) |
  | macOS Apple Silicon | `seine-macos-aarch64.tar.gz` | CPU (Metal experimental) |
  | macOS Intel | `seine-macos-x86_64.tar.gz` | CPU only |

  - Source: `seine@v0.2.26:README.md`, `.github/workflows/release.yml`, `docs/guides/nvidia.md`; official Linux archives verified against release `SHA256SUMS.txt`, then inspected with `readelf --version-info` (both require GLIBC_2.39). Checked 2026-10-26.
- There is no Windows ARM64 build (release matrix). The Windows zip bundles the CUDA 12.8.61 NVRTC DLLs, so no CUDA Toolkit is needed.
  - Source: `seine@master:docs/guides/nvidia.md:26-27` (checked 2026-10-25)
- **CPU kernels:**
  - x86: scalar, AVX2, AVX-512, and a streaming-store variant
  - Arm: NEON with a SHA3 fast path
  - Seine compares them while mining and switches only after two confirmed gains.
  - Source: `seine@master:docs/PERFORMANCE.md:81-83`, `docs/guides/autotuning.md:26-37` (checked 2026-10-25)
- The README tagline lists "AVX2, AVX-512, Arm NEON/SHA3 and NVIDIA Blackwell".
  - Source: `seine@master:README.md:23-25` (checked 2026-10-25)
- **NVIDIA:**
  - mines on every detected NVIDIA GPU alongside the CPU, and the rates add
  - kernels compile at startup with NVRTC
  - the first launch autotunes and caches the result per GPU
  - Source: `seine@master:docs/guides/nvidia.md:3-4`, `:23-24`, `:37-38` (checked 2026-10-25)
- **AMD GPU (historical; superseded by v0.3.0, see §11):** experimental HIP backend for RDNA3 (wave32) only:
  - requires a source build with `--features amd`
  - "never auto-selected"
  - not in any release archive
  - Source: `seine@master:docs/guides/nvidia.md:50-53`, `Cargo.toml:42-49`, `docs/AMD_BRINGUP.md:8-12`, `release.yml:13-31` (checked 2026-10-25)
- **Apple Metal:** "experimental and is never auto-selected". On the M4 Max, CPU-only mining beat Metal and CPU+Metal.
  - Source: `seine@master:docs/guides/apple-silicon.md:35-37` (checked 2026-10-25)
- **RAM:** "Each CPU thread needs about 2 GB of RAM." Each worker hashes inside a 2 GiB arena.
  - Source: `seine@master:README.md:57`, `docs/guides/linux-hugepages.md:3` (checked 2026-10-25)
- **Zero configuration:**
  - run `./seine`, paste an address or `@shortname`, accept the default pool
  - "Threads, kernels, memory and CPU placement tune themselves."
  - Source: `seine@master:README.md:26-27`, `:47-55` (checked 2026-10-25)
- **Auto-tuning while mining:**
  - starts mining immediately with no offline sweep
  - compares thread counts and kernels with baseline/candidate/baseline windows on real mining work
  - backs off when RAM gets tight
  - caches choices
  - Source: `seine@master:docs/guides/autotuning.md:3-24`, `:50-59` (checked 2026-10-25)
- Live tuning "does not guarantee the global optimum immediately on an unfamiliar machine."
  - Source: `seine@master:docs/guides/autotuning.md:98-99` (checked 2026-10-25)
- Tuning never reconnects to the pool or restarts GPU backends.
  - Source: `seine@master:docs/guides/autotuning.md:86-87` (checked 2026-10-25)
- **Memory headroom (v0.2.43):** automatic sizing reserves 5% of effective total RAM, capped at 1 GiB by default. An explicit `--cpu-memory-reserve-percent` uses the requested percentage without that cap; explicit thread counts override automatic sizing. Workers park and release arenas under memory pressure.
  - Source: `seine@v0.2.43:src/config.rs`, especially `memory_reserve_bytes`, `auto_cpu_memory_bytes` and `cpu_capacity_from_budget` (checked October 27, 2026, 9:32 PM EDT); see `research/release-v0.2.43-2026-10-27.md`.
- **Huge pages (Linux/WSL):**
  - "about +4–6% with 2 MiB pages and a further +2% with 1 GiB pages on the validation host"
  - the README summarizes this as "+4–8% on CPU"
  - 1,024 two-MiB pages per thread
  - Source: `seine@master:docs/guides/linux-hugepages.md:3-6`, `:15`; `README.md:75` (checked 2026-10-25)
- **NUMA / L3 placement (Linux):**
  - default `--cpu-affinity auto` spreads threads across NUMA nodes and L3 groups, one per physical core before SMT siblings
  - measured +73% (ordinary pages) and +108% (2 MiB pages) on a two-node EPYC 7R13 VM at 8 threads
  - +16.7% on an EPYC 8434P at 24 threads
  - Source: `seine@master:docs/guides/large-servers.md:3-12` (checked 2026-10-25)
  - Caveat: "Multi-socket NUMA servers have not been benchmarked yet." (`docs/PERFORMANCE.md:145`)
- **Control API:**
  - local REST + SSE (`/v1/...`, `/v1/events/stream`)
  - Prometheus at `GET /metrics`
  - default bind is loopback `127.0.0.1:9977`; a non-loopback bind needs `--api-allow-unsafe-bind`
  - `--service` idles until `POST /v1/miner/start`
  - Source: `seine@master:docs/API.md:18-42`, `:65-76`; `docs/guides/configuration.md:97-106` (checked 2026-10-25)
- **Solo mining:** `./seine --mode daemon` against your own synced Blocknet node, using its API cookie. Pool mining is the default.
- **Solo mining on a pool (seine v0.3.1+):** put `solo:` in front of the address or shortname (`-u solo:ADDRESS`, `solo:@name`, any letter case). bntpool.com pays only for blocks that connection finds: the whole block reward less `pool_fee_pct`, through normal payouts after 60 confirmations; nothing from other miners' blocks. Each connection picks its mode, so one address can run solo and PPLNS rigs. Seine's balance panel labels the account `Solo`; daemon mode ignores the prefix. Stats: Type column (Pool/Solo) on Workers, estimated rewards, Blocks and Luck.
  - Source: `seine@v0.3.1:docs/guides/configuration.md:146-156`, `src/miner/tui.rs:1328`; `blocknet-pool@a46bbd1:README.md` "Solo Mining", `frontend/src/lib/solo.ts` (checked 2026-11-04)
  - Source: `seine@master:docs/guides/solo-mining.md:1-36` (checked 2026-10-25)
- Seine checks GitHub for updates at startup and every 12 h. This can be turned off with `--disable-update-check`.
  - Source: `seine@master:docs/guides/configuration.md:69-72` (checked 2026-10-25)
- **Terminology:** say "**threads**", never "workers".
  - PERFORMANCE.md defines a thread as "one hashing thread with its own 2 GiB arena, the same number you set with `--threads` or see as 'Threads' on the dashboard".
  - Source: `seine@master:docs/PERFORMANCE.md:20-21`; owner preference (checked 2026-10-25)
  - Note: some seine guides and the CHANGELOG still say "workers" (e.g. `docs/guides/linux-hugepages.md:21`, `CHANGELOG.md:14`). Do not copy that wording.

---

## 5. Performance numbers (pre-Faraday Argon2id; H/s = completed hashes per second)

**Historical "best per generation" snapshot** (superseded for current headline claims; retain for dated comparisons), from `seine@master:docs/assets/charts/benchmarks.json:112-143` (checked 2026-10-25).

**Earlier update 2026-10-25 (superseded by the 9355P result below):** the owner approved **49.4 H/s for AMD EPYC · Zen 5 (EPYC 9R45, 32 threads)** for the site, from a new 32-thread run. It replaced the 44.7 H/s / 24-thread headline below. The site and introductory guides at that time used 68.3 H/s on EPYC 9355P, with 64 mining threads, 192 GiB RAM and manually prepared 1 GiB pages; see the detailed measurement at the end of this sheet and `site/src/data/benchmarks.json`.
  - Source: AWS m8a.8xlarge, seine v0.2.22, avx512-256-stream kernel, 32 threads, 49.39 H/s mean of 4 runs, 2026-10-25 (in-progress AWS hardware campaign).
  - Evidence: `seine:experiments/readme-hardware-20261025/summary.json`, host "zen5-turin" (AMD EPYC 9R45, 32 cores, no SMT), in seine PR #59 (`codex/zen5-turin-result`), which also updates `benchmarks.json`, `PERFORMANCE.md` and the README. TODO: switch this to the master path once #59 merges.


| Label (big type) | Exact chip (small text) | H/s |
|---|---|---|
| AMD EPYC · Zen 5 | EPYC 9R45 server · 24 threads | **44.7** (44.742) |
| AMD EPYC · Zen 4 | EPYC 9R14 server · 24 threads | **35.7** (35.698) |
| Apple M4 Max | 48 GB MacBook Pro · 16 threads | **30.9** (30.94) |
| Arm server | AWS Graviton4 · 16 threads | **21.5** (21.503) |
| Intel Xeon | Sapphire Rapids 8488C · 12 threads | **13.1** (13.099) |

Caveats and other numbers:
- The README chart alt text matches: "AMD EPYC Zen 5 44.7 H/s, EPYC Zen 4 35.7, Apple M4 Max 30.9, Arm server (AWS Graviton4) 21.5, Intel Xeon 13.1".
  - Source: `seine@master:README.md:63` (checked 2026-10-25)
- Cloud rows are AWS EC2 guests with explicit 2 MiB huge pages. Instance sizes and thread counts differ, so a row is "what seine achieved on that guest rather than a full socket".
  - "Larger-instance runs are in progress and will replace these numbers."
  - Source: `seine@master:docs/PERFORMANCE.md:43-46`, `:137-139` (checked 2026-10-25)
- The M4 Max 30.94 H/s is a maximum-throughput setting: 16 threads, `auto`, and "macOS used about 3 GiB of swap". The balanced setting is 29.26 H/s at 14 threads with `pcore-only`. The guide says "about 29–31 H/s".
  - Source: `seine@master:docs/guides/apple-silicon.md:3-17`; `docs/PERFORMANCE.md:54-55` (checked 2026-10-25)
- Desktop: AMD Ryzen 9 9950X3D reaches 10.31 H/s (16 threads, native Windows, regular pages, streaming kernel).
  - Source: `seine@master:docs/PERFORMANCE.md:56` (checked 2026-10-25)
- **Full per-CPU table** (H/s @ threads), from `seine@master:docs/PERFORMANCE.md:29-41` (checked 2026-10-25):

  | Vendor | CPU | H/s @ threads |
  |---|---|---|
  | AMD | EPYC 9R45 (Zen 5 Turin) | 44.74 @ 24 |
  | AMD | EPYC 9R14 (Zen 4 Genoa) | 35.70 @ 24 |
  | AMD | EPYC 7R13 (Zen 3 Milan) | 11.63 @ 16 |
  | AMD | EPYC 7R32 (Zen 2 Rome) | 6.01 @ 12 |
  | AMD | EPYC 7571 (Zen 1 Naples) | 4.60 @ 16 |
  | Intel | Xeon 8488C (Sapphire Rapids) | 13.10 @ 12 |
  | Intel | Xeon 6975P-C (Granite Rapids) | 12.92 @ 12 |
  | Intel | Xeon 8375C (Ice Lake) | 12.21 @ 12 |
  | Intel | Xeon 8275CL (Cascade Lake) | 10.25 @ 12 |
  | Intel | Xeon E5-2666 v3 (Haswell) | 9.72 @ 11 |
  | Arm | Graviton4 | 21.50 @ 16 |
  | Arm | Graviton3 | 18.17 @ 16 |
  | Arm | Graviton2 | 11.05 @ 16 |

- **Kernel gains** (same machine, alternating runs), from `seine@master:docs/PERFORMANCE.md:66-73` (checked 2026-10-25):

  | Machine | Kernel change | Gain |
  |---|---|---|
  | Graviton4 | NEON → SHA3 + prefetch | +56% (13.80 → 21.50) |
  | Ryzen 9 9950X3D | 256-bit AVX-512 → streaming | +46% |
  | Graviton3 | NEON → SHA3 + prefetch | +45% |
  | EPYC 9R45 | AVX2 → streaming | +30% (34.47 → 44.74) |
  | EPYC 9R14 | AVX2 → streaming | +27% |
  | Graviton2 | portable NEON tuning | +4% |

- Streaming-kernel auto-selection currently covers only "the validated Turin CPU identity" (AMD family 26, model 2).
  - The Zen 4 9R14 number used the streaming kernel in a benchmark. Whether seine auto-selects it on Zen 4 is not stated. See §7.
  - Source: `seine@master:CHANGELOG.md:12-17`, `docs/guides/autotuning.md:39-41` (checked 2026-10-25)
- NVIDIA results must use a dated, named-host measurement. The owner authorized the October 25 NVIDIA setup guide with a fresh GPU-only/CPU-plus-GPU comparison; see the new evidence below. Do not extrapolate its RTX 5090 result to other cards or reuse the older WSL figure as a current result.

---

## 6. Claims to avoid

From the Blocknet marketing kit, close to verbatim:
- **Post-quantum (owner decision 2026-10-25):** the blanket ban is lifted. Posts may describe the Faraday upgrade's planned post-quantum spend proofs, attributed to blocknetcrypto.com and in the future tense. Don't call today's chain post-quantum: it uses Ed25519/Ristretto, CLSAG and Bulletproofs (classical elliptic-curve crypto).
  - Source: owner decision; `marketing/README.md` (Copy and claims); https://blocknetcrypto.com/#faraday (checked 2026-10-25)
- "Avoid 'untraceable' and 'anonymous' (they invite regulator and exchange trouble); say 'private'."
  - Source: `marketing/README.md:79`; `marketing/brand/taglines.md:54` (checked 2026-10-25)
- "Don't say 'more private than Monero' (same ring size today, and Monero's FCMP++ will exceed it)." Also avoid "Most private coin".
  - Source: `marketing/README.md:80`; `marketing/brand/taglines.md:53` (checked 2026-10-25)
- "Avoid the `$BNT` cashtag; it collides with Bancor."
  - Source: `marketing/README.md:81`; `marketing/brand/taglines.md:56` (checked 2026-10-25)
- Positioning: "Lead with usability and fair launch, not 'most private'."
  - Source: `marketing/brand/taglines.md:3-4` (checked 2026-10-25)

From owner preferences for seine marketing:
- Use the highest measured number per generation, with familiar names.
- Say "threads", not "workers".
- Earlier hardware-buying copy omitted the old RTX 5090 result. The October 25 NVIDIA setup guide is a newly authorized, measured same-host mode comparison; it makes no card-buying, economics or maximum-throughput claim.
- No "faster every release" copy.
- Source: owner preference, stated 2026-10-24/25

Added by this sheet from the evidence:
- Distinguish the miner dev fee from the pool fee. Check live `/api/info` and the miner version before quoting a combined rate.
- Don't write "no dev fee" or "no dev tax" about seine.
  - The tagline "No premine. No dev tax." (`taglines.md:30`) is true of the **protocol** only. Released v0.2.26 charges 1% or 2.5%, depending on the pool; v0.2.36 uses 2%. See §4.
- Don't write "ASIC-proof" or "botnet-proof". The code claims design intent only ("resist ASICs", "impractical", `pow.rs:1-4`). "Designed to resist ASICs" is the safe wording.
  - Faraday's planned lighter PoW (§0) undercuts memory-hardness messaging after ~Feb 2027.
- Don't claim Windows ARM64 builds or production Metal support (§4). AMD GPU support shipped in v0.3.0; see §11.
- Don't present M4 Max 30.9 H/s as a balanced or default setting. It used 16 threads and swap. The balanced figure is 29.3 H/s.
- Don't claim multi-socket NUMA results (`PERFORMANCE.md:145`).
- Don't write "fastest Blocknet miner" as a proven comparison. The README asserts it (`README.md:6`), but the repo has no head-to-head comparison against another miner or the daemon's built-in miner. Treat it as a brand tagline only.
- Don't quote "current block reward", network hashrate or pool hashrate as evergreen numbers. They change monthly or live.
- Don't link or cite blocknet.org (a different project, token BLOCK).

**Contradictions found between sources**
- **Address length.**
  - `marketing/brand/taglines.md:16` says "Pay a name, not a 95-character string."
  - The encoding (68 bytes base58, `wallet/wallet.go:244-256`) gives **92–93 characters**.
  - Use "a 90-plus-character string" or fix the tagline.
- **Reward example in CLI docs.**
  - `blocknet@master:docs/reference-core.md:733` shows "Reward: 72.325093035 BNT", and `:293` shows "72.325 BNT coinbase".
  - The code's initial reward is 72,325,093,035 atomic units = **723.25 BNT**, at 1e8 atomic units per BNT (`miner.go:18`, `cli.go:1010`).
  - The docs example is off by 10×. Do not quote it.
- **Post-quantum rule vs official site.** Resolved 2026-10-25: the owner lifted the ban; see the post-quantum bullet above.
- **Pool fee.** Production bntpool.com and the example config both use **0%**, verified 2026-10-25 at 11:59 PM EDT.
- **Huge pages.** README "+4–8%" vs guide "+4–6% (2 MiB) + further 2% (1 GiB)". These are consistent once combined; quote the guide's split.

---

## 7. Open questions / unverified

- **Ticker on exchanges:** is BNT the official listed ticker anywhere? Are there any exchange listings? The site states neither. No price or market data was checked.
- **Pre-relaunch chain:** code calls the 2026-02-15 genesis a "relaunch". History of the first launch (~2026-02-05 to 02-15), and whether any coins carried over, is unverified. Genesis has no coinbase, so nothing carried over on-chain.
- **Fees burned?** Coinbase must equal the subsidy exactly (`block.go:663-680`). Fees seem unclaimed, but no doc states this. Confirm with the maintainer before writing "fees are burned".
- **Faraday effect on emission and block time:** the October 27 official post proposes 10-minute target blocks and a doubled block reward, preserving the planned daily issuance rate. These remain proposed rules, not live-chain behavior; see the dated update below.
- **Faraday miner support:** when seine ships Faraday support, and whether fees stay the same, is not public yet.
- **Other Stratum miners on bntpool:** login requires protocol v2 + `submit_claimed_hash`. Whether any non-seine miner works is unknown.
- **bntpool payout interval** in production. Also, are there regional endpoints? `eu.bntpool.com` appears only in a unit test (`seine@master:src/dev_fee.rs:279`). Do not advertise it.
- **Zen 4 streaming auto-selection:** the 35.7 H/s used the streaming kernel, but automatic eligibility is stated only for Turin (Zen 5).
- **GPU economics:** an internal RTX 5090 benchmark shows ~8.9 H/s steady state (WSL, March 2026). That is below a 9950X3D CPU (10.3 H/s).
  - Source: `seine@master:docs/NVIDIA_5090_TUNING.md:107-113`
  - This historical result remains excluded from current marketing. The newly authorized October 25 NVIDIA guide uses fresh native-Windows paired measurements, documented in `batches/2026-10-nvidia-guide/research.md`, with no economics claim.
- **Energy / power per H/s:** `seine@master:docs/CPU_ENERGY.md` exists but was not reviewed for this sheet.
- **Launch-date phrasing:** "launched February 2026" is supported by genesis (2026-02-15 EST). The official site gives no launch date ("block 1" only).
- **"Blocknet ID" product name:** confirm the official name of the blocknet.id service ("Blocknet ID" vs "short names") for consistent capitalization.

---

---

## 9. Added by blog batch 1 (2026-10-25)

Verified by the batch-1 writers; full citations are in `batches/2026-10-batch-1/<slug>.md`.

- **Payment links:** `blocknet://<addr|name>?amount=&memo=` and `bntpay.com/<addr|$name>?amount=&memo=`; `amount` is required, `memo` optional; a pasted link opens the normal confirm step. Use `$name` in link examples (`blocknet://@name` misparses). Source: `blocknet-core@pool:cli.go:821-886`, `cli_payment_link_test.go:38-40` (what-is-blocknet, blocknet-id-pay-by-name notes).
- **Memos:** encrypted with the sender-receiver shared secret, so only those two can read them; short or empty memos are padded with random bytes so they look like full ones. Source: `blocknet-core@pool:wallet/memo.go:22-34`, `:65-89`; `wallet/builder.go:294-317`.
- **Privacy details:** ring size fixed "to prevent information leakage from variable ring sizes" (`crypto.go:659-660`); ring members must be real on-chain outputs (`transaction.go:1007-1009`); key images stop double spends without revealing the spent output (`transaction.go:51`, `:452-461`); consensus checks inputs = outputs + fee (`transaction.go:1060-1074`); Dandelion++ routes rotate every 10 minutes (`p2p/dandelion.go:30`). (blocknet-privacy-features notes)
- **Blocknet ID:** the wallet verifies an Ed25519 signature over handle, address and update time using the key at `https://blocknet.id/.well-known/blocknet-id.json` (`handle_resolver.go:107-184`) and prints the resolved address as verified before you confirm (`cli_cmd_wallet.go:89-99`). blocknet.id (live 2026-10-25): search a name, paste your address, "Claim free" or "Register"; one-time price by length, 100 BNT at 8 characters up to 6,000 BNT for one, names of 9+ characters free under a current promotion. The pool resolves names at Stratum login (`blocknet-pool:crates/pool-runtime/src/stratum.rs:681-698`); seine resolves the name itself and keeps that address for the run (`seine:src/miner/mod.rs:229-275`). Product name: "Blocknet ID shortname". (blocknet-id-pay-by-name notes)
- **seine on macOS:** defaults to `pcore-only` scheduling (`seine:src/config.rs:123-126`); uses at most the performance cores plus half the efficiency cores, 14 threads on a 12+4 M4 Max (`config.rs:2783-2806`); in v0.2.43, automatic threads = min(core limit, floor((available RAM − min(5% of effective total RAM, 1 GiB) − GPU reservation) ÷ 2 GiB)). The macOS archive unpacks `seine`, `README.md`, `SEINE-LICENSE.txt` and `SHA256SUMS.txt`; the binary is ad-hoc signed, so allow it in Privacy & Security or run `xattr -d com.apple.quarantine ./seine` (`docs/guides/apple-silicon.md:44-48`). (mine-blocknet-on-mac notes)
- **Zen 5 run details:** AWS m8a.8xlarge, EPYC 9R45, 32 cores, four 32 MiB L3 groups, 128 GiB; streaming kernel 37.76 H/s at 16 threads, 46.08 at 24, 49.39 at 32 (seine PR #59 `experiments/readme-hardware-20261025/summary.json`). (blocknet-mining-hardware-hashrate notes)
- **Argon2id background:** RFC 9106 (2021, IRTF CFRG): Argon2id is the variant implementations must support; 1,024-byte blocks with a BLAKE2b-based compression; the first half of the first pass is data-independent. A hash writes about 2 GiB and reads about 2 GiB. seine kernels: streaming (64-block cached ring plus streaming stores), prefetching of the next reference block, NEON+SHA3 XAR, AVX-512 native rotates (`seine:pow-kernel/src/fixed_argon.rs`, `avx512_256.rs`). (argon2id-proof-of-work-explained notes)
- **Faraday quotes** (https://blocknetcrypto.com/#faraday): "Built so a quantum computer cannot forge a spend."; "Today a spend hides among 16 outputs. After Faraday, it hides among every output on the chain."; "Planned near the first anniversary of block 1."
- **Control API token (seine v0.2.23, released 2026-10-25):** every endpoint except `/v1/health` needs a bearer token, written to `control-api.cookie` (owner-only, deleted on shutdown); `--api-token` and `--api-cookie` set it explicitly; CORS is off by default and non-local `Host` headers are rejected. Source: `seine:CHANGELOG.md:9-19`, `docs/API.md:40-46`, `docs/MINER_FLAGS.md:127-133`. (batch 2, mine-blocknet-on-servers notes)
- **NUMA placement detail:** on a two-node EPYC 7R13 at 8 threads, OS order put all threads on node 0; seine put four on each node. On an EPYC 8434P at 24 threads, seine spread threads across all eight cache groups. Measured 2026-10-24 on the same binary. Source: `seine:CPU_OPTIMIZATION_LOG.md:781-818`, `:866-915`.


## 10. Windows setup and newer bare-metal evidence (2026-10-25)

These additions use `seine@f96abd9` (v0.2.25 documentation). They supersede the earlier **overall** headline in §5, while the older cloud measurements remain separate evidence.

- **EPYC 9355P: 68.256 H/s (68.3 rounded)** on a 32-core/64-thread bare-metal Linux socket, 192 GiB across twelve DDR5-4800 channels, fixed 64 mining threads, streaming kernel, explicit 1 GiB pages and a configured 280 W package cap. Ratio of total completed hashes to total elapsed time across four fresh 120-second invocations including startup and shutdown. Manual configuration, not an automatic-default claim. Source: `experiments/amd-page-size-20261025/RESULTS.md` and `benchmark-records.json`; frozen executable and source provenance recorded in `batches/2026-10-seo-next/research.md`.
- **Windows release:** `seine-windows-x86_64.zip`, accompanied by archive checksums in release `SHA256SUMS.txt`; the ZIP bundles CUDA 12.8.61 NVRTC DLLs. Extract all files and keep them together. A supported NVIDIA driver is needed; a separate CUDA Toolkit installation is not. Source: `.github/workflows/release.yml:83-110,138-158`, `docs/guides/windows-large-pages.md:41-44`, `docs/guides/nvidia.md`.
- **Windows defaults:** `seine.exe` prompts for an address/registered shortname and pool; bntpool default is `stratum+tcp://bntpool.com:3333`. Settings default to `./seine-data/seine.config.json` relative to launch directory. Shortnames resolve afresh for each run. Source: `docs/guides/configuration.md:8-28,78-88`.
- **Optional Windows large pages:** grant Lock pages in memory to the mining account and sign out/back in. `secpol.msc` is available on Pro, Enterprise and Education. Default automatic mode falls back to regular pages. Source: `docs/guides/windows-large-pages.md:3-31`.
- **PowerShell archive verification:** `Get-FileHash .\seine-windows-x86_64.zip -Algorithm SHA256`, compared against the matching release archive's checksum. Source: https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.utility/get-filehash (checked 2026-10-25).

## NVIDIA setup evidence added October 25, 2026

These details were checked at `seine@77a3a97` against the v0.2.26 CLI help;
they supplement the earlier source snapshot above.

- Default backend selection includes the CPU and detected NVIDIA devices. Explicit
  `--backend cpu,nvidia --nvidia-devices 0` selects CPU plus device 0;
  `--backend nvidia` selects GPU-only mining. Device indices must exist on the host.
  Sources: `src/config.rs:373–386,1475–1507`; `docs/guides/nvidia.md:3–16`.
- The Windows x86_64 release bundles `nvrtc64_120_0.dll` and
  `nvrtc-builtins64_128.dll`, from NVRTC 12.8.61, alongside `seine.exe`.
  Its display driver is still required. Linux x86_64 requires the driver and
  compatible NVRTC runtime libraries installed separately. Sources:
  `.github/workflows/release.yml:83–103`; `docs/guides/nvidia.md:20–33`.
- The default NVIDIA tuning cache is `seine-data/seine.nvidia-autotune.json`.
  GPU lane selection uses available device memory, reserves headroom and checks
  allocations; advertised VRAM divided by 2 GiB is not a guaranteed lane count.
  Sources: `src/config.rs:130,370,1155–1158`;
  `src/backend/nvidia.rs:530–585,2874–2882,3350–3358`.

- **Fresh NVIDIA mode comparison, October 25, 2026:** native Windows 11, seine
  v0.2.26, RTX 5090, Ryzen 9 9950X3D, 125.4 GiB usable RAM, driver 610.88.
  Three interleaved pairs, each with one 20-second warmup and three measured
  20-second backend rounds: GPU-only **7.338482 H/s (7.3 rounded)**;
  CPU-plus-GPU **12.881818 H/s (12.9 rounded)**. Automatic settings selected 13 GPU
  lanes and 6/7/7 CPU threads; regular Windows pages; existing desktop apps open.
  This is a named-session setup example, without economics, maximum-throughput
  or other-card claims. Source: `batches/2026-10-nvidia-guide/research.md` and its
  six raw reports, paired comparison, host metadata and accepted-GPU-share record.

## Windows release verification (October 25, 2026)

The official seine v0.2.26 Windows ZIP has SHA-256
`75adac2aa25626b547f18e85b4ff20e4b6a9cf2872bc641ca2220229ffa364a5`.
Native Windows PowerShell verification matched the separate release checksum
asset. That asset checks archives; the identically named manifest inside the ZIP
checks extracted files (`.github/workflows/release.yml:105-110,138`). Matching a
hash verifies correspondence to that reference, not a malware verdict. See
`batches/2026-10-download-verification/research.md` for primary Microsoft sources,
actual command output, capture provenance and failure-case checks.

## RAM planning and share outcomes — checked October 26, 2026

- Public seine v0.2.26 CPU arenas are 2 GiB per thread. Automatic sizing reserves
  5% of effective total RAM in addition to accounting for current use; explicit
  thread counts override that reserve but retain memory checks. Sources and
  arithmetic: `research/ram-planning-2026-10-26.md`.
- Pool submission acknowledgment, verification and payout eligibility are distinct.
  Assignment difficulty and template+nonce deduplication govern shares. Sources:
  sibling pool `frontend/blog/editorial/share-outcomes-2026-10-26.md`.

## Miner updates and difficulty (October 26, 2026)

- v0.2.36 configuration: default `./seine-data/seine.config.json` is relative to
  the process working directory; `--data-dir` overrides the base. Saved fields
  cover connection details; preserve launch flags separately. Sources:
  `seine@v0.2.36:src/config.rs`, `src/user_config.rs`, `docs/guides/configuration.md`.
- Update notices run at startup and every 12 hours; they announce releases and
  do not replace the executable. Source: `src/update_check.rs`.
- Current difficulty rules: 300-second block target, 60 solve intervals in LWMA,
  recalculated per block. Source: `blocknet-core@c646196a:block.go`,
  `expectedDifficultyFromParent` and `nextDifficultyLocked`. Target division:
  `crypto-rs/src/pow.rs:blocknet_difficulty_to_target`.
- Full evidence and publication notes: `research/update-and-difficulty-2026-10-26.md`.

## Local benchmark procedure and reward maturity (October 26, 2026)

- Public seine v0.2.36 `--bench` runs locally without pool or daemon. Warmup
  rounds are excluded from measured summaries; preparation and in-flight work
  can extend elapsed time beyond requested intervals. Compare like modes and
  preserve JSON reports, environment, cache policy and all repeated pairs.
- The pool's confirmation depth is current chain height minus found-block height,
  starting at zero, with a separate node hash identity check. The public setting
  checked October 26 is 60 blocks and the minimum payment was 0.1 BNT (1 BNT since 2026-11-02). The wallet
  coinbase maturity rule also uses a depth of 60; a later payout transaction has
  its own receipt and spendability state. Five hours is target arithmetic only.
- Full source pins and verification: `research/benchmarks-and-confirmations-2026-10-26.md`.

## CPU utilization and separate fee stages (October 26, 2026)

- OS CPU utilization, local device H/s and pool-estimated address hashrate are
  separate observations. Active hashing threads differ from process threads;
  each active CPU mining thread uses a 2 GiB arena. Low utilization alone does
  not demonstrate a memory-bandwidth bottleneck or a need for more threads.
- Public seine v0.2.36 documents 2% developer mining time across pools and solo,
  with the first 10 minutes excluded. Scheduling rounds mean a short window
  need not match 2% exactly. bntpool separately reported a 0% pool fee on this date.
- For gross estimates, retained fractions multiply. Do not apply a miner-time
  factor again when the input already reflects it; confirmed balances and
  recorded payments must not have the same fees deducted a second time.
- Full sources and boundaries: `research/cpu-usage-and-fees-2026-10-26.md`.

## Windows sign-in startup and multiple computers (October 26, 2026)

- Public v0.2.36 defaults to `./seine-data` relative to the working directory,
  with connection settings in `seine.config.json`. Use an absolute `--data-dir`
  and correct scheduler working directory to preserve a working installation.
- Existing saved rig names are not rewritten by a CLI override; keep the explicit
  `--pool-worker` value in subsequent launch commands when overriding a copied
  configuration. Source: v0.2.36 `src/config.rs:858–910,2445–2483`.
- Task Scheduler ignore-new applies to the same scheduled task, not separately
  started miners. A sign-in task runs in the selected user's session; disabling
  the task does not stop an existing miner.
- Several computers can use the same receiving address with different rig names.
  Pool history groups by address/name; old shares keep their original name after
  renaming. Rewards are accounted by address, not separate balances per rig.
- Primary links and pinned pool source: [batch research](research/windows-startup-and-multiple-miners-2026-10-26.md).

## Configuration persistence (verified October 26, 2026)

v0.2.36 reads `seine.config.json` under `--data-dir` (default `./seine-data`, relative to the working directory). Its saved schema contains connection fields, not every CLI option. Explicit connection flags override existing saved values without rewriting them; missing-file bootstrap writes the initial inputs. Preserve other fields when editing, including the generated `dev_fee_pool_worker`. Dashboard thread changes are session-only. See `research/configuration-and-address-2026-10-26.md` for source functions and pool destination semantics.

## Stop diagnosis and wallet receipt (October 26, 2026)

v0.2.37 prints handled fatal errors before exit; `--ui plain` selects logs while still running mining. A backend fault can be isolated while remaining active backends continue. OS termination, sleep and a live reconnecting process require distinct observations. Wallet node synchronization and wallet output scanning are separate progress checks; ordinary public address searches do not reproduce private wallet balances. Source references and verification limits: `research/miner-stops-and-wallet-receipt-2026-10-26.md`.

## Desktop sharing and connection quality (October 26, 2026)

v0.2.38 supports CPU-only/NVIDIA-only selection, an automatic CPU search ceiling and explicit per-backend counts. Efficiency is a count/hashrate policy, not a CPU-utilization or measured-power cap. Dashboard count changes restart backends and reconnect, remain session-only and disable CPU autotuning for that run. Pool transport carries jobs, qualifying submissions and replies; local H/s and network bandwidth measure different work. See `research/desktop-and-network-2026-10-26.md` for source anchors, controlled-comparison procedure and verification limits.

## Electricity and intermittent sessions (October 26, 2026)

Energy budgeting distinguishes component sensors from whole-PC consumption and full from incremental use. The efficiency profile is a hashrate/thread selection policy, not a measured-watt cap. bntpool's current six-hour window uses share timestamps; reconnecting does not renew previous shares, and payout processing does not require a currently connected miner. Source anchors, explicit arithmetic assumptions and the stop/return example are recorded in `research/electricity-and-part-time-2026-10-26.md`.

## Falling hashrate and wallet transaction fees (October 26, 2026)

A running miner's current/session/device/pool measurements have different scopes; diagnosis compares like-for-like observations and resource evidence. Thermal/power causes need model-specific readings, not a universal threshold. The reviewed core send path estimates size from inputs/outputs/ring structure, with 1,000 atomic units minimum and 10 atomic units per estimated byte; 100,000,000 atomic units = 1 BNT. These are dated implementation defaults. CLI v1.2.0 displays a dry-run fee before confirmation, while the later send builds separately. See `research/hashrate-and-fees-2026-10-26.md` for pinned public sources, arithmetic and scope.

## Faraday plan and node lifecycle (October 26, 2026)

The official Faraday page describes a future network upgrade, with tentative activation around February 16, 2027 and a proposed one-year wallet migration period ending around February 16, 2028. Old-format coins would become unspendable after that window. Do not describe planned cryptography, mining rules or speedups as shipped behavior. CLI v1.2.0 manages a background core separately from attach; setup schedules updates, and outer status can trigger automatic update checks that restart unpinned managed cores. Attach status is distinct. Pinned sources and scope are recorded in `research/faraday-and-node-2026-10-26.md`.

## Payment and explorer workflow (October 27, 2026)

CLI v1.2.0 at `948af4de8b7a3e3cddc86a329c7d2d003cb3fd6b` previews fixed-amount sends before explicit confirmation, then prints the transaction ID and explorer link. History combines incoming outputs and recorded outgoing sends. The public explorer searches block height/hash or transaction hash, shows public fees and transaction structure, and leaves ordinary transfer amounts private. Mempool means zero confirmations; included count is current height minus containing height plus one. Private receipt is checked in the wallet. These are dated source/UI observations; details, screenshots and source locations are in [payments-and-explorer-2026-10-27.md](research/payments-and-explorer-2026-10-27.md).

## Wallet scanning and viewing access (October 27, 2026)

CLI v1.2.0 distinguishes core download from wallet output scanning: `sync` scans downloaded blocks; `outputs` can report scan progress, and `balance` warns about lag using the wallet's scanned height. `viewkeys` authorizes with the full wallet password and creates an encrypted viewing file with a separately chosen password. That file watches incoming funds and cannot send or sign; its private viewing material remains sensitive and is not a spending backup. Details, source lines and intent boundaries: [wallet sync and viewing research](research/wallet-sync-and-view-only-2026-10-27.md).

## Payment proofs and message signatures (October 27, 2026)

CLI v1.2.0 and core v0.22.0 provide distinct workflows: transaction proof generation returns a transaction key; public verification separately checks key validity and recipient output matches. Message signing uses the full wallet's spending key, accepts up to 1024 bytes, and returns a signature without broadcasting a payment. Public signature verification checks address/message/signature; challenge freshness and intended authorization remain application rules. Source pins, line references and the explorer UI observation are recorded in [payment proofs and messages research](research/payment-proofs-and-messages-2026-10-27.md).

## Wallet backup and recovery (October 27, 2026)

- Released CLI v1.2.0 supports loading a full wallet-file copy with its existing password or importing the original recovery seed into a newly named file with a new password. It lists files in the configured core wallet directory and does not overwrite same-name local backups. Seed display is not available for every wallet; file recovery and seed recovery are separate paths. Source: `blocknetprivacy/blocknet@948af4de8b7a3e3cddc86a329c7d2d003cb3fd6b:attach_wallet.go:98-337,1072-1268`.
- Core v0.22.0 imports into a new file, refuses existing-file overwrite and starts wallet catch-up scanning. Verify the full address and scan state after recovery; no universal restoration of local payment annotations or outgoing-history records is claimed. Source: `blocknetprivacy/core@5a0a5b510c1dce46f4eeb12c33a291052e44e7b3:api_handlers.go:1980-2087`; CLI `attach_wallet.go:757-882`. See `research/wallet-backup-restore-2026-10-27.md`.

## Release guard refresh (October 27, 2026)

At the earlier October 27 check, v0.2.39 was the latest published seine release. Its fee implementation, NVIDIA setup guide, configuration guide and release workflow are byte-identical to the reviewed v0.2.38 sources; all five platform archives remain available. The mining, NVIDIA, antivirus and Linux setup articles still describe supported behavior. Existing version-labelled benchmark and feature history remains historical. The pool calculator’s flat 2% miner-fee assumption remains valid; no pool deployment is needed for this verification. Source: GitHub latest-release API and `git diff v0.2.38..v0.2.39` for the four paths pinned in `release-facts.json`.

## Faraday official explanation (October 27, 2026)

Source: https://blocknetcrypto.com/blog/2026-10-27-how-blocknet-privacy-survives-quantum-computers/ (retrieved October 27, 2026 EDT). This supersedes the earlier overview where more specific. The post explicitly says Faraday is still being built and tested and its numbers, dates and design can change.

- Planned February 2027 activation at block 103,680; homepage approximate February 16 remains a tentative calendar estimate.
- Hash-based spend proofs built with Plonky3 and ML-KEM payment detection. The privacy set is the set of **Faraday coins**, growing as coins enter the format, not every old-format coin immediately.
- One year after activation to migrate; unmigrated coins become permanently unspendable. Migrated coins wait about one week before spending. Exact migration steps remain forthcoming; current wallet backups do not migrate coins.
- Existing 12-word recovery phrases continue; new wallets receive 24 words. Key-imported wallets without recovery words need a new wallet and a coin transfer. Proposed addresses are about 1,700 characters, with copy/paste, QR codes and Blocknet ID for usability.
- Proposed target block time doubles from 5 to 10 minutes while block reward doubles, preserving planned daily issuance. No invented live reward or hardware-performance claim.
- Maximum reorg depth becomes 60 blocks; newly mined coins wait 60 blocks (about 10 hours at target). Payment safety remains 10 confirmations (about 100 minutes at target). These are timing estimates, not guarantees.
- Individual spend proofs are combined into a block proof and discarded after finality. The post describes development testing; do not turn its results into an independent audit, our own benchmark or released-mainnet performance.
- Mining changes get a separate post; no future seine flags or hardware ranking are announced. Node auto-update statements must be read alongside actual version-pinning policy.

See `research/faraday-official-post-2026-10-27.md` for the refresh scope.

## Wallet, node and miner roles — October 27, 2026

- The official core combines node, wallet and mining functionality; the attached CLI shell is a client and closing it leaves the background core running. Mining is separately controlled. Source: CLI v1.2.0 `948af4d`, `docs/reference-core.md:21–23,700–735,962–969`.
- Saving `wallet_file` remembers a path, not an unlocked wallet session. After starting the core, attach and use `load` with the wallet password. Source: same pin, `docs/reference-wallet.md:90–94`.
- The wallet can scan previously downloaded blocks to find its outputs. Receipt does not require the receiving wallet application to be online; its later displayed balance depends on chain and wallet scan progress. Derived from same pin, `docs/reference-core.md:588–616`, and current-chain output/scanner behavior.
- seine defaults to pool mode; the mining node connection belongs to the pool in that workflow. Its separate daemon mode needs the user's running, authenticated core. Source: seine v0.2.39 `8a822f7`, `docs/guides/solo-mining.md`, `src/config.rs`. Detailed evidence and scope: `batches/2026-10-wallet-node-miner/research.md`.

## Desktop wallet interface reference

The visual guide uses GUI v0.22.0 (`661fff98e047f395489a0d2eba88f572c2a89935`), verified October 27, 2026 (EDT). Its Send action first requests a dry-run fee preview; Confirm Send authorizes the payment separately. Dashboard distinguishes node sync, wallet scan and spendable/pending balances. The new-wallet flow requests backup and confirmation of 12 recovery words. These are versioned interface facts, not promises about future releases. See [desktop-wallet research](batches/2026-10-desktop-wallet/research.md) for pinned source locations and screenshot methodology.

## First-receipt visual reference

The first-BNT guide uses the same pinned GUI v0.22.0 as the desktop walkthrough. History opens a transaction detail with its hash, status, block, confirmations and the wallet-owned received amount; Dashboard supplies the spendable reading after sync and scanning. The 1.25 BNT receipt, fee, hash and chain readings in the captures are labeled local demo data. No new payout constants or receipt timing guarantees are introduced. See [first-BNT research](batches/2026-10-first-blocknet/research.md).

## Desktop wallet update behavior (v0.22.0)

The released GUI opening screen displays the application package version. Both About fields, Wallet Version and Daemon Version, call the bundled daemon’s version command in this release. The tray Quit action stops API events and the daemon before exit; on macOS, closing the window only hides it. Backup Wallet File copies the active encrypted wallet file to the chosen location; multi-wallet users need a separate backup for each wallet. The application directory holds the wallet and active-wallet selection separately from the installed executable. See [wallet update research](batches/2026-10-wallet-update/research.md) for pinned source and release assets.

## Release verification: v0.2.41 (October 27, 2026)

The earlier release check verified v0.2.41. The fee source, NVIDIA setup guide, configuration guide and release workflow remain byte-identical to the previously reviewed v0.2.39 sources, with the same five platform archive names and flat 2% miner fee. v0.2.40–41 change CPU kernel selection, tuning and Linux startup; existing setup directions remain applicable, and version-labeled measurements remain historical. No new performance claims are added in this verification. Pool calculator fee assumptions remain unchanged; its historical reviewed-version label is not a latest-version claim. Source: GitHub release API, v0.2.41 commit and four SHA-256 hashes recorded in `release-facts.json`.

## Ordinary transfer timing (core/GUI v0.22.0)

The released wallet tests ordinary output maturity using `currentHeight - output.BlockHeight >= 10`; coinbase maturity is separately 60. Its transaction details count the containing block as confirmation one (`chainHeight - blockHeight + 1`), so an ordinary output reaching ten blocks of wallet depth can show eleven confirmations in transaction details. The five-minute target implies roughly 50 minutes after inclusion, plus the prior inclusion wait; this is arithmetic, not measured latency or a guarantee. Before inclusion, incoming mempool funds appear separately as `pending_unconfirmed`, outside confirmed wallet history and balance. See [transfer-time research](batches/2026-10-transfer-time/research.md).

## Release verification: v0.2.42 (October 27, 2026)

The latest release check verifies v0.2.42, published at 4:28:42 PM EDT. The fee source, NVIDIA guide, configuration guide and release workflow match the four previously reviewed hashes exactly; the five platform archives and flat 2% miner fee remain unchanged. Existing setup and antivirus guidance remains applicable. This update adds no performance claims and preserves versioned historical measurements. The pool calculator renders identical HTML in all 20 languages before and after its release-record refresh, so that metadata-only change needs no production restart.

## Payment disclosure and Faraday coin-set scope (October 27, 2026)

Released core v0.22.0 distinguishes public transaction fields from wallet-private amounts and memos. A transaction proof is deliberate disclosure: `handleVerifyProof` receives transaction hash, transaction key and recipient address, and returns matched amount and recoverable memo. View-only keys expose incoming payment detection without the private spend key; possession of an exported key is not revoked by changing a file password. Network relay does not erase off-chain invoice, website or conversation records. See [privacy/Faraday evidence](batches/2026-10-privacy-faraday/research.md).

The detailed official October 27 Faraday post describes the complete set of **Faraday coins**, growing as coins enter that format. It does not establish that every old-format historical output is already eligible. Proposed hash-based spending proofs and ML-KEM payment detection perform different jobs. Project design figures are roughly 1.4 MB per spend proof and 1 MB per combined block proof, with individual proofs discarded after finality. These are proposal figures, not our measurements or released-network claims; fees remain unsettled. NIST FIPS 203 standardizes a key-encapsulation mechanism, not a signature or certification of Blocknet.

## Transaction anatomy and change (October 27, 2026)

Released core v0.22.0's ordinary Transfer selects mature inputs, accounts for recipient total plus fee and returns positive remainder as change using the sending wallet's keys. TransferWithInputs can split change; zero remainder creates no change output. TransferAll deducts the fee from selected spendable funds and creates a recipient output. Output count does not count distinct people. The public fee is a separate transaction field. The illustrative 10 = 3 + 6.9995 + 0.0005 BNT example is assumed arithmetic, not a fee quote or observed transaction. Source and exact reader-job boundaries: [transaction walkthrough research](batches/2026-10-transaction-walkthrough/research.md).

## Currency and privacy by default (October 28, 2026)

Released core v0.22.0 (`5a0a5b510c1dce46f4eeb12c33a291052e44e7b3`) uses 8,640-block schedule months and reaches its 2 BNT/block tail at height 414,720 (48 × 8,640). Approximately 100 million BNT is an initial-distribution/progress reference, not a hard cap; cumulative issuance continues. At the current five-minute target the tail corresponds to 288 × 2 = 576 BNT/day, or 210,240 BNT over 365 target-paced days. These are derived target rates, not guaranteed production or circulating-supply quotes. Cumulative issuance starts at height 1; the genesis block has no coinbase. Ordinary transfers, fees, maturity and pool payouts do not mint the block reward again. Fee destination is not asserted.

The official October 27 Faraday proposal describes continuity of chain/history/balances and a paired five-to-ten-minute target change with doubled reward at the corresponding stage. The matched-stage arithmetic is 288 × R = 144 × (2 × R). Its approximately 343/685 BNT block rewards and roughly 98,700 BNT/day are rounded illustrations; no final future tail amount is inferred. Dates/parameters remain proposals.

Privacy claims here concern ordinary spending. Coinbase has a separate validation path and no spending inputs. Normal inputs require ring size16; one-time output keys, hidden amounts and encrypted memos support ordinary payment privacy. Public structure/fees and participants' records remain. Relay reduces origin exposure without guaranteeing hidden IP addresses in every situation. Deliberate sharing can disclose payment details. Fungibility is a conceptual currency property supported by reduced public payment-history information; no universal acceptance, stable price or return is promised. Detailed evidence: [currency/privacy research](batches/2026-10-currency-privacy/research.md).

## Currency rule enforcement and hidden-amount verification (October 28, 2026)

Released core v0.22.0 (`5a0a5b510c1dce46f4eeb12c33a291052e44e7b3`) validates candidate blocks before accumulated-work fork choice (`block.go:1741–1805`); work, wallet balance or publishing a repository change is not a substitute for validation. Coinbase output commitment and encoded amount must match the height-based reward using publicly derived blinding (`block.go:543–574`). Ordinary spending separately verifies authorization and signed binding, valid amount ranges and commitment balance, and key-image non-reuse (`transaction.go:1002–1090`). Optional trusted-checkpoint fast sync can skip expensive historical PoW/signature/range-proof re-verification; do not claim that every local node rechecks every historical proof. A 64-bit range runs from zero through 2^64 − 1 inclusive. Payment proofs or viewing access may deliberately disclose ordinary payment information. Full source references, deployment-policy scope and intent boundaries: `batches/2026-10-currency-trust/research.md`.

## Generic atomic swaps and the public VEIL announcement (October 28, 2026)

- Generic HTLC examples connect conditional transfers through a shared hash/preimage and different refund deadlines; the claim branch also requires the intended recipient's signature. Refund conditions make a spend eligible, rather than broadcasting a refund automatically. BIP 199 is a closed explanatory proposal, not a Blocknet specification. Sources: https://github.com/bitcoin/bips/blob/master/bip-0199.mediawiki, https://github.com/bitcoin/bips/blob/master/bip-0065.mediawiki and https://arxiv.org/abs/1801.09515. See batch research for claim/sequence boundaries.
- The official Blocknet overview names VEIL atomic swaps and presents their post-quantum redesign as part of proposed Faraday. This public statement does not establish a currently usable pair, command, service or specific swap protocol. Only the announcement is described; the new general HTLC diagram must not be represented as VEIL's design. Source: https://blocknetcrypto.com/#faraday (checked October 28).


## 11. Setup requirements and settings in v0.3.0 (checked November 2, 2026)

Checked against the published v0.3.0 Linux and Windows archives and `seine@v0.3.0`.

- **AMD GPUs:** RX 6000, RX 7000, RX 9000 and Radeon PRO, on Windows x86_64 and Linux x86_64. Discrete cards are selected automatically; integrated Radeon needs `--backend amd`. Windows needs the AMD driver. Linux needs the amdgpu driver and the ROCm HIP runtime. Source: `docs/guides/amd.md`, `CHANGELOG.md`, `seine.exe` from the v0.3.0 ZIP.
- **Linux NVIDIA:** the driver is enough. NVRTC is used when installed; otherwise seine loads built-in kernels for GeForce GTX 10 series and newer. Source: `docs/guides/nvidia.md`.
- **Linux glibc:** the v0.3.0 binary needs symbols up to GLIBC_2.30; the README states 2.31 or newer (Ubuntu 20.04, Debian 11). Source: `objdump -T` on the release binary; `README.md`. This supersedes the 2.39 requirement in earlier notes.
- **Backends:** `--backend` takes `cpu`, `nvidia`, `amd` and `metal`, comma-separated. `--nvidia-devices` needs `nvidia` in `--backend`, or seine stops with an error naming the flag. A `--threads` count larger than memory allows stops with `configured CPU lanes need ~N GiB RAM`. `--backend amd` with no runtime stops with `all GPU backends failed to start`. Source: runs of the v0.3.0 Linux binary against an unreachable pool, November 2, 2026.
- **Thread keys (v0.3.1):** `-` and `+` remove or add one thread per CPU backend for the current run only; the count is not saved. The change applies to the running CPU threads while GPUs and the pool connection keep running. While tuning is still choosing a count, the keys leave it alone and the dashboard shows how long tuning has left. With a fixed `--threads`, the change restarts mining and keeps session statistics. In v0.3.0 and earlier every change restarted the mining backends and reconnected the pool. `--threads` turns thread tuning off. Source: `seine@v0.3.1:docs/guides/configuration.md` (Dashboard controls); v0.3.0 log line `cpu-autotune | disabled; using configured thread count`.
- **Saved settings:** `seine.config.json` holds the mode, address, pool, worker name and dev fee worker name. Backend and thread flags are not saved. Automatic updates restart with the same arguments. Source: first-run file from the v0.3.0 binary; `docs/guides/configuration.md`.

## 12. v0.3.1 features used in the November 4, 2026 posts

Checked against `seine@v0.3.1` (33d94e2) and live bntpool.com `/api/info` on November 4, 2026.

- **Automatic updates:** check at startup and every 12 h; header `update vX.Y.Z available`, then a 60 s countdown `updating to vX.Y.Z in 42s` while mining continues; `u` installs now, `s` skips that version (remembered in the data directory). Signed archive verified, the new executable must start before it replaces the old one, then seine restarts with the same arguments and logs `updated to seine vX.Y.Z`. Work that needs a newer release pauses mining (`installing seine vX.Y.Z now: mining is paused until Seine is updated`) and installs at once, even a skipped version. `--no-auto-update` keeps only the notice; self-built binaries only notify. Upgrade notes for v0.2.45 (Linux exits once after installing; Windows double-run with restarting scripts) and the HiveOS/mmpOS package switch: `CHANGELOG.md` v0.3.1. Source: `docs/guides/configuration.md:110-127`, `src/miner/tui.rs` `update_spans`, `src/update_check.rs:215`.
- **CPU tuning:** dashboard `Tuning:` rows and their meanings, keep the lowest thread count that matches the fastest, no scheduled tests once `done`, re-tune triggers (new version change, sustained hashrate move, freed RAM, hardware/OS/limit change), `seine.cpu-autotune.json`, `--cpu-profile efficiency`, `--disable-cpu-autotune-threads`. Source: `docs/guides/autotuning.md`.
- **Dashboard layout (v0.3.1 renderer):** MINING panel shows `Difficulty`, `Rejected`, `Accepted` in pool mode (`Submitted`, `Stale`, `Accepted` in daemon mode); CONFIG shows `Pool`, `Threads` (`[+] [-]` hint), `Worker`, `Tuning`; POOL BALANCE shows `Pending`, `Paid`, `Miner`/`Solo`, `Min payout`; log scroll keys arrows/PgUp/PgDn/Home/End, title `LOG  paused, End to follow`; gold diamonds mark accepted shares and found blocks; current hashrate windows 5 s (NVIDIA 30 s). Source: `src/miner/tui.rs`, `src/miner/hashrate_tracker.rs:6`, `src/miner/mining_tui.rs:20`. Blog capture: `export_blog_dashboard_capture` in `src/miner/thread_showcase.rs`, demo values (Ryzen 9 9950X 16.63 H/s, 21 threads, from the desktop chart).
- **AMD GPUs:** see §11; plus per-card lanes (~9 on a 16 GB RDNA2-4 card), `amd#N` device rows with `online (initialized in background, N lanes)`, RDNA3 `high` performance level when root, crash guard after two runtime crashes (retry after next update or with `amd` listed). Source: `docs/guides/amd.md`, `src/control_api/mod.rs:3142`.
- **HiveOS/mmpOS packages:** `seine-hiveos-X.Y.Z.tar.gz`, `seine-mmpos-X.Y.Z.tar.gz`; flight sheet fields, imported mmpOS profile, GPU rigs mine GPUs only, CPU-only rigs mine CPU, CUDA 12 driver (RTX 50: 570+), mmpOS image Ubuntu 20.04+, upgrade by changing the URL version, HiveOS log path. Source: `docs/guides/hiveos.md`, `docs/guides/mmpos.md`.
- **Farm proxy:** `--proxy-listen`, rigs pay their own valid full address (up to 16, 10 min slot release), invalid/missing pays `--address`, per-rig pool stats on bntpool.com, one-minute summary log, up to 64 pool connections per payout address, `solo:` rigs supported, memos not carried. Source: `docs/guides/farm-proxy.md`, `src/proxy.rs:627-641`, `src/config.rs:2859-2864`.
- **Exchange payouts:** `--payout-memo`, `--no-payout-memo`, prompt for known shared addresses, unattended runs must pass the flag; bntpool memo minimum 10 BNT (`memo_min_payout_amount`), personal 1 BNT. Source: `docs/EXCHANGE_PAYOUTS.md`; bntpool.com `/api/info` 2026-11-04 20:55 EST.
