# Backlog

Current cross-site priorities: [shared publishing plan](cross-site-publishing-plan.md).
Its October 28 queue takes precedence over older deferred suggestions below.

## Current editorial direction — October 27, 2026

Prioritize ordinary newcomer questions and broader Blocknet fundamentals: choosing software, receiving and sending, first mining setup, and understanding balances. Improve the existing owner when a question already has a guide. Defer narrow merchant/invoice workflows. Competitor comparisons and work requiring rented hardware remain on hold by owner preference.

Published: `blocknet-wallet-node-miner` explains the software roles and which applications must stay running. Published: `blocknet-desktop-wallet` adds the ordinary desktop create/open, backup, receive, send-review and history journey with released-interface screenshots and labeled demo data.

Published: `get-first-blocknet` guides newcomers through choosing a transfer or mining, then identifying the first receipt and spendable balance.

Researched topics not yet written, in priority order. Batch 1 (2026-10-25) took the hardware,
Mac, Argon2id, what-is-Blocknet, Blocknet ID and privacy topics; pool-side topics went to the
bntpool.com blog. Each owns one search job; check
`keyword-map.md` before starting, and move the row there when the post is published. Evidence
and SERP notes for each topic are in `research/2026-10-keyword-research.md`.

The owner chose (2026-10-25) to write Argon2id-specific posts now, for the traffic before the
Faraday upgrade (planned ~2027-02-16). Date them, and plan a rewrite or retirement when the
proof of work changes.

| # | Slug | Primary cluster | Search job | Evidence needed |
|---|---|---|---|---|
| 2 | `argon2id-vs-randomx` | randomx vs argon2; blocknet vs monero mining | How Argon2id differs from RandomX for a Monero miner, and which machines suit which | Same-machine XMRig vs seine runs on 4-6 hosts |
| 4 | `blocknet-mining-pools` | blocknet pool; blocknet mining pool list | Which pools exist and how fees and payouts compare | Dated table of every pool; disclose that we run bntpool.com |
| 5 | `blocknet-faraday-upgrade-miners` | blocknet faraday; blocknet hard fork 2027 | What miners must change for Faraday, and when | Final public spec and a seine release that supports it; publish around Jan 2027 |

Dropped 2026-10-25 per owner: a BNT-vs-BLOCK explainer (assume readers know Blocknet) and
"is CPU mining worth it" (invites an unfavorable verdict).

## Blocknet topics (general blog)

Coin-level posts that build search presence for Blocknet itself and link to seine and
bntpool.com. Facts come from fact-sheet section 1; demand is unverified (no SERP check yet), so
check search results before drafting.

| Slug | Primary cluster | Search job |
|---|---|---|
| Published: `blocknet-faraday-upgrade` | blocknet faraday; blocknet post-quantum | Existing guide refreshed from the official October 27 post; update it when public plans change |

Low-confidence demand, revisit with Search Console data: AVX-512 on Zen 5, laptop mining.

Published November 4, 2026 (v0.3.1 cycle): `mine-blocknet-amd-gpu`, `mine-blocknet-hiveos-mmpos`, `blocknet-mining-farm-proxy`; refreshed `update-seine-miner` (automatic updates), `seine-automatic-cpu-tuning`, `seine-dashboard-explained` (v0.3.1 renderer capture), `miner-keeps-stopping`, and solo/1 BNT minimum passages. Pool side: `solo-mine-blocknet-on-bntpool`, `mine-blocknet-to-exchange`.

## Off-site work that matters as much as posts

- Get Blocknet and seine listed on miningpoolstats, hashrate.no, whattomine and Kryptex (deferred
  by the owner on 2026-10-25; requests go out from the owner's accounts).
- Ask upstream for a "Mine" link from blocknetcrypto.com to seineminer.com.
- Give bntpool.com a crawlable title and description ("Blocknet mining pool").
- Rewrite the seine and blocknet-pool GitHub descriptions with "Blocknet miner/pool".
- Offer corrections to the pages ranking for our terms (powhub.top, cassh100k's guide).

Covered 2026-10-25: zero pool hashrate and connection errors now belong to the expanded
[bntpool tracking guide](https://bntpool.com/blog/track-your-miner-on-bntpool/#zero-hashrate),
including all 20 languages. Do not create a competing Seine article.

Published 2026-10-25: `mine-blocknet-nvidia-gpu` owns NVIDIA setup, runtime diagnosis, accepted-share verification and the measured RTX 5090 GPU-only/CPU-plus-GPU comparison. See `batches/2026-10-nvidia-guide/research.md`. Other cards, including RTX 4090, still need their own measurements before adding results.

Published 2026-10-25: `antivirus-flags-crypto-miner` owns Windows download warnings, official-release checksum verification and reporting unresolved detections. It includes an actual v0.2.26 Windows PowerShell verification record and release-assets capture.

Published October 26, 2026: `how-much-ram-to-mine-blocknet` covers memory arithmetic, current-release sizing and allocation troubleshooting. No new hardware measurements were commissioned; channel-scaling benchmarks remain separate future research.

Published October 27, 2026: `update-blocknet-wallet` covers routine desktop application updates, preserving wallet data, fully quitting, choosing the official package and verifying the same address and synchronized history. Available in all 20 languages; no new hardware measurements.

Published October 27, 2026: `blocknet-transfer-time` answers the ordinary payment timing question in all 20 languages: broadcast, block inclusion, ten later blocks, spendable balance, and checks for sender/recipient. Three released-wallet screenshots use labeled local demo data, not measured real transfers.

Published October 27, 2026: `blocknet-payment-visibility` and `blocknet-faraday-privacy`, each in all 20 languages, with four original explanatory graphics. The existing privacy overview's Faraday section now distinguishes eligible Faraday coins from all historical outputs. User requested privacy/Faraday coverage and approved publication.

Published October 28, 2026: `blocknet-transaction-explained`, all 20 languages, with an original numbered transaction-accounting diagram. User approved the proposed walkthrough with “proceed”; Android remains deferred.

October 28, 2026: user approved all four currency/privacy topics. Published three new owners (`blocknet-supply`, `blocknet-fungibility`, `blocknet-faraday-monetary-policy`) and materially refreshed `blocknet-privacy-features`, each in all 20 languages. Added article-specific graphics and corrected adjacent Faraday-set wording. Android and hardware-rental work remain outside this batch.

October 28, 2026: user approved currency-trust coverage. Added `who-controls-blocknet` for developer/miner/node/holder authority and rule enforcement; expanded the existing `bulletproofs-explained` owner for authorization, private amount conservation, repeated-spend rejection and separate reward issuance. Both use the 20-language pipeline and original diagrams. No hardware rental or competitor comparison.

October 28, 2026: user approved `blocknet-atomic-swaps`, then explicitly chose a general atomic-swap explainer with a short public VEIL announcement section. Explains conditional funding, claims, refund paths and disclosure; generic A/B diagram is not a VEIL implementation or a supported pair. No private specification or unverified swap procedure is published. Uses the 20-language pipeline and existing topic/list presentation.
