// Every published article has one topic; translations share the source slug.
export const TOPICS = [
  {
    "id": "getting-started",
    "key": "topicGettingStarted"
  },
  {
    "id": "hardware",
    "key": "topicHardware"
  },
  {
    "id": "troubleshooting",
    "key": "topicTroubleshooting"
  },
  {
    "id": "rewards",
    "key": "topicRewards"
  },
  {
    "id": "blocknet",
    "key": "topicBlocknet"
  }
] as const;
const groups: Record<string, readonly string[]> = {
  "getting-started": [
    "blocknet-transfer-time",
    "update-blocknet-wallet",
    "get-first-blocknet",
    "blocknet-wallet-node-miner",
    "blocknet-desktop-wallet",
    "configure-seine-miner",
    "start-miner-automatically-windows",
    "update-seine-miner",
    "how-to-mine-blocknet",
    "mine-blocknet-on-windows",
    "mine-blocknet-on-linux",
    "mine-blocknet-on-mac",
    "blocknet-wallet-for-mining",
    "how-crypto-mining-works"
  ],
  "hardware": [
    "mining-electricity-use",
    "mine-while-using-pc",
    "miner-cpu-usage",
    "benchmark-seine-miner",
    "seine-automatic-cpu-tuning",
    "how-much-ram-to-mine-blocknet",
    "mine-blocknet-nvidia-gpu",
    "mine-blocknet-amd-gpu",
    "mine-blocknet-hiveos-mmpos",
    "mine-blocknet-on-servers",
    "blocknet-mining-farm-proxy",
    "blocknet-mining-hardware-hashrate",
    "mine-crypto-on-old-pc-or-laptop",
    "huge-pages-blocknet-mining",
    "cpu-mining-2026",
    "argon2id-proof-of-work-explained"
  ],
  "troubleshooting": [
    "blocknet-wallet-not-syncing",
    "mining-hashrate-dropping",
    "miner-keeps-stopping",
    "antivirus-flags-crypto-miner",
    "seine-dashboard-explained"
  ],
  "blocknet": [
    "blocknet-atomic-swaps",
    "who-controls-blocknet",
    "blocknet-faraday-monetary-policy",
    "blocknet-fungibility",
    "blocknet-supply",
    "blocknet-transaction-explained",
    "blocknet-payment-visibility",
    "blocknet-faraday-privacy",
    "blocknet-wallet-backup-restore",
    "blocknet-payment-proof",
    "blocknet-sign-verify-message",
    "blocknet-view-only-wallet",
    "send-receive-blocknet",
    "blocknet-explorer-guide",
    "blocknet-faraday-upgrade",
    "run-blocknet-node",
    "blocknet-transaction-fees",
    "dandelion-plus-plus-explained",
    "proof-of-work-vs-proof-of-stake",
    "blocknet-payment-links",
    "what-is-blocknet",
    "seed-phrase-explained",
    "blocknet-privacy-features",
    "what-is-a-privacy-coin",
    "bulletproofs-explained",
    "blocknet-encrypted-memos",
    "stealth-addresses-explained",
    "blocknet-id-pay-by-name",
    "ring-signatures-explained"
  ]
};
export function topicFor(slug: string) {
    const matches = TOPICS.filter(t => groups[t.id]?.includes(slug));
    if (matches.length !== 1) throw new Error(`Assign exactly one blog topic to ${slug}`);
    return matches[0];
}
