# Blocknet wallets hub — research (2026-11-05)

Intent: a reader searching "blocknet wallet" / "bnt wallet" wants to choose which wallet
application to install. Nearest owners: `blocknet-wallet-for-mining` (create a wallet and get a
mining address, with a short which-wallet section for miners) and `blocknet-desktop-wallet`
(desktop walkthrough). This post adds the comparison of all three applications (official desktop,
official command line, Gyroid), their platforms, download verification and moving a wallet
between them, then routes to the task guides.

Search evidence: Search Console 2026-10-24..11-03, seineminer.com, query "blocknet wallet":
6 impressions, 2 clicks, average position 6.3, landing on wallet posts; Google (en/us) top results
for "blocknet wallet" are unrelated-project documentation.

## Sources (checked 2026-11-05)

- Desktop wallet: https://github.com/blocknetprivacy/blocknet-gui/releases/latest = v0.23.0
  (2026-10-29). Assets: Windows x64 .exe; Linux x64 AppImage, .deb, .rpm, Flatpak; macOS Apple
  silicon .dmg and Intel .app.zip; Android arm64 .apk; SHA256SUMS.txt.
- Command-line wallet: https://github.com/blocknetprivacy/blocknet/releases/latest = v1.2.1
  (2026-10-28). Assets: Linux x64 and arm64, macOS x64 and arm64, Windows x64, Android arm64 zips;
  SHA256SUMS.txt. It installs, starts and updates the Blocknet core and offers an interactive
  wallet shell (fact sheet "Wallet, node and miner roles").
- Core: https://github.com/blocknetprivacy/core/releases/latest = v0.23.0 (2026-10-29).
- Gyroid: https://github.com/obselate/gyroid, MIT license, latest release v0.2.9 (2026-11-04).
  README: "A Blocknet (BNT) wallet ... One binary has the graphical interface and the command-line
  tools." Release assets: linux-x64 tar.gz, win-x64 zip, macOS arm64 dmg, SHA256SUMS.
  Release notes requirements: Linux x64 glibc 2.38+, Wayland session, Vulkan driver; Windows x64
  Vulkan driver; macOS Apple silicon, macOS 14+. "Gyroid uses an installed Blocknet core. If none
  is installed, it downloads the latest release and checks it against that release's
  SHA256SUMS.txt."
  Author obselate is a member of the blocknetprivacy GitHub organization (API membership check
  204, 2026-11-05) and a contributor to blocknetprivacy/core.
  Screens (src/Gyroid/Views/Pages): Library, Overview, Receive, Send, History, Messages, Contacts,
  Mining, Network, Seed, Settings, Sync, Tools. Services include Blocknet ID lookup
  (Services/Send/BlocknetId.gs), blocknet:// payment links (PaymentLink.gs), address book,
  auto-lock (WalletLockScheduler.gs).
  CLI usage text (src/Gyroid/Views/Cli/Cli.gs:618-633): status, balance, address, unlock, lock,
  node status|start|stop|purge, history [--limit N], message list|send, send <address> <amount>
  [--memo TEXT] [--dry-run], send --resume, sync, unload, load <filename.dat>, sign/verify,
  prove/verify-proof, viewkeys --show, view-only create|import, settings auto-lock.

## Screenshots (2026-11-05)

`public/assets/blog/gyroid-first-sync.jpg` and `gyroid-wallet-library.jpg` are real captures of
Gyroid v0.2.9 (linux-x64 release, archive verified against the release SHA256SUMS) running on
Ubuntu 24.04 under a headless weston 13 session (pixman renderer, Mesa lavapipe Vulkan), with an
isolated scratch home. First start downloaded the latest core and synced mainnet from Gyroid's
built-in checkpoint (72,000) to tip 72,179 in about 15 minutes. The library capture is before any
wallet existed; its data-folder path line is blurred. No wallet contents, keys or recovery words
appear. Captured with weston-screenshooter.
