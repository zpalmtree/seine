# Exchange payout preview

Serve the captured Seine components with:

```sh
python3 -m http.server 5198 --directory docs/showcases
```

Open http://localhost:5198/exchange-payouts/.

The page exports the real Ratatui dashboard using synthetic account data. The
startup prompt is captured from the same function used for first-run and saved
addresses. These are renderer fixtures, not a live exchange payout. No personal
memo is included. Source blob IDs are embedded in the page.

Regenerate after changing the interface:

```sh
cargo test --no-default-features --bin seine export_memo_prompt_showcase -- --ignored
cargo test --no-default-features --bin seine export_exchange_payout_showcase -- --ignored
```

Use the configured Playwright MCP to capture each section at desktop (1100 px)
and mobile (390 px) widths; review and replace the images in `captures/`.

Validated: 77 configuration tests, 18 dashboard tests, 34 pool-runtime tests,
and 75 pool-related tests; default-feature `cargo check --bin seine` passed.
Desktop and mobile captures cover mining, waiting for the minimum, an empty
balance, reconnecting, rejected memo support, personal wallets, older pools,
and the missing/invalid memo prompts. The rejected state also covers mining
being disabled until the pool acknowledges the memo.

The minimum shares the Paid row, aligned right when it fits. Narrow layouts
retain a separate line to keep both values readable. The 18 dashboard tests
and desktop/mobile captures were rerun after this layout change.
