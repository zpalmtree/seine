# Exchange payouts in Seine

Use the full exchange deposit address and your own deposit memo:

```sh
./seine --pool-url POOL_URL --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
```

Keep the memo separate from the address in Seine. Its case, leading zeros and
literal characters are preserved. Known exchange addresses prompt for a missing
memo during interactive startup. Personal wallets need no memo or extra setup.

## Compatible pool formats

- Pools advertising `payout_memo_v1` receive a separate `payout_memo` login field.
  They must acknowledge the exact address-plus-memo account. A successful login
  without that acknowledgement does not enable mining.
- The verified Longhorn endpoints `stratum+ssl://us.longhornpool.net:3334` and
  `stratum+ssl://asia.longhornpool.net:3334` receive `ADDRESS?memo=MEMO` in the
  login address. Their TLS certificate is verified against the hostname, and
  their protocol-v2 login reply must advertise `submit_claimed_hash`. Longhorn
  validates the memo as 1–124 printable ASCII characters without spaces. Its
  reply currently supplies neither an account echo nor a payout minimum.
  [Longhorn's connection page](https://longhornpool.net/start) generates commands
  using the same suffix format.

The Longhorn adapter applies only to those exact TLS endpoints. It is selected
again for every reconnect or pool switch. Unknown pools never receive an
address-suffix retry or an address-only login in place of the supplied memo.
Other memo formats need a verified adapter before they can be used.

## Minimums and stats

The payout minimum is optional display information. Seine shows a positive
integer `min_payout_atomic` reported by the accepted login, and hides the label
when it is missing or invalid. Missing minimum information does not invalidate
an otherwise accepted memo destination. No default minimum is guessed.

Stats use the accepted pool and its account format. Longhorn balances are read
from its HTTPS API with the entire address-plus-memo encoded as one path
segment; the response must echo that exact account. A failed lookup never falls
back to the shared address. Replies from a previous pool are ignored after
switching pools. Account paths are redacted from local telemetry errors.

## Verification

The automated tests cover endpoint selection, literal memo bytes, optional
minimums, rejected account acknowledgements, pre-login jobs, encoded stats
paths, wrong-account responses and pool changes. A separate opt-in integration
check connects to both Longhorn endpoints, verifies login and receipt of work,
then disconnects without submitting shares or starting any mining backend:

```sh
cargo test --no-default-features --bin seine longhorn_live_memo_handshake -- --ignored
```

A successful handshake does not test an actual exchange deposit or payout.
