# Mining to an exchange

You can mine straight to your exchange deposit address. Copy the address and your
deposit memo from the exchange's deposit page:

```sh
./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
```

Copy the memo exactly. Capital letters and leading zeros matter, and a wrong memo can
send your coins to the wrong account. Personal wallets don't need a memo.

## Starting Seine

The memo is required for known exchange addresses such as SafeTrade. If you start
Seine in a terminal without one, it asks for the memo and saves it. Services and other
unattended miners need `--payout-memo` on the command line, or Seine won't start.

To go back to a personal wallet, set the new address with `--no-payout-memo`.

Memos need a direct pool connection. They don't work in solo mode or through the farm
proxy.

## Which pools work

The pool has to support memos:

- **bntpool.com** supports them. The minimum payout to a memo is 10 BNT.
- **Longhorn** supports them on `stratum+ssl://us.longhornpool.net:3334` and
  `stratum+ssl://asia.longhornpool.net:3334`. Its memos must be printable ASCII with no
  spaces.

If a pool doesn't confirm your memo, Seine won't mine on it, so your work can't be paid
to the wrong place.

## Checking your balance

Each address and memo pair has its own balance. On the pool's stats page, enter both
the address and the memo.
