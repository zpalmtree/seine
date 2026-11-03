# Farm proxy

Run one Seine as a proxy on your local network and point every rig at it. The pool
sees a single connection, while each rig mines its own share of the work.

Use it when you run many rigs and want:

- one place to set the pool, payout address and backup pools;
- one outbound pool connection instead of one per rig.

## Start the proxy

```bash
./seine --proxy-listen 0.0.0.0:3333 \
        --pool-url stratum+tcp://bntpool.com:3333 \
        --address YOUR_ADDRESS --pool-worker farm-1
```

The proxy does not mine, so it needs little CPU or memory and can run on any machine
the rigs can reach. Every pool option works here: TLS (`stratum+ssl://`), backup pools,
and `--pool-proxy` for a SOCKS5 proxy. See [pool connections](configuration.md#pool-connections).

## Point the rigs at it

On each rig, use the proxy's LAN address as the pool:

```bash
./seine --pool-url 192.168.1.10:3333 --address YOUR_ADDRESS --pool-worker rig-07
```

Mining OS flight sheets work the same way: put the proxy's address in the pool URL field.

## What to expect

- **Payouts.** Each rig's shares pay the address the rig logs in with, so rigs mining to
  different wallets can share one proxy (up to 16 addresses). A rig whose address is
  missing or invalid pays the proxy's `--address`. When 16 addresses are in use, a rig
  with another address is turned away until one of them has had no rigs for 10 minutes,
  and a rig whose address the pool refuses is turned away with the pool's reason.
- **Stats.** On pools that credit shares per rig, such as bntpool.com, each rig shows on
  the pool dashboard under its own worker name. Other pools show the farm under the
  proxy's `--pool-worker` name. The proxy also logs connected rigs, accepted and rejected
  shares, an estimated hashrate and its pool connections every minute, and logs each rig
  as it connects and disconnects.
- **Dev fee.** Each rig still runs its own dev-fee rounds. They go straight to
  bntpool.com, or through the proxy when the rig cannot reach bntpool.com, and in both
  cases they pay the dev fee address.
- **Pool connections.** The proxy opens as many pool connections as the farm needs for
  every rig to get work on each new block, which for most farms is one or a few, and up to
  64 per payout address for very large ones. On pools that hand out extra work on request
  it tops up without interrupting rigs; on other pools the extra connections bring the
  work instead.
- **Upgrades.** At the Faraday network upgrade, rigs on a Seine release without support
  for it are asked to update and disconnected, the same way the pool treats them.

## Security

Anyone who can reach the proxy's port can mine through it, and their shares pay your
address. Listen on a LAN address or firewall the port; do not expose it to the internet.
