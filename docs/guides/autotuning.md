# How automatic tuning works

Seine starts mining immediately and tunes while it works, on real mining work. It
finds the thread count and CPU kernel that hash fastest, uses no more threads than
that takes, then stops testing. Nothing here needs configuring; this page explains
what it does.

`--disable-cpu-autotune-threads` turns off both thread and kernel tuning. An explicit
`--threads` count stays fixed, and Seine still picks the kernel for it.

## What you see

The dashboard's `Tuning:` row says where things stand:

| Row | Meaning |
|---|---|
| `trying kernels, about 4 min left` | Comparing CPU kernels while mining |
| `12 vs 16 threads, about 3 min left` | Comparing thread counts: the thread count moves between the two while it measures |
| `update: 12 vs 14 threads, ~2 min` | A new version changed something for your CPU |
| `checking saved settings` | Confirming last run's result still holds |
| `done` | Tuned. The thread count stays put unless something changes |
| `rechecking 12 vs 16 threads` | Another look at a result that was too close to call |
| `low on RAM, best is 12 threads` | Running fewer threads until RAM is free |
| `waiting for RAM` | Not enough free RAM for one thread yet |

The log has one line when tuning starts and says why, and one when it finishes, for
example `tuned to 12 threads`. Plain logs (`--ui plain`) and the control API also list
each comparison.

## Finding the best settings

Each comparison runs the current setting, then the candidate, then the current setting
again, so a change in load does not count for or against the candidate. All of it is
real mining, and a candidate that is clearly slower is cut short.

Seine starts from the best it knows: this machine's saved result, then a starting
point for your CPU, then an estimate from your cores and free RAM. It compares any
kernels worth trying on your CPU, then searches thread counts in growing steps until
adding threads stops helping, then narrows down. A count that matches the fastest
one within 0.25% is as good as it, so Seine keeps the lowest such count and the RAM
stays free for your other programs. It only moves to fewer threads once the
measurements show little is lost. A result too close to call is checked again straight
away, then a few more times half an hour apart.

Kernels your CPU cannot run are never tried, and kernels known not to win on your CPU
are skipped. Most machines finish within minutes; a slow or busy machine can take
longer because each measurement needs enough completed hashes to mean something.

`--cpu-profile efficiency` keeps the lowest count within 25% of the fastest instead.

## When tuning runs again

Once tuned, Seine runs no scheduled tests. It looks again only when:

- **A new version changes something that affects speed.** It starts on your saved
  result, tests what changed (a new kernel, an improved kernel even if it lost before,
  or another change that could move the best thread count) and skips everything else.
  A version that changes nothing about hashing runs no tests at all.
- **Hashrate moves by more than 7% for 10 minutes**, for example when another program
  starts or stops using the CPU. If fewer threads then win, Seine checks now and then
  whether it can go back.
- **More RAM becomes free** and RAM was what held the thread count back.
- **Your CPU, operating system or CPU limits change**, or a CPU backend stops.

Results are saved in `seine.cpu-autotune.json` in the data directory.

## Memory pressure and recovery

Available RAM is checked once per second. Workers finish their current hash before
parking and releasing their arenas and reference tables. Linux budgets honor nested
cgroup limits, including `memory.high`, and credit clean inactive file cache that
can be reclaimed. Linux pressure checks prefer the miner's own cgroup so unrelated
host stalls do not cause false reductions. Sustained memory stalls or Windows
low-memory notifications reduce the operating count when arena headroom is also
tight; brief reclaim spikes do not force a reduction. On macOS, memory-pressure
warnings alone decide when running workers go, because free memory moves as the
system compresses it.
The CPU backend can pause at zero workers when no complete arena fits, then retry
after 30 seconds of sustained headroom. Released Linux HugeTLB arenas return to the
reserved hugepage pool; the miner does not resize that OS pool. Parked worker slots
follow the CPU and configured worker ceiling, with a startup cap for Linux
cgroup-v2 task limits that leaves room for control and networking threads. If
automatic worker creation still runs out of resources, startup cleans up and
retries with fewer slots; explicit fixed counts report failure instead. The nonce
stride remains fixed. Raising a RAM limit can activate more provisioned workers;
raising a task limit requires restarting to provision additional slots. A task
budget too small for the CPU and supporting threads reports a startup error.
Only active arenas count against the refreshed memory allowance. Failed allocation
attempts back off, starting at five minutes.


## What tuning never interrupts

Tuning does not reconnect to the pool or restart GPU backends. Surviving CPU
workers keep their arenas; parked lanes retain their nonce positions and resume
within the same disjoint nonce allocation. Manual thread changes disable automatic
adjustment for that run. The tuner preserves a previously learned higher count when
a later run is temporarily limited by RAM or worker slots. Cache schema 10 records remain usable
as starting points; schema 11 also stores measurements and the resource-limit flag.
Hardware, build, affinity/topology, and page-mode mismatches discard the seed.

## Offline benchmarks

Offline `--bench` runs retain the warmed kernel autotune sweep and finalist
confirmation. Their measurements are kept distinct from productive mining samples.
Live tuning improves its estimate over time; it does not guarantee the global
optimum immediately on an unfamiliar machine.

See also [large servers](large-servers.md) for CPU placement and memory headroom, and the
[performance results](../PERFORMANCE.md) for measured kernel comparisons.
