# CPU thread control feedback

Open `index.html` directly, or run from the repository root:

```sh
python3 -m http.server 8769 --directory docs/showcases/thread-controls
```

Showcase URL: http://localhost:8769/

The page renders nine fixture states at 120-column desktop and 40-column
mobile terminal widths using the actual Ratatui dashboard. It includes ready,
checking RAM, an already pending change, the maximum and minimum counts,
insufficient RAM, an unavailable control channel, no CPU backend, and a
successful replacement session. The page records the renderer's Git blob ID.
`desktop.png` and `mobile.png` are captures through the shared Playwright MCP.

Regenerate the page after changing the renderer or feedback:

```sh
cargo +1.93.0 test --locked --no-default-features -p seine export_thread_controls_showcase -- --ignored
```

Thread increases are checked against current effective available RAM, the
configured reserve and any Metal reservation. Existing CPU arena backing is
credited once; configured but unallocated workers do not create headroom.
A preflight rejection preserves the running session and allows retrying.
Decreases remain available when the machine is under memory pressure.

Manual thread overrides also recheck RAM after teardown, before replacement
backends start. If memory disappears during teardown, the replacement fails
with an explanation instead of forcing the allocation. The checks are best
effort, not an OS memory reservation; other processes can still create pressure
after admission. Manual thread selection continues to disable autotuning.
While automatic tuning is still picking the count, the keys leave it alone and
say how long tuning has left and that `--threads N` sets a count at start; that
state is in the tuning showcase (`docs/showcases/autotune-startup`).
