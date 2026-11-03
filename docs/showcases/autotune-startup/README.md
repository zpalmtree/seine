# Tuning in the dashboard

Showcase URL: http://localhost:8770/

From the repository root:

```sh
python3 -m http.server 8770 --directory docs/showcases/autotune-startup
```

`index.html` renders the real dashboard with synthetic data at 120 and 46 terminal
columns, for every tuning state: trying kernels, comparing thread counts, a thread key
pressed while tuning picks the count, tuned, another look at a close result, a restart on
the saved settings, an update that improved this CPU's kernel, an update that found something faster, low on RAM,
RAM returned and no room for one thread. Regenerate it with:

```sh
cargo test --bin seine export_autotune_showcase -- --ignored
```

The page labels the source revision it was rendered from. `desktop.png` and `mobile.png`
were captured and reviewed through the shared Playwright MCP at 1100 px and 390 px.
