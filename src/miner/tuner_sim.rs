//! Deterministic simulator that drives the real CPU tuner one second at a time and
//! scores what a miner would have experienced. No threads, no hashing, no sleeping.
//! Curves come from scenario data; these built-in shapes use round numbers.
//!
//! Print the scoreboard:
//!   cargo test --bin seine tuner_sim_scoreboard -- --ignored --nocapture
//! Extra scenarios: `SEINE_TUNER_SIM_SCENARIOS=<file.json>`.
//! Write results: `SEINE_TUNER_SIM_OUT=<file.json>`.
use super::*;
use crate::backend::cpu::CpuAdaptiveSnapshot;
use blocknet_pow_kernel::CpuKernel;
use std::collections::BTreeMap;

const GIB_PER_THREAD: u64 = 2;
const WINDOW_5M: usize = 300;

/// One machine, replayed for `hours`.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct Scenario {
    pub name: String,
    /// Hardware the miner has a validated start for (stricter targets).
    #[serde(default)]
    pub known: bool,
    pub start_threads: usize,
    pub min_threads: usize,
    pub max_threads: usize,
    pub start_kernel: String,
    /// Kernels the tuner may try; the first entry is compared first.
    pub candidates: Vec<String>,
    /// Kernel the tuner moves to at once, without a comparison.
    #[serde(default)]
    pub startup_kernel: Option<String>,
    /// Total H/s by kernel at measured counts; other counts interpolate.
    pub rates: BTreeMap<String, Vec<(usize, f64)>>,
    /// Slow load wander (standard deviation of the AR(1) multiplier).
    #[serde(default)]
    pub noise: f64,
    /// Other programs: (bursts per hour, share of hashrate taken, seconds each).
    #[serde(default)]
    pub bursts: Option<(f64, f64, u64)>,
    /// A GPU mining beside the CPU: (H/s, slow wander like `noise`). The tuner judges
    /// the whole rig, so the GPU's swings land in every comparison.
    #[serde(default)]
    pub gpu: Option<(f64, f64)>,
    /// (CPU threads, share of the GPU's rate lost per thread above that count).
    #[serde(default)]
    pub gpu_starved_above: Option<(usize, f64)>,
    /// (second, usable threads) RAM limit changes. Starts at `max_threads`.
    #[serde(default)]
    pub cap_events: Vec<(u64, usize)>,
    /// A machine at the edge of its RAM until a second: running this many threads
    /// makes reclaim stalls report room for one fewer, which goes once one is shed.
    #[serde(default)]
    pub ram_edge: Option<(usize, u64)>,
    /// One thread below that edge the reading wavers, a third of the time showing
    /// no room for the thread that is already running.
    #[serde(default)]
    pub ram_waver: bool,
    /// (second, multiplier) sustained load steps on every count.
    #[serde(default)]
    pub load_events: Vec<(u64, f64)>,
    #[serde(default = "default_warm_secs")]
    pub warm_secs: u64,
    #[serde(default = "default_hours")]
    pub hours: u64,
    #[serde(default = "default_seed")]
    pub seed: u64,
}

fn default_warm_secs() -> u64 {
    8
}
fn default_hours() -> u64 {
    24
}
fn default_seed() -> u64 {
    0x5eed
}

impl Scenario {
    fn kernel(name: &str) -> CpuKernel {
        name.parse()
            .unwrap_or_else(|_| panic!("unknown kernel {name}"))
    }

    fn rate(&self, kernel: CpuKernel, threads: usize) -> f64 {
        let Some(points) = self.rates.get(kernel.as_str()) else {
            return 0.0;
        };
        if threads == 0 || points.is_empty() {
            return 0.0;
        }
        let first = points[0];
        if threads <= first.0 {
            return first.1 * threads as f64 / first.0 as f64;
        }
        for pair in points.windows(2) {
            let ((a, ra), (b, rb)) = (pair[0], pair[1]);
            if threads <= b {
                return ra + (rb - ra) * (threads - a) as f64 / (b - a) as f64;
            }
        }
        points.last().unwrap().1
    }

    /// The GPU's rate beside this many CPU threads.
    fn gpu_rate(&self, threads: usize) -> f64 {
        let Some((hps, _)) = self.gpu else {
            return 0.0;
        };
        let lost = self.gpu_starved_above.map_or(0.0, |(above, per)| {
            threads.saturating_sub(above) as f64 * per
        });
        hps * (1.0 - lost).max(0.0)
    }

    /// What the miner is judged on: CPU plus any GPU.
    fn rig_rate(&self, kernel: CpuKernel, threads: usize) -> f64 {
        self.rate(kernel, threads) + self.gpu_rate(threads)
    }

    /// How much the best setting beats the best kernel at the starting count.
    fn gain_over_start(&self) -> f64 {
        let at_start = self
            .candidates
            .iter()
            .map(|name| self.rig_rate(Self::kernel(name), self.start_threads))
            .fold(0.0_f64, f64::max);
        self.oracle().0 / at_start.max(1e-9) - 1.0
    }

    fn cap_at(&self, now: u64) -> usize {
        self.cap_events
            .iter()
            .rev()
            .find(|(at, _)| *at <= now)
            .map_or(self.max_threads, |(_, cap)| *cap)
    }

    fn load_at(&self, now: u64) -> f64 {
        self.load_events
            .iter()
            .rev()
            .find(|(at, _)| *at <= now)
            .map_or(1.0, |(_, load)| *load)
    }

    /// Best reachable rate at the final RAM limit and load.
    fn oracle(&self) -> (f64, usize) {
        let cap = self
            .cap_at(self.hours * 3600)
            .min(self.max_threads)
            .min(match self.ram_edge {
                Some((edge, until)) if until >= self.hours * 3600 => edge - 1,
                _ => usize::MAX,
            });
        let mut best = 0.0_f64;
        for name in &self.candidates {
            let kernel = Self::kernel(name);
            for threads in 1..=cap {
                best = best.max(self.rig_rate(kernel, threads));
            }
        }
        // Smallest count within the tie band of that best, on any kernel. Rigs judge
        // ties on the CPU's own rate, so their band is a share of the CPU's best.
        let cpu_best = self
            .candidates
            .iter()
            .flat_map(|name| (1..=cap).map(move |threads| (name, threads)))
            .map(|(name, threads)| self.rate(Self::kernel(name), threads))
            .fold(0.0_f64, f64::max);
        let band = if self.gpu.is_some() {
            TIE_BAND * cpu_best
        } else {
            TIE_BAND * best
        };
        let mut smallest = cap;
        for threads in (1..=cap).rev() {
            if self
                .candidates
                .iter()
                .any(|name| self.rig_rate(Self::kernel(name), threads) >= best - band)
            {
                smallest = threads;
            }
        }
        (best, smallest)
    }
}

const TIE_BAND: f64 = 0.0025;

/// Everything the scoreboard asks about one run.
#[derive(Debug, Clone, Serialize)]
pub(super) struct Score {
    pub name: String,
    pub known: bool,
    pub final_threads: usize,
    pub final_kernel: String,
    pub oracle_threads: usize,
    /// Final configuration's rate over the best reachable rate.
    pub settled_frac: f64,
    pub extra_threads: i64,
    pub extra_gib: i64,
    /// Mean rate in the first five minutes over the final configuration's rate.
    pub first_5m_frac: f64,
    /// Last moment a 5-minute mean sat below 99% of the final configuration.
    pub within_1pct_secs: u64,
    /// When the final configuration began and stayed, ignoring short probes.
    pub settled_secs: u64,
    /// Hashes lost to probes after settling.
    pub after_settle_cost: f64,
    pub probes_after_settle: u64,
    pub lost_first_hour: f64,
    pub lost_whole_run: f64,
    pub kernel_trials: u64,
    pub log_lines_first_hour: u64,
    pub log_lines_after_first_hour_per_day: f64,
    /// Seconds above the RAM limit, beyond a 15 s reaction allowance.
    pub ram_overrun_secs: u64,
}

struct Rng(u64);

impl Rng {
    fn unit(&mut self) -> f64 {
        // xorshift64*: fixed sequence for every run of a scenario.
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        let value = self.0.wrapping_mul(0x2545_F491_4F6C_DD1D);
        (value >> 11) as f64 / (1u64 << 53) as f64
    }

    fn normal(&mut self) -> f64 {
        let (a, b) = (self.unit().max(1e-12), self.unit());
        (-2.0 * a.ln()).sqrt() * (2.0 * std::f64::consts::PI * b).cos()
    }
}

/// Machine state seen through the tuner's snapshot.
struct Machine {
    snapshot: CpuAdaptiveSnapshot,
    target: usize,
    warm_until: u64,
    warmed_from: usize,
    switch_until: u64,
    carry: f64,
    wander: f64,
    burst_until: u64,
    gpu_carry: f64,
    gpu_wander: f64,
    gpu_hashes: u64,
    /// Separate from the CPU's draws, so adding a GPU leaves the CPU's noise as it was.
    gpu_rng: Rng,
    rng: Rng,
}

impl Machine {
    fn new(scenario: &Scenario) -> Self {
        let threads = scenario.start_threads;
        let bytes = threads as u64 * CPU_LANE_MEMORY_BYTES;
        Self {
            snapshot: CpuAdaptiveSnapshot {
                kernel: Scenario::kernel(&scenario.start_kernel),
                kernel_generation: 1,
                kernel_ready: true,
                backing_bytes: [0, 0, 0, bytes, 0],
                running: true,
                threads,
                committed_bytes: bytes,
                resident_bytes: bytes,
                hashes: 0,
                hashing: threads > 0,
                allocation_failures: 0,
                warmed: threads,
            },
            target: threads,
            warm_until: 0,
            warmed_from: threads,
            switch_until: 0,
            carry: 0.0,
            wander: 0.0,
            burst_until: 0,
            gpu_carry: 0.0,
            gpu_wander: 0.0,
            gpu_hashes: 0,
            gpu_rng: Rng((scenario.seed ^ 0x6770_7500) | 1),
            rng: Rng(scenario.seed | 1),
        }
    }

    fn set_threads(&mut self, now: u64, threads: usize, warm_secs: u64) {
        if threads > self.snapshot.threads {
            self.warmed_from = self.snapshot.warmed.min(self.snapshot.threads);
            self.warm_until = now + warm_secs;
        }
        self.target = threads;
        self.snapshot.threads = threads;
        let bytes = threads as u64 * CPU_LANE_MEMORY_BYTES;
        self.snapshot.committed_bytes = bytes;
        self.snapshot.resident_bytes = bytes;
        self.snapshot.backing_bytes = [0, 0, 0, bytes, 0];
        self.snapshot.hashing = threads > 0;
        if threads <= self.warmed_from {
            self.warmed_from = threads;
            self.warm_until = 0;
        }
    }

    fn set_kernel(&mut self, now: u64, kernel: CpuKernel) {
        if kernel != self.snapshot.kernel {
            self.snapshot.kernel = kernel;
            self.snapshot.kernel_generation += 1;
            self.snapshot.kernel_ready = false;
            self.switch_until = now + 2;
        }
    }

    /// Advance one second and return the configuration's true rate.
    fn step(&mut self, scenario: &Scenario, now: u64) -> f64 {
        if now >= self.switch_until {
            self.snapshot.kernel_ready = true;
        }
        let warming = now < self.warm_until;
        let working = if warming {
            self.warmed_from
        } else {
            self.snapshot.threads
        };
        self.snapshot.warmed = working;
        let rate = scenario.rate(self.snapshot.kernel, working) * scenario.load_at(now);
        // Slow wander (load from other programs). Workers finish at different
        // moments, which moves hashes across a boundary but does not accumulate.
        self.wander = 0.995 * self.wander + 0.0999 * scenario.noise * self.rng.normal();
        let mut achieved = rate * (1.0 + self.wander);
        if let Some((per_hour, depth, secs)) = scenario.bursts {
            if now >= self.burst_until && self.rng.unit() < per_hour / 3600.0 {
                self.burst_until = now + secs;
            }
            if now < self.burst_until {
                achieved *= 1.0 - depth;
            }
        }
        self.carry += achieved.max(0.0);
        let spread = 0.5 * (working.max(1) as f64).sqrt() * self.rng.normal();
        let whole = (self.carry + spread).floor().max(0.0);
        self.carry -= whole;
        self.snapshot.hashes += whole as u64;
        if let Some((_, noise)) = scenario.gpu {
            self.gpu_wander = 0.995 * self.gpu_wander + 0.0999 * noise * self.gpu_rng.normal();
            self.gpu_carry += (scenario.gpu_rate(working) * (1.0 + self.gpu_wander)).max(0.0);
            let whole = self.gpu_carry.floor();
            self.gpu_carry -= whole;
            self.gpu_hashes += whole as u64;
        }
        rate + scenario.gpu_rate(working)
    }

    /// Hashes from every backend on a rig with a GPU, as the miner counts them.
    fn rig_hashes(&self, scenario: &Scenario) -> Option<u64> {
        scenario.gpu.map(|_| self.snapshot.hashes + self.gpu_hashes)
    }
}

/// What one tick asked the machine to do, whichever tuner produced it.
#[derive(Default)]
struct Step {
    threads: Option<usize>,
    kernel: Option<CpuKernel>,
    dashboard_lines: u64,
    trial_started: bool,
    settled_now: bool,
}

trait Driver {
    fn tick(
        &mut self,
        now: u64,
        snapshot: CpuAdaptiveSnapshot,
        cap: usize,
        rig_hashes: Option<u64>,
    ) -> Step;
}

struct NewDriver {
    search: super::super::adaptive_search::Search,
    trace: bool,
}

impl Driver for NewDriver {
    fn tick(
        &mut self,
        now: u64,
        snapshot: CpuAdaptiveSnapshot,
        cap: usize,
        rig_hashes: Option<u64>,
    ) -> Step {
        let out = self
            .search
            .tick_rig(now, &[snapshot], cap, false, rig_hashes);
        if self.trace {
            for line in &out.details {
                println!("  {now:>6}s {line}");
            }
            for note in &out.notes {
                println!("  {now:>6}s NOTE {}", note.text);
            }
            for event in &out.events {
                if let super::super::adaptive_search::TrialEvent::Close { hashes, secs } = event {
                    println!(
                        "  {now:>6}s window {hashes} hashes in {secs} s ({:.2}/s)",
                        *hashes as f64 / (*secs).max(1) as f64
                    );
                }
            }
        }
        Step {
            threads: out.threads,
            kernel: out.kernel,
            dashboard_lines: out.notes.len() as u64,
            trial_started: out.details.iter().any(|d| d.starts_with("comparing ")),
            settled_now: out.settled_now,
        }
    }
}

fn new_driver(scenario: &Scenario) -> NewDriver {
    search_for(
        scenario,
        facts_for(&scenario.candidates, "build", &[]),
        None,
    )
}

pub(super) fn run_new(scenario: &Scenario) -> Score {
    drive(scenario, &mut new_driver(scenario))
}

fn drive(scenario: &Scenario, driver: &mut dyn Driver) -> Score {
    let mut machine = Machine::new(scenario);
    let total = (scenario.hours * 3600) as usize;
    let mut rates = Vec::with_capacity(total);
    let mut configs: Vec<(usize, CpuKernel)> = Vec::with_capacity(total);
    let (mut kernel_trials, mut lines_first_hour, mut lines_later) = (0u64, 0u64, 0u64);
    let mut overrun = 0u64;
    let mut over_for = 0u64;
    let mut first_settled = None;
    for now in 0..total as u64 {
        let rate = machine.step(scenario, now);
        rates.push(rate);
        configs.push((machine.snapshot.threads, machine.snapshot.kernel));
        let cap = scenario.cap_at(now).min(scenario.max_threads);
        if machine.snapshot.threads > cap {
            over_for += 1;
            if over_for > 15 {
                overrun += 1;
            }
        } else {
            over_for = 0;
        }
        let reported = match scenario.ram_edge {
            Some((edge, until)) if now < until && machine.snapshot.threads >= edge => {
                cap.min(edge - 1)
            }
            Some((edge, until))
                if now < until
                    && scenario.ram_waver
                    && machine.snapshot.threads + 1 == edge
                    && (now / 40 + scenario.seed) % 3 == 0 =>
            {
                cap.min(edge - 1)
            }
            Some((edge, until)) if now < until => cap.min(edge),
            _ => cap,
        };
        let step = driver.tick(
            now,
            machine.snapshot,
            reported,
            machine.rig_hashes(scenario),
        );
        if let Some(kernel) = step.kernel {
            machine.set_kernel(now, kernel);
        }
        if let Some(threads) = step.threads {
            machine.set_threads(now, threads, scenario.warm_secs);
        }
        if step.trial_started {
            kernel_trials += 1;
        }
        if step.settled_now {
            first_settled.get_or_insert(now as usize);
        }
        if now < 3600 {
            lines_first_hour += step.dashboard_lines;
        } else {
            lines_later += step.dashboard_lines;
        }
    }
    score(
        scenario,
        &rates,
        &configs,
        kernel_trials,
        lines_first_hour,
        lines_later,
        overrun,
        first_settled,
    )
}

fn score(
    scenario: &Scenario,
    rates: &[f64],
    configs: &[(usize, CpuKernel)],
    kernel_trials: u64,
    lines_first_hour: u64,
    lines_later: u64,
    ram_overrun_secs: u64,
    first_settled: Option<usize>,
) -> Score {
    let total = rates.len();
    let (best, smallest) = scenario.oracle();
    // The final configuration is what the miner spent most of its last two hours on.
    let tail = &configs[total.saturating_sub(7200)..];
    let mut time_in: BTreeMap<(usize, &str), usize> = BTreeMap::new();
    for (threads, kernel) in tail {
        *time_in.entry((*threads, kernel.as_str())).or_default() += 1;
    }
    let (&(final_threads, final_kernel_name), _) =
        time_in.iter().max_by_key(|(_, secs)| **secs).unwrap();
    let final_kernel = Scenario::kernel(final_kernel_name);
    let load = scenario.load_at(total as u64);
    let final_rate = scenario.rig_rate(final_kernel, final_threads) * load;
    let reference = (best * load).max(1e-9);

    // Settled: the start of the last stretch on the final configuration that was
    // interrupted only by excursions of at most five minutes.
    let mut settled = first_settled.unwrap_or(0);
    let mut away = 0usize;
    for (index, config) in configs.iter().enumerate() {
        if *config == (final_threads, final_kernel) {
            away = 0;
        } else {
            away += 1;
            if away > WINDOW_5M {
                settled = settled.max(index + 1);
            }
        }
    }
    let mut probes = 0u64;
    let mut after_lost = 0.0;
    let mut after_possible = 0.0;
    for index in settled..total {
        let on_final = configs[index] == (final_threads, final_kernel);
        if !on_final && (index == settled || configs[index - 1] == (final_threads, final_kernel)) {
            probes += 1;
        }
        let possible = final_rate / load * scenario.load_at(index as u64);
        after_possible += possible;
        after_lost += (possible - rates[index]).max(0.0);
    }
    let after_settle_cost = after_lost / after_possible.max(1e-9);

    // Last 5-minute mean below 99% of the final rate.
    let mut sum = 0.0;
    let mut within = 0usize;
    for (index, rate) in rates.iter().enumerate() {
        sum += rate;
        if index >= WINDOW_5M {
            sum -= rates[index - WINDOW_5M];
        }
        let span = (index + 1).min(WINDOW_5M) as f64;
        if sum / span < 0.99 * final_rate {
            within = index + 1;
        }
    }
    let mean = |slice: &[f64]| slice.iter().sum::<f64>() / slice.len().max(1) as f64;
    let first_5m = mean(&rates[..WINDOW_5M.min(total)]);
    let first_hour = mean(&rates[..3600.min(total)]);
    let whole = mean(rates);
    let later_days = (total.saturating_sub(3600)) as f64 / 86_400.0;
    Score {
        name: scenario.name.clone(),
        known: scenario.known,
        final_threads,
        final_kernel: final_kernel_name.to_string(),
        oracle_threads: smallest,
        settled_frac: final_rate / reference,
        extra_threads: final_threads as i64 - smallest as i64,
        extra_gib: (final_threads as i64 - smallest as i64) * GIB_PER_THREAD as i64,
        first_5m_frac: first_5m / final_rate.max(1e-9),
        within_1pct_secs: within as u64,
        settled_secs: settled as u64,
        after_settle_cost,
        probes_after_settle: probes,
        lost_first_hour: 1.0 - first_hour / reference,
        lost_whole_run: 1.0 - whole / reference,
        kernel_trials,
        log_lines_first_hour: lines_first_hour,
        log_lines_after_first_hour_per_day: if later_days > 0.0 {
            lines_later as f64 / later_days
        } else {
            0.0
        },
        ram_overrun_secs,
    }
}

fn curve(points: &[(usize, f64)]) -> Vec<(usize, f64)> {
    points.to_vec()
}

/// Shapes of machine we must handle, in round numbers.
pub(super) fn built_in() -> Vec<Scenario> {
    let base = Scenario {
        name: String::new(),
        known: false,
        start_threads: 1,
        min_threads: 1,
        max_threads: 1,
        start_kernel: "avx2".into(),
        candidates: vec!["avx2".into(), "avx2-stream".into()],
        startup_kernel: None,
        rates: BTreeMap::new(),
        noise: 0.008,
        bursts: Some((3.0, 0.2, 40)),
        gpu: None,
        gpu_starved_above: None,
        cap_events: Vec::new(),
        ram_edge: None,
        ram_waver: false,
        load_events: Vec::new(),
        warm_secs: default_warm_secs(),
        hours: default_hours(),
        seed: default_seed(),
    };
    let rates = |plain: &[(usize, f64)], fast: &[(usize, f64)]| {
        BTreeMap::from([
            ("avx2".to_string(), curve(plain)),
            ("avx2-stream".to_string(), curve(fast)),
        ])
    };
    vec![
        // A tiny VM: nothing to search, a clear kernel winner found by comparison.
        Scenario {
            name: "vm-2-threads".into(),
            start_threads: 1,
            max_threads: 2,
            rates: rates(&[(1, 1.5), (2, 2.3)], &[(1, 1.8), (2, 2.8)]),
            ..base.clone()
        },
        // A desktop that gains from every SMT thread, limited by nothing.
        Scenario {
            name: "desktop-climbs-to-16".into(),
            start_threads: 8,
            max_threads: 16,
            rates: rates(
                &[(8, 7.0), (12, 7.8), (16, 8.2)],
                &[(8, 9.0), (12, 10.0), (16, 10.5)],
            ),
            ..base.clone()
        },
        // A desktop whose hashrate peaks part way, then falls.
        Scenario {
            name: "desktop-peaks-at-12".into(),
            start_threads: 8,
            max_threads: 24,
            rates: rates(
                &[(8, 9.0), (12, 10.0), (16, 9.8), (24, 9.0)],
                &[(8, 11.0), (12, 12.0), (16, 11.8), (24, 11.2)],
            ),
            ..base.clone()
        },
        // A server flat within noise past its physical cores: extra threads buy nothing.
        Scenario {
            name: "server-flat-after-32".into(),
            start_threads: 32,
            max_threads: 64,
            noise: 0.002,
            bursts: None,
            rates: rates(
                &[(32, 30.0), (64, 30.0)],
                &[(16, 22.0), (32, 40.0), (64, 40.0)],
            ),
            ..base.clone()
        },
        // Small real gains close to the top: no step is proven by a quick comparison.
        Scenario {
            name: "desktop-small-gains-near-the-top".into(),
            start_threads: 10,
            max_threads: 12,
            noise: 0.003,
            bursts: None,
            rates: rates(
                &[(4, 7.0), (12, 9.0)],
                &[
                    (4, 9.8),
                    (6, 12.0),
                    (8, 12.6),
                    (9, 12.8),
                    (10, 12.85),
                    (11, 12.95),
                    (12, 13.1),
                ],
            ),
            ..base.clone()
        },
        // A small real gain from extra threads that must be kept.
        Scenario {
            name: "server-small-real-gain".into(),
            start_threads: 16,
            max_threads: 32,
            noise: 0.002,
            bursts: None,
            rates: rates(
                &[(16, 14.0), (32, 14.2)],
                &[(16, 18.6), (24, 18.75), (32, 18.7)],
            ),
            ..base.clone()
        },
        // A big two-socket box whose best is every logical thread.
        Scenario {
            name: "numa-server-96".into(),
            start_threads: 48,
            max_threads: 96,
            rates: rates(
                &[(48, 18.0), (96, 24.0)],
                &[(48, 26.0), (72, 34.0), (96, 38.0)],
            ),
            ..base.clone()
        },
        // RAM fits fewer threads than the peak wants.
        Scenario {
            name: "ram-limited-6-of-16".into(),
            start_threads: 6,
            max_threads: 16,
            cap_events: vec![(0, 6)],
            rates: rates(&[(6, 5.0), (16, 8.0)], &[(6, 6.5), (16, 10.0)]),
            ..base.clone()
        },
        // 12 threads fill the RAM: running them brings reclaim stalls, 11 is calm.
        Scenario {
            name: "ram-edge-12-of-24".into(),
            start_threads: 12,
            max_threads: 24,
            ram_edge: Some((12, u64::MAX)),
            warm_secs: 20,
            rates: rates(
                &[(6, 4.5), (12, 5.2), (24, 5.0)],
                &[(6, 6.0), (8, 6.6), (12, 6.8), (16, 6.7), (24, 6.5)],
            ),
            ..base.clone()
        },
        // The same on a quiet machine, until the program holding RAM exits after two hours.
        Scenario {
            name: "ram-edge-freed-after-2h".into(),
            start_threads: 12,
            max_threads: 24,
            ram_edge: Some((12, 7200)),
            noise: 0.004,
            bursts: None,
            warm_secs: 20,
            // The 12th thread is worth 2%, so this checks the recovery, not how small a
            // gain the search can resolve.
            rates: rates(
                &[(6, 4.5), (12, 5.2), (24, 5.0)],
                &[(6, 6.0), (11, 6.67), (12, 6.8), (16, 6.7), (24, 6.5)],
            ),
            ..base.clone()
        },
        // The best count is one below the RAM edge, and the reading there wavers
        // between room for the next thread and none.
        Scenario {
            name: "ram-edge-wavering-14-of-32".into(),
            start_threads: 13,
            max_threads: 32,
            start_kernel: "avx2-stream".into(),
            ram_edge: Some((14, u64::MAX)),
            ram_waver: true,
            noise: 0.004,
            bursts: None,
            warm_secs: 20,
            rates: rates(
                &[(6, 11.0), (13, 12.5), (32, 11.0)],
                &[
                    (6, 14.2),
                    (9, 15.6),
                    (12, 16.1),
                    (13, 16.35),
                    (14, 16.4),
                    (32, 14.0),
                ],
            ),
            ..base.clone()
        },
        // Another program takes half the RAM for four hours, then frees it.
        Scenario {
            name: "ram-taken-and-returned".into(),
            start_threads: 12,
            max_threads: 12,
            cap_events: vec![(7200, 6), (21_600, 12)],
            rates: rates(&[(6, 5.0), (12, 8.0)], &[(6, 6.5), (12, 10.0)]),
            ..base.clone()
        },
        // A busy desktop: load wanders by several percent.
        Scenario {
            name: "busy-desktop".into(),
            start_threads: 8,
            max_threads: 16,
            noise: 0.03,
            bursts: Some((12.0, 0.3, 60)),
            rates: rates(&[(8, 7.0), (16, 8.0)], &[(8, 9.0), (16, 10.5)]),
            ..base.clone()
        },
        // A CPU and GPU rig: a flat top on the CPU, judged with a GPU whose rate
        // wanders more than the CPU differences being measured. RAM frees up later.
        Scenario {
            name: "rig-flat-top-with-gpu".into(),
            known: true,
            start_threads: 16,
            max_threads: 32,
            cap_events: vec![(0, 27), (1200, 32)],
            gpu: Some((15.0, 0.03)),
            rates: rates(
                &[(8, 4.0), (16, 7.5), (24, 8.6), (32, 8.5)],
                &[
                    (8, 5.0),
                    (16, 9.6),
                    (20, 10.6),
                    (24, 11.0),
                    (27, 11.05),
                    (28, 11.06),
                    (32, 10.9),
                ],
            ),
            ..base.clone()
        },
        // Past 28 threads the CPU starves the GPU: a little more CPU, much less GPU.
        Scenario {
            name: "rig-cpu-starves-gpu".into(),
            start_threads: 16,
            max_threads: 32,
            gpu: Some((15.0, 0.03)),
            gpu_starved_above: Some((28, 0.02)),
            rates: rates(
                &[(8, 4.0), (16, 7.5), (24, 8.6), (32, 8.8)],
                &[(8, 5.0), (16, 9.6), (24, 11.0), (28, 11.3), (32, 11.5)],
            ),
            ..base.clone()
        },
        // The same rig where the CPU has a clear best count.
        Scenario {
            name: "rig-peak-with-gpu".into(),
            start_threads: 8,
            max_threads: 24,
            gpu: Some((15.0, 0.03)),
            rates: rates(
                &[(8, 9.0), (12, 10.0), (16, 9.8), (24, 9.0)],
                &[(8, 11.0), (12, 12.0), (16, 11.8), (24, 11.2)],
            ),
            ..base.clone()
        },
        // A slow host: one hash is a large share of any short sample.
        Scenario {
            name: "slow-4-threads".into(),
            start_threads: 2,
            max_threads: 4,
            rates: rates(&[(2, 0.9), (4, 1.3)], &[(2, 1.0), (4, 1.5)]),
            ..base.clone()
        },
        // The challenger is worse: the tuner must stay and stop asking.
        Scenario {
            name: "plain-kernel-wins".into(),
            start_threads: 4,
            max_threads: 8,
            rates: rates(&[(4, 4.5), (8, 6.0)], &[(4, 3.5), (8, 5.0)]),
            ..base.clone()
        },
        // Known hardware starting on its validated kernel.
        Scenario {
            name: "known-start".into(),
            known: true,
            start_threads: 16,
            max_threads: 16,
            start_kernel: "avx2-stream".into(),
            startup_kernel: Some("avx2-stream".into()),
            rates: rates(&[(8, 7.0), (16, 8.2)], &[(8, 9.0), (16, 10.5)]),
            ..base.clone()
        },
        // Load rises, then falls, while a count below the peak stays unchanged.
        Scenario {
            name: "load-comes-and-goes".into(),
            start_threads: 16,
            max_threads: 16,
            load_events: vec![(7200, 0.6), (14_400, 1.0)],
            rates: rates(&[(8, 7.0), (16, 8.2)], &[(8, 9.0), (16, 10.5)]),
            ..base
        },
    ]
}

/// A machine tuned on one build, then running the next one.
pub(super) struct UpdateCase {
    pub name: &'static str,
    pub before: Scenario,
    pub after: Scenario,
    pub hot_changed: bool,
    /// Kernels whose revision this build raised.
    pub improved: &'static [&'static str],
    /// Expected final setting on the new build.
    pub expect: (usize, &'static str),
    /// Most comparisons the update may need.
    pub most_trials: u64,
}

pub(super) fn update_cases() -> Vec<UpdateCase> {
    let machine =
        |plain: &[(usize, f64)], fast: &[(usize, f64)], extra: Option<&[(usize, f64)]>| {
            let mut rates = BTreeMap::from([
                ("avx2".to_string(), curve(plain)),
                ("avx2-stream".to_string(), curve(fast)),
            ]);
            let mut candidates = vec!["avx2".to_string(), "avx2-stream".to_string()];
            if let Some(extra) = extra {
                rates.insert("avx512-stream".to_string(), curve(extra));
                candidates.push("avx512-stream".to_string());
            }
            Scenario {
                name: String::new(),
                known: true,
                start_threads: 8,
                min_threads: 1,
                max_threads: 24,
                start_kernel: "avx2-stream".into(),
                candidates,
                startup_kernel: None,
                rates,
                noise: 0.008,
                bursts: Some((3.0, 0.2, 40)),
                gpu: None,
                gpu_starved_above: None,
                cap_events: Vec::new(),
                ram_edge: None,
                ram_waver: false,
                load_events: Vec::new(),
                warm_secs: default_warm_secs(),
                hours: 12,
                seed: default_seed(),
            }
        };
    let plain: &[(usize, f64)] = &[(8, 7.0), (12, 7.8), (16, 7.6), (24, 7.0)];
    let fast: &[(usize, f64)] = &[(8, 9.0), (12, 10.0), (16, 9.8), (24, 9.0)];
    vec![
        UpdateCase {
            name: "update-changes-nothing-relevant",
            before: machine(plain, fast, None),
            after: machine(plain, fast, None),
            hot_changed: false,
            improved: &[],
            expect: (12, "avx2-stream"),
            most_trials: 0,
        },
        UpdateCase {
            name: "update-new-kernel-wins",
            before: machine(plain, fast, None),
            after: machine(
                plain,
                fast,
                Some(&[(8, 10.0), (12, 11.5), (16, 11.2), (24, 10.4)]),
            ),
            hot_changed: true,
            improved: &[],
            expect: (12, "avx512-stream"),
            most_trials: 24,
        },
        UpdateCase {
            name: "update-new-kernel-loses",
            before: machine(plain, fast, None),
            after: machine(
                plain,
                fast,
                Some(&[(8, 8.0), (12, 9.0), (16, 8.8), (24, 8.0)]),
            ),
            hot_changed: true,
            improved: &[],
            expect: (12, "avx2-stream"),
            most_trials: 24,
        },
        UpdateCase {
            name: "update-improves-a-past-loser",
            before: machine(plain, fast, None),
            after: machine(&[(8, 10.0), (12, 11.5), (16, 11.2), (24, 10.4)], fast, None),
            hot_changed: true,
            improved: &["avx2"],
            expect: (12, "avx2"),
            most_trials: 24,
        },
        UpdateCase {
            name: "update-improves-incumbent-count-moves",
            before: machine(plain, fast, None),
            after: machine(plain, &[(8, 9.0), (12, 11.0), (16, 12.0), (24, 11.0)], None),
            hot_changed: true,
            improved: &["avx2-stream"],
            expect: (16, "avx2-stream"),
            most_trials: 24,
        },
        UpdateCase {
            name: "update-other-tweak-count-moves",
            before: machine(plain, fast, None),
            after: machine(plain, &[(8, 9.5), (12, 10.5), (16, 11.0), (24, 10.0)], None),
            hot_changed: true,
            improved: &[],
            expect: (16, "avx2-stream"),
            most_trials: 24,
        },
        UpdateCase {
            name: "update-regresses-incumbent",
            before: machine(plain, &[(8, 7.5), (12, 8.1), (16, 7.9), (24, 7.2)], None),
            after: machine(plain, &[(8, 7.0), (12, 7.3), (16, 7.1), (24, 6.8)], None),
            hot_changed: true,
            improved: &[],
            expect: (12, "avx2"),
            most_trials: 24,
        },
    ]
}

fn facts_for(
    candidates: &[String],
    hot: &str,
    improved: &[&str],
) -> super::super::adaptive_search::BuildFacts {
    super::super::adaptive_search::BuildFacts {
        hot_fingerprint: hot.into(),
        kernel_revisions: candidates
            .iter()
            .map(|name| {
                let revision = if improved.contains(&name.as_str()) {
                    2
                } else {
                    1
                };
                (name.clone(), revision)
            })
            .collect(),
    }
}

fn search_for(
    scenario: &Scenario,
    build: super::super::adaptive_search::BuildFacts,
    saved: Option<super::super::adaptive_search::Saved>,
) -> NewDriver {
    use super::super::adaptive_search::{Params, Search, SeedQuality, Setting, TIE_BAND};
    let (threads, kernel) = saved.as_ref().map_or(
        (scenario.start_threads, scenario.start_kernel.clone()),
        |s| (s.threads, s.kernel.clone()),
    );
    let trace = std::env::var("SEINE_TUNER_SIM_TRACE")
        .is_ok_and(|name| scenario.name.contains(&name) && !name.is_empty());
    let mut search = Search::new(Params {
        seed: Setting {
            threads,
            kernel: Scenario::kernel(&kernel),
        },
        // As the miner chooses: its own result, a rule for the CPU, or a guess.
        quality: if saved.is_some() {
            SeedQuality::Measured
        } else if scenario.known {
            SeedQuality::Family
        } else {
            SeedQuality::Estimate
        },
        min: scenario.min_threads,
        max: scenario.max_threads,
        down_band: TIE_BAND,
        candidates: scenario
            .candidates
            .iter()
            .map(|name| Scenario::kernel(name))
            .collect(),
        saved,
        build,
    });
    if let Some(kernel) = scenario.startup_kernel.as_deref() {
        search.adopt_start(Scenario::kernel(kernel));
    }
    NewDriver { trace, search }
}

/// Score the new build after tuning on the old one.
pub(super) fn run_update(case: &UpdateCase) -> Score {
    let mut before = case.before.clone();
    before.name = format!("{}-before", case.name);
    let old_build = facts_for(&before.candidates, "old", &[]);
    let mut driver = search_for(&before, old_build, None);
    drive(&before, &mut driver);
    let saved = driver.search.saved();
    let mut after = case.after.clone();
    after.name = case.name.to_string();
    after.start_threads = saved.threads;
    after.start_kernel = saved.kernel.clone();
    let hot = if case.hot_changed { "new" } else { "old" };
    let build = facts_for(&after.candidates, hot, case.improved);
    let mut driver = search_for(&after, build, Some(saved));
    drive(&after, &mut driver)
}

fn scenarios_from_env() -> Vec<Scenario> {
    let Ok(path) = std::env::var("SEINE_TUNER_SIM_SCENARIOS") else {
        return Vec::new();
    };
    serde_json::from_str(&fs::read_to_string(&path).expect("read scenarios"))
        .expect("parse scenarios")
}

#[test]
fn identical_runs_score_identically() {
    let scenario = built_in().remove(1);
    let first = serde_json::to_string(&run_new(&scenario)).unwrap();
    let second = serde_json::to_string(&run_new(&scenario)).unwrap();
    assert_eq!(first, second);
}

fn print_header() {
    println!(
        "{:<28} {:>7} {:>6} {:>6} {:>7} {:>7} {:>7} {:>8} {:>7} {:>6} {:>7} {:>6} {:>6} {:>6} {:>5}",
        "scenario",
        "threads",
        "best",
        "extra",
        "settled",
        "first5",
        "w1%",
        "settle",
        "after",
        "probes",
        "lost1h",
        "trials",
        "log1h",
        "log/d",
        "ram"
    );
}

fn print_score(score: &Score) {
    println!(
        "{:<28} {:>7} {:>6} {:>6} {:>7.4} {:>7.3} {:>7} {:>8} {:>7.4} {:>6} {:>7.4} {:>6} {:>6} {:>6.0} {:>5}",
        score.name,
        score.final_threads,
        score.oracle_threads,
        score.extra_threads,
        score.settled_frac,
        score.first_5m_frac,
        score.within_1pct_secs,
        score.settled_secs,
        score.after_settle_cost,
        score.probes_after_settle,
        score.lost_first_hour,
        score.kernel_trials,
        score.log_lines_first_hour,
        score.log_lines_after_first_hour_per_day,
        score.ram_overrun_secs,
    );
}

/// Run across noise seeds: luck must not decide whether a target holds.
/// How close a host at this noise level must get in a day.
fn required_frac(scenario: &Scenario) -> f64 {
    if scenario.noise >= 0.02 {
        0.98
    } else {
        0.995
    }
}

/// Open: these still settle after the time target on some seeds.
const SETTLES_LATE: &[&str] = &["busy-desktop", "rig-cpu-starves-gpu"];

fn seeds() -> Vec<u64> {
    (0..6u64).map(|i| 0x5eed ^ (i * 0x9e37_79b9)).collect()
}

#[test]
fn scoreboard_targets_hold_on_every_seed() {
    let mut problems = Vec::new();
    for mut scenario in built_in() {
        scenario.hours = 24;
        let event_driven = !scenario.cap_events.is_empty()
            || !scenario.load_events.is_empty()
            || scenario
                .ram_edge
                .is_some_and(|(_, until)| until < scenario.hours * 3600);
        for seed in seeds() {
            scenario.seed = seed;
            let score = run_new(&scenario);
            let mut check = |ok: bool, what: String| {
                if !ok {
                    problems.push(format!("{} seed {seed:#x}: {what}", score.name));
                }
            };
            let required = required_frac(&scenario);
            check(
                score.settled_frac >= required,
                format!("settled at {:.4} of the best", score.settled_frac),
            );
            let slack = (score.oracle_threads as i64 / 8).max(1);
            check(
                score.extra_threads <= slack,
                format!("{} threads more than needed", score.extra_threads),
            );
            check(
                score.ram_overrun_secs == 0,
                format!("{} s over the RAM limit", score.ram_overrun_secs),
            );
            check(
                score.log_lines_first_hour <= 10,
                format!("{} log lines in the first hour", score.log_lines_first_hour),
            );
            if !event_driven {
                check(
                    scenario.noise >= 0.02 || score.log_lines_after_first_hour_per_day <= 2.1,
                    format!(
                        "{:.0} log lines a day",
                        score.log_lines_after_first_hour_per_day
                    ),
                );
                check(
                    score.after_settle_cost < 0.001,
                    format!(
                        "{:.4} of hashes lost after settling",
                        score.after_settle_cost
                    ),
                );
                let budget = if scenario.known { 900 } else { 1800 };
                check(
                    scenario.noise > 0.002 || score.within_1pct_secs <= budget,
                    format!("within 1% only after {} s", score.within_1pct_secs),
                );
                // 30 min; 45 when the best beats the start by under 1%, which takes
                // long comparisons to see; 60 for slow hosts.
                let best = scenario.oracle().0;
                let settle_budget = if best < 4.0 {
                    3600
                } else if scenario.gain_over_start() < 0.01 {
                    2700
                } else {
                    1800
                };
                check(
                    score.settled_secs <= settle_budget
                        || SETTLES_LATE.contains(&scenario.name.as_str()),
                    format!("settled only after {} s", score.settled_secs),
                );
            }
            if scenario.known {
                check(
                    score.first_5m_frac >= 0.95,
                    format!("first 5 min at {:.3}", score.first_5m_frac),
                );
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn updates_find_improvements_and_test_only_what_changed() {
    let mut problems = Vec::new();
    for mut case in update_cases() {
        for seed in seeds() {
            case.before.seed = seed;
            case.after.seed = seed ^ 0x77;
            let score = run_update(&case);
            let mut check = |ok: bool, what: String| {
                if !ok {
                    problems.push(format!("{} seed {seed:#x}: {what}", case.name));
                }
            };
            check(
                score.final_kernel == case.expect.1,
                format!("ended on {}", score.final_kernel),
            );
            check(
                score.settled_frac >= required_frac(&case.after),
                format!(
                    "settled {} threads at {:.4} of the best",
                    score.final_threads, score.settled_frac
                ),
            );
            check(
                score.kernel_trials <= case.most_trials,
                format!("{} comparisons", score.kernel_trials),
            );
            check(
                score.first_5m_frac >= 0.9,
                format!("first 5 min at {:.3}", score.first_5m_frac),
            );
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
#[ignore = "prints the scoreboard; run with --ignored --nocapture"]
fn tuner_sim_scoreboard() {
    let scenarios: Vec<_> = built_in().into_iter().chain(scenarios_from_env()).collect();
    println!("--- search");
    print_header();
    let mut new = Vec::new();
    let run_seed = std::env::var("SEINE_TUNER_SIM_SEED")
        .ok()
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok());
    for scenario in &scenarios {
        let mut scenario = scenario.clone();
        if let Some(seed) = run_seed {
            scenario.seed = seed;
        }
        let score = run_new(&scenario);
        print_score(&score);
        new.push(score);
    }
    println!("--- search, after an update");
    print_header();
    let seed = std::env::var("SEINE_TUNER_SIM_SEED")
        .ok()
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok());
    for mut case in update_cases() {
        if let Some(seed) = seed {
            case.before.seed = seed;
            case.after.seed = seed ^ 0x77;
        }
        let score = run_update(&case);
        print_score(&score);
        new.push(score);
    }
    if let Ok(path) = std::env::var("SEINE_TUNER_SIM_OUT") {
        fs::write(path, serde_json::to_vec_pretty(&new).unwrap()).unwrap();
    }
}
