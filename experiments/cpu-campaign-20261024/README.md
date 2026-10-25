# CPU architecture campaign, 2026-10-24

Baseline: released **v0.2.21**, source
`359265e325d85c9c7b6e3eed7b5b126fcec63f9c`. Work began at 10:27:13 UTC.
New experiments stop by 17:57:13 UTC; confirmation, review, documentation and
cleanup have the final 2.5 hours, ending no later than 20:27:13 UTC.

This directory holds reproducible campaign controllers and the [results](RESULTS.md).
It does not change default kernel selection. Raw reports, downloaded binaries,
cloud credentials and private resource identifiers stay in ignored
`data/cpu-campaign-20261024/`. No GPU or energy measurements are part of this work.
The current PoW remains Argon2id v1.3, 2 GiB, one pass and one lane per hash.

Every rental initially reserves three hours of compute, disk and IPv4 charges
plus a $1 transfer allowance. After **both instance termination and disk deletion
are verified**, unused reservation is released using elapsed time rounded up to
whole hours; the entire transfer allowance remains. Original reservations,
cleanup timestamps, released amounts and cumulative budget history are retained.
This conservative reconciliation permits final-binary rechecks within the same
$40 cumulative cap; it does not reset spending or release a live lease's budget.
`test_budget_reservations.py` checks rounding, cleanup gates and idempotence
without making AWS calls.

## Experiment plan and evidence rules

| Area | Question | Required control |
| --- | --- | --- |
| Ryzen 9950X3D | Cache CCD, frequency CCD, spreading, physical cores and SMT | Same binary, page backing, worker count and total RAM; reversed order, then longer paired runs |
| Native Graviton | Generic NEON versus SHA3-enabled NEON; targeted follow-ups | Native full-size RustCrypto reference parity; generic build kept as fallback control |
| Intel Xeon | AVX2, AVX-512, experimental 256-bit AVX-512 by worker count | Same binary, physical-first CPU list, verified regular/HugeTLB backing |
| Independent hashes | Can DD-only interleaving hide latency on one core? | Two full arenas in every mode; sequential dual-arena, ordinary same-core workers, SMT and separate-core controls identified separately |
| Automatic tuning | Resource changes, cache reuse, process restart and opt-out | Real released miner against a loopback pool fixture; bounded cgroups and no public pool traffic |

Full-size reference parity precedes timing. Rates count all completed hashes and
include fence/overrun time. Short screens select follow-up work; they do not prove
a production gain. Longer alternating runs must beat the strongest applicable
shipping control without hiding worker-count regressions. Failed samples and
losing experiments are retained as evidence. Nothing experimental enters default
dispatch solely because the hardware advertises an ISA.

**Build isolation:** never share a Cargo target directory between source revisions
or feature profiles in performance comparisons. The first Ice Lake streaming
follow-up was excluded after `nm` found `fill_streaming_ring` in its purported
rebuilt baseline: Cargo had reused an experimental library from the shared target.
Its immutable archive is retained with an exclusion sidecar. The published-binary
ISA screen and separately built interhash runner are unaffected. Fresh controls
and candidates use separate target directories, full-size checks, and symbol
provenance. Frozen ARM controls and candidates were built in separate targets
before transfer; final native confirmations verify their digests and reference
parity without rebuilding them on each guest.

**Order correction (15:48 UTC):** early matrix/screen scripts used an index in
an already-reversed outer configuration loop, which could cancel the intended
per-configuration variant reversal. Original executed scripts and data are
preserved; affected normalized evidence is explicitly order-limited. Current
controllers rotate by stable configuration index, and `matrix.py` checks actual
completed order before writing `COMPLETE`. `test_matrix_order.py` exercises the
real controller without launching processes or touching OS settings, including
four-pass balance. Confirmation export additionally requires COMPLETE, every
report from an even-pass run, and reversal verified from completed records.
Windows screening refuses to resume older-protocol data. Native
Windows four-pair confirmation and direct interhash reversal were unaffected.
Corrected ARM confirmation has passed. A later final-build Xeon 6975P-C
regression signal still blocks ARM PR readiness; see the results for the full
stack comparison and causal limitations.

## Initial observations

Native Windows reports two L3 groups on this 9950X3D: 96 MiB on logical CPUs
0–15 and 32 MiB on 16–31. Adjacent logical CPU pairs are SMT siblings. WSL reports
only one L3 group and therefore cannot establish physical CCD placement here.
The native large-page pilot failed with Windows allocation error 1450; subsequent
desktop comparisons explicitly require regular backing, at most eight workers
(16 GiB arenas), plus at least 10 GiB of free host-memory headroom before launch.
This is a shared desktop, so longer comparisons must account for host variability.

The v0.2.21 Windows tuning-cache fingerprint omits allowed CPU IDs. Commit
`001f1e3` adds a separately tested correctness fix so equal-sized masks selecting
different CCDs cannot reuse each other's tuning identity. This is independent of
any proposed change to worker placement.

The interhash experiment is isolated in branch
`codex/interhash-campaign-20261024`, initially commit `8432f45`, with its feature
disabled by default. The development/initial-screen time box is 10:44–12:14 UTC.
Small reference tests cover slice boundaries, multiple kernels and dirty reuse;
Native Ice Lake full-size parity passed for all four modes, three nonce pairs,
and dirty arena reuse. Two opposite-order screens rejected all paired variants
against ordinary SMT workers on the same physical core and equal 4 GiB arenas.
One-element interleaving beat sequential use of two arenas but still lost to
that stronger same-core control. No default changes are proposed. Historical Zen 3
interleaving losses are recorded in CPU journal attempt 34. Historical Apple
attempt 31 mixes total pair throughput and per-lane rates; this campaign compares
total completed throughput directly and reports allocated cores and RAM.

## Retained research code

The inert patches [interhash-research.patch](interhash-research.patch) and
[streaming-ring-research.patch](streaming-ring-research.patch) reproduce the
isolated experiment branches against v0.2.21. Both pass `git apply --check`
against that source. Apply each in a different disposable worktree, and give
each source revision and feature profile its own Cargo target directory.
They are evidence artifacts; applying this PR does not compile either patch.

The interhash patch corresponds to `8432f45`; build its Linux-only runner with
`cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --features interhash-experiment --example interhash_experiment`.
The `interhash-experiment` feature is off by default. `interhash_screen.py`
supplies verified physical/SMT CPU controls and equal total RAM.

The streaming patch corresponds to `d283877`, rebased from the coordinated EPYC
research. Its isolated build deliberately routes manual `avx512-256` selection
to the research ring, so it must never replace a normal release binary. Build
CPU-only Seine and `--example verify_streaming_dispatch` in its isolated target.
The ring needs a distinct production identity and applicable worker-switch
validation before any future promotion. `intel_ring_screen.sh` checks the
research candidate against matched CPU-only AVX2 and full AVX-512 controls.

## Cloud accounting and ownership

`cloud.py` uses the existing `seine-bench` profile in `us-east-1`. It checks the
shared Standard-vCPU quota before every launch. The EPYC owner verified cleanup at 11:06 UTC. This campaign then acquired
the 32-vCPU quota for one Graviton3 and one Ice Lake guest. Later generations
replace terminated guests; launches must not race another session.
Only resources tagged `Experiment=cpu-campaign-20261024` are owned here.

Live AWS Pricing API queries at 10:29 UTC returned:

| Instance | Linux On-Demand USD/hour |
| --- | ---: |
| m7g.4xlarge | 0.65280 |
| m8g.4xlarge | 0.71808 |
| c6i.4xlarge | 0.68000 |
| c7i.4xlarge | 0.71400 |
| c8i.4xlarge | 0.74968 |

Additional live queries at 11:44 UTC returned m6g.4xlarge at $0.616/hour
and c4.4xlarge at $0.796/hour for older architecture/fallback coverage.

Separate live API queries confirmed gp3 at $0.08/GB-month and in-use public IPv4
at $0.005/hour. Each launch reserves three hours of instance/storage/address cost
plus a $1 transfer contingency against the **$40 incremental cap**. Root disks
are 16 GiB gp3 with deletion on termination. No extra provisioned IOPS/throughput
are requested. The ledger retains every lease and its original reservation;
only verified closed leases release unused reserved hours as described above.
The cumulative cap includes conservative closed-lease charges and every active
lease's full reservation.

The guest schedules an absolute-deadline shutdown before installing tools. EC2
converts that shutdown to termination independently of the controlling session.
The controller verifies `/run/systemd/shutdown/scheduled` before experiments.
The original investigation leases lasted at most 150 minutes and expired by
20:00 UTC. After the user explicitly requested merging the good PRs, one final
accepted-subset confirmation lease received a 20:20 UTC independent deadline,
within the unchanged 20:27:13 campaign finish bound and $40 cap. The previous
17 rentals had already been verified cleaned up. Normal completion
terminates earlier and verifies instance termination, root-volume deletion and
eventual deletion of the task-owned security group and SSH keys.

## Scripts

- `bootstrap.sh`: install the pinned toolchain on a disposable guest, build from
  the exact baseline archive, and run native reference checks and screening.
- `screen.py`: alternating ISA/build screening with page, error, OOM, swap and
  steal-time checks; restores the guest's initially empty HugeTLB pool.
- `interhash_screen.py`: up to three alternating equal-RAM passes after full-size
  parity, with an early-rejection gate after two opposite-order passes; uses the
  separately built research-only executable.
- `resource_lifecycle.py`: real miner process tests under bounded CPU/memory
  controls. A service-private hosts file redirects the built-in developer pool
  to the local fixture, with additional loopback-only IP restrictions.
- `cloud.py`: launch/inspect/copy/terminate task-owned leases and record cleanup.
  SSH retries are bounded and limited to an exact pre-connection timeout;
  remote errors, resets, partial output and command timeouts are never replayed.
  `test_cloud_ssh_retry.py` verifies those boundaries without contacting AWS.
- `summarize_costs.py`: after verified final cleanup, estimate compute, root-disk
  and IPv4 costs from live prices and lease intervals; disclose the separate
  transfer contingency and preserve the $40 reservation cap.
- `start_arm_final_confirm.py`: corrected native G2/G3 confirmation using
  transfer-frozen generic ARM binaries and fresh full-size parity.
- `start_accepted_confirm.py`: separately confirm PRs #40/#41/#43/#45 against
  baseline after the merge instruction, excluding held drafts #44/#46. Its
  source identity is pinned and the outer job is bounded to 25 minutes.
- `start_x86_final_confirm.py`: final frozen baseline/candidate AVX2 and full
  AVX-512 regressions on already-screened Intel generations, with fresh parity.
- `start_joint_confirm.py`: bounded confirmation of the separate frozen joint
  tuning prototype. Native parity and a fixed-kernel regression gate precede
  four fresh 900-second pool windows in control/candidate/candidate/control
  order and the nine-phase resource lifecycle. Insufficient deadline, regression
  or failed validation is retained; completion does not imply a performance win.
- `prepare_payload.py`, `start_guest.py`: build a per-file SHA256 input manifest,
  verify the uploaded archive and start a bounded process on a timer-verified lease.
- `arm_generation.sh`: native fallback/feature checks and separate-target ARM
  generation comparisons.
- `recovery_validate.sh`: frozen repaired candidate, full native tests and
  reference parity, real two-arena growth, then the resource lifecycle.
- `hardened_validate.sh`, `queue_hardened.py`: freeze combined candidate `636e688`,
  wait for prior timing, then run native workspace/reference checks, full pool
  task-budget startup and the complete resource lifecycle.
- `pid_pool_probe.py`: full loopback pool runtime on 16 CPUs with 40 tasks and
  one arena's RAM budget; checks actual tasks, residency, productive hashes and
  cleanup. `pid_probe.py` separately forces a backend-only 12-task spawn race.
- `portable_regression.sh`: transfer-frozen, same-profile x86 control/candidate
  binaries; native full-size parity and alternating architecture regressions.
- `summarize_joint.py`: retain all completed ABBA windows, actual CPU identity,
  logged-work comparisons, tuning trajectories and explicit scope interruptions.
  A successful execution protocol is not a performance promotion decision.
- `summarize_matrix.py`, `summarize_lifecycle.py`: verify completion and export
  paired gains or actual resident-arena lifecycle evidence.
- `summarize_provenance.py`: verify frozen binary digests and archived matrix
  inputs, then publish sanitized executable/source identities. Runtime Git-label
  environment overrides are retained as annotations, not executable identity.
- `export_evidence.py`: normalize schema-14 rounds while retaining raw SHA256
  identities and completed-work/fence accounting.
- `reproduce_codegen_review.py --data <retained-data-directory>`: inspect the
  three frozen x86 miner ELFs without executing them. Reproduce instruction
  comparisons while retaining operand structure, branch targets and explicitly
  unverified metadata references. This does not prove throughput equivalence.
- `joint_tuner_probe.py`, `queue_joint_probe.py`: bounded natural tuning against
  a loopback fixture with independent service/rental deadlines, real productive
  work checks, CPU/RAM bounds and verified cleanup. The initial queued probe was
  cancelled before mining to prioritize the Cascade Lake regression.
- Portable regression and ARM confirmation launchers accept an explicit frozen
  `--bundle`; they verify its source identity and every binary digest. Earlier
  `636e688` bundles remain immutable alongside the `f20e7bb` code-generation fix.

- `intel_controls.sh`, `arm_clean_confirm.sh`: authoritative isolated-target
  controls and confirmation builds, including published-binary comparisons and
  regular-page ARM checks.
- `matrix.py`: follow-up cross-build comparisons with completed-work accounting,
  checked backing, immutable binary identities and bounded memory.
- `arm_followup.sh`, `arm_combined.sh`, `intel_streaming.sh`: frozen source builds,
  native parity, and explicit candidate comparisons. These never change shipping
  defaults; read their pinned commit/archive identities before rerunning.
- `collect.py`: archives completed stages between timing phases, checks the
  downloaded archive digest, and verifies every individual file against a manifest.
- `windows_bench.py`, `windows_matrix.py`, `windows_confirm.py`: reproduce the
  recorded 9950X3D masks and native Windows measurements from WSL. Set
  `SEINE_WINDOWS_BENCH_DIR` to an owned temp directory's WSL path,
  `SEINE_WINDOWS_RELEASE_ZIP` to the verified v0.2.21 Windows release archive,
  and `SEINE_WINDOWS_GPUQ_STATE` to the native coordinator state file's Windows
  path. Run the helper's `setup()` once before the matrix. The matrices assume
  the recorded native topology; verify masks before using another machine.
  The original executed scripts are retained unchanged beside raw evidence;
  tracked copies make their local paths configurable. Early shell follow-up
  scripts now also use isolated Cargo targets; the exact originally executed
  scripts remain in the archived evidence, including the excluded Intel run.

## Primary references for ARM investigation

- [Arm instruction-set reference](https://documentation-service.arm.com/static/67e40f3398aa3c3b6eea6a85): XAR is an XOR followed by a per-64-bit-lane rotation and requires FEAT_SHA3.
- [AWS Graviton technical guide](https://aws.github.io/graviton/): generation and architecture context; guest feature detection remains authoritative for execution.

## Hardware identity references

Guest `lscpu`, CPU flags and topology are retained with every stage. AWS
[compute instance specifications](https://docs.aws.amazon.com/ec2/latest/instancetypes/co.html)
and [instance FAQs](https://aws.amazon.com/ec2/faqs/) provide generation context;
C5 can expose Skylake or Cascade Lake, so the actual guest model controls the
label. [C8i](https://aws.amazon.com/ec2/instance-types/c8i/) uses custom Xeon 6
processors; the tested guest reports Xeon 6975P-C. Instance labels alone never
authorize execution of an optional ISA.

## Results

See [RESULTS.md](RESULTS.md) for collected measurements, exclusions, review PRs
and remaining work. No campaign performance change has been merged or released.
