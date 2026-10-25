# Windows tuning cache affinity validation

Change: include the currently permitted logical CPU IDs and canonical physical
core groups in the shared CPU autotuning host fingerprint. Both worker-count
and kernel caches use that fingerprint. An equal-sized mask selecting another
CCD or a different SMT layout must not reuse the previous tuning identity.
Incomplete topology still retains the permitted mask. Other platforms retain
their existing identity calculation.

Validated on 2026-10-24 against v0.2.21, Rust 1.93.0:

- Linux CPU-only workspace tests: 479 passed, four intentionally ignored.
- Native Windows x86-64 GNU test binary: both fingerprint unit tests passed.
- Native Windows ignored mask test, run alone: mask `0x1` yielded
  `{0}:Some([[0]])`, mask `0x80000000` yielded `{31}:Some([[31]])`, and the
  original `0xffffffff` mask and fingerprint were restored successfully.
- Scoped pinned rustfmt and `git diff --check` passed.
- Strict CPU-only binary Clippy reports 61 existing diagnostics. Every source
  file named by those diagnostics is byte-identical to the parent master;
  none is in the changed `src/backend/cpu.rs` file. Unrelated lint cleanup is
  outside this change.

Commands:

```sh
cargo +1.93.0 test --locked --workspace --no-default-features -- --test-threads=1
cargo +1.93.0 clippy --locked --no-default-features --bin seine -- -D warnings
rustup run 1.93.0 rustfmt --check --edition 2021 --config skip_children=true src/backend/cpu.rs
```

The Windows test executable was cross-built for `x86_64-pc-windows-gnu`, then
executed on native Windows using filters `affinity_fingerprint` and
`windows_fingerprint_changes_with_process_cpu_mask --ignored --test-threads=1`.
Its SHA256 was `49c8352d480e7a6ea959686a46f2efbd19c9a880ba82367340de62d4a224eea1`.
The native mask-test stdout SHA256 was
`dcbb3ee057b3f59a583dd6e333c15fc890599442b1bfc0e2a09963cea2621ab0`.

The host was a Ryzen 9950X3D with 32 logical CPUs. This establishes actual
process-mask sensitivity on that machine; it does not establish behavior across
multiple Windows processor groups. Performance-placement experiments are a
separate campaign and do not alter placement policy in this change.
