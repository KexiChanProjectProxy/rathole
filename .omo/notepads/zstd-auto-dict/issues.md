# Issues — zstd-auto-dict

Problems and gotchas encountered during work on this plan.

_Auto-scaffolded by /start-work. Append new entries below - never overwrite._

---

## [2026-08-29] Atlas: MIN_TRAIN_FACTOR naming collision RESOLVED (not a bug)

`src/compression/train.rs::MIN_TRAIN_FACTOR = 1` (pub) and `src/config.rs::MIN_TRAIN_FACTOR = 100` (private, module-local) are DIFFERENT constants for DIFFERENT purposes — no Rust name collision (different modules), no code fix needed:
- `train.rs`'s value is the empirically-found MINIMUM ratio at which `zstd::dict::from_continuous` does not error on a tiny synthetic 7-sample test corpus. It's a "does the C call not throw" floor, not a quality guarantee.
- `config.rs`'s value (100) is the PRODUCTION safety validation for `compression_sample_window >= K * compression_dictionary_max_size`, mirroring zstd's own official guidance that a good training corpus should be roughly ~100x the target dictionary size. This is intentionally much stricter than train.rs's bare-minimum-to-not-error value.

**Guidance for todo 11 (integration test)**: when constructing the test service config, the chosen `compression_sample_window`/`compression_dictionary_max_size` pair must satisfy config.rs's validation (window >= 100 * max_size), NOT train.rs's TEST_WINDOW (which uses ratio 1 and would be REJECTED by config validation if used directly as service config). Suggested test values: `compression_dictionary_max_size = 4096`, `compression_sample_window = 409600` (100x, still small/fast for a test). Do not copy train.rs's TEST_WINDOW constant directly into a service TOML config.

## Todo 3 — training-factor integration

- `src/config.rs` currently uses the required placeholder `MIN_TRAIN_FACTOR: usize = 100`, while todo 1 empirically landed `MIN_TRAIN_FACTOR = 1` but has not exported it for config validation. Per todo 3 scope, the source TODO remains and todo 4+ integration must reconcile this known disagreement.
