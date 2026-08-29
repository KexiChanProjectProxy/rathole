# Learnings — zstd-auto-dict

Conventions, patterns, and successful approaches discovered during work on this plan.

_Auto-scaffolded by /start-work. Append new entries below - never overwrite._

---

## 2026-08-29 — Todo 1: zstd dictionary training thresholds

- Landed `MIN_TRAIN_FACTOR = 1`, `MIN_SAMPLE_COUNT = 7`, `TEST_MAX_DICT = 4096`, and `TEST_WINDOW = 4096`.
- Empirical probe used repetitive JSON-like corpora from eight fixed `StdRng` seeds. Six samples failed for every seed, while seven samples succeeded for every seed at both the 4096-byte test scale and the 112640-byte production dictionary scale with a corpus window equal to `max_size`; this establishes the minimum integer factor and sample count for this corpus shape.
- Pinned the optional dependency as `zstd = "0.13"`; Cargo resolved `zstd v0.13.3` and the single shared `zstd-sys v2.0.16+zstd.1.5.7` through `zstd-safe v7.2.4` for both `zstd` and `async-compression`.
