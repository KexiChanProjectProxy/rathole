# Learnings — zstd-auto-dict

Conventions, patterns, and successful approaches discovered during work on this plan.

_Auto-scaffolded by /start-work. Append new entries below - never overwrite._

---

## 2026-08-29 — Todo 1: zstd dictionary training thresholds

- Landed `MIN_TRAIN_FACTOR = 1`, `MIN_SAMPLE_COUNT = 7`, `TEST_MAX_DICT = 4096`, and `TEST_WINDOW = 4096`.
- Empirical probe used repetitive JSON-like corpora from eight fixed `StdRng` seeds. Six samples failed for every seed, while seven samples succeeded for every seed at both the 4096-byte test scale and the 112640-byte production dictionary scale with a corpus window equal to `max_size`; this establishes the minimum integer factor and sample count for this corpus shape.
- Pinned the optional dependency as `zstd = "0.13"`; Cargo resolved `zstd v0.13.3` and the single shared `zstd-sys v2.0.16+zstd.1.5.7` through `zstd-safe v7.2.4` for both `zstd` and `async-compression`.

## Todo 3 — server auto-dictionary configuration

- Landed defaults after validation for zstd services: `compression_auto_dictionary = true`, `compression_sample_window = 134217728`, and `compression_dictionary_max_size = 112640`. A configured static `compression_dictionary` forces auto-dictionary to `false`; explicit `true` emits ``Service {name}: static `compression_dictionary` takes precedence; disabling `compression_auto_dictionary```.
- Exact validation errors:
  - ``Service {name}: `compression_auto_dictionary` requires `compression = "zstd"` to be set``
  - ``Service {name}: `compression_sample_window` requires `compression = "zstd"` to be set``
  - ``Service {name}: `compression_dictionary_max_size` requires `compression = "zstd"` to be set``
  - ``Service {name}: `compression` requires compression support; recompile with compression-zstd``
  - ``Service {name}: `compression_dictionary_max_size` must not exceed 16777216 bytes``
  - ``Service {name}: `compression_sample_window` must be at least 100 times `compression_dictionary_max_size```
- Fixing the example-file extension filter exposed no broken example configuration. The test now discovers 19 TOML files and asserts the list is non-empty; the existing compression example remains intentionally skipped because its referenced `service.dict` is not shipped.
