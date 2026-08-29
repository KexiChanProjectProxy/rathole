# Issues — zstd-auto-dict

Problems and gotchas encountered during work on this plan.

_Auto-scaffolded by /start-work. Append new entries below - never overwrite._

---

## Todo 3 — training-factor integration

- `src/config.rs` currently uses the required placeholder `MIN_TRAIN_FACTOR: usize = 100`, while todo 1 empirically landed `MIN_TRAIN_FACTOR = 1` but has not exported it for config validation. Per todo 3 scope, the source TODO remains and todo 4+ integration must reconcile this known disagreement.
