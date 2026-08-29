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

## 2026-08-29 — Todo 4: server per-service compression state plumbing

- Map key type: `ServiceDigest`, the existing alias `type ServiceDigest = protocol::Digest` (`protocol::Digest` is the SHA-256 service-name digest).
- Map type: `type ServiceCompressionStateMap = Arc<RwLock<HashMap<ServiceDigest, Arc<ServiceCompressionState>>>>`; `Server<T>` owns it as `service_compression_states`.
- Stable scaffold types: `SampleBuffer`; `SamplerState::{Sampling(SampleBuffer), Trained, Failed}`; `Generation { digest: protocol::Digest }`; and `ServiceCompressionState { sampler: Mutex<SamplerState>, generation_tx: watch::Sender<Option<Arc<Generation>>>, generation_rx: watch::Receiver<Option<Arc<Generation>>> }`.
- `handle_connection` changed from `async fn handle_connection<T>(conn, services, control_channels, server_config) -> Result<()>` to `async fn handle_connection<T>(conn, services, control_channels, service_compression_states, server_config) -> Result<()>`.
- `do_control_channel_handshake` changed from `async fn do_control_channel_handshake<T>(conn, services, control_channels, service_digest, server_config) -> Result<()>` to `async fn do_control_channel_handshake<T>(conn, services, control_channels, service_compression_states, service_digest, server_config) -> Result<()>`.
- Server-side `ControlChannelHandle::new` changed from `fn new(conn, service, server_config) -> ControlChannelHandle<T>` to `fn new(conn, service, server_config, compression_state) -> ControlChannelHandle<T>`, where `compression_state: Option<Arc<ServiceCompressionState>>` is retained by the handle for later todo 5/6 plumbing.
- Lazy creation is restricted to validated TCP services matching zstd + no configured static dictionary + `compression_auto_dictionary == Some(true)`. Both hot-reload `Add` and `Delete` remove the service digest from the runtime map; the handshake rechecks the current service config while holding the services read guard so an in-flight stale handshake cannot recreate an evicted entry after reload.

## 2026-08-29 — Todo 6: control-channel dictionary generation pushes

- Server `ControlChannel` now owns `generation_rx: Option<watch::Receiver<Option<Arc<Generation>>>>`, cloned in `ControlChannelHandle::new` from `compression_state.as_ref().map(|state| state.generation_rx.clone())` alongside the TCP pool receiver clone.
- `ControlChannel::run` calls `push_generation` before entering its `tokio::select!` loop when a current generation exists, then pushes once after each successful `generation_rx.changed()` notification. The initial read uses `borrow_and_update()` so the already-pushed generation is marked seen and is not immediately pushed a second time.
- Client-visible contract for todo 9: the server sends `ControlChannelCmd::UpdateCompressionDict` once when a control channel is established if a trained generation already exists, and once per later generation change; the payload uses `generation.digest` and `generation.dictionary.bytes.to_vec()`.
