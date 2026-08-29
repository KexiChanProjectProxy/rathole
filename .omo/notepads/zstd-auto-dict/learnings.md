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

## Todo 2 — control-channel dictionary frame

- `ControlChannelCmd` tags are bincode u32 little-endian values: `CreateDataChannel = 0` (`00 00 00 00`), `HeartBeat = 1` (`01 00 00 00`), and `UpdateCompressionDict = 2` (`02 00 00 00`).
- `UpdateCompressionDict` wire layout is: tag (`u32` little-endian, 4 bytes) + digest (`Digest`, 32 bytes) + dictionary length (`u64` little-endian, 8 bytes) + dictionary payload (exactly `length` bytes).
- Receivers reject dictionary lengths above `MAX_DICT_PUSH_BYTES` (16 MiB) before allocating the payload buffer.

## 2026-08-29 — Todo 4: server per-service compression state plumbing

- Map key type: `ServiceDigest`, the existing alias `type ServiceDigest = protocol::Digest` (`protocol::Digest` is the SHA-256 service-name digest).
- Map type: `type ServiceCompressionStateMap = Arc<RwLock<HashMap<ServiceDigest, Arc<ServiceCompressionState>>>>`; `Server<T>` owns it as `service_compression_states`.
- Stable scaffold types: `SampleBuffer`; `SamplerState::{Sampling(SampleBuffer), Trained, Failed}`; `Generation { digest: protocol::Digest }`; and `ServiceCompressionState { sampler: Mutex<SamplerState>, generation_tx: watch::Sender<Option<Arc<Generation>>>, generation_rx: watch::Receiver<Option<Arc<Generation>>> }`.
- `handle_connection` changed from `async fn handle_connection<T>(conn, services, control_channels, server_config) -> Result<()>` to `async fn handle_connection<T>(conn, services, control_channels, service_compression_states, server_config) -> Result<()>`.
- `do_control_channel_handshake` changed from `async fn do_control_channel_handshake<T>(conn, services, control_channels, service_digest, server_config) -> Result<()>` to `async fn do_control_channel_handshake<T>(conn, services, control_channels, service_compression_states, service_digest, server_config) -> Result<()>`.
- Server-side `ControlChannelHandle::new` changed from `fn new(conn, service, server_config) -> ControlChannelHandle<T>` to `fn new(conn, service, server_config, compression_state) -> ControlChannelHandle<T>`, where `compression_state: Option<Arc<ServiceCompressionState>>` is retained by the handle for later todo 5/6 plumbing.
- Lazy creation is restricted to validated TCP services matching zstd + no configured static dictionary + `compression_auto_dictionary == Some(true)`. Both hot-reload `Add` and `Delete` remove the service digest from the runtime map; the handshake rechecks the current service config while holding the services read guard so an in-flight stale handshake cannot recreate an evicted entry after reload.

## 2026-08-29 — Todo 5: snapshot-consistent data-channel compression generation

- Final `Generation` fields are `digest: protocol::Digest`, `dictionary: LoadedDictionary`, `tcp_cmd_bytes: Vec<u8>`, and `udp_cmd_bytes: Vec<u8>`. `Generation::new(LoadedDictionary) -> bincode::Result<Generation>` derives the digest and pre-serializes both zstd data-channel commands once.
- `fn tcp_generation_snapshot(generation_rx: &watch::Receiver<Option<Arc<Generation>>>) -> Option<Arc<Generation>>` performs the single `borrow().clone()` used for one TCP visitor. `run_tcp_connection_pool` reads command bytes and the wrapping dictionary from that same returned `Arc<Generation>`; an already-spawned channel therefore retains its generation across later watch swaps.
- `wrap_stream` changed from `wrap_stream(stream, &Option<Arc<CompressionCtx>>)` to `wrap_stream(stream, compression_enabled: bool, dictionary: Option<&LoadedDictionary>)`, allowing static/no-dictionary contexts and auto-generation snapshots to share the existing `MaybeCompressed`/`ZstdStream` wrapping logic.

## 2026-08-29 — Todo 6: control-channel dictionary generation pushes

- Server `ControlChannel` now owns `generation_rx: Option<watch::Receiver<Option<Arc<Generation>>>>`, cloned in `ControlChannelHandle::new` from `compression_state.as_ref().map(|state| state.generation_rx.clone())` alongside the TCP pool receiver clone.
- `ControlChannel::run` calls `push_generation` before entering its `tokio::select!` loop when a current generation exists, then pushes once after each successful `generation_rx.changed()` notification. The initial read uses `borrow_and_update()` so the already-pushed generation is marked seen and is not immediately pushed a second time.
- Client-visible contract for todo 9: the server sends `ControlChannelCmd::UpdateCompressionDict` once when a control channel is established if a trained generation already exists, and once per later generation change; the payload uses `generation.digest` and `generation.dictionary.bytes.to_vec()`.

## 2026-08-29 — Todo 7: TCP visitor plaintext sampling tee

- `SampleBuffer` now lives in `src/server/sampler.rs` with public trainer inputs `data: Vec<u8>` and `sizes: Vec<usize>`, plus private bookkeeping `pending_size: usize` and `window: usize`. Both vectors start empty and grow on demand; no sample-window-sized reservation is performed. Boundaries merge sub-256-byte poll writes/reads, split at 16 KiB, and are finalized when the window is reached so `sizes.iter().sum() == data.len()` for valid configured windows.
- `ServiceCompressionState` now uses `std::sync::Mutex<SamplerState>` for the short, non-awaiting poll-path critical section and owns `sampling_active: AtomicBool` plus `sampling_ready: AtomicBool`. `SamplingStream` calls `record_plaintext`, whose first operation is `sampling_active.load(Relaxed)`; false returns before lock acquisition/allocation. Reaching the window stores `sampling_active = false` and publishes `sampling_ready = true` with `Release`.
- Todo 8 ready hook: call `ServiceCompressionState::take_ready_samples(&self) -> Option<SampleBuffer>`. It atomically claims readiness with `sampling_ready.swap(false, AcqRel)`, locks `sampler`, and moves the completed `data`/`sizes` vectors out of `SamplerState::Sampling` exactly once. The state remains `Sampling` with empty vectors but `sampling_active == false`; after training, todo 8 must lock `sampler` and replace it with `SamplerState::Trained` on success or `SamplerState::Failed` on failure. No training or generation swap occurs in todo 7.
- TCP visitor wrap/no-wrap helper: `tcp_sampling_state(Option<&Arc<ServiceCompressionState>>) -> Option<Arc<ServiceCompressionState>>`. It returns `Some` only for an active `SamplerState::Sampling`, and returns `None` for `Trained`, `Failed`, inactive/window-complete, or absent states. `run_tcp_connection_pool` wraps only the visitor `TcpStream`; the compressed data channel remains wrapped solely by `MaybeCompressed`/`ZstdStream` as before.

## 2026-08-29 — Todo 9: client per-service dictionary cache

- `DictCache` owns `watch::Sender<Arc<HashMap<protocol::Digest, Arc<Vec<u8>>>>>` plus a `tokio::sync::Mutex<VecDeque<protocol::Digest>>` FIFO insertion-order queue capped at four generations. Its API is `fn new() -> Self`, `async fn insert(&self, digest: protocol::Digest, dictionary: Arc<Vec<u8>>)`, `fn lookup(&self, digest: &protocol::Digest) -> Option<Arc<Vec<u8>>>`, and `fn subscribe(&self) -> watch::Receiver<Arc<HashMap<protocol::Digest, Arc<Vec<u8>>>>>` (the concrete return type is named `DictSnapshot` in `src/client.rs`).
- `Client::spawn_service_handles` constructs exactly one `Arc<DictCache>` per service invocation, before iterating `ClientConfig::remote_addr`, and passes `Arc::clone` into every `ControlChannelHandle`; the same cache is retained by each reconnecting `ControlChannel` and cloned into every `RunDataChannelArgs` created by that service's control-channel sessions.
- `handle_compression_dict_update(cache: &DictCache, digest: protocol::Digest, dictionary: Vec<u8>)` validates pushes with the exact existing helper call `protocol::digest(&dictionary)` before insertion. A mismatch warns and is discarded; feature-off builds warn with the required unsupported-feature message and discard the parsed bytes.

## 2026-08-29 — Todo 8: one-shot online dictionary training

- Exact trigger/spawn point: `src/server.rs::get_or_create_service_compression_state` calls `src/server/training.rs::spawn_dictionary_training` only in the `HashMap::Entry::Vacant` branch, immediately after constructing the new `Arc<ServiceCompressionState>`. `record_plaintext` calls `sampling_notify.notify_one()` when it publishes `sampling_ready = true`; the per-service task atomically claims the corpus through `take_ready_samples()` and exits after its single success or failure.
- Global serialization primitive: `src/server/training.rs` defines `static DICTIONARY_TRAINING_SEMAPHORE: tokio::sync::Semaphore = Semaphore::const_new(1);`. Its permit is acquired before entering `tokio::task::spawn_blocking(train_dictionary(...))` and held until blocking training completes.
- Exact success log message and fields: `info!(service = %service_name, sample_bytes, dict_bytes, "trained compression dictionary")`. The verbatim message is `trained compression dictionary`.
- Exact failure log message and fields: `warn!(service = %service_name, error = %error, "dictionary training failed; service stays dictionary-less")`. The verbatim message is `dictionary training failed; service stays dictionary-less`.
- Both exits consume and drop the moved `SampleBuffer`; success publishes `generation_tx.send_replace(Some(Arc::new(generation)))` then sets `SamplerState::Trained`, while every training/generation-construction error sets terminal `SamplerState::Failed`. Neither terminal state can re-buffer or retry.

## 2026-08-29 — Todo 10: bounded client dictionary resolution

- Final signature: `async fn resolve_client_dict(dict_digest: protocol::Digest, service: &ClientServiceConfig, cache: &DictCache) -> Result<Option<Arc<Vec<u8>>>>`.
- Resolution order is unchanged zero-digest handling (including the existing static-dictionary warning), then `DictCache::lookup`, then a matching configured static dictionary, then a race-free `watch` snapshot re-check plus `changed()` wait bounded by `tokio::time::timeout(Duration::from_secs(5), ...)`.
- Exact timeout error text: `Service {service.name}: timed out waiting for pushed compression dictionary — server expects digest {hex::encode(dict_digest)}, client has {hex::encode(client_digest)}`.
- True failure semantics: the visitor connection is dropped, no retry occurs at this layer. The resolver error propagates through data-channel setup, so the client never completes that data channel and the server-side `copy_bidirectional` task ends.
