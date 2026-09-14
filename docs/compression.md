# Compression

rathole can compress TCP and UDP data-channel payloads with zstd, per service. The server decides whether a service is compressed. Clients never set `compression` themselves. Putting `compression = "zstd"` under `[client.services.X]` is a TOML unknown-field error.

This was built for short-lived plaintext HTTP tunnels (Loki, ClickHouse, Langfuse, and similar JSON/text backends). A trained per-service dictionary removes the cold-start penalty that plain zstd pays on those short connections. rathole can train that dictionary automatically from live TCP traffic, or you can ship a static file.

Keys live in the [configuration specification](../README.md#configuration). Compression sits on the plaintext side of the transport: compress, then encrypt, when TLS, Noise, or WSS is also enabled. That makes it complementary to [transport encryption](transport.md), not a replacement.

A working unified-mode example is in [examples/compression.toml](../examples/compression.toml).

## Mechanism

Set `compression = "zstd"` on `[server.services.X]`. When a visitor arrives, the server opens a data channel and sends `StartForwardTcpZstd` / `StartForwardUdpZstd`. The command carries a 32-byte SHA256 dictionary digest. An all-zero digest means plain zstd, no dictionary.

The client reads that command, checks its local dictionary against the digest, then wraps the data channel in a zstd stream. Payload bytes are compressed before they hit TLS/Noise/WebSocket, and decompressed after they come out. Compression runs on tokio's blocking thread pool in batches of up to 128 KiB, so a slow level never stalls the async workers that serve other connections. zstd frames are self-describing: the encoder's compression level is not on the wire, and any decoder can read any level.

No compression happens on the control channel. Unset `compression` leaves the wire byte-identical to previous rathole versions. Automatic dictionary training, when it runs, still leaves the control channel uncompressed: it sends dictionary bytes as a separate command. See [Automatic dictionaries](#automatic-dictionaries).

Default release builds include the `compression-zstd` Cargo feature. The `embedded` feature set does not, so a tiny router build has no zstd code.

## Per-service dictionaries

Optional. On the server, set `compression_dictionary = "path/to/dict"` next to `compression = "zstd"`. A dictionary without `compression` is rejected at config load.

On the client, set only `compression_dictionary = "path/to/dict"`. There is no client `compression` key.

The two files must be byte-identical. rathole hashes each file with SHA256 at config load, and the server sends its digest on every compressed data channel. If the client's digest does not match, the client bails before any payload is exchanged.

Relative paths resolve against the config file's directory, not the process working directory. Absolute paths are used as-is. That matters under systemd, where the CWD is often `/`.

Any file of raw bytes is a valid zstd dictionary for correctness. Training is what makes a dictionary *effective* (better ratios on small or short-lived payloads). You do not need `zstd --train` for the feature to function.

To have rathole train a dictionary from live traffic instead of a file, omit `compression_dictionary` and see [Automatic dictionaries](#automatic-dictionaries). A configured static file always wins: auto-training is forced off for that service.

## Training a dictionary

For a static `compression_dictionary` file, training is an offline operator task. rathole does not ship dictionaries. The [automatic dictionaries](#automatic-dictionaries) path trains one in memory from live TCP traffic instead.

Collect a representative corpus of the service's traffic, then:

```sh
zstd --train /path/to/sample/payloads/*.json -o service.dict --maxdict=112640
```

Copy the resulting `service.dict` to both hosts, byte-identical. Point `compression_dictionary` at it on both sides. The server also needs `compression = "zstd"`. The client's key is dictionary-only.

`--maxdict=112640` is zstd's default max (110 KiB). Pick a corpus that looks like production: same JSON shapes, same HTTP headers, same query patterns.

## Automatic dictionaries

When `compression = "zstd"` is set and `compression_dictionary` is unset, the server trains one zstd dictionary per TCP service from live traffic. Sampling, training, and the resulting generation stay in memory. Nothing is written to disk. Services do not share dictionaries.

The server copies plaintext visitor traffic in both directions into a per-service sample buffer. Once accumulated bytes reach `compression_sample_window`, it trains one dictionary with `zstd::dict::from_continuous`, publishes that dictionary as a generation, and pushes it to the client on the control channel (`UpdateCompressionDict`). TCP data channels opened after that point use the trained dictionary automatically. Channels already open keep the generation they were created with.

One generation is produced per service, then frozen for the lifetime of that service's runtime state. There is no periodic retraining.

Until training succeeds, new data channels use plain zstd (an all-zero digest), same as a zstd service with no dictionary. On success the server logs `trained compression dictionary`.

> **Upgrade all clients before upgrading the server.**
>
> A server that has this feature, talking to an old client that predates it, will push `UpdateCompressionDict` on the control channel. The old client cannot interpret that command. The control channel then reconnects roughly once per second, indefinitely, until the client is upgraded.
>
> This is a different and more severe failure mode than the data-channel-level "Old client, new server" case under [Failure modes](#failure-modes). Auto-dictionary defaults to on whenever `compression = "zstd"` is set without a static dictionary, so the warning applies to any zstd service, not only ones that set the new keys.

### Config keys

These keys are server-side only. Putting them under `[client.services.X]` is a TOML unknown-field error. The client needs no dictionary file on the auto-dictionary path. It receives the push. The client also never sets `compression_level`.

- `compression_auto_dictionary` (bool). Default `true` when `compression = "zstd"` and `compression_dictionary` is unset. Set `false` to opt out. A configured static `compression_dictionary` forces this to `false`. An explicit `true` alongside a static dictionary still uses the file, and logs:

  ```
  Service {name}: static `compression_dictionary` takes precedence; disabling `compression_auto_dictionary`
  ```

- `compression_sample_window` (u64, bytes). Default `134217728` (128 MiB).

- `compression_dictionary_max_size` (u64, bytes). Default `112640` (110 KiB). Must not exceed `16777216` (16 MiB, the control-channel push cap).

- `compression_level` (i32). zstd level `1..=22`. Default `19`. This process's encoder uses it. The peer's encoder is independent: a 0.5.4 client still encodes at 3; a 0.5.6 client encodes at 9; 0.5.7+ encodes at 19. Decoders accept any level. Changing this key does not require a client upgrade.

Config load fails unless `compression_sample_window` is at least 100 times `compression_dictionary_max_size`. That 100× floor follows zstd's guidance that a useful training corpus is about 100 times the target dictionary size. Exact errors:

```
Service {name}: `compression_auto_dictionary` requires `compression = "zstd"` to be set
Service {name}: `compression_sample_window` requires `compression = "zstd"` to be set
Service {name}: `compression_dictionary_max_size` requires `compression = "zstd"` to be set
Service {name}: `compression_level` requires `compression = "zstd"` to be set
Service {name}: `compression_level` must be between 1 and 22
Service {name}: `compression_dictionary_max_size` must not exceed 16777216 bytes
Service {name}: `compression_sample_window` must be at least 100 times `compression_dictionary_max_size`
```

### UDP

UDP services never participate in automatic dictionary training. A UDP service uses one long-lived data channel, so zstd already keeps compression context across the whole connection. There is no second stream to apply a trained dictionary to, and no cross-stream benefit to capture. Static `compression_dictionary` still works for UDP exactly as before. Only the auto-training path is TCP-only.

### Unknown digest

When a client's data channel needs a dictionary generation it has not received yet, it waits up to 5 seconds for the server to push it. If that wait times out, that one visitor's TCP connection is dropped. There is no retry at this layer. Other visitors and the service itself are unaffected. The client error is:

```
Service {name}: timed out waiting for pushed compression dictionary — server expects digest {hex}, client has {hex}
```

### Training failure

If the corpus cannot produce a valid dictionary (too little data, too repetitive or degenerate, and similar zstd rejections), the service stays dictionary-less for that runtime lifetime. The server logs a warning:

```
dictionary training failed; service stays dictionary-less
```

It does not retry and does not re-buffer. Later traffic is forwarded with plain zstd.

### Hot reload and restart

Any change to a service's config fields, even unrelated ones such as `nodelay`, is a service-level hot reload. The control channel for that service drops and is recreated. That eviction also drops the trained generation and any accumulated samples, so the service trains again from scratch on the next window of traffic. A server-wide restart does the same for every service. Nothing is persisted, so a fresh generation must always be retrained after either kind of restart or reload.

If early traffic is a poor sample of later traffic, `touch` the config (or otherwise re-save it) to force a retrain. Same idea as the static-dictionary reload note under [Hot reload](#hot-reload).

### Memory

While sampling, a service holds up to `compression_sample_window` bytes in memory (128 MiB at the default). Training via `zstd::dict::from_continuous` then adds a transient peak roughly proportional to the corpus size. On memory-constrained devices, lower `compression_sample_window` and `compression_dictionary_max_size` together (keep the 100× ratio) or set `compression_auto_dictionary = false`.

### Plaintext in transit

The pushed dictionary contains raw excerpts of that service's real traffic. It travels on the control channel, which is plaintext TCP unless TLS, Noise, or WSS is configured. An on-path observer of an unencrypted control channel can recover a distilled sample of the service's traffic. This sits next to the [CRIME-style caveat](#crime-style-caveat): compression plus an unencrypted control channel leaks more than compression alone. Use an encrypted transport when auto-dictionary is enabled.

## Failure modes

- **Dictionary mismatch or missing.** The server configured a dictionary, and the client's file is missing, different, or absent. The client stops before any payload moves:

  ```
  Service {name}: compression dictionary mismatch — server expects digest {hex}, client has {hex}
  ```

  A client with no dictionary reports an all-zero digest on the "client has" side.

- **Client has a dictionary, server does not.** The server is running plain zstd (no `compression_dictionary`). The connection proceeds with plain zstd. The client's dictionary is unused for that request. The client logs a `WARN`:

  ```
  compression_dictionary configured but server did not request a dictionary
  ```

- **Old client, new server.** A pre-compression rathole client does not understand the Zstd data-channel commands. It fails to deserialize them (typically `Failed to deserialize data cmd`). The handshake fails. The server then requests a fresh data channel for every visitor that arrives, rate-bounded by visitor arrival. You will see repeated data-channel-creation retries in server logs until every client for that service is upgraded. Upgrade clients before enabling `compression` on a service.

- **Old client, new server, automatic dictionary.** Worse than the data-channel case above. After training, or on a later control-channel connect once a generation exists, the server pushes `UpdateCompressionDict` on the control channel. A client that predates this feature fails to interpret that command. The control channel then reconnects roughly once per second, indefinitely, until the client is upgraded. **Upgrade all clients before upgrading the server** whenever `compression = "zstd"` is used, because auto-dictionary defaults to `true`. See [Automatic dictionaries](#automatic-dictionaries).


- **Binary built without `compression-zstd`.** Protocol variants still deserialize. The client then errors explicitly:

  ```
  Service {name}: server requires zstd compression but this binary lacks feature compression-zstd
  ```

  Config load is equally strict: `compression` or `compression_dictionary` on a feature-off binary fails with `recompile with compression-zstd`.

- **Unknown auto-dictionary digest.** The client waits up to 5 seconds for the server to push a generation it has not seen yet. On timeout, that one visitor's TCP connection is dropped. There is no retry at this layer. See [Unknown digest](#unknown-digest).

- **Client sets `compression`.** Hard parse error naming the unknown field `compression`. Compression is server-side only.

## Hot reload

Changing `compression`, `compression_level`, or `compression_dictionary` values in the config file is a normal service-level hot reload. The control channel for that service drops and the client reconnects, same as any other service field change. For automatic dictionaries, any field change on that service (even `nodelay`) also discards the trained generation and the sample buffer, so the service retrains from scratch. See [Automatic dictionaries](#automatic-dictionaries).

Edits to the dictionary file itself, without touching the config, trigger nothing. The watcher only watches the config file, not files it references. After you retrain a dictionary, `touch` the config (or otherwise re-save it) on both sides so rathole reloads and picks up the new bytes.

## CRIME-style caveat

Combining compression with a scheme where attacker-influenced plaintext and secret data share the same compressed stream can, in principle, open a side channel for size-based inference (the classic CRIME class of attack against TLS+compression). If a single compressed stream carries both secrets and attacker-controlled input, evaluate whether enabling compression is appropriate for that service.

Automatic dictionaries add a related leak on the control channel: the pushed dictionary is a distilled sample of real traffic. That channel is plaintext TCP unless TLS, Noise, or WSS is configured. See [Plaintext in transit](#plaintext-in-transit).

## When not to use it

Compression is wasted CPU on already-compressed or high-entropy payloads: video, images, ciphertext, gzipped bodies. TLS-passthrough services are the same story. rathole forwards opaque bytes and never sees plaintext to compress.

Enable it for compressible traffic that rathole can see as plaintext. JSON and text HTTP (the Loki / ClickHouse / Langfuse case) is the intended fit.
