# Compression

rathole can compress TCP and UDP data-channel payloads with zstd, per service. The server decides whether a service is compressed. Clients never set `compression` themselves. Putting `compression = "zstd"` under `[client.services.X]` is a TOML unknown-field error.

This was built for short-lived plaintext HTTP tunnels (Loki, ClickHouse, Langfuse, and similar JSON/text backends). A trained per-service dictionary removes the cold-start penalty that plain zstd pays on those short connections.

Keys live in the [configuration specification](../README.md#configuration). Compression sits on the plaintext side of the transport: compress, then encrypt, when TLS, Noise, or WSS is also enabled. That makes it complementary to [transport encryption](transport.md), not a replacement.

A working unified-mode example is in [examples/compression.toml](../examples/compression.toml).

## Mechanism

Set `compression = "zstd"` on `[server.services.X]`. When a visitor arrives, the server opens a data channel and sends a new command (`StartForwardTcpZstd` or `StartForwardUdpZstd`) that carries a 32-byte SHA256 dictionary digest. An all-zero digest means plain zstd, no dictionary.

The client reads that command, checks its local dictionary against the digest, then wraps the data channel in a zstd stream. Payload bytes are compressed before they hit TLS/Noise/WebSocket, and decompressed after they come out.

No compression happens on the control channel. Unset `compression` leaves the wire byte-identical to previous rathole versions.

Default release builds include the `compression-zstd` Cargo feature. The `embedded` feature set does not, so a tiny router build has no zstd code.

## Per-service dictionaries

Optional. On the server, set `compression_dictionary = "path/to/dict"` next to `compression = "zstd"`. A dictionary without `compression` is rejected at config load.

On the client, set only `compression_dictionary = "path/to/dict"`. There is no client `compression` key.

The two files must be byte-identical. rathole hashes each file with SHA256 at config load, and the server sends its digest on every compressed data channel. If the client's digest does not match, the client bails before any payload is exchanged.

Relative paths resolve against the config file's directory, not the process working directory. Absolute paths are used as-is. That matters under systemd, where the CWD is often `/`.

Any file of raw bytes is a valid zstd dictionary for correctness. Training is what makes a dictionary *effective* (better ratios on small or short-lived payloads). You do not need `zstd --train` for the feature to function.

## Training a dictionary

Training is an offline operator task. rathole does not train or ship dictionaries.

Collect a representative corpus of the service's traffic, then:

```sh
zstd --train /path/to/sample/payloads/*.json -o service.dict --maxdict=112640
```

Copy the resulting `service.dict` to both hosts, byte-identical. Point `compression_dictionary` at it on both sides. The server also needs `compression = "zstd"`. The client's key is dictionary-only.

`--maxdict=112640` is zstd's default max (110 KiB). Pick a corpus that looks like production: same JSON shapes, same HTTP headers, same query patterns.

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

- **Binary built without `compression-zstd`.** Protocol variants still deserialize. The client then errors explicitly:

  ```
  Service {name}: server requires zstd compression but this binary lacks feature compression-zstd
  ```

  Config load is equally strict: `compression` or `compression_dictionary` on a feature-off binary fails with `recompile with compression-zstd`.

- **Client sets `compression`.** Hard parse error naming the unknown field `compression`. Compression is server-side only.

## Hot reload

Changing `compression` or `compression_dictionary` values in the config file is a normal service-level hot reload. The control channel for that service drops and the client reconnects, same as any other service field change.

Edits to the dictionary file itself, without touching the config, trigger nothing. The watcher only watches the config file, not files it references. After you retrain a dictionary, `touch` the config (or otherwise re-save it) on both sides so rathole reloads and picks up the new bytes.

## CRIME-style caveat

Combining compression with a scheme where attacker-influenced plaintext and secret data share the same compressed stream can, in principle, open a side channel for size-based inference (the classic CRIME class of attack against TLS+compression). If a single compressed stream carries both secrets and attacker-controlled input, evaluate whether enabling compression is appropriate for that service.

## When not to use it

Compression is wasted CPU on already-compressed or high-entropy payloads: video, images, ciphertext, gzipped bodies. TLS-passthrough services are the same story. rathole forwards opaque bytes and never sees plaintext to compress.

Enable it for compressible traffic that rathole can see as plaintext. JSON and text HTTP (the Loki / ClickHouse / Langfuse case) is the intended fit.
