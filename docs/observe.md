# Observation

The server can expose a local, unauthenticated HTTP endpoint with per-service connection counts, visitor-facing and data-channel byte counters, and compression ratio.

This is **not** the configuration HTTP API mentioned in the README. It is read-only runtime stats.

## Config

```toml
[server]
bind_addr = "0.0.0.0:2333"
observe_addr = "127.0.0.1:4077" # Optional. Default: unset (disabled)
```

Unset `observe_addr` keeps the extra listener and the byte-counting wrappers off.

Bind loopback. There is no token, TLS, or allowlist on this socket. A non-loopback bind logs a warning and still serves.

Hot-reload does not move `observe_addr`. Changing it requires a process restart. Service add/delete updates the stats map.

## Endpoints

| Path | Response |
| --- | --- |
| `GET /` or `GET /stats` | JSON snapshot |
| `GET /metrics` | Prometheus text |
| `GET /health` | `ok` |

`HEAD` and other methods return 405. Unknown paths return 404.

## JSON

```json
{
  "uptime_secs": 3600,
  "services": [
    {
      "name": "clickhouse",
      "type": "tcp",
      "bind_addr": "0.0.0.0:8123",
      "control_connected": true,
      "connections_active": 2,
      "connections_total": 1500,
      "bytes_in": 1000000,
      "bytes_out": 2000000,
      "wire_bytes_in": 200000,
      "wire_bytes_out": 400000,
      "compression": "zstd",
      "compression_ratio": 5.0,
      "dictionary": {
        "mode": "auto",
        "state": "trained",
        "digest": "ab…"
      }
    }
  ]
}
```

Meanings:

- `control_connected`: a client control channel is up for this service.
- `connections_active` / `connections_total`: TCP visitor sessions currently forwarding / accepted since process start (or since the service was last hot-reloaded).
- `data_channels_reused`: TCP visitor sessions served over a reused data channel; omitted from JSON when 0. See [Connection reuse](connection-reuse.md).
- `bytes_in` / `bytes_out`: visitor-facing payload. Inbound is public → client; outbound is client → public.
- `wire_bytes_*`: bytes on the data channel after optional zstd. For uncompressed TCP this matches the visitor counters, plus 3 bytes of framing per write when `connection_reuse` is on.
- `datagrams_in` / `datagrams_out`: UDP only; omitted from JSON when both are 0.
- `compression_ratio`: `(bytes_in + bytes_out) / (wire_bytes_in + wire_bytes_out)` when `compression` is set and wire bytes are non-zero. Larger is better. Absent when compression is off.
- `dictionary.mode`: `static` (file), `auto` (trained from traffic), or `none` (plain zstd). `state` is `sampling` / `trained` / `failed` for auto dictionaries.

Services are sorted by name. Counters reset when a service is removed and re-added via hot-reload.

## Prometheus

The same numbers as labeled series (`service`, `type`). `rathole_compression_ratio` is omitted when compression is off. Label values escape `\`, `"`, and newlines.
