# Connection reuse

By default a TCP data channel forwards exactly one visitor. When the visitor is done, the data channel is closed, and the next visitor needs a new one: a TCP connect plus the transport handshake (TLS, Noise, WebSocket upgrade). `server.tcp_pool_size` hides that cost for a few visitors by opening channels in advance. It does not remove it, and a burst larger than the pool still waits for handshakes.

With `connection_reuse = true`, a data channel is kept after its visitor leaves and serves the next one. A service with many short connections (plain HTTP, health checks, metrics scrapes) settles on a handful of long-lived data channels, and stops paying for connects and handshakes.

The server decides, per service. There is no client key: putting `connection_reuse` under `[client.services.X]` is a TOML unknown-field error.

```toml
[server.services.web]
bind_addr = "0.0.0.0:8080"
connection_reuse = true
connection_reuse_max_idle = 64 # Optional
```

Keys live in the [configuration specification](../README.md#configuration).

> **Upgrade all clients before turning this on.**
>
> A client that predates this feature cannot interpret `StartForwardTcpReuse`. It logs `Unknown DataChannelCmd tag: 4` and drops the data channel, so every visitor of that service is disconnected. Leaving `connection_reuse` unset keeps the wire byte-identical to previous versions.

## Mechanism

Closing the connection is how a plain data channel says "this visitor is done". A reusable channel has to say it in-band, so the forwarded bytes are framed:

```text
frame := kind:u8  len:u16le  payload[len]
```

- `DATA` carries up to 65535 payload bytes.
- `FIN` ends one direction gracefully. It is the visitor's or the local service's TCP half-close, carried across the tunnel. The other direction keeps flowing.
- `RST` aborts the session: the visitor reset its connection, the client could not reach `local_addr`, and so on. The receiving side drops its end of the forwarded connection.

Each direction carries any number of `DATA` frames and then exactly one `FIN` or `RST`. Once both directions have ended, the session is over and the connection sits on a frame boundary.

The server opens every session with a data channel command, `StartForwardTcpReuse` or `StartForwardTcpZstdReuse`, the same way it opens a one-shot channel. After a session the client waits on the same connection for the next command. The server puts the channel back into the service's pool and prefers it over a never-used channel for the next visitor.

A frame's header and payload leave in a single write, so framing costs 3 bytes per write and no extra packet or TLS record. One visitor still owns one connection at a time. There is no multiplexing, and no head-of-line blocking between visitors.

## Compression

Framing sits below zstd: each session gets its own zstd stream inside the frames. A reused channel therefore picks up the current [dictionary generation](compression.md#automatic-dictionaries) on its next session. Without reuse a channel keeps the generation it was created with.

## The pool

`server.tcp_pool_size` still pre-opens that many channels per service. On top of it, up to `connection_reuse_max_idle` finished channels wait for visitors (default 64). A channel that finishes while that many are already waiting is closed. `connection_reuse_max_idle = 0` closes every channel after its first visitor, which is the old behavior at the price of the framing.

A visitor that finds a finished channel waiting does not trigger `CreateDataChannel`. New connections are made only when concurrency grows past what is idle.

An idle channel may die: the client restarted, a NAT dropped the mapping. Before the server hands one to a visitor, it checks that the connection is not closed or errored, and asks for a new channel if it is. TCP keepalive (`transport.tcp.keepalive_secs`) is what surfaces a silently dead peer. Keep it on for services that reuse.

Idle channels never outlive their control channel. When it goes away (restart, hot-reload removing the service, heartbeat timeout), both sides close the idle data channels. Sessions in progress run to completion, as they do without reuse.

## When a channel is not reused

A channel is closed instead of kept when:

- The connection itself failed, or the peer sent something that is not a valid frame.
- The session was aborted and the peer did not end its direction within 5 seconds, or kept sending more than 4 MiB.
- `connection_reuse_max_idle` channels are already waiting.
- The control channel is gone.

None of these affect other visitors. The next visitor gets another channel.

## Observing it

With [`observe_addr`](observe.md) set, `data_channels_reused` (`rathole_data_channels_reused_total`) counts visitor sessions that were served over a reused channel. Compared with `connections_total` it tells how many connects and handshakes were saved. `wire_bytes_*` include the frame headers.

## UDP

A UDP service already uses one long-lived data channel for all visitors. `connection_reuse` on a UDP service is a config error:

```
Service {name}: `connection_reuse` is only supported for `type = "tcp"`
Service {name}: `connection_reuse_max_idle` requires `connection_reuse = true` to be set
```
