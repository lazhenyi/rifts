# Rift Realtime Protocol / 1.0

[![CI](https://github.com/lazhenyi/rift/actions/workflows/ci.yml/badge.svg)](https://github.com/lazhenyi/rift/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/rift.svg)](https://crates.io/crates/rift)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE-MIT)

A Rust server implementation of the **Rift Realtime Protocol (Rift/1)** — a modern, real-time
bidirectional messaging protocol designed to replace raw WebSocket + event-string models with
stronger semantics, similar to Socket.IO but with explicit guarantees.

## Protocol Specification

Rift/1 is defined by [doc.md](doc.md) in this repository. Key features:

- **Explicit message classes** — event, command, reply, state, datagram, stream, snapshot, system
- **Reliable offset-based recovery** — reconnect resume via per-topic offsets and snapshots
- **Schema-first** — every event is typed, validated, and registered
- **Topic model** — topic profiles with retention, ordering, auth, and fanout policies
- **Backpressure & flow control** — explicit strategies (pause, drop-volatile, coalesce, downgrade)
- **Pluggable transports** — WebSocket, WebTransport, native TCP
- **Structured errors** — stable error codes for all protocol layers
- **Observability** — built-in counters for connections, messages, latency, and backpressure

## Crate Features

| Feature | Description |
|---------|-------------|
| `websocket` *(default)* | Standalone WebSocket transport via `tokio-tungstenite` |
| `axum` | Axum framework WebSocket adapter |
| `actix-web` | Actix-web WebSocket adapter |
| `warp` | Warp framework WebSocket adapter |
| `ntex` | Ntex framework WebSocket adapter |
| `tokio-frameworks` | Shortcut: axum + warp |
| `all-frameworks` | Shortcut: all framework adapters |

## Quick Start

```rust
use rift::RiftServer;
use std::sync::Arc;
use tokio::sync::Notify;

#[tokio::main]
async fn main() -> rift::Result<()> {
    let shutdown = Arc::new(Notify::new());
    let server = RiftServer::builder()
        .websocket_transport()
        .build()?;
    server.run("127.0.0.1:9000".parse().unwrap(), shutdown).await?;
    Ok(())
}
```

## Module Structure

| Module | Purpose |
|--------|---------|
| `frame` | Frame envelope, types, flags, codec, priority |
| `codec` | CBOR + JSON codecs with negotiation |
| `protocol` | Version, close codes, error codes, heartbeat, handshake |
| `message` | Eight message classes (event, command, reply, state, …) |
| `topic` | Topic profiles, retention, ordering, in-memory store |
| `session` | Sessions, auth, resume, offset tracking |
| `broker` | In-memory broker, router, fanout, dedupe, snapshots |
| `ack` | Acknowledgement system (9 ack types) |
| `flow` | Backpressure controller + token-bucket rate limiter |
| `transport` | Transport abstraction + WebSocket, framework adapters |
| `server` | `RiftServer` builder and event loop |
| `connection` | Per-connection state machine (spec §5 lifecycle) |

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please open an issue or pull request.
