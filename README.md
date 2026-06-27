# yrs-engineio

Yrs synchronization protocol over [Engine.IO](https://github.com/socketio/engine.io),
built on top of [`engineioxide`](https://crates.io/crates/engineioxide).

This library is an extension over [Yjs](https://yjs.dev)/[Yrs](https://github.com/y-crdt/y-crdt)
Conflict-Free Replicated Data Types (CRDT) message exchange protocol. It wires a
[`yrs::sync::Awareness`](https://docs.rs/yrs) document to Engine.IO connections so
that document updates and awareness are gossiped between every peer collaborating
on the same logical document.

It was inspired by — and is a near-direct port of —
[`yrs-axum`](https://github.com/vagmi/yrs-axum). The y-sync core (`BroadcastGroup`,
`conn`, custom `Protocol` support) is unchanged; only the transport layer differs:
instead of an Axum raw WebSocket, connections are driven by `engineioxide` sockets
through the `EngineSink` / `EngineStream` adapters.

## Why Engine.IO

Engine.IO negotiates a transport (HTTP long-polling → WebSocket) and transparently
falls back when WebSockets are blocked by a proxy, which a raw WebSocket cannot do.
It also exposes a single connection endpoint, so the room/document is selected via a
handshake query parameter rather than the URL path.

## Transport adapters

The broadcast core is transport-agnostic: `BroadcastGroup::subscribe` accepts any
`Sink<Vec<u8>>` + `Stream<Item = Result<Vec<u8>, E>>` pair. This crate provides two
adapters over an `engineioxide` socket:

- **`EngineSink<D>`** — wraps an `Arc<Socket<D>>` and implements `futures::Sink<Vec<u8>>`,
  emitting each frame as an Engine.IO binary message via `socket.emit_binary`.
- **`EngineStream`** — wraps a [`kanal`](https://crates.io/crates/kanal)
  `AsyncReceiver<Vec<u8>>` and implements `futures::Stream`. Inbound binary frames are
  forwarded into the channel from the Engine.IO message handler; the stream ends when
  the channel closes (i.e. on disconnect).

## Example

Implement `engineioxide`'s `EngineIoHandler`. On connect, create a channel, store the
sender so inbound frames can be pushed into it, and spawn a task that subscribes the
connection to a shared `BroadcastGroup`:

```rust
use std::sync::Arc;
use engineioxide::handler::EngineIoHandler;
use engineioxide::socket::{DisconnectReason, Socket};
use tokio::sync::Mutex;
use yrs::sync::Awareness;
use yrs::{Doc, Transact};
use yrs_engineio::broadcast::BroadcastGroup;
use yrs_engineio::engineio::{EngineSink, EngineStream};

#[derive(Default)]
struct Data {
    inbox: parking_lot::Mutex<Option<kanal::AsyncSender<Vec<u8>>>>,
}

#[derive(Clone)]
struct Handler {
    // A real server resolves the BroadcastGroup per room from the handshake query.
    group: Arc<BroadcastGroup>,
}

impl EngineIoHandler for Handler {
    type Data = Data;

    fn on_connect(self: Arc<Self>, socket: Arc<Socket<Data>>) {
        let (tx, rx) = kanal::unbounded_async();
        *socket.data.inbox.lock() = Some(tx);

        let group = self.group.clone();
        tokio::spawn(async move {
            let sink = Arc::new(Mutex::new(EngineSink(socket.clone())));
            let stream = EngineStream::new(rx);
            let sub = group.subscribe(sink, stream);
            let _ = sub.completed().await;
            socket.close(DisconnectReason::TransportClose);
        });
    }

    fn on_disconnect(&self, socket: Arc<Socket<Data>>, _reason: DisconnectReason) {
        let _ = socket.data.inbox.lock().take();
    }

    fn on_message(self: &Arc<Self>, _msg: engineioxide::Str, _socket: Arc<Socket<Data>>) {}

    fn on_binary(self: &Arc<Self>, data: bytes::Bytes, socket: Arc<Socket<Data>>) {
        if let Some(tx) = socket.data.inbox.lock().as_ref() {
            let _ = tx.try_send(data.to_vec());
        }
    }
}

async fn make_group() -> Arc<BroadcastGroup> {
    let awareness = Arc::new(Awareness::new(Doc::new()));
    // pending message buffer of up to 64 updates
    Arc::new(BroadcastGroup::new(awareness, 64).await)
}
```

Mount the handler with `engineioxide::service::EngineIoService` (configuring `req_path`
to your endpoint) and serve it, e.g. via `axum`'s `route_service`.

## Custom protocol extensions

The [y-sync](https://crates.io/crates/yrs) protocol can be extended, and this crate
supports it via `BroadcastGroup::subscribe_with`:

```rust
use yrs::sync::{Awareness, Error, Message, Protocol};

struct EchoProtocol;
impl Protocol for EchoProtocol {
    fn missing_handle(
        &self,
        _awareness: &Awareness,
        tag: u8,
        data: Vec<u8>,
    ) -> Result<Option<Message>, Error> {
        // messages with tags unknown to the y-sync protocol are echoed back
        Ok(Some(Message::Custom(tag, data)))
    }
}

// ... later, when registering a connection:
// let sub = group.subscribe_with(sink, stream, EchoProtocol);
```

## License

[The MIT License](./LICENSE). Port of [yrs-axum](https://github.com/vagmi/yrs-axum),
which is itself based on [yrs-warp](https://github.com/y-crdt/yrs-warp).
