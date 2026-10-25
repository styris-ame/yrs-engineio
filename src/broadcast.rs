#![allow(dead_code)]
use crate::{unique_origin, AwarenessRef, Direct, Handler, Locked};
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::{Arc, Weak};
use tokio::select;
use tokio::sync::broadcast::error::{RecvError, SendError};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use yrs::encoding::read::Cursor;
use yrs::encoding::write::Write;
use yrs::sync::protocol::{MSG_SYNC, MSG_SYNC_UPDATE};
use yrs::sync::{Awareness, Error, Message, MessageReader, Protocol};
use yrs::updates::decoder::DecoderV1;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{ClientID, Doc, Origin, Transact};

/// A message fanned out to subscribers. `origin` identifies the subscription whose change caused
/// it, so that subscription can skip its own echo.
#[derive(Clone)]
struct Outgoing {
    origin: Option<Origin>,
    payload: Vec<u8>,
}

/// A broadcast group can be used to propagate updates produced by yrs [yrs::Doc] and [Awareness]
/// structures in a binary form that conforms to a y-sync protocol.
///
/// New receivers can subscribe to a broadcasting group via [BroadcastGroup::subscribe] method.
pub struct BroadcastGroup {
    observer_key: Origin,
    doc: Doc,
    awareness_ref: AwarenessRef,
    sender: Sender<Outgoing>,
    receiver: Receiver<Outgoing>,
    awareness_updater: JoinHandle<()>,
}

impl BroadcastGroup {
    /// Creates a new [BroadcastGroup] over a provided `awareness` instance. All changes triggered
    /// by this awareness structure or its underlying document will be propagated to all subscribers
    /// which have been registered via [BroadcastGroup::subscribe] method.
    ///
    /// The overflow of the incoming events that needs to be propagates will be buffered up to a
    /// provided `buffer_capacity` size.
    pub async fn new(awareness: AwarenessRef, buffer_capacity: usize) -> Self {
        let (sender, receiver) = channel(buffer_capacity);
        // Unique per group, so multiple groups (or user observers) never replace each other's
        // callbacks on a shared [Doc] or [Awareness].
        let observer_key = unique_origin("broadcast");
        let doc = awareness.read().doc().clone();

        // Observers hold weak senders so a callback that outlives the group (e.g. if unobserving
        // failed on drop) keeps nothing alive.
        let sink = sender.downgrade();
        doc.transact_mut()
            .observe_update_v1(observer_key.clone(), move |txn, u| {
                // we manually construct msg here to avoid update data copying
                let mut encoder = EncoderV1::new();
                encoder.write_var(MSG_SYNC);
                encoder.write_var(MSG_SYNC_UPDATE);
                encoder.write_buf(&u.update);
                if let Some(sink) = sink.upgrade() {
                    let _ = sink.send(Outgoing {
                        origin: txn.origin().cloned(),
                        payload: encoder.to_vec(),
                    });
                }
            });

        let (tx, rx) = kanal::unbounded_async();
        awareness
            .write()
            .on_update(observer_key.clone(), move |_awareness, e, origin| {
                let _ = tx.try_send((origin.cloned(), e.all_changes()));
            });

        let awareness_c = Arc::downgrade(&awareness);
        let sink = sender.downgrade();
        let awareness_updater = tokio::task::spawn(async move {
            while let Ok((origin, changed_clients)) = rx.recv().await {
                let (Some(awareness), Some(sink)) = (awareness_c.upgrade(), sink.upgrade()) else {
                    return;
                };
                let update = awareness.read().update_with_clients(changed_clients);
                match update {
                    Ok(update) => {
                        let payload = Message::Awareness(update).encode_v1();
                        if sink.send(Outgoing { origin, payload }).is_err() {
                            tracing::warn!("couldn't broadcast awareness update");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("error while computing awareness update: {}", e)
                    }
                }
            }
        });
        BroadcastGroup {
            observer_key,
            doc,
            awareness_ref: awareness,
            awareness_updater,
            sender,
            receiver,
        }
    }

    /// Returns a reference to an underlying [Awareness] instance.
    pub fn awareness(&self) -> &AwarenessRef {
        &self.awareness_ref
    }

    /// Returns the underlying [Doc], accessible without taking the awareness lock.
    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    /// Broadcasts user message to all active subscribers. Returns error if message could not have
    /// been broadcasted.
    pub fn broadcast(&self, msg: Vec<u8>) -> Result<(), SendError<Vec<u8>>> {
        self.sender
            .send(Outgoing {
                origin: None,
                payload: msg,
            })
            .map_err(|e| SendError(e.0.payload))?;
        Ok(())
    }

    /// Subscribes a new connection - represented by `sink`/`stream` pair implementing a futures
    /// Sink and Stream protocols - to a current broadcast group.
    ///
    /// Returns a subscription structure, which can be dropped in order to unsubscribe or awaited
    /// via [Subscription::completed] method in order to complete of its own volition (due to
    /// an internal connection error or closed connection).
    ///
    /// Document messages are applied directly to the [Doc]; only awareness messages take the
    /// awareness lock. Changes made by this connection are not echoed back to it.
    pub fn subscribe<Sink, Stream, E>(&self, sink: Arc<Mutex<Sink>>, stream: Stream) -> Subscription
    where
        Sink: SinkExt<Vec<u8>> + Send + Sync + Unpin + 'static,
        Stream: StreamExt<Item = Result<Vec<u8>, E>> + Send + Unpin + 'static,
        <Sink as futures_util::Sink<Vec<u8>>>::Error: std::error::Error + Send + Sync,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.spawn(sink, stream, Direct)
    }

    /// Subscribes a new connection - represented by `sink`/`stream` pair implementing a futures
    /// Sink and Stream protocols - to a current broadcast group.
    ///
    /// Returns a subscription structure, which can be dropped in order to unsubscribe or awaited
    /// via [Subscription::completed] method in order to complete of its own volition (due to
    /// an internal connection error or closed connection).
    ///
    /// Unlike [BroadcastGroup::subscribe], this method can take [Protocol] parameter that allows to
    /// customize the y-sync protocol behavior. Every message is handled under the awareness write
    /// lock, since [Protocol] handlers require `&mut Awareness`.
    pub fn subscribe_with<Sink, Stream, E, P>(
        &self,
        sink: Arc<Mutex<Sink>>,
        stream: Stream,
        protocol: P,
    ) -> Subscription
    where
        Sink: SinkExt<Vec<u8>> + Send + Sync + Unpin + 'static,
        Stream: StreamExt<Item = Result<Vec<u8>, E>> + Send + Unpin + 'static,
        <Sink as futures_util::Sink<Vec<u8>>>::Error: std::error::Error + Send + Sync,
        E: std::error::Error + Send + Sync + 'static,
        P: Protocol + Send + Sync + 'static,
    {
        self.spawn(sink, stream, Locked(protocol))
    }

    fn spawn<Sink, Stream, E, H>(&self, sink: Arc<Mutex<Sink>>, mut stream: Stream, handler: H) -> Subscription
    where
        Sink: SinkExt<Vec<u8>> + Send + Sync + Unpin + 'static,
        Stream: StreamExt<Item = Result<Vec<u8>, E>> + Send + Unpin + 'static,
        <Sink as futures_util::Sink<Vec<u8>>>::Error: std::error::Error + Send + Sync,
        E: std::error::Error + Send + Sync + 'static,
        H: Handler,
    {
        let origin = unique_origin("conn");
        let sink_task = {
            let sink = sink.clone();
            let origin = origin.clone();
            let mut receiver = self.sender.subscribe();
            tokio::spawn(async move {
                loop {
                    let msg = match receiver.recv().await {
                        Ok(msg) => msg,
                        Err(RecvError::Closed) => return Ok(()),
                        // Dropping updates would silently desync the peer; end the connection so
                        // it reconnects and resyncs instead.
                        Err(RecvError::Lagged(n)) => {
                            return Err(Error::Other(format!("subscriber lagged behind by {n} messages").into()))
                        }
                    };
                    if msg.origin.as_ref() == Some(&origin) {
                        continue;
                    }
                    let mut sink = sink.lock().await;
                    if let Err(e) = sink.send(msg.payload).await {
                        return Err(Error::Other(Box::new(e)));
                    }
                }
            })
        };
        let stream_task = {
            let awareness = self.awareness_ref.clone();
            let doc = self.doc.clone();

            tokio::spawn(async move {
                // Dropped on any exit, including abort, clearing awareness states this peer set.
                let mut controlled = ControlledClients {
                    awareness: Arc::downgrade(&awareness),
                    ids: HashSet::new(),
                };
                let payload = handler.start(&awareness)?;
                if !payload.is_empty() {
                    let mut s = sink.lock().await;
                    s.send(payload).await.map_err(|e| Error::Other(e.into()))?;
                }
                while let Some(res) = stream.next().await {
                    let data = res.map_err(|e| Error::Other(Box::new(e)))?;
                    let mut decoder = DecoderV1::new(Cursor::new(&data));
                    for msg in MessageReader::new(&mut decoder) {
                        let msg = msg?;
                        if let Message::Awareness(update) = &msg {
                            controlled.track(update.clients.iter().map(|(id, e)| (*id, e.json.as_ref() == "null")));
                        }
                        if let Some(reply) = handler.handle(&awareness, &doc, &origin, msg)? {
                            let mut sink = sink.lock().await;
                            sink.send(reply.encode_v1())
                                .await
                                .map_err(|e| Error::Other(Box::new(e)))?;
                        }
                    }
                }
                Ok(())
            })
        };

        Subscription {
            sink_task,
            stream_task,
        }
    }
}

impl Drop for BroadcastGroup {
    fn drop(&mut self) {
        self.awareness_updater.abort();
        self.awareness_ref.write().unobserve_update(self.observer_key.clone());
        if self.doc.unobserve_update_v1(self.observer_key.clone()).is_err() {
            tracing::warn!("doc busy while dropping broadcast group; update observer left as a no-op");
        }
    }
}

/// Awareness client IDs a peer has announced. Like y-websocket, their states are removed when the
/// peer goes away, so other peers (and later joiners) don't see stale presence.
struct ControlledClients {
    awareness: Weak<RwLock<Awareness>>,
    ids: HashSet<ClientID>,
}

impl ControlledClients {
    fn track(&mut self, entries: impl Iterator<Item = (ClientID, bool)>) {
        for (id, removed) in entries {
            if removed {
                self.ids.remove(&id);
            } else {
                self.ids.insert(id);
            }
        }
    }
}

impl Drop for ControlledClients {
    fn drop(&mut self) {
        if self.ids.is_empty() {
            return;
        }
        if let Some(awareness) = self.awareness.upgrade() {
            let mut awareness = awareness.write();
            for id in self.ids.drain() {
                awareness.remove_state(id);
            }
        }
    }
}

/// A subscription structure returned from [BroadcastGroup::subscribe], which represents a
/// subscribed connection. It can be dropped in order to unsubscribe or awaited via
/// [Subscription::completed] method in order to complete of its own volition (due to an internal
/// connection error or closed connection).
#[derive(Debug)]
pub struct Subscription {
    sink_task: JoinHandle<Result<(), Error>>,
    stream_task: JoinHandle<Result<(), Error>>,
}

impl Subscription {
    /// Consumes current subscription, waiting for it to complete. If an underlying connection was
    /// closed because of failure, an error which caused it to happen will be returned.
    ///
    /// This method doesn't invoke close procedure. If you need that, drop current subscription instead.
    pub async fn completed(mut self) -> Result<(), Error> {
        let res = select! {
            r1 = &mut self.sink_task => r1,
            r2 = &mut self.stream_task => r2,
        };
        res.map_err(|e| Error::Other(e.into()))?
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.sink_task.abort();
        self.stream_task.abort();
    }
}

#[cfg(test)]
mod test {
    use crate::broadcast::BroadcastGroup;
    use futures_util::{ready, SinkExt, StreamExt};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::time::timeout;
    use tokio_util::sync::PollSender;
    use yrs::sync::awareness::AwarenessUpdateEntry;
    use yrs::sync::{Awareness, AwarenessUpdate, Error, Message, SyncMessage};
    use yrs::updates::decoder::Decode;
    use yrs::updates::encoder::Encode;
    use yrs::{ClientID, Doc, ReadTxn, StateVector, Text, Transact};

    #[derive(Debug)]
    pub struct ReceiverStream<T> {
        inner: tokio::sync::mpsc::Receiver<T>,
    }

    impl<T> ReceiverStream<T> {
        /// Create a new `ReceiverStream`.
        pub fn new(recv: tokio::sync::mpsc::Receiver<T>) -> Self {
            Self { inner: recv }
        }
    }

    impl<T> futures_util::Stream for ReceiverStream<T> {
        type Item = Result<T, Error>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match ready!(self.inner.poll_recv(cx)) {
                None => Poll::Ready(None),
                Some(v) => Poll::Ready(Some(Ok(v))),
            }
        }
    }

    fn test_channel(capacity: usize) -> (PollSender<Vec<u8>>, ReceiverStream<Vec<u8>>) {
        let (s, r) = tokio::sync::mpsc::channel::<Vec<u8>>(capacity);
        let s = PollSender::new(s);
        let r = ReceiverStream::new(r);
        (s, r)
    }

    const TIMEOUT: Duration = Duration::from_secs(5);

    async fn next_msg(receiver: &mut ReceiverStream<Vec<u8>>) -> Message {
        let data = timeout(TIMEOUT, receiver.next())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed")
            .unwrap();
        Message::decode_v1(&data).unwrap()
    }

    fn awareness_update(client_id: u64, clock: u32, json: &str) -> Vec<u8> {
        Message::Awareness(AwarenessUpdate {
            clients: HashMap::from([(
                ClientID::new(client_id),
                AwarenessUpdateEntry {
                    clock,
                    json: json.into(),
                },
            )]),
        })
        .encode_v1()
    }

    #[tokio::test]
    async fn broadcast_changes() -> Result<(), Box<dyn std::error::Error>> {
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");
        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let group = BroadcastGroup::new(awareness.clone(), 1).await;

        let (server_sender, mut client_receiver) = test_channel(1);
        let (mut client_sender, server_receiver) = test_channel(1);
        let _sub1 = group.subscribe(Arc::new(Mutex::new(server_sender)), server_receiver);

        // Consume the initial SyncStep1 + Awareness payload sent by protocol.start()
        let init_msg = client_receiver
            .next()
            .await
            .expect("should receive initial sync message")
            .map(|data| Message::decode_v1(&data).unwrap());
        assert!(
            matches!(init_msg, Ok(Message::Sync(SyncMessage::SyncStep1(_)))),
            "first message should be SyncStep1, got: {:?}",
            init_msg
        );

        // check update propagation
        {
            text.push(&mut group.doc().transact_mut(), "a");
        }
        let msg = client_receiver.next().await;
        let msg = msg.map(|x| Message::decode_v1(&x.unwrap()).unwrap());
        assert_eq!(
            msg,
            Some(Message::Sync(SyncMessage::Update(vec![
                1, 1, 1, 0, 4, 1, 4, 116, 101, 115, 116, 1, 97, 0,
            ])))
        );

        // check awareness update propagation
        awareness.write().set_local_state(r#"{"key":"value"}"#)?;

        let msg = client_receiver.next().await;
        let msg = msg.map(|x| Message::decode_v1(&x.unwrap()).unwrap());
        assert_eq!(
            msg,
            Some(Message::Awareness(AwarenessUpdate {
                clients: HashMap::from([(
                    ClientID::new(1),
                    AwarenessUpdateEntry {
                        clock: 1,
                        json: r#""{\"key\":\"value\"}""#.into(),
                    },
                )]),
            }))
        );

        // check sync state request/response
        {
            client_sender
                .send(Message::Sync(SyncMessage::SyncStep1(StateVector::default())).encode_v1())
                .await?;
            let msg = client_receiver.next().await;
            let msg = msg.map(|x| Message::decode_v1(&x.unwrap()).unwrap());
            assert_eq!(
                msg,
                Some(Message::Sync(SyncMessage::SyncStep2(vec![
                    1, 1, 1, 0, 4, 1, 4, 116, 101, 115, 116, 1, 97, 0,
                ])))
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn own_changes_are_not_echoed() -> Result<(), Box<dyn std::error::Error>> {
        let awareness = Arc::new(RwLock::new(Awareness::new(Doc::with_client_id(1))));
        let group = BroadcastGroup::new(awareness.clone(), 16).await;

        let (a_sink, mut a_rx) = test_channel(16);
        let (mut a_tx, a_stream) = test_channel(16);
        let _a = group.subscribe(Arc::new(Mutex::new(a_sink)), a_stream);
        let (b_sink, mut b_rx) = test_channel(16);
        let (_b_tx, b_stream) = test_channel(16);
        let _b = group.subscribe(Arc::new(Mutex::new(b_sink)), b_stream);
        assert!(matches!(next_msg(&mut a_rx).await, Message::Sync(SyncMessage::SyncStep1(_))));
        assert!(matches!(next_msg(&mut b_rx).await, Message::Sync(SyncMessage::SyncStep1(_))));

        let update = {
            let client = Doc::with_client_id(2);
            let text = client.get_or_insert_text("test");
            text.push(&mut client.transact_mut(), "a");
            let update = client.transact().encode_state_as_update_v1(&StateVector::default());
            update
        };
        a_tx.send(Message::Sync(SyncMessage::Update(update.clone())).encode_v1()).await?;
        a_tx.send(awareness_update(2, 1, r#"{"user":"a"}"#)).await?;

        // B receives both; A receives neither, so the next thing A sees is the SyncStep2 reply.
        assert_eq!(next_msg(&mut b_rx).await, Message::Sync(SyncMessage::Update(update)));
        assert!(matches!(next_msg(&mut b_rx).await, Message::Awareness(_)));
        a_tx.send(Message::Sync(SyncMessage::SyncStep1(StateVector::default())).encode_v1()).await?;
        assert!(matches!(next_msg(&mut a_rx).await, Message::Sync(SyncMessage::SyncStep2(_))));
        // Echoes go through the broadcast task, so they could trail the direct reply.
        assert!(timeout(Duration::from_millis(200), a_rx.next()).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn awareness_cleared_on_disconnect() -> Result<(), Box<dyn std::error::Error>> {
        let awareness = Arc::new(RwLock::new(Awareness::new(Doc::with_client_id(1))));
        let group = BroadcastGroup::new(awareness.clone(), 16).await;

        let (a_sink, mut a_rx) = test_channel(16);
        let (mut a_tx, a_stream) = test_channel(16);
        let a = group.subscribe(Arc::new(Mutex::new(a_sink)), a_stream);
        let (b_sink, mut b_rx) = test_channel(16);
        let (_b_tx, b_stream) = test_channel(16);
        let _b = group.subscribe(Arc::new(Mutex::new(b_sink)), b_stream);
        next_msg(&mut a_rx).await;
        next_msg(&mut b_rx).await;

        a_tx.send(awareness_update(2, 1, r#"{"user":"a"}"#)).await?;
        assert!(matches!(next_msg(&mut b_rx).await, Message::Awareness(_)));

        // Closing A's inbound stream ends its subscription.
        drop(a_tx);
        timeout(TIMEOUT, a.completed()).await??;

        match next_msg(&mut b_rx).await {
            Message::Awareness(update) => assert_eq!(&*update.clients[&ClientID::new(2)].json, "null"),
            other => panic!("expected awareness removal, got {other:?}"),
        }
        // Later joiners no longer receive the stale state.
        assert!(!awareness.read().update()?.clients.contains_key(&ClientID::new(2)));

        Ok(())
    }
}
