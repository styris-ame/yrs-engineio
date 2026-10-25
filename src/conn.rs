#![allow(dead_code)]
use futures_util::sink::SinkExt;
use futures_util::StreamExt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use yrs::encoding::read::Cursor;
use yrs::sync::{Error, MessageReader, Protocol};
use yrs::updates::decoder::DecoderV1;
use yrs::updates::encoder::Encode;
use yrs::{Doc, Origin};

use crate::{unique_origin, AwarenessRef, Direct, Handler, Locked};

/// Connection handler over a pair of message streams, which implements a Yjs/Yrs awareness and
/// update exchange protocol.
///
/// This connection implements Future pattern and can be awaited upon in order for a caller to
/// recognize whether underlying websocket connection has been finished gracefully or abruptly.
#[derive(Debug)]
pub struct Connection<Sink, Stream> {
    processing_loop: JoinHandle<Result<(), Error>>,
    awareness: AwarenessRef,
    doc: Doc,
    inbox: Arc<Mutex<Sink>>,
    _stream: PhantomData<Stream>,
}

impl<Sink, Stream, E> Connection<Sink, Stream>
where
    Sink: SinkExt<Vec<u8>, Error = E> + Send + Sync + Unpin + 'static,
    E: Into<Error> + Send + Sync,
{
    pub async fn send(&self, msg: Vec<u8>) -> Result<(), Error> {
        let mut inbox = self.inbox.lock().await;
        match inbox.send(msg).await {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn close(self) -> Result<(), E> {
        let mut inbox = self.inbox.lock().await;
        inbox.close().await
    }

    pub fn sink(&self) -> Weak<Mutex<Sink>> {
        Arc::downgrade(&self.inbox)
    }
}

impl<Sink, Stream, E> Connection<Sink, Stream>
where
    Stream: StreamExt<Item = Result<Vec<u8>, E>> + Send + Sync + Unpin + 'static,
    Sink: SinkExt<Vec<u8>, Error = E> + Send + Sync + Unpin + 'static,
    E: Into<Error> + Send + Sync,
{
    /// Wraps incoming [WebSocket] connection and supplied [Awareness] accessor into a new
    /// connection handler capable of exchanging Yrs/Yjs messages.
    ///
    /// While creation of new [AxumConn] always succeeds, a connection itself can possibly fail
    /// while processing incoming input/output. This can be detected by awaiting for returned
    /// [AxumConn] and handling the awaited result.
    ///
    /// Document messages are applied directly to the [Doc]; only awareness messages take the
    /// awareness lock.
    pub fn new(awareness: AwarenessRef, sink: Sink, stream: Stream) -> Self {
        Self::spawn(awareness, sink, stream, Direct)
    }

    /// Returns an underlying [Awareness] structure, that contains client state of that connection.
    pub fn awareness(&self) -> &AwarenessRef {
        &self.awareness
    }

    /// Returns the underlying [Doc], accessible without taking the awareness lock.
    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    /// Wraps incoming [WebSocket] connection and supplied [Awareness] accessor into a new
    /// connection handler capable of exchanging Yrs/Yjs messages.
    ///
    /// While creation of new [AxumConn] always succeeds, a connection itself can possibly fail
    /// while processing incoming input/output. This can be detected by awaiting for returned
    /// [AxumConn] and handling the awaited result.
    ///
    /// Every message is handled under the awareness write lock, since [Protocol] handlers
    /// require `&mut Awareness`.
    pub fn with_protocol<P>(awareness: AwarenessRef, sink: Sink, stream: Stream, protocol: P) -> Self
    where
        P: Protocol + Send + Sync + 'static,
    {
        Self::spawn(awareness, sink, stream, Locked(protocol))
    }

    fn spawn<H: Handler>(awareness: AwarenessRef, sink: Sink, mut stream: Stream, handler: H) -> Self {
        let doc = awareness.read().doc().clone();
        let sink = Arc::new(Mutex::new(sink));
        let inbox = sink.clone();
        let loop_sink = Arc::downgrade(&sink);
        let loop_awareness = Arc::downgrade(&awareness);
        let loop_doc = doc.clone();
        let origin = unique_origin("conn");
        let processing_loop: JoinHandle<Result<(), Error>> = spawn(async move {
            // at the beginning send SyncStep1 and AwarenessUpdate
            let payload = {
                let awareness = loop_awareness.upgrade().unwrap();
                handler.start(&awareness)?
            };
            if !payload.is_empty() {
                if let Some(sink) = loop_sink.upgrade() {
                    let mut s = sink.lock().await;
                    if let Err(e) = s.send(payload).await {
                        return Err(e.into());
                    }
                } else {
                    return Ok(()); // parent ConnHandler has been dropped
                }
            }

            while let Some(input) = stream.next().await {
                match input {
                    Ok(data) => {
                        if let Some(mut sink) = loop_sink.upgrade() {
                            if let Some(awareness) = loop_awareness.upgrade() {
                                match Self::process(&handler, &awareness, &loop_doc, &origin, &mut sink, data).await {
                                    Ok(()) => { /* continue */ }
                                    Err(e) => {
                                        return Err(e);
                                    }
                                }
                            } else {
                                return Ok(()); // parent ConnHandler has been dropped
                            }
                        } else {
                            return Ok(()); // parent ConnHandler has been dropped
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            Ok(())
        });
        Connection {
            processing_loop,
            awareness,
            doc,
            inbox,
            _stream: PhantomData,
        }
    }

    async fn process<H: Handler>(
        handler: &H,
        awareness: &AwarenessRef,
        doc: &Doc,
        origin: &Origin,
        sink: &mut Arc<Mutex<Sink>>,
        input: Vec<u8>,
    ) -> Result<(), Error> {
        let mut decoder = DecoderV1::new(Cursor::new(&input));
        let reader = MessageReader::new(&mut decoder);
        for r in reader {
            let msg = r?;
            if let Some(reply) = handler.handle(awareness, doc, origin, msg)? {
                let mut sender = sink.lock().await;
                if let Err(e) = sender.send(reply.encode_v1()).await {
                    tracing::debug!("connection failed to send back the reply");
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
}

impl<Sink, Stream> Drop for Connection<Sink, Stream> {
    fn drop(&mut self) {
        self.processing_loop.abort();
    }
}

impl<Sink, Stream> Unpin for Connection<Sink, Stream> {}

impl<Sink, Stream> Future for Connection<Sink, Stream> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.processing_loop).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Other(e.into()))),
            Poll::Ready(Ok(r)) => Poll::Ready(r),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::broadcast::BroadcastGroup;
    use crate::conn::Connection;
    use bytes::{Bytes, BytesMut};
    use futures_util::SinkExt;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::{TcpListener, TcpSocket};
    use tokio::sync::{Mutex, Notify};
    use tokio::task;
    use tokio::task::JoinHandle;
    use tokio::time::{sleep, timeout};
    use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite, LengthDelimitedCodec};
    use yrs::sync::{Awareness, Error, Message, SyncMessage};
    use yrs::updates::encoder::Encode;
    use parking_lot::RwLock;
    use yrs::{Doc, GetString, Text, Transact};

    #[derive(Debug, Default)]
    struct YrsCodec(LengthDelimitedCodec);

    impl Encoder<Vec<u8>> for YrsCodec {
        type Error = Error;

        fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
            self.0.encode(Bytes::from(item), dst)?;
            Ok(())
        }
    }

    impl Decoder for YrsCodec {
        type Item = Vec<u8>;
        type Error = Error;

        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if let Some(bytes) = self.0.decode(src)? {
                Ok(Some(bytes.freeze().to_vec()))
            } else {
                Ok(None)
            }
        }
    }

    type WrappedStream = FramedRead<OwnedReadHalf, YrsCodec>;
    type WrappedSink = FramedWrite<OwnedWriteHalf, YrsCodec>;

    async fn start_server(
        addr: SocketAddr,
        bcast: BroadcastGroup,
    ) -> Result<JoinHandle<()>, Box<dyn std::error::Error>> {
        let server = TcpListener::bind(addr).await?;
        Ok(tokio::spawn(async move {
            let mut subscribers = Vec::new();
            while let Ok((stream, _)) = server.accept().await {
                let (reader, writer) = stream.into_split();
                let stream = WrappedStream::new(reader, YrsCodec::default());
                let sink = WrappedSink::new(writer, YrsCodec::default());
                let sub = bcast.subscribe(Arc::new(Mutex::new(sink)), stream);
                subscribers.push(sub);
            }
        }))
    }

    async fn client(
        addr: SocketAddr,
        doc: Doc,
    ) -> Result<Connection<WrappedSink, WrappedStream>, Box<dyn std::error::Error>> {
        let stream = TcpSocket::new_v4()?.connect(addr).await?;
        let (reader, writer) = stream.into_split();
        let stream: WrappedStream = WrappedStream::new(reader, YrsCodec::default());
        let sink: WrappedSink = WrappedSink::new(writer, YrsCodec::default());
        Ok(Connection::new(
            Arc::new(RwLock::new(Awareness::new(doc))),
            sink,
            stream,
        ))
    }

    fn create_notifier(doc: &Doc) -> Arc<Notify> {
        let n = Arc::new(Notify::new());
        let notify = n.clone();
        doc.observe_update_v1("notify", move |_, _| notify.notify_waiters())
            .unwrap();
        n
    }

    const TIMEOUT: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn change_introduced_by_server_reaches_subscribed_clients(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = SocketAddr::from_str("127.0.0.1:6600").unwrap();
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");
        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let _server = start_server(server_addr.clone(), bcast).await?;

        let doc = Doc::new();
        let n = create_notifier(&doc);
        let c1 = client(server_addr.clone(), doc).await?;

        {
            text.push(&mut awareness.read().doc().transact_mut(), "abc");
        }

        timeout(TIMEOUT, n.notified()).await?;

        {
            let doc = c1.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abc".to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn subscribed_client_fetches_initial_state() -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = SocketAddr::from_str("127.0.0.1:6601").unwrap();
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");

        text.push(&mut doc.transact_mut(), "abc");

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let _server = start_server(server_addr.clone(), bcast).await?;

        let doc = Doc::new();
        let n = create_notifier(&doc);
        let c1 = client(server_addr.clone(), doc).await?;

        timeout(TIMEOUT, n.notified()).await?;

        {
            let doc = c1.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abc".to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn changes_from_one_client_reach_others() -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = SocketAddr::from_str("127.0.0.1:6602").unwrap();
        let doc = Doc::with_client_id(1);
        let _text = doc.get_or_insert_text("test");

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let _server = start_server(server_addr.clone(), bcast).await?;

        let d1 = Doc::with_client_id(2);
        let c1 = client(server_addr.clone(), d1).await?;
        // by default changes made by document on the client side are not propagated automatically
        {
            let sink = c1.sink();
            let doc = c1.doc();
            doc.observe_update_v1("forward", move |_, e| {
                let update = e.update.to_owned();
                if let Some(sink) = sink.upgrade() {
                    task::spawn(async move {
                        let msg = Message::Sync(SyncMessage::Update(update)).encode_v1();
                        let mut sink = sink.lock().await;
                        sink.send(msg).await.unwrap();
                    });
                }
            })
            .unwrap();
        }

        let d2 = Doc::with_client_id(3);
        let n2 = create_notifier(&d2);
        let c2 = client(server_addr.clone(), d2).await?;

        {
            let doc = c1.doc();
            let text = doc.get_or_insert_text("test");
            text.push(&mut doc.transact_mut(), "def");
        }

        timeout(TIMEOUT, n2.notified()).await?;

        {
            let doc = c2.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "def".to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn client_failure_doesnt_affect_others() -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = SocketAddr::from_str("127.0.0.1:6604").unwrap();
        let doc = Doc::with_client_id(1);
        let _ = doc.get_or_insert_text("test");

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let _server = start_server(server_addr.clone(), bcast).await?;

        let d1 = Doc::with_client_id(2);
        let c1 = client(server_addr.clone(), d1).await?;
        // by default changes made by document on the client side are not propagated automatically
        {
            let sink = c1.sink();
            let doc = c1.doc();
            doc.observe_update_v1("forward", move |_, e| {
                let update = e.update.to_owned();
                if let Some(sink) = sink.upgrade() {
                    task::spawn(async move {
                        let msg = Message::Sync(SyncMessage::Update(update)).encode_v1();
                        let mut sink = sink.lock().await;
                        sink.send(msg).await.unwrap();
                    });
                }
            })
            .unwrap();
        }

        let d2 = Doc::with_client_id(3);
        let n2 = create_notifier(&d2);
        let c2 = client(server_addr.clone(), d2).await?;

        let d3 = Doc::with_client_id(4);
        let n3 = create_notifier(&d3);
        let c3 = client(server_addr.clone(), d3).await?;

        {
            let doc = c1.doc();
            let text = doc.get_or_insert_text("test");
            text.push(&mut doc.transact_mut(), "abc");
        }

        // on the first try both C2 and C3 should receive the update
        sleep(TIMEOUT).await;

        {
            let doc = c2.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abc".to_string());
        }
        {
            let doc = c3.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abc".to_string());
        }

        // drop client, causing abrupt ending
        drop(c3);
        drop(n3);
        // C2 notification subscription has been realized, we need to refresh it
        drop(n2);

        let n2 = {
            let doc = c2.doc();
            create_notifier(doc)
        };

        {
            let doc = c1.doc();
            let text = doc.get_or_insert_text("test");
            text.push(&mut doc.transact_mut(), "def");
        }

        timeout(TIMEOUT, n2.notified()).await.unwrap();

        {
            let doc = c2.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abcdef".to_string());
        }

        Ok(())
    }
}
