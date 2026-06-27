use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use engineioxide::socket::Socket;
use futures_util::Stream;
use kanal::AsyncReceiver;
use yrs::sync::Error;

pub struct EngineSink<D>(pub Arc<Socket<D>>)
where
    D: Default + Send + Sync + 'static;

impl<D> futures_util::Sink<Vec<u8>> for EngineSink<D>
where
    D: Default + Send + Sync + 'static,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        self.0.emit_binary(Bytes::from(item)).map_err(|e| Error::Other(Box::new(e)))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

type BoxedStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send>>;

pub struct EngineStream {
    inner: BoxedStream,
}

impl EngineStream {
    pub fn new(rx: AsyncReceiver<Vec<u8>>) -> Self {
        let inner = futures_util::stream::unfold(rx, |rx| async move {
            match rx.recv().await {
                Ok(item) => Some((Ok(item), rx)),
                Err(_) => None,
            }
        });
        Self { inner: Box::pin(inner) }
    }
}

impl Stream for EngineStream {
    type Item = Result<Vec<u8>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
