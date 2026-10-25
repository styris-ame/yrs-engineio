use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The lock behind [AwarenessRef], re-exported so callers always build a matching one.
pub use parking_lot::RwLock;
use yrs::sync::{Awareness, DefaultProtocol, Error, Message, Protocol, SyncMessage};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::{Encoder, EncoderV1};
use yrs::{Doc, Origin, ReadTxn, Transact, Update};

pub mod broadcast;
pub mod conn;
pub mod engineio;

/// Shared [Awareness] handle. Since yrs 0.27 awareness mutation requires `&mut`, so it lives
/// behind a lock. Hold it briefly and never across an `.await`.
pub type AwarenessRef = Arc<RwLock<Awareness>>;

/// Returns a process-unique [Origin], used both as an observer key and as the origin of
/// changes applied on behalf of a connection.
pub(crate) fn unique_origin(kind: &str) -> Origin {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    Origin::from(format!("yrs-engineio/{kind}/{}", NEXT_ID.fetch_add(1, Ordering::Relaxed)))
}

/// Strategy for answering y-sync messages on behalf of a connection.
pub(crate) trait Handler: Send + Sync + 'static {
    /// Encodes the payload sent to a peer as soon as it connects.
    fn start(&self, awareness: &AwarenessRef) -> Result<Vec<u8>, Error>;

    /// Handles a message from the peer whose changes are tagged with `origin`.
    fn handle(&self, awareness: &AwarenessRef, doc: &Doc, origin: &Origin, msg: Message) -> Result<Option<Message>, Error>;
}

/// [DefaultProtocol] semantics, but document messages go straight to the [Doc] (which is
/// internally synchronized) so only awareness messages contend on the awareness lock.
pub(crate) struct Direct;

impl Handler for Direct {
    fn start(&self, awareness: &AwarenessRef) -> Result<Vec<u8>, Error> {
        let mut encoder = EncoderV1::new();
        DefaultProtocol.start(&awareness.read(), &mut encoder)?;
        Ok(encoder.to_vec())
    }

    fn handle(&self, awareness: &AwarenessRef, doc: &Doc, origin: &Origin, msg: Message) -> Result<Option<Message>, Error> {
        match msg {
            Message::Sync(SyncMessage::SyncStep1(sv)) => {
                Ok(Some(Message::Sync(SyncMessage::SyncStep2(doc.transact().encode_state_as_update_v1(&sv)))))
            }
            Message::Sync(SyncMessage::SyncStep2(update) | SyncMessage::Update(update)) => {
                let update = Update::decode_v1(&update)?;
                doc.transact_mut_with(origin.clone()).apply_update(update)?;
                Ok(None)
            }
            Message::Auth(Some(reason)) => Err(Error::PermissionDenied { reason }),
            Message::Auth(None) => Ok(None),
            Message::AwarenessQuery => Ok(Some(Message::Awareness(awareness.read().update()?))),
            Message::Awareness(update) => {
                awareness.write().apply_update_with(update, origin.clone())?;
                Ok(None)
            }
            Message::Custom(tag, _) => Err(Error::Unsupported(tag)),
        }
    }
}

/// Runs a custom [Protocol], which requires exclusive awareness access for every message.
/// [Protocol] handlers can't tag changes with an origin, so they are echoed back to their sender.
pub(crate) struct Locked<P>(pub P);

impl<P: Protocol + Send + Sync + 'static> Handler for Locked<P> {
    fn start(&self, awareness: &AwarenessRef) -> Result<Vec<u8>, Error> {
        let mut encoder = EncoderV1::new();
        self.0.start(&awareness.read(), &mut encoder)?;
        Ok(encoder.to_vec())
    }

    fn handle(&self, awareness: &AwarenessRef, _doc: &Doc, _origin: &Origin, msg: Message) -> Result<Option<Message>, Error> {
        self.0.handle_message(&mut awareness.write(), msg)
    }
}
