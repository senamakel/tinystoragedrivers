//! The seam between the client and a bus: one call shape, no `TinyBus` types.
use std::fmt;

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, BlobStreamHeadWithExpiry, BlobWithExpiry, PutBlobStreamRequest,
    PutBlobStreamUntilRequest, PutBlobUntilRequest, StorageFailure, StorageKind, WatchChanged,
};
use tinystoragedrivers_core::BlobReader;
use tokio::sync::broadcast;

/// Body of a streamed blob transfer: the bytes of a `TinyBus` stream.
pub type BlobBody = BlobReader;

/// The five objects of `ai.tinyhumans.tinystorage.Storage`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Object {
    /// Versioned JSON documents.
    Documents,
    /// Append-only logs.
    Streams,
    /// Opaque bytes by key.
    Blobs,
    /// Encrypted secrets; confidential route only.
    Secrets,
    /// Durable at-least-once message queues.
    Queues,
}

impl Object {
    /// The wire object name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Documents => tinystoragedrivers_bus::DOCUMENTS_OBJECT,
            Self::Streams => tinystoragedrivers_bus::STREAMS_OBJECT,
            Self::Blobs => tinystoragedrivers_bus::BLOBS_OBJECT,
            Self::Secrets => tinystoragedrivers_bus::SECRETS_OBJECT,
            Self::Queues => tinystoragedrivers_bus::QUEUES_OBJECT,
        }
    }

    /// Parse a wire object name; unknown names are refused.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        [
            Self::Documents,
            Self::Streams,
            Self::Blobs,
            Self::Secrets,
            Self::Queues,
        ]
        .into_iter()
        .find(|object| object.as_str() == name)
    }
}

/// How the server's host admitted the caller. `Secrets` requires
/// [`Admission::Confidential`]; the other objects accept either.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    /// An ordinary private-route caller.
    Private,
    /// An attested in-process caller allowed to reach `Secrets`.
    Confidential,
}

/// A call that did not produce a reply value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireError {
    /// The server answered with a sanitized storage failure.
    Storage(StorageFailure),
    /// The transport itself failed (closed, oversize frame, undecodable reply).
    Transport(String),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(failure) => write!(f, "storage failure: {:?}", failure.kind),
            Self::Transport(reason) => write!(f, "transport failure: {reason}"),
        }
    }
}

impl std::error::Error for WireError {}

/// Sends storage bus messages to a server bound to one caller.
///
/// The transport carries no scope: the server end is already bound to the
/// caller's scope, backend and grant by the host.
#[async_trait]
pub trait StorageTransport: Send + Sync + fmt::Debug {
    /// One request/reply on `object`'s `selector`. `request` and the reply are
    /// the typed bus messages as JSON values.
    ///
    /// # Errors
    /// A storage failure from the server or a transport failure.
    async fn call(
        &self,
        object: Object,
        selector: &str,
        request: Value,
    ) -> Result<Value, WireError>;

    /// `Blobs.PutStream`: metadata plus a body stream of exactly `head.len`
    /// bytes. Replies with the stored blob's metadata (a `BlobInfo` value).
    ///
    /// # Errors
    /// A storage failure from the server or a transport failure.
    async fn put_blob_stream(
        &self,
        head: PutBlobStreamRequest,
        body: BlobBody,
    ) -> Result<Value, WireError>;

    /// `Blobs.GetStream`: the blob's metadata and a body stream, or `None`.
    ///
    /// # Errors
    /// A storage failure from the server or a transport failure.
    async fn get_blob_stream(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobBody)>, WireError>;

    /// `Blobs.PutUntil`: store bytes with the exact absolute expiry, or no
    /// expiry when the field is `None`.
    ///
    /// The default keeps existing transports source-compatible and refuses
    /// this new selector without falling back to relative TTL or plain Put.
    async fn put_blob_until(&self, request: PutBlobUntilRequest) -> Result<Value, WireError> {
        let _ = request;
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unsupported,
        }))
    }

    /// `Blobs.PutStreamUntil`: store a stream with the exact absolute expiry.
    ///
    /// The default refuses without consuming or forwarding the body.
    async fn put_blob_stream_until(
        &self,
        head: PutBlobStreamUntilRequest,
        body: BlobBody,
    ) -> Result<Value, WireError> {
        let _ = (head, body);
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unsupported,
        }))
    }

    /// `Blobs.GetWithExpiry`: atomically return a small blob and its exact
    /// absolute expiry.
    async fn get_blob_with_expiry(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<BlobWithExpiry>, WireError> {
        let _ = request;
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unsupported,
        }))
    }

    /// `Blobs.GetStreamWithExpiry`: atomically return blob metadata, exact
    /// absolute expiry, and the body stream.
    async fn get_blob_stream_with_expiry(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHeadWithExpiry, BlobBody)>, WireError> {
        let _ = request;
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unsupported,
        }))
    }

    /// A receiver of `WatchChanged` signals for this caller binding. Subscribe
    /// before opening a watch so no signal is missed.
    fn signals(&self) -> broadcast::Receiver<WatchChanged>;
}
