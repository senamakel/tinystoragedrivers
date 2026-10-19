//! Confidential fragmented migration delivery, independent of any bus.
//!
//! Adapters negotiate protocol version 1 before accepting messages. Every
//! transport exchange must be confidential. Frames contain at most 128 KiB raw
//! bytes. This bounds frames, **not memory**: complete JSON requests/replies and
//! mapped streams are assembled, exactly as in the original migration runner.
//! See the module README for lifecycle and owner-adapter requirements.
//! This layer performs no owner authorization, storage binding or writes.
mod client;
mod server;
mod types;
pub use client::MigrationDeliveryClient;
pub use server::{CleanupGuard, DEFAULT_MAX_IN_FLIGHT, IDLE_TTL_MS, MigrationExchangeServer};
pub use tinystoragedrivers_schema::{
    BlobTargetRequest, ExchangeErrorKind, MIGRATION_CHUNK_BYTES, MIGRATION_EXCHANGE_VERSION,
    MigrationExchangeReply, MigrationExchangeRequest, MigrationPayload, MigrationPayloadReply,
    StreamEntriesRequest, StreamMapMode, StreamPlan, StreamPlanRequest,
};
pub use types::{
    ExchangeClock, MigrationPayloadHandler, MigrationTransport, PeerIdentity, TransferIds,
    TransformFuture,
};
#[cfg(test)]
#[path = "client_tests.rs"]
mod client_tests;

#[cfg(test)]
#[path = "parity_tests.rs"]
mod parity_tests;
