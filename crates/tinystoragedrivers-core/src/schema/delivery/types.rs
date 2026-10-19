//! Runtime-only ports. Trusted peer identities never come from a payload.
use super::{
    MigrationExchangeReply, MigrationExchangeRequest, MigrationPayload, MigrationPayloadReply,
};
use crate::schema::BoxFuture;

/// Confidential exchange future. Transport failures are sanitized by the client.
pub type TransformFuture<'a, T> = BoxFuture<'a, crate::Result<T>>;

/// Transport adapter supplied by the owner host.
///
/// **Every call must be confidential and excluded from framework monitoring**,
/// including Begin, Cancel, errors and all fragments. Naming a member does not
/// enforce this; the owning adapter must use its framework's confidential API.
pub trait MigrationTransport: Send + Sync {
    /// Send one confidential exchange message to its owner.
    fn exchange<'a>(
        &'a self,
        owner: &'a str,
        request: MigrationExchangeRequest,
    ) -> TransformFuture<'a, MigrationExchangeReply>;
}

/// Stateless owner converter; never binds storage or performs migration writes.
pub trait MigrationPayloadHandler: Send + Sync {
    /// Convert a complete payload. The server sanitizes all returned errors.
    fn transform(&self, payload: MigrationPayload) -> MigrationPayloadReply;
}

/// Identity obtained exclusively from trusted dispatch context, never serde.
#[derive(Clone, PartialEq, Eq)]
pub struct PeerIdentity(String);
impl PeerIdentity {
    /// Construct in a trusted adapter using the actual dispatch peer identity.
    #[must_use]
    pub fn new(identity: impl Into<String>) -> Self {
        Self(identity.into())
    }
}

/// Monotonic clock injected by the runtime owner.
pub trait ExchangeClock: Send + Sync {
    /// Milliseconds on an arbitrary monotonic epoch.
    fn now_ms(&self) -> u64;
}

/// Runtime token generator. IDs must be unpredictable, unique ASCII tokens.
pub trait TransferIds: Send + Sync {
    /// Return a fresh 1–64 byte alphanumeric, hyphen or underscore token.
    fn next_id(&self) -> String;
}

pub(super) fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

struct Buffer(Vec<u8>);
impl std::io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .try_reserve(bytes.len())
            .map_err(|_| std::io::ErrorKind::OutOfMemory)?;
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Fallible incremental JSON serialization, with no advertised-length reserve.
pub(super) fn encode(value: &impl serde::Serialize) -> Result<Vec<u8>, ()> {
    let mut buffer = Buffer(Vec::new());
    serde_json::to_writer(&mut buffer, value).map_err(|_| ())?;
    Ok(buffer.0)
}

impl std::fmt::Debug for PeerIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerIdentity(..)")
    }
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
