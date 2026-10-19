//! Broker queue names: scope prefix, dead and delay queues, delay buckets.
//!
//! A scope becomes `tsd/<h>/`, `<h>` the FNV-1a 64 hash of the scope string in
//! 16 hex digits, so a scope of any length fits AMQP's 255-byte names and two
//! scopes never share a prefix by accident. `/` cannot occur in a
//! [`validate_queue_name`](tinystoragedrivers_core::queue::validate_queue_name)
//! name, so no queue collides with another's dead or delay queue.

use tinystoragedrivers_core::{Result, Scope, StorageError};

/// Shortest delay bucket, as a power of two: 2^8 = 256 ms.
pub(crate) const MIN_DELAY_SHIFT: u32 = 8;
/// Longest delay bucket, as a power of two: 2^28 ms, about 3.1 days.
pub(crate) const MAX_DELAY_SHIFT: u32 = 28;
/// The longest delay a message can be given, in milliseconds.
pub const MAX_DELAY_MS: u64 = 1 << MAX_DELAY_SHIFT;

/// FNV-1a 64: stable across processes, platforms and releases, which a
/// `std` hasher is not.
fn fnv1a(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// The name prefix of every queue of `scope`.
pub(crate) fn prefix(scope: &Scope) -> String {
    format!("tsd/{:016x}/", fnv1a(scope.as_str()))
}

/// The broker queues behind one logical queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Names {
    /// Deliverable messages.
    pub(crate) main: String,
    /// Dead letters.
    pub(crate) dead: String,
}

impl Names {
    pub(crate) fn new(prefix: &str, queue: &str) -> Self {
        let main = format!("{prefix}{queue}");
        let dead = format!("{main}/dead");
        Self { main, dead }
    }

    /// The wait queue that holds messages for `bucket_ms`.
    pub(crate) fn delay(&self, bucket_ms: u64) -> String {
        format!("{}/delay/{bucket_ms}", self.main)
    }

    /// Every wait queue, shortest first.
    pub(crate) fn delays(&self) -> impl Iterator<Item = (u64, String)> + '_ {
        buckets().map(|bucket| (bucket, self.delay(bucket)))
    }
}

/// Every delay bucket in milliseconds, shortest first.
pub(crate) fn buckets() -> impl Iterator<Item = u64> {
    (MIN_DELAY_SHIFT..=MAX_DELAY_SHIFT).map(|shift| 1u64 << shift)
}

/// The bucket a delay of `ms` waits in: the smallest power of two at least
/// `ms`, and at least 256 ms. `None` for no delay.
///
/// # Errors
///
/// `InvalidInput` for a delay longer than [`MAX_DELAY_MS`].
pub(crate) fn bucket(ms: u64) -> Result<Option<u64>> {
    if ms == 0 {
        return Ok(None);
    }
    if ms > MAX_DELAY_MS {
        return Err(StorageError::invalid_input(format!(
            "a rabbitmq delay is at most {MAX_DELAY_MS} ms"
        )));
    }
    Ok(Some(ms.next_power_of_two().max(1 << MIN_DELAY_SHIFT)))
}

#[cfg(test)]
#[path = "names_tests.rs"]
mod tests;
