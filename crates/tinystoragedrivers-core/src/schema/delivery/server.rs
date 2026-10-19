//! Peer-bound transient assembly. Conversion never holds the state mutex.
use super::{
    ExchangeClock, ExchangeErrorKind, MIGRATION_CHUNK_BYTES, MigrationExchangeReply,
    MigrationExchangeRequest, MigrationPayload, MigrationPayloadHandler, MigrationPayloadReply,
    PeerIdentity, TransferIds, types,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinHandle};

/// Default simultaneous assembly/conversion/reply slots.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 16;
/// Receiving and ready buffers expire after sixty idle seconds.
pub const IDLE_TTL_MS: u64 = 60_000;

struct Transfer {
    peer: PeerIdentity,
    stage: Stage,
}
enum Stage {
    Receiving {
        expected: u64,
        bytes: Vec<u8>,
        touched: u64,
    },
    Executing {
        discard: bool,
    },
    Ready {
        bytes: Vec<u8>,
        next: usize,
        touched: u64,
    },
}
impl Stage {
    fn upload(
        &mut self,
        offset: u64,
        fragment: &[u8],
        now: u64,
        state: &State,
    ) -> MigrationExchangeReply {
        let Stage::Receiving {
            expected,
            bytes,
            touched,
        } = self
        else {
            return error(ExchangeErrorKind::WrongSequence);
        };
        if offset != bytes.len() as u64 {
            return error(ExchangeErrorKind::WrongSequence);
        }
        let Some(next) = offset.checked_add(fragment.len() as u64) else {
            return error(ExchangeErrorKind::InvalidRequest);
        };
        if fragment.is_empty() || fragment.len() > MIGRATION_CHUNK_BYTES || next > *expected {
            return error(ExchangeErrorKind::InvalidRequest);
        }
        if state.reserve(bytes, fragment.len()).is_err() {
            return error(ExchangeErrorKind::Unavailable);
        }
        bytes.extend_from_slice(fragment);
        *touched = now;
        MigrationExchangeReply::Uploaded { next_offset: next }
    }
    fn download(&mut self, offset: u64, now: u64) -> MigrationExchangeReply {
        let Stage::Ready {
            bytes,
            next,
            touched,
        } = self
        else {
            return error(ExchangeErrorKind::WrongSequence);
        };
        if offset != *next as u64 || *next == bytes.len() {
            return error(ExchangeErrorKind::WrongSequence);
        }
        let end = next
            .checked_add(MIGRATION_CHUNK_BYTES)
            .map_or(bytes.len(), |end| end.min(bytes.len()));
        let mut chunk = Vec::new();
        if chunk.try_reserve(end - *next).is_err() {
            return error(ExchangeErrorKind::Unavailable);
        }
        chunk.extend_from_slice(&bytes[*next..end]);
        *next = end;
        *touched = now;
        MigrationExchangeReply::Chunk {
            offset,
            bytes: chunk,
        }
    }
}
struct State {
    transfers: Mutex<HashMap<String, Transfer>>,
    clock: Arc<dyn ExchangeClock>,
    max: usize,
    reserve: fn(&mut Vec<u8>, usize) -> Result<(), ()>,
}
fn reserve_bytes(bytes: &mut Vec<u8>, additional: usize) -> Result<(), ()> {
    bytes.try_reserve(additional).map_err(|_| ())
}
impl State {
    fn reserve(&self, bytes: &mut Vec<u8>, additional: usize) -> Result<(), ()> {
        (self.reserve)(bytes, additional)
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Transfer>> {
        self.transfers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn expire(&self) {
        let now = self.clock.now_ms();
        self.lock().retain(|_, t| match &t.stage {
            Stage::Receiving { touched, .. } | Stage::Ready { touched, .. } => {
                now.saturating_sub(*touched) < IDLE_TTL_MS
            }
            Stage::Executing { .. } => true,
        });
    }
}

/// Owns transient exchange state, never migration storage or authorization.
///
/// Construct before module lazy initialization. Retain a [`CleanupGuard`] for
/// the module lifetime and call [`Self::disconnect`] with trusted peer context.
/// Cloning shares state; dropping the last server releases idle buffers.
#[derive(Clone)]
pub struct MigrationExchangeServer {
    state: Arc<State>,
    handler: Arc<dyn MigrationPayloadHandler>,
    ids: Arc<dyn TransferIds>,
}

/// Periodic expiry owner. Dropping it aborts the reaper; no strong state is
/// retained between ticks. Modules must retain this guard for their lifetime.
#[derive(Debug)]
pub struct CleanupGuard(JoinHandle<()>);
impl Drop for CleanupGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn error(kind: ExchangeErrorKind) -> MigrationExchangeReply {
    MigrationExchangeReply::Error { kind }
}

impl MigrationExchangeServer {
    /// Build a server with an explicit concurrency limit (normally 16).
    #[must_use]
    pub fn new(
        handler: Arc<dyn MigrationPayloadHandler>,
        clock: Arc<dyn ExchangeClock>,
        ids: Arc<dyn TransferIds>,
        max_in_flight: usize,
    ) -> Self {
        Self {
            state: Arc::new(State {
                transfers: Mutex::new(HashMap::new()),
                clock,
                max: max_in_flight,
                reserve: reserve_bytes,
            }),
            handler,
            ids,
        }
    }
    /// Start periodic idle expiry on the current runtime.
    ///
    /// # Errors
    /// Returns Unavailable outside a Tokio runtime or for a zero interval.
    pub fn cleanup_guard(&self, interval: Duration) -> crate::Result<CleanupGuard> {
        if interval.is_zero() {
            return Err(crate::StorageError::unavailable(
                "migration cleanup unavailable",
            ));
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| crate::StorageError::unavailable("migration cleanup unavailable"))?;
        let weak = Arc::downgrade(&self.state);
        Ok(CleanupGuard(runtime.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(state) = weak.upgrade() else {
                    break;
                };
                state.expire();
            }
        })))
    }
    /// Sweep idle assembly/reply buffers; executing converters never expire.
    pub fn expire(&self) {
        self.state.expire();
    }
    /// Discard a disconnected peer's buffers. Executing jobs retain admission
    /// until their synchronous converter actually returns.
    pub fn disconnect(&self, peer: &PeerIdentity) {
        self.state.lock().retain(|_, t| {
            if &t.peer != peer {
                return true;
            }
            if let Stage::Executing { discard } = &mut t.stage {
                *discard = true;
                true
            } else {
                false
            }
        });
    }
    fn begin(&self, peer: &PeerIdentity, request_len: u64) -> MigrationExchangeReply {
        let mut transfers = self.state.lock();
        if transfers.len() >= self.state.max {
            return error(ExchangeErrorKind::Busy);
        }
        for _ in 0..8 {
            let id = self.ids.next_id();
            if !types::valid_id(&id) || transfers.contains_key(&id) {
                continue;
            }
            if transfers.try_reserve(1).is_err() {
                return error(ExchangeErrorKind::Unavailable);
            }
            transfers.insert(
                id.clone(),
                Transfer {
                    peer: peer.clone(),
                    stage: Stage::Receiving {
                        expected: request_len,
                        bytes: Vec::new(),
                        touched: self.state.clock.now_ms(),
                    },
                },
            );
            return MigrationExchangeReply::Begun { transfer: id };
        }
        error(ExchangeErrorKind::Unavailable)
    }
    /// Process one message using the caller's trusted identity. All failures
    /// are static; no backend, input, serde, or panic details cross the wire.
    pub async fn handle(
        &self,
        peer: &PeerIdentity,
        request: MigrationExchangeRequest,
    ) -> MigrationExchangeReply {
        self.expire();
        if let MigrationExchangeRequest::Begin { request_len } = request {
            return self.begin(peer, request_len);
        }
        let id = match &request {
            MigrationExchangeRequest::Upload { transfer, .. }
            | MigrationExchangeRequest::Execute { transfer }
            | MigrationExchangeRequest::Download { transfer, .. }
            | MigrationExchangeRequest::Finish { transfer }
            | MigrationExchangeRequest::Cancel { transfer } => transfer.clone(),
            MigrationExchangeRequest::Begin { .. } => {
                return error(ExchangeErrorKind::InvalidRequest);
            }
        };
        if !types::valid_id(&id) {
            return error(ExchangeErrorKind::UnknownTransfer);
        }
        let (runtime, handler, bytes, mut finalizer, tx, completion) = {
            let mut transfers = self.state.lock();
            let Some(t) = transfers.get_mut(&id).filter(|t| &t.peer == peer) else {
                return error(ExchangeErrorKind::UnknownTransfer);
            };
            let now = self.state.clock.now_ms();
            match request {
                MigrationExchangeRequest::Cancel { .. } => {
                    if let Stage::Executing { discard } = &mut t.stage {
                        *discard = true;
                    } else {
                        transfers.remove(&id);
                    }
                    return MigrationExchangeReply::Cancelled;
                }
                MigrationExchangeRequest::Upload {
                    offset,
                    bytes: fragment,
                    ..
                } => {
                    return t.stage.upload(offset, &fragment, now, &self.state);
                }
                MigrationExchangeRequest::Download { offset, .. } => {
                    return t.stage.download(offset, now);
                }
                MigrationExchangeRequest::Finish { .. } => {
                    let Stage::Ready { bytes, next, .. } = &t.stage else {
                        return error(ExchangeErrorKind::WrongSequence);
                    };
                    if *next != bytes.len() {
                        return error(ExchangeErrorKind::Incomplete);
                    }
                    transfers.remove(&id);
                    return MigrationExchangeReply::Finished;
                }
                MigrationExchangeRequest::Execute { .. } => {
                    let Stage::Receiving {
                        expected, bytes, ..
                    } = &t.stage
                    else {
                        return error(ExchangeErrorKind::WrongSequence);
                    };
                    if bytes.len() as u64 != *expected {
                        return error(ExchangeErrorKind::Incomplete);
                    }
                    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                        return error(ExchangeErrorKind::Unavailable);
                    };
                    let Stage::Receiving { bytes, .. } =
                        std::mem::replace(&mut t.stage, Stage::Executing { discard: false })
                    else {
                        return error(ExchangeErrorKind::WrongSequence);
                    };
                    let state = Arc::downgrade(&self.state);
                    let handler = Arc::clone(&self.handler);
                    let (tx, rx) = oneshot::channel();
                    // Completion belongs to the server job, not the RPC waiter.
                    // Dropping the waiter cannot strand an Executing admission slot.
                    let finalizer = ExecutionFinalizer {
                        state,
                        id,
                        active: true,
                    };
                    (runtime, handler, bytes, finalizer, tx, rx)
                }
                MigrationExchangeRequest::Begin { .. } => {
                    return error(ExchangeErrorKind::InvalidRequest);
                }
            }
        };
        // Submission to a stopped runtime drops the closure synchronously.
        // Release the transfers lock first: its finalizer also locks the state.
        runtime.spawn_blocking(move || {
            run_conversion(&mut finalizer, handler.as_ref(), &bytes, tx);
        });
        completion
            .await
            .unwrap_or_else(|_| error(ExchangeErrorKind::Unavailable))
    }
}

// A queued blocking job can be dropped by runtime shutdown before it runs.
// Its admission must be released in that case too. This guard also covers any
// unwinding outside the converter's catch_unwind boundary.
struct ExecutionFinalizer {
    state: Weak<State>,
    id: String,
    active: bool,
}
impl Drop for ExecutionFinalizer {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Some(state) = self.state.upgrade() {
            let mut transfers = state.lock();
            if matches!(
                transfers.get(&self.id).map(|t| &t.stage),
                Some(Stage::Executing { .. })
            ) {
                transfers.remove(&self.id);
            }
        }
    }
}
fn run_conversion(
    finalizer: &mut ExecutionFinalizer,
    handler: &dyn MigrationPayloadHandler,
    bytes: &[u8],
    tx: oneshot::Sender<MigrationExchangeReply>,
) {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let payload = serde_json::from_slice::<MigrationPayload>(bytes)
            .map_err(|_| ExchangeErrorKind::InvalidPayload)?;
        let reply = handler.transform(payload);
        // Never serialize owner-controlled diagnostics, including serde errors.
        let failed = match &reply {
            MigrationPayloadReply::Documents(r) => r.is_err(),
            MigrationPayloadReply::BlobTarget(r) => r.is_err(),
            MigrationPayloadReply::StreamPlan(r) => r.is_err(),
            MigrationPayloadReply::StreamEntries(r) => r.is_err(),
        };
        if failed {
            return Err(ExchangeErrorKind::TransformFailed);
        }
        types::encode(&reply).map_err(|()| ExchangeErrorKind::Unavailable)
    }))
    .unwrap_or(Err(ExchangeErrorKind::TransformFailed));
    let Some(state) = finalizer.state.upgrade() else {
        return;
    };
    let id = &finalizer.id;
    let mut transfers = state.lock();
    let discard = !matches!(
        transfers.get(id).map(|t| &t.stage),
        Some(Stage::Executing { discard: false })
    );
    if discard {
        transfers.remove(id);
        finalizer.active = false;
        let _ = tx.send(MigrationExchangeReply::Cancelled);
        return;
    }
    match outcome {
        Ok(bytes) => {
            let reply_len = bytes.len() as u64;
            if let Some(t) = transfers.get_mut(id) {
                t.stage = Stage::Ready {
                    bytes,
                    next: 0,
                    touched: state.clock.now_ms(),
                };
            }
            let _ = tx.send(MigrationExchangeReply::Ready { reply_len });
        }
        Err(kind) => {
            transfers.remove(id);
            let _ = tx.send(error(kind));
        }
    }
    // Disarm while still holding the completion lock, so token reuse cannot
    // make a later destructor remove an unrelated executing transfer.
    finalizer.active = false;
}

impl std::fmt::Debug for MigrationExchangeServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationExchangeServer")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
