//! `QueueStore` over the `Queues` object.
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::{MAX_BATCH_OPS, MAX_QUEUE_MESSAGE_BYTES};
use tinystoragedrivers_core::queue::{
    DeadLetter, Delivery, Enqueued, NewMessage, QueueFeatures, QueueSpec, QueueStats, QueueStore,
    Receipt, Retry,
};
use tinystoragedrivers_core::{Result, StorageError, async_trait};

use super::Wire;
use crate::convert::queues::{
    dead_letter_from_wire, delivery_from_wire, message_to_wire, receipt_to_wire, retry_to_wire,
};
use crate::transport::Object;

/// The queue port over a bus. Leases, delays and dedupe windows run on the
/// host's clock; the reported [`QueueFeatures`] are the host queue's.
#[derive(Clone, Debug)]
pub struct BusQueues {
    wire: Wire,
    features: QueueFeatures,
}

impl BusQueues {
    pub(super) fn new(wire: Wire, features: QueueFeatures) -> Self {
        Self { wire, features }
    }

    async fn call<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        selector: &str,
        request: &Req,
    ) -> Result<Resp> {
        self.wire
            .call(Object::Queues, selector, request, None)
            .await
    }
}

fn receipt_request(queue: &str, receipt: &Receipt) -> wire::ReceiptRequest {
    wire::ReceiptRequest {
        queue: queue.to_owned(),
        receipt: receipt_to_wire(receipt),
    }
}

#[async_trait]
impl QueueStore for BusQueues {
    fn features(&self) -> QueueFeatures {
        self.features
    }

    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()> {
        let request = wire::EnsureQueueRequest {
            queue: queue.to_owned(),
            spec: wire::QueueSpec {
                max_attempts: spec.max_attempts,
                dedupe_window_ms: spec.dedupe_window_ms,
            },
        };
        self.call("EnsureQueue", &request).await
    }

    /// Batches above [`MAX_BATCH_OPS`] go out as several calls; messages are
    /// stored independently either way.
    async fn enqueue(&self, queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>> {
        let messages: Vec<_> = messages.iter().map(message_to_wire).collect();
        if messages
            .iter()
            .any(|message| message.payload.to_string().len() > MAX_QUEUE_MESSAGE_BYTES)
        {
            return Err(StorageError::invalid_input(
                "a queue message payload over the bus is limited to 256 KiB",
            ));
        }
        let mut out = Vec::with_capacity(messages.len());
        for chunk in messages.chunks(MAX_BATCH_OPS) {
            let request = wire::EnqueueRequest {
                queue: queue.to_owned(),
                messages: chunk.to_vec(),
            };
            let reply: wire::EnqueueResponse = self.call("Enqueue", &request).await?;
            out.extend(reply.into_iter().map(|enqueued| Enqueued {
                id: enqueued.id,
                duplicate: enqueued.duplicate,
            }));
        }
        Ok(out)
    }

    /// One call; the host may return fewer than `max` when its reply cap is
    /// reached, which is allowed by the port ("up to `max`").
    async fn receive(
        &self,
        queue: &str,
        max: u32,
        visibility_ms: u64,
        consumer: &str,
    ) -> Result<Vec<Delivery>> {
        let request = wire::ReceiveRequest {
            queue: queue.to_owned(),
            max,
            visibility_ms,
            consumer: consumer.to_owned(),
        };
        let reply: wire::ReceiveResponse = self.call("Receive", &request).await?;
        Ok(reply.into_iter().map(delivery_from_wire).collect())
    }

    async fn ack(&self, queue: &str, receipt: &Receipt) -> Result<bool> {
        self.call("Ack", &receipt_request(queue, receipt)).await
    }

    async fn nack(&self, queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool> {
        let request = wire::NackRequest {
            queue: queue.to_owned(),
            receipt: receipt_to_wire(receipt),
            retry: retry_to_wire(retry),
        };
        self.call("Nack", &request).await
    }

    async fn extend(&self, queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool> {
        let request = wire::ExtendRequest {
            queue: queue.to_owned(),
            receipt: receipt_to_wire(receipt),
            visibility_ms,
        };
        self.call("Extend", &request).await
    }

    async fn stats(&self, queue: &str) -> Result<QueueStats> {
        let request = wire::QueueRequest {
            queue: queue.to_owned(),
        };
        let stats: wire::QueueStatsReply = self.call("Stats", &request).await?;
        Ok(QueueStats {
            ready: stats.ready,
            delayed: stats.delayed,
            leased: stats.leased,
            dead: stats.dead,
        })
    }

    /// One call; the host clamps `limit` to its page and byte caps.
    async fn dead_letters(&self, queue: &str, limit: usize) -> Result<Vec<DeadLetter>> {
        let request = wire::DeadLettersRequest {
            queue: queue.to_owned(),
            limit,
        };
        let reply: wire::DeadLettersResponse = self.call("DeadLetters", &request).await?;
        Ok(reply.into_iter().map(dead_letter_from_wire).collect())
    }

    async fn redrive(&self, queue: &str, id: &str) -> Result<bool> {
        let request = wire::RedriveRequest {
            queue: queue.to_owned(),
            id: id.to_owned(),
        };
        self.call("Redrive", &request).await
    }

    async fn purge(&self, queue: &str) -> Result<u64> {
        let request = wire::QueueRequest {
            queue: queue.to_owned(),
        };
        self.call("Purge", &request).await
    }
}
