//! `Queues` selectors, answered from the bound storage's queue port
//! ([`ScopedStorage::queues`](tinystoragedrivers_core::ScopedStorage::queues)).
use serde_json::Value;
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::{
    MAX_BATCH_OPS, MAX_INLINE_BYTES, MAX_PAGE_ITEMS, MAX_QUEUE_MESSAGE_BYTES,
};

use super::{Reply, StorageServer, decode, encode, invalid, native};
use crate::convert::queues::{
    dead_letter_to_wire, delivery_to_wire, features_to_wire, message_from_wire, receipt_from_wire,
    retry_from_wire, spec_from_wire, stats_to_wire,
};

/// Framing one delivery adds around its payload (receipt, times, keys).
const DELIVERY_FRAMING: usize = 512;

fn fits(payload: &Value) -> bool {
    payload.to_string().len() <= MAX_QUEUE_MESSAGE_BYTES
}

fn encoded_len(delivery: &wire::QueueDelivery) -> usize {
    delivery.payload.to_string().len()
        + delivery.group.as_ref().map_or(0, String::len)
        + delivery.receipt.id.len()
        + delivery.receipt.token.len()
        + DELIVERY_FRAMING
}

impl StorageServer {
    /// Admit a request naming `queue`: always without a manifest; with one,
    /// only a declared queue, once the manifest has been applied.
    async fn admit_queue(&self, queue: &str) -> Result<(), wire::StorageFailure> {
        let Some(manifest) = &self.queue_manifest else {
            return Ok(());
        };
        self.queues_declared
            .get_or_try_init(|| async {
                native(manifest.apply(self.storage.queues().as_ref()).await)
            })
            .await?;
        if manifest.get(queue).is_none() {
            return Err(wire::StorageFailure {
                kind: wire::StorageKind::NotFound,
            });
        }
        Ok(())
    }

    pub(super) async fn queues(&self, selector: &str, request: Value) -> Reply {
        let queues = self.storage.queues();
        match selector {
            "Features" => encode(&features_to_wire(queues.features())),
            "EnsureQueue" => self.ensure_queue(decode(request)?).await,
            "Enqueue" => self.enqueue(decode(request)?).await,
            "Receive" => {
                let r: wire::ReceiveRequest = decode(request)?;
                self.admit_queue(&r.queue).await?;
                self.receive(&r).await
            }
            "Ack" => {
                let r: wire::ReceiptRequest = decode(request)?;
                self.admit_queue(&r.queue).await?;
                encode(&native(
                    queues.ack(&r.queue, &receipt_from_wire(r.receipt)).await,
                )?)
            }
            "Nack" => {
                let r: wire::NackRequest = decode(request)?;
                self.admit_queue(&r.queue).await?;
                let receipt = receipt_from_wire(r.receipt);
                let retry = retry_from_wire(r.retry);
                encode(&native(queues.nack(&r.queue, &receipt, retry).await)?)
            }
            "Extend" => {
                let r: wire::ExtendRequest = decode(request)?;
                self.admit_queue(&r.queue).await?;
                let receipt = receipt_from_wire(r.receipt);
                encode(&native(
                    queues.extend(&r.queue, &receipt, r.visibility_ms).await,
                )?)
            }
            "Stats" => {
                let r: wire::QueueRequest = decode(request)?;
                self.admit_queue(&r.queue).await?;
                encode(&stats_to_wire(native(queues.stats(&r.queue).await)?))
            }
            "DeadLetters" => self.dead_letters(decode(request)?).await,
            "Redrive" => {
                let r: wire::RedriveRequest = decode(request)?;
                self.admit_queue(&r.queue).await?;
                encode(&native(queues.redrive(&r.queue, &r.id).await)?)
            }
            "Purge" => {
                let r: wire::QueueRequest = decode(request)?;
                self.admit_queue(&r.queue).await?;
                encode(&native(queues.purge(&r.queue).await)?)
            }
            _ => Err(invalid()),
        }
    }

    /// With a manifest the host declared the queue already; a caller may only
    /// agree with the declared spec.
    async fn ensure_queue(&self, r: wire::EnsureQueueRequest) -> Reply {
        let spec = spec_from_wire(r.spec);
        if let Some(manifest) = &self.queue_manifest {
            self.admit_queue(&r.queue).await?;
            return match manifest.get(&r.queue) {
                Some(declared) if declared.spec() == spec => encode(&()),
                _ => Err(wire::StorageFailure {
                    kind: wire::StorageKind::Conflict,
                }),
            };
        }
        native(self.storage.queues().ensure_queue(&r.queue, &spec).await)?;
        encode(&())
    }

    async fn enqueue(&self, r: wire::EnqueueRequest) -> Reply {
        self.admit_queue(&r.queue).await?;
        if r.messages.len() > MAX_BATCH_OPS
            || r.messages.iter().any(|message| !fits(&message.payload))
        {
            return Err(invalid());
        }
        let messages: Vec<_> = r.messages.into_iter().map(message_from_wire).collect();
        let enqueued = native(self.storage.queues().enqueue(&r.queue, &messages).await)?;
        encode(
            &enqueued
                .into_iter()
                .map(|e| wire::EnqueuedMessage {
                    id: e.id,
                    duplicate: e.duplicate,
                })
                .collect::<Vec<_>>(),
        )
    }

    async fn dead_letters(&self, r: wire::DeadLettersRequest) -> Reply {
        self.admit_queue(&r.queue).await?;
        let limit = r.limit.min(MAX_PAGE_ITEMS);
        let dead = native(self.storage.queues().dead_letters(&r.queue, limit).await)?;
        let mut size = 0usize;
        let mut items = Vec::with_capacity(dead.len());
        for letter in dead {
            let letter = dead_letter_to_wire(letter);
            size = size.saturating_add(
                letter.payload.to_string().len() + letter.reason.len() + DELIVERY_FRAMING,
            );
            if !items.is_empty() && size > MAX_INLINE_BYTES {
                break;
            }
            items.push(letter);
        }
        encode(&items)
    }

    /// Lease in rounds sized so that one more maximal message always fits the
    /// inline byte cap: a lease is never taken that the reply cannot carry.
    /// A message larger than [`MAX_QUEUE_MESSAGE_BYTES`] (enqueued natively,
    /// not over the bus) can still overshoot; the transport then refuses the
    /// frame and the lease lapses back to the queue.
    async fn receive(&self, r: &wire::ReceiveRequest) -> Reply {
        let queues = self.storage.queues();
        let want = usize::try_from(r.max)
            .unwrap_or(usize::MAX)
            .min(MAX_PAGE_ITEMS);
        if want == 0 || r.visibility_ms == 0 {
            // Let the port name the invalid input; nothing is leased.
            native(
                queues
                    .receive(&r.queue, r.max, r.visibility_ms, &r.consumer)
                    .await,
            )?;
            return encode(&Vec::<wire::QueueDelivery>::new());
        }
        let mut out: Vec<wire::QueueDelivery> = Vec::new();
        let mut size = 0usize;
        while out.len() < want {
            let room = MAX_INLINE_BYTES.saturating_sub(size)
                / (MAX_QUEUE_MESSAGE_BYTES + DELIVERY_FRAMING);
            let round = room.min(want - out.len());
            if round == 0 {
                break;
            }
            let round_max = u32::try_from(round).unwrap_or(u32::MAX);
            let batch = native(
                queues
                    .receive(&r.queue, round_max, r.visibility_ms, &r.consumer)
                    .await,
            )?;
            let short = batch.len() < round;
            for delivery in batch {
                let delivery = delivery_to_wire(delivery);
                size = size.saturating_add(encoded_len(&delivery));
                out.push(delivery);
            }
            if short {
                break;
            }
        }
        encode(&out)
    }
}

#[cfg(test)]
#[path = "queues_tests.rs"]
mod tests;
