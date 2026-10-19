//! Queue types between their native and wire shapes. The two differ only in
//! that the wire carries payloads as inline JSON text and spells `Retry` as a
//! tagged object, so every conversion is total.
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_core::queue::{
    DeadLetter, Delivery, NewMessage, QueueFeatures, QueueSpec, QueueStats, Receipt, Retry,
};

pub(crate) fn message_to_wire(message: &NewMessage) -> wire::QueueMessage {
    wire::QueueMessage {
        payload: message.payload.clone(),
        priority: message.priority,
        delay_ms: message.delay_ms,
        group: message.group.clone(),
        dedupe_key: message.dedupe_key.clone(),
    }
}

pub(crate) fn message_from_wire(message: wire::QueueMessage) -> NewMessage {
    NewMessage {
        payload: message.payload,
        priority: message.priority,
        delay_ms: message.delay_ms,
        group: message.group,
        dedupe_key: message.dedupe_key,
    }
}

pub(crate) fn receipt_to_wire(receipt: &Receipt) -> wire::QueueReceipt {
    wire::QueueReceipt {
        id: receipt.id.clone(),
        token: receipt.token.clone(),
    }
}

pub(crate) fn receipt_from_wire(receipt: wire::QueueReceipt) -> Receipt {
    Receipt {
        id: receipt.id,
        token: receipt.token,
    }
}

pub(crate) fn retry_to_wire(retry: Retry) -> wire::QueueRetry {
    match retry {
        Retry::After(delay_ms) => wire::QueueRetry::After { delay_ms },
        Retry::DeadLetter(reason) => wire::QueueRetry::DeadLetter { reason },
    }
}

pub(crate) fn retry_from_wire(retry: wire::QueueRetry) -> Retry {
    match retry {
        wire::QueueRetry::After { delay_ms } => Retry::After(delay_ms),
        wire::QueueRetry::DeadLetter { reason } => Retry::DeadLetter(reason),
    }
}

pub(crate) fn delivery_to_wire(delivery: Delivery) -> wire::QueueDelivery {
    wire::QueueDelivery {
        receipt: receipt_to_wire(&delivery.receipt),
        payload: delivery.payload,
        attempt: delivery.attempt,
        priority: delivery.priority,
        group: delivery.group,
        enqueued_ms: delivery.enqueued_ms,
        lease_until_ms: delivery.lease_until_ms,
    }
}

pub(crate) fn delivery_from_wire(delivery: wire::QueueDelivery) -> Delivery {
    Delivery {
        receipt: receipt_from_wire(delivery.receipt),
        payload: delivery.payload,
        attempt: delivery.attempt,
        priority: delivery.priority,
        group: delivery.group,
        enqueued_ms: delivery.enqueued_ms,
        lease_until_ms: delivery.lease_until_ms,
    }
}

pub(crate) fn dead_letter_to_wire(dead: DeadLetter) -> wire::QueueDeadLetter {
    wire::QueueDeadLetter {
        id: dead.id,
        payload: dead.payload,
        attempt: dead.attempt,
        reason: dead.reason,
        group: dead.group,
        enqueued_ms: dead.enqueued_ms,
    }
}

pub(crate) fn dead_letter_from_wire(dead: wire::QueueDeadLetter) -> DeadLetter {
    DeadLetter {
        id: dead.id,
        payload: dead.payload,
        attempt: dead.attempt,
        reason: dead.reason,
        group: dead.group,
        enqueued_ms: dead.enqueued_ms,
    }
}

pub(crate) fn features_to_wire(features: QueueFeatures) -> wire::QueueFeaturesReply {
    wire::QueueFeaturesReply {
        dedupe: features.dedupe,
        groups: features.groups,
        priorities: features.priorities,
        delay: features.delay,
    }
}

pub(crate) fn features_from_wire(features: wire::QueueFeaturesReply) -> QueueFeatures {
    QueueFeatures {
        dedupe: features.dedupe,
        groups: features.groups,
        priorities: features.priorities,
        delay: features.delay,
    }
}

pub(crate) fn spec_from_wire(spec: wire::QueueSpec) -> QueueSpec {
    QueueSpec {
        max_attempts: spec.max_attempts,
        dedupe_window_ms: spec.dedupe_window_ms,
    }
}

pub(crate) fn stats_to_wire(stats: QueueStats) -> wire::QueueStatsReply {
    wire::QueueStatsReply {
        ready: stats.ready,
        delayed: stats.delayed,
        leased: stats.leased,
        dead: stats.dead,
    }
}
