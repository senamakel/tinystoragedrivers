//! Queue checks that need no control over time: what any [`QueueStore`]
//! honors, whoever owns its clock (a bus host, a broker).
//!
//! Leases are taken for a minute, so none lapses while the checks run, and
//! nothing waits on a delay. Optional semantics are checked only when the
//! queue reports them in [`QueueFeatures`](crate::queue::QueueFeatures);
//! otherwise the check confirms a feature-free queue still delivers.

use serde_json::json;

use super::{fails, ok, unique};
use crate::error::ErrorKind;
use crate::queue::{Delivery, NewMessage, QueueSpec, QueueStats, QueueStore, Retry};

/// Long enough that no lease lapses during a check.
const HOLD_MS: u64 = 60_000;

fn text(delivery: &Delivery) -> &str {
    delivery.payload["t"].as_str().unwrap_or_default()
}

fn texts(batch: &[Delivery]) -> Vec<&str> {
    batch.iter().map(text).collect()
}

fn msg(t: &str) -> NewMessage {
    NewMessage::new(json!({ "t": t }))
}

pub(super) async fn run(q: &dyn QueueStore) {
    order_and_ack(q).await;
    nack_and_dead_letters(q).await;
    extend(q).await;
    delay(q).await;
    groups(q).await;
    dedupe(q).await;
    isolation_and_purge(q).await;
    invalid_input(q).await;
}

async fn order_and_ack(q: &dyn QueueStore) {
    let name = unique("qp_order");
    ok(
        q.enqueue(
            &name,
            &[msg("a"), msg("b"), msg("urgent").priority(5), msg("c")],
        )
        .await,
        "enqueue",
    );
    let batch = ok(q.receive(&name, 3, HOLD_MS, "w").await, "receive");
    let expected: [&str; 3] = if q.features().priorities {
        ["urgent", "a", "b"]
    } else {
        ["a", "b", "urgent"]
    };
    assert_eq!(
        texts(&batch),
        expected,
        "priority first, then enqueue order"
    );
    assert!(
        batch.iter().all(|d| d.attempt == 1),
        "first delivery is attempt 1"
    );
    assert_eq!(
        ok(q.stats(&name).await, "stats"),
        QueueStats {
            ready: 1,
            delayed: 0,
            leased: 3,
            dead: 0
        }
    );
    for delivery in &batch {
        assert!(ok(q.ack(&name, &delivery.receipt).await, "ack"));
    }
    assert!(
        !ok(q.ack(&name, &batch[0].receipt).await, "ack twice"),
        "a settled receipt no longer holds the message"
    );
    let rest = ok(q.receive(&name, 10, HOLD_MS, "w").await, "receive rest");
    assert_eq!(texts(&rest), ["c"]);
    assert!(
        ok(q.receive(&name, 10, HOLD_MS, "w").await, "drained").is_empty(),
        "leased messages are not delivered twice"
    );
    assert!(ok(q.ack(&name, &rest[0].receipt).await, "ack rest"));
    assert_eq!(ok(q.stats(&name).await, "empty"), QueueStats::default());
}

async fn nack_and_dead_letters(q: &dyn QueueStore) {
    let name = unique("qp_nack");
    ok(
        q.ensure_queue(
            &name,
            &QueueSpec {
                max_attempts: 2,
                dedupe_window_ms: 0,
            },
        )
        .await,
        "declare",
    );
    ok(q.enqueue(&name, &[msg("retry")]).await, "enqueue");
    let first = ok(q.receive(&name, 1, HOLD_MS, "w").await, "receive").remove(0);
    assert!(ok(
        q.nack(&name, &first.receipt, Retry::After(0)).await,
        "nack"
    ));
    assert!(
        !ok(
            q.nack(&name, &first.receipt, Retry::After(0)).await,
            "nack twice"
        ),
        "a nacked receipt no longer holds the message"
    );
    let second = ok(q.receive(&name, 1, HOLD_MS, "w").await, "retry").remove(0);
    assert_eq!(text(&second), "retry");
    assert_eq!(second.attempt, 2, "a nack counts as an attempt");
    assert!(ok(
        q.nack(&name, &second.receipt, Retry::After(0)).await,
        "final nack"
    ));
    assert!(
        ok(q.receive(&name, 1, HOLD_MS, "w").await, "exhausted").is_empty(),
        "a message past its attempts is not delivered"
    );

    ok(q.enqueue(&name, &[msg("bad")]).await, "enqueue bad");
    let bad = ok(q.receive(&name, 1, HOLD_MS, "w").await, "receive bad").remove(0);
    assert!(ok(
        q.nack(&name, &bad.receipt, Retry::DeadLetter("unparseable".into()))
            .await,
        "dead-letter"
    ));
    assert_eq!(ok(q.stats(&name).await, "stats").dead, 2);
    let dead = ok(q.dead_letters(&name, 10).await, "dead letters");
    let reasons: Vec<_> = dead.iter().map(|d| d.reason.as_str()).collect();
    assert_eq!(reasons, ["max attempts exceeded", "unparseable"]);
    assert_eq!(dead[0].payload["t"], "retry");
    assert_eq!(dead[0].attempt, 2);
    assert_eq!(dead[1].payload["t"], "bad");
    assert_eq!(
        ok(q.dead_letters(&name, 1).await, "limited").len(),
        1,
        "dead letters honor the limit"
    );
    assert!(ok(q.redrive(&name, &dead[1].id).await, "redrive"));
    assert!(!ok(q.redrive(&name, &dead[1].id).await, "redrive twice"));
    let again = ok(q.receive(&name, 1, HOLD_MS, "w").await, "after redrive").remove(0);
    assert_eq!(text(&again), "bad");
    assert_eq!(again.attempt, 1, "redrive resets the attempt count");
    assert!(ok(q.ack(&name, &again.receipt).await, "ack redriven"));
    assert_eq!(ok(q.stats(&name).await, "one dead").dead, 1);
}

async fn extend(q: &dyn QueueStore) {
    let name = unique("qp_extend");
    ok(q.enqueue(&name, &[msg("long")]).await, "enqueue");
    let held = ok(q.receive(&name, 1, HOLD_MS, "w").await, "receive").remove(0);
    assert!(ok(q.extend(&name, &held.receipt, HOLD_MS).await, "extend"));
    assert!(
        ok(q.receive(&name, 1, HOLD_MS, "w").await, "still leased").is_empty(),
        "an extended lease is still held"
    );
    assert!(ok(q.ack(&name, &held.receipt).await, "ack after extend"));
    assert!(!ok(
        q.extend(&name, &held.receipt, HOLD_MS).await,
        "extend settled"
    ));
}

async fn delay(q: &dyn QueueStore) {
    if !q.features().delay {
        return;
    }
    let name = unique("qp_delay");
    ok(
        q.enqueue(&name, &[msg("later").delay_ms(HOLD_MS), msg("now")])
            .await,
        "enqueue",
    );
    let stats = ok(q.stats(&name).await, "delayed");
    assert_eq!((stats.ready, stats.delayed), (1, 1));
    let got = ok(q.receive(&name, 10, HOLD_MS, "w").await, "receive");
    assert_eq!(texts(&got), ["now"], "a delayed message waits");
    assert!(ok(q.ack(&name, &got[0].receipt).await, "ack"));
    assert!(!ok(
        q.nack(&name, &got[0].receipt, Retry::After(HOLD_MS)).await,
        "nack settled"
    ));
    assert_eq!(ok(q.purge(&name).await, "purge delayed"), 1);
}

async fn groups(q: &dyn QueueStore) {
    if !q.features().groups {
        return;
    }
    let name = unique("qp_group");
    ok(
        q.enqueue(
            &name,
            &[
                msg("a1").group("a"),
                msg("a2").group("a"),
                msg("b1").group("b"),
                msg("free"),
            ],
        )
        .await,
        "enqueue",
    );
    let batch = ok(q.receive(&name, 10, HOLD_MS, "w").await, "receive");
    assert_eq!(
        texts(&batch),
        ["a1", "b1", "free"],
        "one leased message per group"
    );
    assert_eq!(batch[0].group.as_deref(), Some("a"));
    assert_eq!(ok(q.stats(&name).await, "a2 waits").ready, 1);
    assert!(ok(q.ack(&name, &batch[0].receipt).await, "ack a1"));
    let next = ok(q.receive(&name, 10, HOLD_MS, "w").await, "a2 after a1");
    assert_eq!(texts(&next), ["a2"]);
    assert_eq!(ok(q.purge(&name).await, "purge"), 3);
}

async fn dedupe(q: &dyn QueueStore) {
    if !q.features().dedupe {
        return;
    }
    let name = unique("qp_dedupe");
    let first = ok(
        q.enqueue(&name, &[msg("one").dedupe_key("k")]).await,
        "enqueue",
    );
    let again = ok(
        q.enqueue(&name, &[msg("one again").dedupe_key("k")]).await,
        "enqueue again",
    );
    assert!(!first[0].duplicate);
    assert!(again[0].duplicate, "a live key is a duplicate");
    assert_eq!(again[0].id, first[0].id);
    let got = ok(q.receive(&name, 10, HOLD_MS, "w").await, "receive");
    assert_eq!(texts(&got), ["one"]);
    assert!(ok(q.ack(&name, &got[0].receipt).await, "ack"));
    let after = ok(
        q.enqueue(&name, &[msg("two").dedupe_key("k")]).await,
        "after ack",
    );
    assert!(
        !after[0].duplicate,
        "without a window the key is forgotten at acknowledgement"
    );
    assert_eq!(ok(q.purge(&name).await, "purge"), 1);
}

async fn isolation_and_purge(q: &dyn QueueStore) {
    let a = unique("qp_iso_a");
    let b = unique("qp_iso_b");
    ok(q.enqueue(&a, &[msg("a1"), msg("a2")]).await, "enqueue a");
    assert!(
        ok(q.receive(&b, 10, HOLD_MS, "w").await, "b is empty").is_empty(),
        "queues do not share messages"
    );
    let held = ok(q.receive(&a, 1, HOLD_MS, "w").await, "lease a").remove(0);
    assert_eq!(ok(q.purge(&a).await, "purge"), 2);
    assert_eq!(ok(q.stats(&a).await, "purged"), QueueStats::default());
    assert!(!ok(q.ack(&a, &held.receipt).await, "ack purged"));
    assert_eq!(ok(q.purge(&b).await, "purge empty"), 0);
}

async fn invalid_input(q: &dyn QueueStore) {
    let name = unique("qp_invalid");
    fails(
        q.enqueue("Bad Name", &[msg("x")]).await,
        ErrorKind::InvalidInput,
        "bad name",
    );
    fails(
        q.enqueue(&name, &[msg("x").priority(10)]).await,
        ErrorKind::InvalidInput,
        "priority past the maximum",
    );
    fails(
        q.receive(&name, 0, HOLD_MS, "w").await,
        ErrorKind::InvalidInput,
        "receive nothing",
    );
    fails(
        q.receive(&name, 1, 0, "w").await,
        ErrorKind::InvalidInput,
        "zero visibility",
    );
    fails(
        q.ensure_queue(
            &name,
            &QueueSpec {
                max_attempts: 0,
                dedupe_window_ms: 0,
            },
        )
        .await,
        ErrorKind::InvalidInput,
        "zero attempts",
    );
}
