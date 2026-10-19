//! Queue checks: the [`DocumentQueue`] semantics every document driver must
//! give the same answers for.
//!
//! The queue reads time from a hand-driven clock, so leases, delays and
//! dedupe windows are exercised without sleeping, on any driver.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;

use super::{empty, fails, ok, unique};
use crate::backend::ScopedStorage;
use crate::error::ErrorKind;
use crate::queue::{self, DocumentQueue, NewMessage, QueueSpec, QueueStats, QueueStore, Retry};

/// A clock the checks move by hand.
struct Time(Arc<AtomicU64>);

impl Time {
    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

fn queue_on(storage: &ScopedStorage) -> (DocumentQueue, Time) {
    let now = Arc::new(AtomicU64::new(1_700_000_000_000));
    let read = Arc::clone(&now);
    let queue = DocumentQueue::new(storage.documents())
        .with_clock(Arc::new(move || read.load(Ordering::SeqCst)));
    (queue, Time(now))
}

fn text(delivery: &queue::Delivery) -> &str {
    delivery.payload["t"].as_str().unwrap_or_default()
}

fn msg(t: &str) -> NewMessage {
    NewMessage::new(json!({ "t": t }))
}

pub(super) async fn run(storage: &ScopedStorage) {
    ok(queue::ensure(storage).await, "ensure queue collections");
    order_and_ack(storage).await;
    lease_expiry(storage).await;
    attempts_and_dead_letters(storage).await;
    delays_and_nack(storage).await;
    groups(storage).await;
    dedupe(storage).await;
    extend(storage).await;
    isolation_and_purge(storage).await;
    invalid_input(storage).await;
}

async fn order_and_ack(storage: &ScopedStorage) {
    let (q, _) = queue_on(storage);
    let name = unique("q_order");
    ok(
        q.enqueue(
            &name,
            &[msg("a"), msg("b"), msg("urgent").priority(5), msg("c")],
        )
        .await,
        "enqueue",
    );
    let batch = ok(q.receive(&name, 3, 1_000, "w").await, "receive");
    let got: Vec<_> = batch.iter().map(text).collect();
    assert_eq!(
        got,
        ["urgent", "a", "b"],
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
    assert!(ok(q.ack(&name, &batch[0].receipt).await, "ack"));
    assert!(
        !ok(q.ack(&name, &batch[0].receipt).await, "ack twice"),
        "a settled receipt no longer holds the message"
    );
    let rest = ok(q.receive(&name, 10, 1_000, "w").await, "receive rest");
    assert_eq!(rest.iter().map(text).collect::<Vec<_>>(), ["c"]);
    assert!(
        ok(q.receive(&name, 10, 1_000, "w").await, "drained").is_empty(),
        "leased messages are not delivered twice"
    );
}

async fn lease_expiry(storage: &ScopedStorage) {
    let (q, time) = queue_on(storage);
    let name = unique("q_lease");
    ok(q.enqueue(&name, &[msg("x")]).await, "enqueue");
    let first = ok(q.receive(&name, 1, 100, "w1").await, "receive").remove(0);
    time.advance(100);
    let second = ok(q.receive(&name, 1, 100, "w2").await, "redeliver").remove(0);
    assert_eq!(second.receipt.id, first.receipt.id);
    assert_eq!(second.attempt, 2, "a lapsed lease counts as an attempt");
    assert!(
        !ok(q.ack(&name, &first.receipt).await, "stale ack"),
        "the lapsed receipt lost the message"
    );
    assert!(ok(q.ack(&name, &second.receipt).await, "ack"));
    assert_eq!(ok(q.stats(&name).await, "empty"), QueueStats::default());
}

async fn attempts_and_dead_letters(storage: &ScopedStorage) {
    let (q, time) = queue_on(storage);
    let name = unique("q_dlq");
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
    ok(q.enqueue(&name, &[msg("poison")]).await, "enqueue");
    for _ in 0..2 {
        assert_eq!(ok(q.receive(&name, 1, 10, "w").await, "receive").len(), 1);
        time.advance(10);
    }
    assert!(
        ok(q.receive(&name, 1, 10, "w").await, "exhausted").is_empty(),
        "a message past its attempts is not delivered"
    );
    assert_eq!(ok(q.stats(&name).await, "stats").dead, 1);
    let dead = ok(q.dead_letters(&name, 10).await, "dead letters");
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].reason, "max attempts exceeded");
    assert_eq!(dead[0].payload["t"], "poison");
    assert!(ok(q.redrive(&name, &dead[0].id).await, "redrive"));
    assert!(!ok(q.redrive(&name, &dead[0].id).await, "redrive twice"));
    let again = ok(q.receive(&name, 1, 10, "w").await, "after redrive");
    assert_eq!(again[0].attempt, 1, "redrive resets the attempt count");

    // A nack past the last attempt dead-letters too, and an explicit
    // dead-letter keeps its reason.
    assert!(ok(
        q.nack(&name, &again[0].receipt, Retry::After(0)).await,
        "nack"
    ));
    let last = ok(q.receive(&name, 1, 10, "w").await, "last attempt");
    assert_eq!(last[0].attempt, 2);
    assert!(ok(
        q.nack(&name, &last[0].receipt, Retry::After(0)).await,
        "final nack"
    ));
    assert_eq!(ok(q.stats(&name).await, "dead again").dead, 1);
    ok(q.enqueue(&name, &[msg("bad")]).await, "enqueue bad");
    let bad = ok(q.receive(&name, 1, 10, "w").await, "receive bad").remove(0);
    assert!(ok(
        q.nack(&name, &bad.receipt, Retry::DeadLetter("unparseable".into()))
            .await,
        "dead-letter"
    ));
    let reasons: Vec<_> = ok(q.dead_letters(&name, 10).await, "reasons")
        .into_iter()
        .map(|d| d.reason)
        .collect();
    assert_eq!(reasons, ["max attempts exceeded", "unparseable"]);
}

async fn delays_and_nack(storage: &ScopedStorage) {
    let (q, time) = queue_on(storage);
    let name = unique("q_delay");
    ok(
        q.enqueue(&name, &[msg("later").delay_ms(500)]).await,
        "enqueue",
    );
    assert_eq!(ok(q.stats(&name).await, "delayed").delayed, 1);
    empty(
        &ok(q.receive(&name, 1, 10, "w").await, "too early"),
        "too early",
    );
    time.advance(500);
    let first = ok(q.receive(&name, 1, 10, "w").await, "due").remove(0);
    assert!(ok(
        q.nack(&name, &first.receipt, Retry::After(300)).await,
        "nack"
    ));
    assert!(
        !ok(
            q.nack(&name, &first.receipt, Retry::After(300)).await,
            "nack twice"
        ),
        "a nacked receipt no longer holds the message"
    );
    assert_eq!(ok(q.stats(&name).await, "backing off").delayed, 1);
    time.advance(299);
    empty(
        &ok(q.receive(&name, 1, 10, "w").await, "backoff"),
        "backoff",
    );
    time.advance(1);
    let again = ok(q.receive(&name, 1, 10, "w").await, "retry due");
    assert_eq!(again[0].attempt, 2);
}

async fn groups(storage: &ScopedStorage) {
    let (q, time) = queue_on(storage);
    let name = unique("q_group");
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
    let batch = ok(q.receive(&name, 10, 1_000, "w").await, "receive");
    assert_eq!(
        batch.iter().map(text).collect::<Vec<_>>(),
        ["a1", "b1", "free"],
        "one leased message per group"
    );
    assert_eq!(ok(q.stats(&name).await, "a2 waits").ready, 1);
    assert!(ok(q.ack(&name, &batch[0].receipt).await, "ack a1"));
    let next = ok(q.receive(&name, 10, 1_000, "w").await, "a2 after a1");
    assert_eq!(next.iter().map(text).collect::<Vec<_>>(), ["a2"]);

    // A delayed retry keeps the group: later messages wait behind it.
    ok(
        q.enqueue(&name, &[msg("c1").group("c"), msg("c2").group("c")])
            .await,
        "enqueue c",
    );
    let c1 = ok(q.receive(&name, 1, 1_000, "w").await, "c1").remove(0);
    assert_eq!(text(&c1), "c1");
    assert!(ok(
        q.nack(&name, &c1.receipt, Retry::After(200)).await,
        "nack c1"
    ));
    assert!(
        ok(q.receive(&name, 10, 1_000, "w").await, "c2 blocked").is_empty(),
        "c2 must not overtake c1's retry"
    );
    time.advance(200);
    let retried = ok(q.receive(&name, 10, 1_000, "w").await, "c1 retried");
    assert_eq!(retried.iter().map(text).collect::<Vec<_>>(), ["c1"]);

    // A lapsed lease frees the group for its own redelivery.
    time.advance(1_000);
    let redelivered = ok(q.receive(&name, 10, 1_000, "w").await, "lapsed");
    assert_eq!(
        redelivered.iter().map(text).collect::<Vec<_>>(),
        ["a2", "b1", "free", "c1"],
        "every lapsed lease comes back, in enqueue order, still one per group"
    );
}

async fn dedupe(storage: &ScopedStorage) {
    let (q, time) = queue_on(storage);
    let name = unique("q_dedupe");
    ok(
        q.ensure_queue(
            &name,
            &QueueSpec {
                max_attempts: 3,
                dedupe_window_ms: 1_000,
            },
        )
        .await,
        "declare",
    );
    let first = ok(
        q.enqueue(&name, &[msg("one").dedupe_key("tg:1")]).await,
        "enqueue",
    );
    let again = ok(
        q.enqueue(&name, &[msg("one again").dedupe_key("tg:1")])
            .await,
        "enqueue again",
    );
    assert!(!first[0].duplicate);
    assert!(again[0].duplicate, "a live key is a duplicate");
    assert_eq!(again[0].id, first[0].id);
    let got = ok(q.receive(&name, 10, 100, "w").await, "receive");
    assert_eq!(got.iter().map(text).collect::<Vec<_>>(), ["one"]);
    assert!(ok(q.ack(&name, &got[0].receipt).await, "ack"));
    assert_eq!(
        ok(q.stats(&name).await, "tombstones are not counted"),
        QueueStats::default()
    );
    let redelivered = ok(
        q.enqueue(&name, &[msg("redelivery").dedupe_key("tg:1")])
            .await,
        "redelivery",
    );
    assert!(
        redelivered[0].duplicate,
        "the window remembers an acked key"
    );
    time.advance(1_000);
    let fresh = ok(
        q.enqueue(&name, &[msg("fresh").dedupe_key("tg:1")]).await,
        "after window",
    );
    assert!(!fresh[0].duplicate, "an expired tombstone is replaced");
    let got = ok(q.receive(&name, 10, 100, "w").await, "receive fresh");
    assert_eq!(got.iter().map(text).collect::<Vec<_>>(), ["fresh"]);
    assert!(ok(q.ack(&name, &got[0].receipt).await, "ack fresh"));
    time.advance(1_000);
    assert_eq!(
        ok(q.prune(&name).await, "prune"),
        1,
        "prune drops expired tombstones"
    );

    // Without a window the key is forgotten at acknowledgement.
    let plain = unique("q_dedupe_plain");
    ok(
        q.enqueue(&plain, &[msg("p").dedupe_key("k")]).await,
        "enqueue",
    );
    let got = ok(q.receive(&plain, 1, 100, "w").await, "receive").remove(0);
    assert!(ok(q.ack(&plain, &got.receipt).await, "ack"));
    let after = ok(
        q.enqueue(&plain, &[msg("p").dedupe_key("k")]).await,
        "again",
    );
    assert!(!after[0].duplicate);
}

async fn extend(storage: &ScopedStorage) {
    let (q, time) = queue_on(storage);
    let name = unique("q_extend");
    ok(q.enqueue(&name, &[msg("long").group("g")]).await, "enqueue");
    let held = ok(q.receive(&name, 1, 100, "w").await, "receive").remove(0);
    assert!(ok(q.extend(&name, &held.receipt, 1_000).await, "extend"));
    time.advance(500);
    assert!(
        ok(q.receive(&name, 1, 100, "w").await, "still leased").is_empty(),
        "an extended lease is still held"
    );
    assert!(ok(q.ack(&name, &held.receipt).await, "ack after extend"));
    assert!(!ok(
        q.extend(&name, &held.receipt, 1_000).await,
        "extend settled"
    ));
}

async fn isolation_and_purge(storage: &ScopedStorage) {
    let (q, _) = queue_on(storage);
    let a = unique("q_iso_a");
    let b = unique("q_iso_b");
    ok(
        q.enqueue(&a, &[msg("for a").group("g"), msg("a2")]).await,
        "enqueue a",
    );
    assert!(
        ok(q.receive(&b, 10, 100, "w").await, "b is empty").is_empty(),
        "queues do not share messages"
    );
    let held = ok(q.receive(&a, 1, 100, "w").await, "lease a").remove(0);
    assert_eq!(ok(q.purge(&a).await, "purge"), 2);
    assert_eq!(ok(q.stats(&a).await, "purged"), QueueStats::default());
    assert!(!ok(q.ack(&a, &held.receipt).await, "ack purged"));
    // The purged group lock is gone: a new message of the group is free.
    ok(
        q.enqueue(&a, &[msg("next").group("g")]).await,
        "enqueue after purge",
    );
    assert_eq!(ok(q.receive(&a, 1, 100, "w").await, "group free").len(), 1);
}

async fn invalid_input(storage: &ScopedStorage) {
    let (q, _) = queue_on(storage);
    let name = unique("q_invalid");
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
        q.receive(&name, 0, 100, "w").await,
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
