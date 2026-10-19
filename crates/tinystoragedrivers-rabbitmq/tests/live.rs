//! The queue backend against a real `RabbitMQ`, named `live_*`.
//!
//! Set `TSD_RABBITMQ_URL` (for example
//! `amqp://guest:guest@127.0.0.1:5672/%2f`) to run them; without it each test
//! prints a note and passes. Queue names are unique per run, so a shared
//! broker is fine. The lapse and delay tests sleep in real time: the broker
//! owns the TTL clock.

#![allow(
    clippy::expect_used,
    reason = "a test helper reports a misconfigured broker by panicking"
)]

use std::time::Duration;

use serde_json::json;
use tinystoragedrivers_core::queue::{NewMessage, QueueSpec, QueueStats, QueueStore, Retry};
use tinystoragedrivers_core::{ErrorKind, Key, Scope, conformance};
use tinystoragedrivers_rabbitmq::{MAX_DELAY_MS, RabbitQueues};

fn url() -> Option<String> {
    std::env::var("TSD_RABBITMQ_URL")
        .ok()
        .filter(|url| !url.is_empty())
}

fn skip(test: &str) {
    eprintln!("{test}: TSD_RABBITMQ_URL is unset, skipping");
}

async fn connect(scope: &Scope) -> Option<RabbitQueues> {
    let url = url()?;
    Some(
        RabbitQueues::connect(&url, scope)
            .await
            .expect("TSD_RABBITMQ_URL is set but the broker cannot be reached"),
    )
}

fn name(label: &str) -> String {
    format!("live_{label}_{}", Key::ulid().as_str().to_lowercase())
}

fn msg(t: &str) -> NewMessage {
    NewMessage::new(json!({ "t": t }))
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn live_queue_conformance() {
    let Some(queues) = connect(&Scope::local()).await else {
        return skip("live_queue_conformance");
    };
    let features = queues.features();
    assert!(features.priorities && features.delay);
    assert!(!features.groups && !features.dedupe);
    conformance::run_queue(&queues).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn live_a_lapsed_lease_comes_back_with_its_attempt_counted() {
    let Some(queues) = connect(&Scope::local()).await else {
        return skip("live_a_lapsed_lease_comes_back_with_its_attempt_counted");
    };
    let q = name("lapse");
    queues
        .ensure_queue(
            &q,
            &QueueSpec {
                max_attempts: 2,
                dedupe_window_ms: 0,
            },
        )
        .await
        .unwrap();
    queues.enqueue(&q, &[msg("x")]).await.unwrap();
    let first = queues.receive(&q, 1, 100, "w").await.unwrap().remove(0);
    assert!(queues.extend(&q, &first.receipt, 150).await.unwrap());
    sleep_ms(500).await;
    assert!(!queues.ack(&q, &first.receipt).await.unwrap(), "lapsed");
    assert!(!queues.extend(&q, &first.receipt, 100).await.unwrap());
    let second = queues.receive(&q, 1, 100, "w").await.unwrap().remove(0);
    assert_eq!(second.attempt, 2);
    assert_eq!(second.receipt.id, first.receipt.id);
    // The last attempt lapses too: the message is dead, not looping.
    sleep_ms(500).await;
    assert_eq!(queues.receive(&q, 1, 100, "w").await.unwrap().len(), 0);
    let dead = queues.dead_letters(&q, 10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].reason, "max attempts exceeded");
    assert_eq!(dead[0].attempt, 2);
    queues.purge(&q).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn live_a_closed_connection_returns_its_deliveries_and_reconnects() {
    let Some(first) = connect(&Scope::local()).await else {
        return skip("live_a_closed_connection_returns_its_deliveries_and_reconnects");
    };
    let second = connect(&Scope::local()).await.unwrap();
    let q = name("crash");
    first.enqueue(&q, &[msg("held")]).await.unwrap();
    let held = first.receive(&q, 1, 60_000, "w").await.unwrap().remove(0);
    assert!(
        second.receive(&q, 1, 60_000, "w").await.unwrap().is_empty(),
        "leased to the first handle"
    );
    assert!(
        !second.ack(&q, &held.receipt).await.unwrap(),
        "a receipt works only on the handle that issued it"
    );
    first.close().await.unwrap();
    sleep_ms(200).await;
    let again = second.receive(&q, 1, 60_000, "w").await.unwrap().remove(0);
    assert_eq!(again.receipt.id, held.receipt.id);
    assert_eq!(again.attempt, 2, "a return after a crash counts");
    // The closed handle reconnects on its next call and has lost its lease.
    assert_eq!(first.stats(&q).await.unwrap().leased, 0);
    assert!(!first.ack(&q, &held.receipt).await.unwrap());
    assert!(second.ack(&q, &again.receipt).await.unwrap());
    assert_eq!(first.stats(&q).await.unwrap(), QueueStats::default());
}

#[tokio::test(flavor = "multi_thread")]
async fn live_delays_round_up_and_backoff_returns_the_message() {
    let Some(queues) = connect(&Scope::local()).await else {
        return skip("live_delays_round_up_and_backoff_returns_the_message");
    };
    let q = name("delay");
    queues
        .enqueue(&q, &[msg("later").delay_ms(300)])
        .await
        .unwrap();
    assert_eq!(queues.stats(&q).await.unwrap().delayed, 1);
    assert_eq!(queues.receive(&q, 1, 60_000, "w").await.unwrap().len(), 0);
    sleep_ms(1_200).await;
    let got = queues.receive(&q, 1, 60_000, "w").await.unwrap().remove(0);
    assert_eq!(got.payload["t"], "later");
    assert!(
        queues
            .nack(&q, &got.receipt, Retry::After(100))
            .await
            .unwrap()
    );
    assert_eq!(queues.receive(&q, 1, 60_000, "w").await.unwrap().len(), 0);
    sleep_ms(800).await;
    let retried = queues.receive(&q, 1, 60_000, "w").await.unwrap().remove(0);
    assert_eq!(retried.attempt, 2);
    assert!(queues.ack(&q, &retried.receipt).await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn live_a_foreign_message_is_dead_lettered() {
    let Some(queues) = connect(&Scope::local()).await else {
        return skip("live_a_foreign_message_is_dead_lettered");
    };
    let q = name("foreign");
    queues.enqueue(&q, &[msg("ours")]).await.unwrap();
    let connection =
        lapin::Connection::connect(&url().unwrap(), lapin::ConnectionProperties::default())
            .await
            .unwrap();
    let channel = connection.create_channel().await.unwrap();
    channel
        .basic_publish(
            "".into(),
            queues.broker_name(&q).unwrap().into(),
            lapin::options::BasicPublishOptions::default(),
            b"not json",
            lapin::BasicProperties::default(),
        )
        .await
        .unwrap();
    sleep_ms(100).await;
    let got = queues.receive(&q, 10, 60_000, "w").await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].payload["t"], "ours");
    let dead = queues.dead_letters(&q, 10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].reason, "not a tinystoragedrivers message");
    assert_eq!(dead[0].payload, json!("not json"));
    assert_eq!(
        queues.purge(&q).await.unwrap(),
        2,
        "one dead, one own lease"
    );
    connection.close(200, "done".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn live_unsupported_options_and_scopes_are_kept_apart() {
    let Some(alice) = connect(&Scope::new("alice").unwrap()).await else {
        return skip("live_unsupported_options_and_scopes_are_kept_apart");
    };
    let bob = connect(&Scope::new("bob").unwrap()).await.unwrap();
    let q = name("scopes");
    for refused in [
        msg("g").group("g"),
        msg("d").dedupe_key("k"),
        msg("late").delay_ms(MAX_DELAY_MS + 1),
        msg("p").priority(10),
    ] {
        assert_eq!(
            alice.enqueue(&q, &[refused]).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    alice.enqueue(&q, &[msg("for alice")]).await.unwrap();
    assert_eq!(bob.receive(&q, 10, 60_000, "w").await.unwrap().len(), 0);
    assert_ne!(alice.broker_name(&q).unwrap(), bob.broker_name(&q).unwrap());
    let got = alice.receive(&q, 1, 60_000, "w").await.unwrap().remove(0);
    assert_eq!(
        alice
            .nack(&q, &got.receipt, Retry::After(MAX_DELAY_MS + 1))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        alice.extend(&q, &got.receipt, 0).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(alice.dead_letters(&q, 0).await.unwrap().len(), 0);
    assert!(!alice.redrive(&q, "missing").await.unwrap());
    assert!(alice.ack(&q, &got.receipt).await.unwrap());
    assert!(format!("{alice:?}").contains("tsd/"));
    assert!(!format!("{alice:?}").contains("guest"));
}

#[tokio::test(flavor = "multi_thread")]
async fn live_an_unreachable_broker_is_unavailable_without_its_url() {
    if url().is_none() {
        return skip("live_an_unreachable_broker_is_unavailable_without_its_url");
    }
    let error = RabbitQueues::connect("amqp://secret:secret@127.0.0.1:1/%2f", &Scope::local())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert!(!error.to_string().contains("secret"));
}
