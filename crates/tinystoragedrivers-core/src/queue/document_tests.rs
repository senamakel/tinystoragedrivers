use super::*;
use crate::backend::StorageBackend;
use crate::memory::MemoryStorage;
use crate::scope::Scope;

fn queue() -> DocumentQueue {
    let storage = MemoryStorage::new()
        .for_scope(&Scope::local())
        .expect("local scope");
    DocumentQueue::new(storage.documents())
}

#[tokio::test]
async fn the_memory_driver_passes_the_queue_checks() {
    let storage = MemoryStorage::new()
        .for_scope(&Scope::local())
        .expect("local scope");
    crate::conformance::run_ports(&storage).await;
}

#[test]
fn sequence_is_increasing_within_a_handle_even_if_the_clock_steps_back() {
    let q = queue();
    let seqs: Vec<String> = [1_000, 1_000, 900, 2_000]
        .into_iter()
        .map(|now| q.next_seq(now).expect("seq"))
        .collect();
    assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]), "{seqs:?}");
}

#[tokio::test]
async fn a_spec_is_read_once_and_cached() {
    let q = queue();
    q.ensure_queue(
        "cached",
        &QueueSpec {
            max_attempts: 3,
            dedupe_window_ms: 5,
        },
    )
    .await
    .expect("declare");
    let other = DocumentQueue::new(&q.docs);
    assert_eq!(other.spec("cached").await.expect("spec").max_attempts, 3);
    assert_eq!(
        other.spec("undeclared").await.expect("spec"),
        QueueSpec::default()
    );
}

#[test]
fn debug_does_not_print_contents() {
    assert!(format!("{:?}", queue()).starts_with("DocumentQueue"));
}

#[test]
fn every_feature_is_supported() {
    let features = queue().features();
    assert!(features.dedupe && features.groups && features.priorities && features.delay);
}
