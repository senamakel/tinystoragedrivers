//! Peer binding, ordering, expiry and owned converter lifetime regressions.
use super::*;
use crate::schema::delivery::BlobTargetRequest;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
struct Handler;
impl MigrationPayloadHandler for Handler {
    fn transform(&self, _: MigrationPayload) -> MigrationPayloadReply {
        MigrationPayloadReply::BlobTarget(Ok(Some("target".into())))
    }
}
#[derive(Default)]
struct Clock(AtomicU64);
impl ExchangeClock for Clock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
#[derive(Default)]
struct Ids(AtomicU64);
impl TransferIds for Ids {
    fn next_id(&self) -> String {
        format!("token{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}
fn server(max: usize) -> (MigrationExchangeServer, Arc<Clock>) {
    let clock = Arc::new(Clock::default());
    (
        MigrationExchangeServer::new(
            Arc::new(Handler),
            clock.clone(),
            Arc::new(Ids::default()),
            max,
        ),
        clock,
    )
}
async fn begin(server: &MigrationExchangeServer, peer: &PeerIdentity, len: u64) -> String {
    let MigrationExchangeReply::Begun { transfer } = server
        .handle(peer, MigrationExchangeRequest::Begin { request_len: len })
        .await
    else {
        panic!("begin")
    };
    transfer
}
fn payload() -> Vec<u8> {
    serde_json::to_vec(&MigrationPayload::BlobTarget(BlobTargetRequest {
        operation: "move".into(),
        version: 1,
        source: "private source credentials".into(),
    }))
    .unwrap()
}
async fn uploaded(server: &MigrationExchangeServer, peer: &PeerIdentity) -> String {
    let bytes = payload();
    let len = bytes.len() as u64;
    let id = begin(server, peer, len).await;
    assert_eq!(
        server
            .handle(
                peer,
                MigrationExchangeRequest::Upload {
                    transfer: id.clone(),
                    offset: 0,
                    bytes
                }
            )
            .await,
        MigrationExchangeReply::Uploaded { next_offset: len }
    );
    id
}
#[tokio::test]
async fn every_phase_hides_wrong_peer_like_an_absent_id() {
    let (server, _) = server(2);
    let a = PeerIdentity::new("a");
    let b = PeerIdentity::new("b");
    let id = begin(&server, &a, 1).await;
    for request in [
        MigrationExchangeRequest::Upload {
            transfer: id.clone(),
            offset: 0,
            bytes: vec![0],
        },
        MigrationExchangeRequest::Execute {
            transfer: id.clone(),
        },
        MigrationExchangeRequest::Download {
            transfer: id.clone(),
            offset: 0,
        },
        MigrationExchangeRequest::Finish {
            transfer: id.clone(),
        },
        MigrationExchangeRequest::Cancel {
            transfer: id.clone(),
        },
    ] {
        assert_eq!(
            server.handle(&b, request).await,
            error(ExchangeErrorKind::UnknownTransfer)
        );
    }
    for id in ["", "fake", &"x".repeat(65), "bad/token", "../other"] {
        assert_eq!(
            server
                .handle(
                    &a,
                    MigrationExchangeRequest::Cancel {
                        transfer: id.into()
                    }
                )
                .await,
            error(ExchangeErrorKind::UnknownTransfer)
        );
    }
    assert_eq!(server.state.lock().len(), 1);
}
#[tokio::test]
async fn rejects_truncated_huge_duplicate_and_out_of_order_uploads() {
    let (server, _) = server(1);
    let peer = PeerIdentity::new("a");
    let id = begin(&server, &peer, u64::MAX).await;
    // Even an enormous advertised length creates an empty, unreserved buffer.
    match &server.state.lock().get(&id).unwrap().stage {
        Stage::Receiving { bytes, .. } => assert_eq!(bytes.capacity(), 0),
        _ => panic!("receiving"),
    }
    for (offset, bytes, kind) in [
        (1, vec![0], ExchangeErrorKind::WrongSequence),
        (0, vec![], ExchangeErrorKind::InvalidRequest),
        (
            0,
            vec![0; MIGRATION_CHUNK_BYTES + 1],
            ExchangeErrorKind::InvalidRequest,
        ),
        (u64::MAX, vec![0], ExchangeErrorKind::WrongSequence),
    ] {
        assert_eq!(
            server
                .handle(
                    &peer,
                    MigrationExchangeRequest::Upload {
                        transfer: id.clone(),
                        offset,
                        bytes
                    }
                )
                .await,
            error(kind)
        );
    }
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Execute {
                    transfer: id.clone()
                }
            )
            .await,
        error(ExchangeErrorKind::Incomplete)
    );
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Upload {
                    transfer: id.clone(),
                    offset: 0,
                    bytes: vec![0]
                }
            )
            .await,
        MigrationExchangeReply::Uploaded { next_offset: 1 }
    );
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Upload {
                    transfer: id.clone(),
                    offset: 0,
                    bytes: vec![0]
                }
            )
            .await,
        error(ExchangeErrorKind::WrongSequence)
    );
    assert_eq!(
        server
            .handle(&peer, MigrationExchangeRequest::Cancel { transfer: id })
            .await,
        MigrationExchangeReply::Cancelled
    );
    let id = begin(&server, &peer, 1).await;
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Upload {
                    transfer: id,
                    offset: 0,
                    bytes: vec![0, 1]
                }
            )
            .await,
        error(ExchangeErrorKind::InvalidRequest)
    );
}
#[tokio::test]
async fn invalid_json_releases_admission_and_returns_static_error() {
    let (server, _) = server(1);
    let peer = PeerIdentity::new("a");
    let bytes = b"PRIVATE invalid json source credentials".to_vec();
    let id = begin(&server, &peer, bytes.len() as u64).await;
    server
        .handle(
            &peer,
            MigrationExchangeRequest::Upload {
                transfer: id.clone(),
                offset: 0,
                bytes,
            },
        )
        .await;
    let reply = server
        .handle(&peer, MigrationExchangeRequest::Execute { transfer: id })
        .await;
    assert_eq!(reply, error(ExchangeErrorKind::InvalidPayload));
    assert_eq!(
        serde_json::to_string(&reply).unwrap(),
        r#"{"type":"error","kind":{"type":"invalid_payload"}}"#
    );
    assert!(server.state.lock().is_empty());
    begin(&server, &peer, 1).await;
}
#[tokio::test]
async fn exact_download_offsets_complete_read_and_single_execute_are_enforced() {
    let (server, _) = server(1);
    let peer = PeerIdentity::new("a");
    let id = uploaded(&server, &peer).await;
    let MigrationExchangeReply::Ready { reply_len } = server
        .handle(
            &peer,
            MigrationExchangeRequest::Execute {
                transfer: id.clone(),
            },
        )
        .await
    else {
        panic!("ready")
    };
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Execute {
                    transfer: id.clone()
                }
            )
            .await,
        error(ExchangeErrorKind::WrongSequence)
    );
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Finish {
                    transfer: id.clone()
                }
            )
            .await,
        error(ExchangeErrorKind::Incomplete)
    );
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Download {
                    transfer: id.clone(),
                    offset: 1
                }
            )
            .await,
        error(ExchangeErrorKind::WrongSequence)
    );
    let MigrationExchangeReply::Chunk { offset, bytes } = server
        .handle(
            &peer,
            MigrationExchangeRequest::Download {
                transfer: id.clone(),
                offset: 0,
            },
        )
        .await
    else {
        panic!("chunk")
    };
    assert_eq!((offset, bytes.len() as u64), (0, reply_len));
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Download {
                    transfer: id.clone(),
                    offset: 0
                }
            )
            .await,
        error(ExchangeErrorKind::WrongSequence)
    );
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Finish {
                    transfer: id.clone()
                }
            )
            .await,
        MigrationExchangeReply::Finished
    );
    assert_eq!(
        server
            .handle(&peer, MigrationExchangeRequest::Finish { transfer: id })
            .await,
        error(ExchangeErrorKind::UnknownTransfer)
    );
}
#[tokio::test]
async fn only_valid_requests_refresh_idle_ttl_in_both_buffer_stages() {
    let (server, clock) = server(2);
    let peer = PeerIdentity::new("a");
    let id = begin(&server, &peer, 2).await;
    clock.0.store(59_000, Ordering::SeqCst);
    server
        .handle(
            &peer,
            MigrationExchangeRequest::Upload {
                transfer: id.clone(),
                offset: 0,
                bytes: vec![1],
            },
        )
        .await;
    clock.0.store(60_001, Ordering::SeqCst);
    server.expire();
    assert_eq!(server.state.lock().len(), 1);
    clock.0.store(118_999, Ordering::SeqCst);
    server
        .handle(
            &peer,
            MigrationExchangeRequest::Upload {
                transfer: id.clone(),
                offset: 0,
                bytes: vec![1],
            },
        )
        .await;
    clock.0.store(119_000, Ordering::SeqCst);
    server.expire();
    assert!(server.state.lock().is_empty());
    let id = uploaded(&server, &peer).await;
    server
        .handle(
            &peer,
            MigrationExchangeRequest::Execute {
                transfer: id.clone(),
            },
        )
        .await;
    clock.0.store(179_000, Ordering::SeqCst);
    server.expire();
    assert!(server.state.lock().is_empty());
    assert_eq!(
        server
            .handle(&peer, MigrationExchangeRequest::Cancel { transfer: id })
            .await,
        error(ExchangeErrorKind::UnknownTransfer)
    );
}
#[tokio::test(start_paused = true)]
async fn periodic_guard_expires_without_requests_and_does_not_keep_server_alive() {
    let (server, clock) = server(1);
    let peer = PeerIdentity::new("a");
    assert!(server.cleanup_guard(Duration::ZERO).is_err());
    let guard = server.cleanup_guard(Duration::from_secs(1)).unwrap();
    begin(&server, &peer, 100).await;
    tokio::task::yield_now().await;
    clock.0.store(IDLE_TTL_MS, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(
        server.state.lock().is_empty(),
        "periodic cleanup runs without another message"
    );
    let weak = Arc::downgrade(&server.state);
    drop(server);
    assert!(weak.upgrade().is_none());
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(guard.0.is_finished());
}
struct Gate {
    entered: tokio::sync::Notify,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    calls: AtomicUsize,
}
impl MigrationPayloadHandler for Gate {
    fn transform(&self, _: MigrationPayload) -> MigrationPayloadReply {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.lock().unwrap().recv().unwrap();
        MigrationPayloadReply::BlobTarget(Ok(Some("target".into())))
    }
}
#[tokio::test]
async fn cancellation_and_disconnect_keep_admission_until_converter_returns() {
    for disconnect in [false, true] {
        let (tx, rx) = std::sync::mpsc::channel();
        let gate = Arc::new(Gate {
            entered: tokio::sync::Notify::new(),
            release: Mutex::new(rx),
            calls: AtomicUsize::new(0),
        });
        let clock = Arc::new(Clock::default());
        let server =
            MigrationExchangeServer::new(gate.clone(), clock.clone(), Arc::new(Ids::default()), 1);
        let peer = PeerIdentity::new("a");
        let id = uploaded(&server, &peer).await;
        let task = {
            let server = server.clone();
            let peer = peer.clone();
            let id = id.clone();
            tokio::spawn(async move {
                server
                    .handle(&peer, MigrationExchangeRequest::Execute { transfer: id })
                    .await
            })
        };
        gate.entered.notified().await;
        clock.0.store(10 * IDLE_TTL_MS, Ordering::SeqCst);
        server.expire();
        assert_eq!(
            server
                .handle(
                    &peer,
                    MigrationExchangeRequest::Execute {
                        transfer: id.clone()
                    }
                )
                .await,
            error(ExchangeErrorKind::WrongSequence)
        );
        if disconnect {
            server.disconnect(&peer);
        } else {
            assert_eq!(
                server
                    .handle(
                        &peer,
                        MigrationExchangeRequest::Cancel {
                            transfer: id.clone()
                        }
                    )
                    .await,
                MigrationExchangeReply::Cancelled
            );
        }
        assert_eq!(
            server
                .handle(
                    &PeerIdentity::new("b"),
                    MigrationExchangeRequest::Begin { request_len: 0 }
                )
                .await,
            error(ExchangeErrorKind::Busy)
        );
        tx.send(()).unwrap();
        assert_eq!(task.await.unwrap(), MigrationExchangeReply::Cancelled);
        assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
        assert!(server.state.lock().is_empty());
        begin(&server, &peer, 1).await;
    }
}
#[tokio::test]
async fn dropped_execute_waiter_does_not_strand_execution_or_resurrect_cancelled_state() {
    let (tx, rx) = std::sync::mpsc::channel();
    let gate = Arc::new(Gate {
        entered: tokio::sync::Notify::new(),
        release: Mutex::new(rx),
        calls: AtomicUsize::new(0),
    });
    let server = MigrationExchangeServer::new(
        gate.clone(),
        Arc::new(Clock::default()),
        Arc::new(Ids::default()),
        1,
    );
    let peer = PeerIdentity::new("a");
    let id = uploaded(&server, &peer).await;
    let task = {
        let server = server.clone();
        let peer = peer.clone();
        tokio::spawn(async move {
            server
                .handle(&peer, MigrationExchangeRequest::Execute { transfer: id })
                .await
        })
    };
    gate.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    server.disconnect(&peer);
    assert_eq!(
        server
            .handle(&peer, MigrationExchangeRequest::Begin { request_len: 1 })
            .await,
        error(ExchangeErrorKind::Busy)
    );
    tx.send(()).unwrap();
    for _ in 0..100_000 {
        if server.state.lock().is_empty() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("server-owned completion did not reclaim the cancelled slot");
}
#[tokio::test]
async fn idle_disconnect_cleans_only_its_peer_and_collision_retries_are_bounded() {
    struct SameId(AtomicUsize);
    impl TransferIds for SameId {
        fn next_id(&self) -> String {
            self.0.fetch_add(1, Ordering::SeqCst);
            "same".into()
        }
    }
    let ids = Arc::new(SameId(AtomicUsize::new(0)));
    let server = MigrationExchangeServer::new(
        Arc::new(Handler),
        Arc::new(Clock::default()),
        ids.clone(),
        2,
    );
    let a = PeerIdentity::new("a");
    begin(&server, &a, 1).await;
    assert_eq!(
        server
            .handle(&a, MigrationExchangeRequest::Begin { request_len: 1 })
            .await,
        error(ExchangeErrorKind::Unavailable)
    );
    assert_eq!(ids.0.load(Ordering::SeqCst), 9);
    server.disconnect(&PeerIdentity::new("b"));
    assert_eq!(server.state.lock().len(), 1);
    server.disconnect(&a);
    assert!(server.state.lock().is_empty());
}
#[tokio::test]
async fn panicking_converter_returns_static_error_and_releases_admission() {
    struct Panics;
    impl MigrationPayloadHandler for Panics {
        fn transform(&self, _: MigrationPayload) -> MigrationPayloadReply {
            panic!("converter panic")
        }
    }
    let server = MigrationExchangeServer::new(
        Arc::new(Panics),
        Arc::new(Clock::default()),
        Arc::new(Ids::default()),
        1,
    );
    let peer = PeerIdentity::new("a");
    let id = uploaded(&server, &peer).await;
    assert_eq!(
        server
            .handle(&peer, MigrationExchangeRequest::Execute { transfer: id })
            .await,
        error(ExchangeErrorKind::TransformFailed)
    );
    assert!(server.state.lock().is_empty());
}

#[tokio::test]
async fn injected_allocation_failure_preserves_buffer_offset_and_retryability() {
    let (mut server, _) = server(1);
    let peer = PeerIdentity::new("a");
    let bytes = payload();
    let len = bytes.len() as u64;
    let id = begin(&server, &peer, len).await;
    Arc::get_mut(&mut server.state).unwrap().reserve = |_, _| Err(());
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Upload {
                    transfer: id.clone(),
                    offset: 0,
                    bytes: bytes.clone()
                }
            )
            .await,
        error(ExchangeErrorKind::Unavailable)
    );
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Execute {
                    transfer: id.clone()
                }
            )
            .await,
        error(ExchangeErrorKind::Incomplete)
    );
    Arc::get_mut(&mut server.state).unwrap().reserve = reserve_bytes;
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Upload {
                    transfer: id.clone(),
                    offset: 0,
                    bytes
                }
            )
            .await,
        MigrationExchangeReply::Uploaded { next_offset: len }
    );
    let mut buffer = Vec::new();
    assert_eq!(server.state.reserve(&mut buffer, usize::MAX), Err(()));
    assert_eq!(buffer.len(), 0);
}

#[tokio::test]
async fn download_before_execution_rejects() {
    let (server, _) = server(1);
    let peer = PeerIdentity::new("a");
    let id = uploaded(&server, &peer).await;
    assert_eq!(
        server
            .handle(
                &peer,
                MigrationExchangeRequest::Download {
                    transfer: id.clone(),
                    offset: 0
                }
            )
            .await,
        error(ExchangeErrorKind::WrongSequence)
    );
}

#[test]
fn abandoned_queued_job_finalizer_reclaims_its_executing_admission() {
    let (server, _) = server(1);
    let peer = PeerIdentity::new("a");
    server.state.lock().insert(
        "queued".into(),
        Transfer {
            peer,
            stage: Stage::Executing { discard: false },
        },
    );
    let finalizer = ExecutionFinalizer {
        state: Arc::downgrade(&server.state),
        id: "queued".into(),
        active: true,
    };
    drop(finalizer);
    assert!(server.state.lock().is_empty());
}
#[tokio::test]
async fn module_drop_releases_state_while_owned_converter_finishes_without_resurrection() {
    let (tx, rx) = std::sync::mpsc::channel();
    let gate = Arc::new(Gate {
        entered: tokio::sync::Notify::new(),
        release: Mutex::new(rx),
        calls: AtomicUsize::new(0),
    });
    let server = MigrationExchangeServer::new(
        gate.clone(),
        Arc::new(Clock::default()),
        Arc::new(Ids::default()),
        1,
    );
    let peer = PeerIdentity::new("a");
    let id = uploaded(&server, &peer).await;
    let task = {
        let server = server.clone();
        let peer = peer.clone();
        tokio::spawn(async move {
            server
                .handle(&peer, MigrationExchangeRequest::Execute { transfer: id })
                .await
        })
    };
    gate.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let weak = Arc::downgrade(&server.state);
    drop(server);
    assert!(
        weak.upgrade().is_none(),
        "blocking conversion owns only weak server state"
    );
    tx.send(()).unwrap();
}

#[tokio::test]
async fn completion_unwind_finalizer_reclaims_execution_after_converter_returns() {
    struct PanicClock(Arc<std::sync::atomic::AtomicBool>);
    impl ExchangeClock for PanicClock {
        fn now_ms(&self) -> u64 {
            assert!(!self.0.swap(false, Ordering::SeqCst), "clock failed");
            0
        }
    }
    struct ArmPanic(Arc<std::sync::atomic::AtomicBool>);
    impl MigrationPayloadHandler for ArmPanic {
        fn transform(&self, _: MigrationPayload) -> MigrationPayloadReply {
            self.0.store(true, Ordering::SeqCst);
            MigrationPayloadReply::BlobTarget(Ok(Some("target".into())))
        }
    }
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server = MigrationExchangeServer::new(
        Arc::new(ArmPanic(flag.clone())),
        Arc::new(PanicClock(flag)),
        Arc::new(Ids::default()),
        1,
    );
    let peer = PeerIdentity::new("a");
    let id = uploaded(&server, &peer).await;
    assert_eq!(
        server
            .handle(&peer, MigrationExchangeRequest::Execute { transfer: id })
            .await,
        error(ExchangeErrorKind::Unavailable)
    );
    for _ in 0..100_000 {
        if server.state.lock().is_empty() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("completion unwind must not strand admission");
}

#[test]
fn stopped_runtime_submission_releases_admission_without_deadlocking() {
    const CHILD: &str = "TINYSTORAGEDRIVERS_STOPPED_RUNTIME_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        drop(runtime);
        handle.block_on(async {
            let (server, _) = server(1);
            let peer = PeerIdentity::new("a");
            let id = uploaded(&server, &peer).await;
            assert_eq!(
                server
                    .handle(&peer, MigrationExchangeRequest::Execute { transfer: id })
                    .await,
                error(ExchangeErrorKind::Unavailable)
            );
            assert!(server.state.lock().is_empty());
            begin(&server, &peer, 1).await;
        });
        return;
    }
    // Runtime rejection drops the blocking closure synchronously. Isolate the
    // regression in a killable child so a mutex deadlock cannot hang the suite.
    let thread = std::thread::current();
    let name = thread.name().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env(CHILD, "1")
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "stopped-runtime child failed: {status}");
            break;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("stopped-runtime submission deadlocked; child killed and reaped");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
