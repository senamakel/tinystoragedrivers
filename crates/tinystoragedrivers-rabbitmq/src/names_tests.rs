//! Names are stable, scoped and collision-free; delays round up.
use super::*;

#[test]
fn the_scope_hash_is_stable_and_scopes_differ() {
    // Pinned: queues declared by an earlier release must still be found.
    assert_eq!(fnv1a(""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a("a"), 0xaf63_dc4c_8601_ec8c);
    let local = prefix(&Scope::local());
    assert!(local.starts_with("tsd/") && local.ends_with('/'));
    assert_eq!(local.len(), "tsd/".len() + 16 + 1);
    assert_ne!(local, prefix(&Scope::new("bob").unwrap()));
}

#[test]
fn dead_and_delay_queues_hang_off_the_main_name() {
    let names = Names::new("tsd/0000000000000000/", "inbound.chat");
    assert_eq!(names.main, "tsd/0000000000000000/inbound.chat");
    assert_eq!(names.dead, "tsd/0000000000000000/inbound.chat/dead");
    assert_eq!(
        names.delay(1024),
        "tsd/0000000000000000/inbound.chat/delay/1024"
    );
    let delays: Vec<_> = names.delays().collect();
    assert_eq!(delays.len(), 21);
    assert_eq!(delays[0].0, 256);
    assert_eq!(delays[20].0, MAX_DELAY_MS);
}

#[test]
fn delays_round_up_to_a_bounded_bucket() {
    assert_eq!(bucket(0).unwrap(), None);
    assert_eq!(bucket(1).unwrap(), Some(256));
    assert_eq!(bucket(256).unwrap(), Some(256));
    assert_eq!(bucket(257).unwrap(), Some(512));
    assert_eq!(bucket(60_000).unwrap(), Some(65_536));
    assert_eq!(bucket(MAX_DELAY_MS).unwrap(), Some(MAX_DELAY_MS));
    assert_eq!(
        bucket(MAX_DELAY_MS + 1).unwrap_err().kind(),
        tinystoragedrivers_core::ErrorKind::InvalidInput
    );
}
