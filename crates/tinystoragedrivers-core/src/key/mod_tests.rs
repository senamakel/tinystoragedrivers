//! Escaping, hashing, splitting, ULIDs and the serde form of `Key`.

use super::*;
use crate::{ErrorKind, MAX_ID_LEN};

fn key(parts: &[&str]) -> Key {
    Key::of(parts).unwrap()
}

#[test]
fn joins_parts_with_a_slash() {
    let k = key(&["job", "7f", "2026-10-16T10:00Z"]);
    assert_eq!(k.as_str(), "job/7f/2026-10-16T10:00Z");
    assert_eq!(k.to_string(), "job/7f/2026-10-16T10:00Z");
    assert_eq!(k.as_ref(), "job/7f/2026-10-16T10:00Z");
    assert!(!k.is_hashed());
}

#[test]
fn accepts_owned_and_borrowed_parts() {
    let owned = Key::of(vec![String::from("a"), String::from("b")]).unwrap();
    assert_eq!(owned, key(&["a", "b"]));
}

#[test]
fn escapes_percent_slash_and_nul() {
    let k = key(&["a/b", "100%", "x\0y"]);
    assert_eq!(k.as_str(), "a%2Fb/100%25/x%00y");
}

#[test]
fn escapes_percent_before_slash_so_escapes_are_not_double_decoded() {
    assert_eq!(key(&["%2F"]).as_str(), "%252F");
    assert_eq!(key(&["%2F"]).split().unwrap(), ["%2F"]);
}

#[test]
fn escapes_a_leading_tilde_so_it_cannot_pose_as_a_hashed_key() {
    let k = key(&["~abc", "~def"]);
    assert_eq!(k.as_str(), "%7Eabc/~def");
    assert!(!k.is_hashed());
    assert_eq!(k.split().unwrap(), ["~abc", "~def"]);
}

#[test]
fn round_trips_awkward_parts() {
    let cases: &[&[&str]] = &[
        &["plain"],
        &["a/b", "c%d", "e\0f"],
        &["//", "%%", "\0\0"],
        &["héllo", "日本語", "🦀/🦀"],
        &["%2F", "%25", "%00", "%7E"],
        &["~", "~~"],
        &["", "a", ""],
        &["", ""],
    ];
    for parts in cases {
        let k = key(parts);
        assert_eq!(
            k.split().as_deref(),
            Some(&parts.iter().map(|p| (*p).to_owned()).collect::<Vec<_>>()[..]),
            "{parts:?}"
        );
    }
}

#[test]
fn allows_empty_parts_but_not_an_empty_part_list() {
    assert_eq!(key(&["a", "", "b"]).as_str(), "a//b");
    assert_eq!(key(&["", ""]).as_str(), "/");
    let err = Key::of(Vec::<&str>::new()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    // One empty part is an empty key, which no document id may be.
    assert_eq!(Key::of([""]).unwrap_err().kind(), ErrorKind::InvalidInput);
}

#[test]
fn keeps_a_key_of_exactly_the_id_limit() {
    let k = Key::of(["a".repeat(MAX_ID_LEN)]).unwrap();
    assert_eq!(k.as_str().len(), MAX_ID_LEN);
    assert!(!k.is_hashed());
}

#[test]
fn hashes_a_key_one_byte_over_the_id_limit() {
    let k = Key::of(["a".repeat(MAX_ID_LEN + 1)]).unwrap();
    assert!(k.is_hashed());
    assert_eq!(k.as_str().len(), 1 + 52);
    assert!(k.as_str().starts_with('~'));
    assert!(
        k.as_str()[1..]
            .bytes()
            .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
    );
    crate::validate_id(k.as_str()).unwrap();
}

#[test]
fn measures_the_limit_on_the_escaped_key() {
    // 171 slashes escape to 513 bytes; 170 escape to 510.
    let over = Key::of(["/".repeat(171)]).unwrap();
    assert!(over.is_hashed());
    let under = Key::of(["/".repeat(170)]).unwrap();
    assert!(!under.is_hashed());
    assert_eq!(under.as_str().len(), 510);
}

#[test]
fn hashed_keys_are_stable_and_distinct() {
    let a = Key::of(["a".repeat(600)]).unwrap();
    let again = Key::of(["a".repeat(600)]).unwrap();
    let other = Key::of(["a".repeat(601)]).unwrap();
    assert_eq!(a, again);
    assert_ne!(a, other);
    // Pinned so a changed digest or alphabet is caught: the id is stored data.
    assert_eq!(
        a.as_str(),
        "~xi24c4dssql7csm6bcdopyjpzw2kwafnieirblq6rcghm3ko24gq"
    );
}

#[test]
fn hashes_the_escaped_key_not_the_raw_parts() {
    let split_differently = Key::of(["a".repeat(300), "a".repeat(300)]).unwrap();
    let joined = Key::of(["a".repeat(300) + "%2F" + &"a".repeat(300)]).unwrap();
    assert!(split_differently.is_hashed() && joined.is_hashed());
    assert_ne!(split_differently, joined);
}

#[test]
fn split_is_none_for_a_hashed_key() {
    assert_eq!(Key::of(["a".repeat(513)]).unwrap().split(), None);
}

#[test]
fn split_is_none_for_a_malformed_escape() {
    for raw in ["a%", "a%2", "a%zz", "a%41"] {
        assert_eq!(Key::new(raw).unwrap().split(), None, "{raw}");
    }
}

#[test]
fn base32_matches_the_rfc_4648_vectors_lowercased_without_padding() {
    for (input, expected) in [
        ("", ""),
        ("f", "my"),
        ("fo", "mzxq"),
        ("foo", "mzxw6"),
        ("foob", "mzxw6yq"),
        ("fooba", "mzxw6ytb"),
        ("foobar", "mzxw6ytboi"),
    ] {
        assert_eq!(base32_lower(input.as_bytes()), expected);
    }
}

#[test]
fn new_applies_the_document_id_rules() {
    assert_eq!(Key::new("a/b").unwrap().as_str(), "a/b");
    assert_eq!(Key::new("").unwrap_err().kind(), ErrorKind::InvalidInput);
    assert_eq!(
        Key::new("a\0b").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        Key::new("a".repeat(MAX_ID_LEN + 1)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(Key::new("a".repeat(MAX_ID_LEN)).is_ok());
    assert!(Key::new("~abc").unwrap().is_hashed());
}

#[test]
fn ulid_is_26_crockford_characters() {
    let k = Key::ulid();
    assert_eq!(k.as_str().len(), 26);
    assert!(
        k.as_str()
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase())
    );
    assert!(!k.is_hashed());
    assert_eq!(k.split().unwrap(), [k.as_str()]);
    assert_ne!(k, Key::ulid());
}

#[test]
fn ulids_sort_by_timestamp() {
    let ts = [0, 1, 1_000, 1_760_000_000_000, 1_760_000_000_001];
    let keys: Vec<Key> = ts.iter().map(|ms| Key::ulid_at(*ms)).collect();
    let mut sorted = keys.clone();
    sorted.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(keys, sorted);
    assert_eq!(Key::ulid_at(1_760_000_000_000).as_str().len(), 26);
}

#[test]
fn ulid_now_sorts_after_an_earlier_timestamp() {
    assert!(Key::ulid().as_str() > Key::ulid_at(1_000).as_str());
}

#[test]
fn serializes_as_a_plain_json_string() {
    let k = key(&["a/b", "c"]);
    assert_eq!(serde_json::to_string(&k).unwrap(), "\"a%2Fb/c\"");
    let back: Key = serde_json::from_str("\"a%2Fb/c\"").unwrap();
    assert_eq!(back, k);
}

#[test]
fn deserializing_applies_the_id_rules() {
    assert!(serde_json::from_str::<Key>("\"\"").is_err());
    assert!(serde_json::from_str::<Key>("\"a\\u0000b\"").is_err());
    assert!(serde_json::from_str::<Key>("5").is_err());
}

// Digests independently calculated with Python's hashlib.sha256 +
// base64.b32encode(...).decode().lower().rstrip('=') on the stated escaped UTF-8.
#[test]
fn ascii_keys_keep_511_and_512_bytes_and_pin_the_513_byte_digest() {
    for length in [511, 512] {
        let raw = "a".repeat(length);
        let actual = Key::of([raw.as_str()]).unwrap();
        assert_eq!(actual.as_str(), raw);
        assert_eq!(actual.split().unwrap(), [raw]);
    }
    assert_eq!(
        Key::of(["a".repeat(513)]).unwrap().as_str(),
        "~ajbfyd23bwv7huvzcfpt65zduavnrph3cu2kburrmfh5ik4brd3a"
    );
}

#[test]
fn unicode_key_limit_counts_utf8_bytes_at_511_512_and_513() {
    let below = "é".repeat(255) + "a";
    let exact = "é".repeat(256);
    for (raw, length) in [(below, 511), (exact, 512)] {
        assert_eq!(raw.len(), length);
        let actual = Key::of([raw.as_str()]).unwrap();
        assert_eq!(actual.as_str(), raw);
        assert_eq!(actual.split().unwrap(), [raw]);
    }
    let over = "é".repeat(256) + "a";
    assert_eq!(over.len(), 513);
    assert_eq!(
        Key::of([over]).unwrap().as_str(),
        "~ygjl26abwbdkpdakap3p2mwcwarpcsguulbru5qo2vv6oh7rgd4q"
    );
}

#[test]
fn escaped_key_limit_counts_encoded_bytes_at_511_512_and_513() {
    for (suffix, length) in [("a", 511), ("ab", 512)] {
        let raw = "/".repeat(170) + suffix;
        let escaped = "%2F".repeat(170) + suffix;
        assert_eq!(escaped.len(), length);
        let actual = Key::of([raw.as_str()]).unwrap();
        assert_eq!(actual.as_str(), escaped);
        assert_eq!(actual.split().unwrap(), [raw]);
    }
    let over = "/".repeat(170) + "abc";
    assert_eq!(
        Key::of([over]).unwrap().as_str(),
        "~oiel4ergnm53dx2i6rqsj2cl2agounvhuqbsvqxsegu4nzfsr54a"
    );
}
