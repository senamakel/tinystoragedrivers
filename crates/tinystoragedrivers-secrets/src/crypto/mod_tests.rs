//! Envelope round trips and OpenHuman golden vectors.
//!
//! The `OPENHUMAN_*` fixtures were produced by OpenHuman's own code path: its
//! `security/keyring/crypto.rs` copied verbatim into a scratch program, with
//! `SecretStore::encrypt`'s `format!("enc2:{}", hex_encode(blob))` and
//! `EncryptedFileBackend::write_map`'s `serde_json::to_vec(HashMap)` then
//! `chacha20_encrypt`, under [`FIXTURE_KEY_HEX`]. Nonces are random, so these
//! are exact bytes OpenHuman wrote, not ones this crate could regenerate.

use tinystoragedrivers_types::ErrorKind;

use super::*;

/// The key every OpenHuman fixture was encrypted under: bytes `0x00..=0x1f`.
pub(crate) const FIXTURE_KEY_HEX: &str =
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

/// `SecretStore::encrypt("sk-test-123")` under [`FIXTURE_KEY_HEX`].
pub(crate) const OPENHUMAN_ENC2: &str =
    "enc2:f73f99285c6ec63d341fbd959393da369f23cc55ba80f91a94d7e93c8749c2d682e5ddd005bea1";

/// A `secrets.enc` written by `EncryptedFileBackend` holding
/// `{"alice:api_key": "sk-live-abc", "alice:oauth": "tok-é"}`, as hex.
pub(crate) const OPENHUMAN_SECRETS_ENC_HEX: &str = "ff4809c7f47bbe052eb02f3c3dd56c0f82483b3f162f6484b94f9e44c764c72ed195080f32a144b2039eddd9a0c312f88bfdb68a3e9bf938bed8f768f2f2fdc66b5d847a67350e8edc154683904433ededfc";

pub(crate) fn fixture_key() -> Zeroizing<[u8; KEY_LEN]> {
    key_from_hex(FIXTURE_KEY_HEX).unwrap()
}

#[test]
fn decrypts_an_enc2_value_openhuman_wrote() {
    let plaintext = decrypt_enc2(&fixture_key(), OPENHUMAN_ENC2).unwrap();
    assert_eq!(plaintext.as_slice(), b"sk-test-123");
}

#[test]
fn decrypts_a_secrets_enc_blob_openhuman_wrote() {
    let blob = hex_decode(OPENHUMAN_SECRETS_ENC_HEX).unwrap();
    let json = decrypt(&fixture_key(), &blob).unwrap();
    let map: std::collections::BTreeMap<String, String> = serde_json::from_slice(&json).unwrap();
    assert_eq!(map["alice:api_key"], "sk-live-abc");
    assert_eq!(map["alice:oauth"], "tok-\u{e9}");
}

#[test]
fn enc2_is_prefix_then_lowercase_hex_of_nonce_ciphertext_tag() {
    let key = fixture_key();
    let value = encrypt_enc2(&key, b"sk-test-123").unwrap();
    let hex = value.strip_prefix("enc2:").unwrap();
    assert!(
        hex.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    assert_eq!(hex.len(), 2 * (NONCE_LEN + b"sk-test-123".len() + TAG_LEN));
    // Same length as OpenHuman's output for the same plaintext.
    assert_eq!(value.len(), OPENHUMAN_ENC2.len());
    assert!(is_enc2(&value));
    assert!(!is_enc2("sk-plain"));
}

#[test]
fn every_encryption_uses_a_fresh_nonce() {
    let key = generate_key().unwrap();
    let a = encrypt(&key, b"same").unwrap();
    let b = encrypt(&key, b"same").unwrap();
    assert_ne!(a[..NONCE_LEN], b[..NONCE_LEN]);
    assert_eq!(decrypt(&key, &a).unwrap().as_slice(), b"same");
}

#[test]
fn an_empty_plaintext_round_trips() {
    let key = generate_key().unwrap();
    let value = encrypt_enc2(&key, b"").unwrap();
    assert_eq!(decrypt_enc2(&key, &value).unwrap().as_slice(), &[] as &[u8]);
}

#[test]
fn uppercase_hex_decrypts_like_lowercase() {
    let upper = format!(
        "enc2:{}",
        OPENHUMAN_ENC2.strip_prefix("enc2:").unwrap().to_uppercase()
    );
    assert_eq!(
        decrypt_enc2(&fixture_key(), &upper).unwrap().as_slice(),
        b"sk-test-123"
    );
}

#[test]
fn a_wrong_key_or_tampering_is_a_crypto_error() {
    let wrong = generate_key().unwrap();
    let error = decrypt_enc2(&wrong, OPENHUMAN_ENC2).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Crypto);

    let mut blob = hex_decode(OPENHUMAN_ENC2.strip_prefix("enc2:").unwrap()).unwrap();
    let last = blob.len() - 1;
    blob[last] ^= 1;
    assert_eq!(
        decrypt(&fixture_key(), &blob).unwrap_err().kind(),
        ErrorKind::Crypto
    );
}

#[test]
fn malformed_values_are_crypto_errors_without_echoing_input() {
    let key = fixture_key();
    for bad in [
        "sk-plaintext",
        "enc2:abc",
        "enc2:zz",
        "enc2:é1",
        "enc2:00",
        "enc2:",
    ] {
        let error = decrypt_enc2(&key, bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Crypto, "{bad}");
        assert!(!error.to_string().contains("sk-plaintext"));
    }
    assert_eq!(
        decrypt(&key, &[0; NONCE_LEN + TAG_LEN - 1])
            .unwrap_err()
            .kind(),
        ErrorKind::Crypto
    );
}

#[test]
fn legacy_enc_values_can_be_decrypted_for_migration() {
    let key = [0x2a; KEY_LEN];
    let encrypted: Vec<u8> = b"legacy token"
        .iter()
        .enumerate()
        .map(|(index, byte)| byte ^ key[index % KEY_LEN])
        .collect();
    let value = format!("enc:{}", hex_encode(&encrypted));
    assert_eq!(
        decrypt_legacy_enc(&key, &value).unwrap().as_slice(),
        b"legacy token"
    );
    assert_eq!(
        decrypt_legacy_enc(&key, "enc:0g").unwrap_err().kind(),
        ErrorKind::Crypto
    );
    assert_eq!(
        decrypt_legacy_enc(&key, "enc2:00").unwrap_err().kind(),
        ErrorKind::Crypto
    );
}

#[test]
fn keys_round_trip_through_hex_and_reject_bad_lengths() {
    let key = generate_key().unwrap();
    let hex = key_to_hex(&key);
    assert_eq!(hex.len(), 64);
    assert_eq!(*key_from_hex(&format!("  {}\n", *hex)).unwrap(), *key);
    assert_eq!(key_from_hex("0011").unwrap_err().kind(), ErrorKind::Crypto);
    assert_eq!(key_from_hex("xyz!").unwrap_err().kind(), ErrorKind::Crypto);
}

#[test]
fn hex_matches_openhuman_encoding() {
    assert_eq!(hex_encode(&[0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
    assert_eq!(hex_decode("000FA5ff").unwrap(), [0x00, 0x0f, 0xa5, 0xff]);
}
