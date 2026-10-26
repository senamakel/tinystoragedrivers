//! ChaCha20-Poly1305 envelopes in OpenHuman's formats.
//!
//! Two encodings, both byte-for-byte what OpenHuman's
//! `security::keyring::{crypto, encrypted_store}` produce and accept:
//!
//! - **Blob**: `nonce (12) ‖ ciphertext ‖ tag (16)`, raw bytes. This is the
//!   whole content of a `secrets.enc` file. See [`encrypt`] and [`decrypt`].
//! - **`enc2:` string**: `enc2:` followed by the blob in lowercase hex (upper
//!   case is accepted on read). This is how an encrypted config field is
//!   written. See [`encrypt_enc2`] and [`decrypt_enc2`].
//!
//! Every nonce is fresh from the OS generator, and no associated data is
//! bound, which is what keeps an existing value decrypting here.
//!
//! Failures are [`ErrorKind::Crypto`](tinystoragedrivers_types::ErrorKind::Crypto)
//! and their messages never contain key or plaintext bytes.
//!
//! ```
//! use tinystoragedrivers_secrets::crypto;
//!
//! let key = crypto::generate_key()?;
//! let stored = crypto::encrypt_enc2(&key, b"sk-live-123")?;
//! assert!(stored.starts_with("enc2:"));
//! assert_eq!(crypto::decrypt_enc2(&key, &stored)?.as_slice(), b"sk-live-123");
//! # Ok::<(), tinystoragedrivers_types::StorageError>(())
//! ```

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use tinystoragedrivers_types::{Result, StorageError};
use zeroize::Zeroizing;

/// Key length in bytes (256-bit).
pub const KEY_LEN: usize = 32;
/// Nonce length in bytes, the prefix of every blob.
pub const NONCE_LEN: usize = 12;
/// Poly1305 tag length in bytes, the suffix of every blob.
pub const TAG_LEN: usize = 16;
/// The prefix of an encrypted string value.
pub const ENC2_PREFIX: &str = "enc2:";
/// Prefix for legacy XOR-obfuscated string values accepted for migration.
pub const LEGACY_ENC_PREFIX: &str = "enc:";

/// A fresh random key from the OS generator.
///
/// # Errors
///
/// [`ErrorKind::Crypto`](tinystoragedrivers_types::ErrorKind::Crypto) when the
/// OS random source fails.
pub fn generate_key() -> Result<Zeroizing<[u8; KEY_LEN]>> {
    let mut key = Zeroizing::new([0u8; KEY_LEN]);
    fill_random(&mut key[..])?;
    Ok(key)
}

/// Fill `buf` from the OS generator, reporting a failure instead of
/// panicking as `RngCore::fill_bytes` would.
fn fill_random(buf: &mut [u8]) -> Result<()> {
    getrandom::fill(buf).map_err(|_| StorageError::crypto("the os random source failed"))
}

/// Encrypt `plaintext` into a `nonce ‖ ciphertext ‖ tag` blob under a fresh
/// random nonce.
///
/// # Errors
///
/// [`ErrorKind::Crypto`](tinystoragedrivers_types::ErrorKind::Crypto) when the
/// OS random source fails, or the cipher refuses the input (only possible
/// beyond its 256 GiB limit).
pub fn encrypt(key: &[u8; KEY_LEN], plaintext: &[u8]) -> Result<Vec<u8>> {
    let key = Key::from(*key);
    let cipher = ChaCha20Poly1305::new(&key);
    let mut nonce = [0u8; NONCE_LEN];
    fill_random(&mut nonce)?;
    let ciphertext = cipher
        .encrypt(&Nonce::from(nonce), plaintext)
        .map_err(|_| StorageError::crypto("encryption failed"))?;
    let mut blob = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    blob.extend_from_slice(&nonce);
    blob.extend_from_slice(&ciphertext);
    Ok(blob)
}

/// Decrypt a blob produced by [`encrypt`] (or OpenHuman's `chacha20_encrypt`).
///
/// # Errors
///
/// [`ErrorKind::Crypto`](tinystoragedrivers_types::ErrorKind::Crypto) when the
/// blob is too short to hold a nonce and tag, or fails authentication (wrong
/// key, or tampered data).
pub fn decrypt(key: &[u8; KEY_LEN], blob: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if blob.len() < NONCE_LEN + TAG_LEN {
        return Err(StorageError::crypto(
            "encrypted blob is too short to hold a nonce and tag",
        ));
    }
    let (nonce, ciphertext) = blob.split_at(NONCE_LEN);
    let nonce = Nonce::try_from(nonce)
        .map_err(|_| StorageError::crypto("encrypted blob has an invalid nonce"))?;
    let key = Key::from(*key);
    let cipher = ChaCha20Poly1305::new(&key);
    cipher
        .decrypt(&nonce, ciphertext)
        .map(Zeroizing::new)
        .map_err(|_| StorageError::crypto("decryption failed: wrong key or tampered data"))
}

/// Encrypt `plaintext` into an `enc2:<hex>` string.
///
/// Unlike OpenHuman's `SecretStore::encrypt`, an empty plaintext is encrypted
/// too rather than passed through; OpenHuman decrypts the result to an empty
/// string.
///
/// # Errors
///
/// As [`encrypt`].
pub fn encrypt_enc2(key: &[u8; KEY_LEN], plaintext: &[u8]) -> Result<String> {
    let blob = encrypt(key, plaintext)?;
    Ok(format!("{ENC2_PREFIX}{}", hex_encode(&blob)))
}

/// Decrypt an `enc2:<hex>` string.
///
/// # Errors
///
/// [`ErrorKind::Crypto`](tinystoragedrivers_types::ErrorKind::Crypto) when the
/// value lacks the `enc2:` prefix, the hex is malformed, or [`decrypt`] fails.
pub fn decrypt_enc2(key: &[u8; KEY_LEN], value: &str) -> Result<Zeroizing<Vec<u8>>> {
    let hex = value
        .strip_prefix(ENC2_PREFIX)
        .ok_or_else(|| StorageError::crypto("value is not an enc2: ciphertext"))?;
    let blob = hex_decode(hex)?;
    decrypt(key, &blob)
}

/// Whether `value` is an `enc2:` ciphertext.
#[must_use]
pub fn is_enc2(value: &str) -> bool {
    value.starts_with(ENC2_PREFIX)
}

/// Decode a legacy `enc:<hex>` value using repeating-key XOR.
///
/// This unauthenticated format is retained only so hosts can migrate old
/// values to [`encrypt_enc2`]. It must not be used to protect new data.
///
/// # Errors
///
/// Returns a crypto error if `value` lacks the `enc:` prefix or contains
/// malformed hexadecimal data.
pub fn decrypt_legacy_enc(key: &[u8; KEY_LEN], value: &str) -> Result<Zeroizing<Vec<u8>>> {
    let hex = value
        .strip_prefix(LEGACY_ENC_PREFIX)
        .ok_or_else(|| StorageError::crypto("value is not a legacy enc: ciphertext"))?;
    let mut bytes = Zeroizing::new(hex_decode(hex)?);
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte ^= key[index % KEY_LEN];
    }
    Ok(bytes)
}

/// Parse host-owned hexadecimal key material. Surrounding whitespace is
/// ignored.
///
/// # Errors
///
/// [`ErrorKind::Crypto`](tinystoragedrivers_types::ErrorKind::Crypto) for
/// malformed hex or a length other than 32 bytes.
pub fn key_from_hex(hex: &str) -> Result<Zeroizing<[u8; KEY_LEN]>> {
    let bytes = Zeroizing::new(hex_decode(hex.trim())?);
    if bytes.len() != KEY_LEN {
        return Err(StorageError::crypto(format!(
            "key must be {KEY_LEN} bytes, found {}",
            bytes.len()
        )));
    }
    let mut key = Zeroizing::new([0u8; KEY_LEN]);
    key.copy_from_slice(&bytes);
    Ok(key)
}

/// A key as lowercase hex, the inverse of [`key_from_hex`].
#[must_use]
pub fn key_to_hex(key: &[u8; KEY_LEN]) -> Zeroizing<String> {
    Zeroizing::new(hex_encode(key))
}

/// Lowercase hex, as OpenHuman's `hex_encode`.
pub(crate) fn hex_encode(data: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

/// Hex of either case, as OpenHuman's `hex_decode`, without its panic on a
/// non-ASCII input.
pub(crate) fn hex_decode(hex: &str) -> Result<Vec<u8>> {
    let (pairs, odd) = hex.as_bytes().as_chunks::<2>();
    if !odd.is_empty() {
        return Err(StorageError::crypto("hex string has odd length"));
    }
    pairs
        .iter()
        .map(|&[high, low]| Ok((nibble(high)? << 4) | nibble(low)?))
        .collect()
}

fn nibble(digit: u8) -> Result<u8> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(StorageError::crypto("hex string contains a non-hex digit")),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
pub(crate) mod tests;
