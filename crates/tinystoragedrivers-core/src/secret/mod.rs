//! Shared, content-free validation for secret names and list prefixes.

use crate::error::{Result, StorageError};

/// The maximum UTF-8 byte length of a secret name or prefix.
pub const MAX_SECRET_NAME_LEN: usize = 256;

/// Validate a complete secret name.
///
/// # Errors
///
/// Returns [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when
/// `name` is empty, longer than [`MAX_SECRET_NAME_LEN`], or contains a control
/// character. The error does not include the supplied name.
pub fn validate_secret_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_SECRET_NAME_LEN {
        return Err(StorageError::invalid_input(format!(
            "secret name must be 1 to {MAX_SECRET_NAME_LEN} bytes"
        )));
    }
    validate_printable(name)
}

/// Validate a list prefix for secret names. Empty is allowed to the `SecretStore`
/// port, whose `list("")` means all secrets; hosts may impose a narrower grant.
///
/// # Errors
///
/// Returns [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when
/// `prefix` is longer than [`MAX_SECRET_NAME_LEN`] or contains a control
/// character. The error does not include the supplied prefix.
pub fn validate_secret_prefix(prefix: &str) -> Result<()> {
    if prefix.len() > MAX_SECRET_NAME_LEN {
        return Err(StorageError::invalid_input(format!(
            "secret name prefix must be at most {MAX_SECRET_NAME_LEN} bytes"
        )));
    }
    validate_printable(prefix)
}

fn validate_printable(text: &str) -> Result<()> {
    if text.chars().any(char::is_control) {
        return Err(StorageError::invalid_input(
            "secret names must not contain control characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
