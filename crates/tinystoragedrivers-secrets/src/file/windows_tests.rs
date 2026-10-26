use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn windows_sharing_violations_retry_until_publication_succeeds() {
    let attempts = AtomicUsize::new(0);
    let result = retry_av_transient(|| {
        if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
            Err(io::Error::from_raw_os_error(32))
        } else {
            Ok(())
        }
    });
    assert!(result.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[test]
fn windows_non_av_errors_do_not_retry() {
    let attempts = AtomicUsize::new(0);
    let result: io::Result<()> = retry_av_transient(|| {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::from_raw_os_error(87))
    });
    assert_eq!(result.unwrap_err().raw_os_error(), Some(87));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}
