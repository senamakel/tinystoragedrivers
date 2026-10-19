//! Builder behavior for optional MongoDB capability requirements.

use super::*;
use tinystoragedrivers_core::{Capability, ErrorKind};

#[test]
fn requirements_default_to_off_and_can_be_enabled_together() {
    assert_eq!(MongoOpenOptions::new(), MongoOpenOptions::default());
    assert_eq!(
        MongoOpenOptions::default()
            .require_fencing()
            .require_transactions(),
        MongoOpenOptions {
            require_fencing: true,
            require_transactions: true,
        }
    );
}

#[test]
fn required_capabilities_fail_with_the_missing_typed_capability() {
    let fencing = MongoOpenOptions::new().require_fencing();
    assert_eq!(
        fencing.validate(false).unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );

    let transactions = MongoOpenOptions::new().require_transactions();
    assert_eq!(
        transactions.validate(false).unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Transactions)
    );
    assert!(fencing.validate(true).is_ok());
    assert!(transactions.validate(true).is_ok());
    assert!(MongoOpenOptions::new().validate(false).is_ok());
}
