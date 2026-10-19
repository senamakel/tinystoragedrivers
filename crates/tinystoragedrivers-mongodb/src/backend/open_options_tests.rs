//! Builder behavior for optional MongoDB capability requirements.

use super::*;

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
