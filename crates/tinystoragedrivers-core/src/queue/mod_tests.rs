use super::*;
use serde_json::json;

#[test]
fn queue_names_are_broker_safe() {
    assert!(validate_queue_name("inbound.telegram-1_a").is_ok());
    assert!(validate_queue_name("").is_err());
    assert!(validate_queue_name("Upper").is_err());
    assert!(validate_queue_name("with space").is_err());
    assert!(validate_queue_name(&"a".repeat(MAX_QUEUE_NAME_LEN)).is_ok());
    assert!(validate_queue_name(&"a".repeat(MAX_QUEUE_NAME_LEN + 1)).is_err());
}

#[test]
fn messages_reject_out_of_range_options() {
    assert!(
        NewMessage::new(json!(1))
            .priority(MAX_PRIORITY)
            .validate()
            .is_ok()
    );
    assert!(
        NewMessage::new(json!(1))
            .priority(MAX_PRIORITY + 1)
            .validate()
            .is_err()
    );
    assert!(NewMessage::new(json!(1)).group("").validate().is_err());
    assert!(NewMessage::new(json!(1)).dedupe_key("").validate().is_err());
}

#[test]
fn the_default_spec_retries_and_forgets_keys_at_ack() {
    let spec = QueueSpec::default();
    assert!(spec.validate().is_ok());
    assert_eq!(spec.max_attempts, 8);
    assert_eq!(spec.dedupe_window_ms, 0);
}

#[test]
fn new_message_round_trips_with_defaults_omitted() {
    let message = NewMessage::new(json!({"t": "x"})).group("g").delay_ms(5);
    let wire = serde_json::to_value(&message).expect("serializes");
    assert!(wire.get("dedupe_key").is_none());
    let back: NewMessage = serde_json::from_value(wire).expect("deserializes");
    assert_eq!(back, message);
}
