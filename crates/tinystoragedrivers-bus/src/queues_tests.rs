//! Queues wire shapes: pinned representations and authority rejection.
use super::*;
use serde_json::json;

#[test]
fn queue_selectors_are_fixed() {
    assert_eq!(QUEUES_OBJECT, "Queues");
    assert_eq!(QUEUES_SELECTORS.len(), 11);
    for selector in QUEUES_SELECTORS {
        assert!(is_queues_selector(selector));
    }
    assert!(!is_queues_selector("Drop"));
}

#[test]
fn enqueue_pins_its_wire_representation() {
    let request = EnqueueRequest {
        queue: "inbound".into(),
        messages: vec![QueueMessage {
            payload: json!({"text": "hi"}),
            priority: 3,
            delay_ms: 0,
            group: Some("chat".into()),
            dedupe_key: None,
        }],
    };
    let wire = serde_json::to_value(&request).unwrap();
    assert_eq!(
        wire,
        json!({"queue":"inbound","messages":[
            {"payload_json":"{\"text\":\"hi\"}","priority":3,"delay_ms":0,"group":"chat"}
        ]})
    );
    assert_eq!(
        serde_json::from_value::<EnqueueRequest>(wire).unwrap(),
        request
    );
    // Options default, so a sparse sender decodes.
    let sparse: QueueMessage = serde_json::from_value(json!({"payload_json":"1"})).unwrap();
    assert_eq!(sparse.priority, 0);
    assert_eq!(sparse.payload, json!(1));
}

#[test]
fn delivery_and_dead_letter_pin_their_wire_representation() {
    let delivery = QueueDelivery {
        receipt: QueueReceipt {
            id: "m1".into(),
            token: "t1".into(),
        },
        payload: json!([1]),
        attempt: 2,
        priority: 0,
        group: None,
        enqueued_ms: 10,
        lease_until_ms: 20,
    };
    let wire = serde_json::to_value(&delivery).unwrap();
    assert_eq!(
        wire,
        json!({"receipt":{"id":"m1","token":"t1"},"payload_json":"[1]","attempt":2,
               "priority":0,"enqueued_ms":10,"lease_until_ms":20})
    );
    assert_eq!(
        serde_json::from_value::<QueueDelivery>(wire).unwrap(),
        delivery
    );
    let dead = QueueDeadLetter {
        id: "m1".into(),
        payload: json!(null),
        attempt: 3,
        reason: "max attempts exceeded".into(),
        group: Some("g".into()),
        enqueued_ms: 5,
    };
    let wire = serde_json::to_value(&dead).unwrap();
    assert_eq!(wire["payload_json"], "null");
    assert_eq!(
        serde_json::from_value::<QueueDeadLetter>(wire).unwrap(),
        dead
    );
}

#[test]
fn retry_and_replies_pin_their_wire_representation() {
    let after = QueueRetry::After { delay_ms: 500 };
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        json!({"kind":"after","delay_ms":500})
    );
    let dead = QueueRetry::DeadLetter {
        reason: "bad".into(),
    };
    assert_eq!(
        serde_json::to_value(&dead).unwrap(),
        json!({"kind":"dead_letter","reason":"bad"})
    );
    let nack = NackRequest {
        queue: "q".into(),
        receipt: QueueReceipt {
            id: "a".into(),
            token: "b".into(),
        },
        retry: dead,
    };
    let wire = serde_json::to_value(&nack).unwrap();
    assert_eq!(serde_json::from_value::<NackRequest>(wire).unwrap(), nack);
    let stats = QueueStatsReply {
        ready: 1,
        delayed: 2,
        leased: 3,
        dead: 4,
    };
    assert_eq!(
        serde_json::to_value(stats).unwrap(),
        json!({"ready":1,"delayed":2,"leased":3,"dead":4})
    );
    let features = QueueFeaturesReply {
        dedupe: true,
        groups: false,
        priorities: true,
        delay: false,
    };
    assert_eq!(
        serde_json::to_value(features).unwrap(),
        json!({"dedupe":true,"groups":false,"priorities":true,"delay":false})
    );
    let spec: QueueSpec = serde_json::from_value(json!({"max_attempts": 4})).unwrap();
    assert_eq!(spec.dedupe_window_ms, 0);
}

#[test]
fn queue_requests_reject_authority_selectors() {
    let forged = [
        json!({"queue":"q","scope":"bob"}),
        json!({"queue":"q","url":"amqp://x"}),
    ];
    for wire in forged {
        assert!(serde_json::from_value::<QueueRequest>(wire).is_err());
    }
    assert!(
        serde_json::from_value::<ReceiveRequest>(
            json!({"queue":"q","max":1,"visibility_ms":1,"consumer":"w","scope":"x"})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<EnsureQueueRequest>(
            json!({"queue":"q","spec":{"max_attempts":1,"db":"x"}})
        )
        .is_err()
    );
    assert!(serde_json::from_value::<RedriveRequest>(json!({"queue":"q","id":"a","x":1})).is_err());
    assert!(
        serde_json::from_value::<DeadLettersRequest>(json!({"queue":"q","limit":1,"x":1})).is_err()
    );
    assert!(
        serde_json::from_value::<ExtendRequest>(
            json!({"queue":"q","receipt":{"id":"a","token":"b"},"visibility_ms":1,"x":1})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<ReceiptRequest>(
            json!({"queue":"q","receipt":{"id":"a","token":"b","x":1}})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<QueueRetry>(json!({"kind":"after","delay_ms":1,"x":1})).is_err()
    );
    assert!(
        serde_json::from_value::<EnqueuedMessage>(json!({"id":"a","duplicate":false,"x":1}))
            .is_err()
    );
}
