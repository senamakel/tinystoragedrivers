use super::*;

#[test]
fn diagnostic_stream_error_keeps_only_category_and_generic_display() {
    let sdk_error = tinybus::Error::StreamAborted {
        reason: "private stream reason".to_owned(),
    };
    let error = diagnostic_io_error(&sdk_error);

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(error.to_string(), "storage blob stream failed");
    assert_eq!(stream_error_category(&error), Some("StreamAborted/Other"));
    assert!(!format!("{error:?}").contains("private stream reason"));
}

#[test]
fn sdk_generated_abort_reasons_map_to_allowlisted_categories_only() {
    let cases = [
        (
            "the stream went idle while being read",
            "StreamAborted/ReadIdle",
        ),
        (
            "the connection closed mid-stream",
            "StreamAborted/ConnectionClosed",
        ),
        ("the sender aborted the stream", "StreamAborted/SenderAbort"),
        (
            "the receiver stopped reading",
            "StreamAborted/ReceiverStoppedReading",
        ),
        (
            "the sender closed a truncated stream",
            "StreamAborted/TruncatedClose",
        ),
        (
            "the stream went idle and was reaped",
            "StreamAborted/IdleReaped",
        ),
        (
            "the receiver never collected the stream",
            "StreamAborted/UncollectedReaped",
        ),
        (
            "the sender exceeded the chunk cap",
            "StreamAborted/ChunkLimit",
        ),
        (
            "the sender wrote chunks out of order",
            "StreamAborted/OutOfOrder",
        ),
        (
            "the sender exceeded the length it may write",
            "StreamAborted/LengthLimit",
        ),
    ];

    for (reason, expected_category) in cases {
        let sdk_error = tinybus::Error::StreamAborted {
            reason: reason.to_owned(),
        };
        let error = diagnostic_io_error(&sdk_error);

        assert_eq!(stream_error_category(&error), Some(expected_category));
        assert_eq!(error.to_string(), "storage blob stream failed");
        assert!(!format!("{error:?}").contains(reason));
    }
}

#[test]
fn other_sdk_error_families_map_to_finite_categories_without_debug_text() {
    let private_text = "private diagnostic test value";
    let errors = [
        (tinybus::Error::ConnectionClosed, "ConnectionClosed"),
        (
            tinybus::Error::Timeout {
                member: tinybus::MemberName::new("Read").unwrap(),
                timeout_ms: 5,
            },
            "Timeout",
        ),
        (
            tinybus::Error::UnknownStream {
                id: private_text.to_owned(),
            },
            "UnknownStream",
        ),
        (
            tinybus::Error::StreamTooLarge { limit: 4 },
            "StreamTooLarge",
        ),
        (
            tinybus::Error::TooManyStreams { limit: 1 },
            "TooManyStreams",
        ),
        (
            tinybus::Error::Transport(private_text.to_owned()),
            "Transport",
        ),
        (
            tinybus::Error::Protocol(private_text.to_owned()),
            "Protocol",
        ),
        (
            tinybus::Error::MethodFailed {
                name: "test.error".to_owned(),
                message: private_text.to_owned(),
            },
            "MethodFailed",
        ),
        (tinybus::Error::Backpressure, "Other"),
    ];

    for (sdk_error, expected_category) in errors {
        let error = diagnostic_io_error(&sdk_error);
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "storage blob stream failed");
        assert_eq!(stream_error_category(&error), Some(expected_category));
        assert!(!format!("{error:?}").contains(private_text));
    }
}
