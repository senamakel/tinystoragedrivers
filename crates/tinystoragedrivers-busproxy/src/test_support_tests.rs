use super::*;

#[test]
fn diagnostic_stream_error_keeps_only_category_and_generic_display() {
    let sdk_error = tinybus::Error::StreamAborted {
        reason: "private stream reason".to_owned(),
    };
    let error = diagnostic_io_error(&sdk_error);

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(error.to_string(), "storage blob stream failed");
    assert_eq!(stream_error_category(&error), Some("StreamAborted"));
    assert!(!format!("{error:?}").contains("private stream reason"));
}
