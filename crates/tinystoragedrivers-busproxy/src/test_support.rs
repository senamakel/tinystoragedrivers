//! Opt-in helpers for observing content-free transport and shutdown stages.

/// Content-free stage in a governed registration's asynchronous shutdown.
#[cfg(feature = "test-stream-diagnostics")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistrationShutdownStage {
    /// The optional legacy endpoint shutdown future was entered.
    LegacyShutdownStarted,
    /// The optional legacy endpoint shutdown future returned.
    LegacyShutdownReturned,
    /// The linked service connection close future was entered.
    ConnectionCloseStarted,
    /// The linked service connection close future returned.
    ConnectionCloseReturned,
}

/// Sender used to observe one registration's finite shutdown stages.
#[cfg(feature = "test-stream-diagnostics")]
#[doc(hidden)]
pub type RegistrationShutdownProbe = tokio::sync::mpsc::UnboundedSender<RegistrationShutdownStage>;

#[cfg(feature = "test-stream-diagnostics")]
use std::fmt;
#[cfg(feature = "test-stream-diagnostics")]
use std::io;

pub(crate) fn sdk_error_category(error: &tinybus::Error) -> &'static str {
    match error {
        tinybus::Error::StreamAborted { reason } => match reason.as_str() {
            "the stream went idle while being read" => "StreamAborted/ReadIdle",
            "the connection closed mid-stream" => "StreamAborted/ConnectionClosed",
            "the sender aborted the stream" => "StreamAborted/SenderAbort",
            "the receiver stopped reading" => "StreamAborted/ReceiverStoppedReading",
            "the sender closed a truncated stream" => "StreamAborted/TruncatedClose",
            "the stream went idle and was reaped" => "StreamAborted/IdleReaped",
            "the receiver never collected the stream" => "StreamAborted/UncollectedReaped",
            "the sender exceeded the chunk cap" => "StreamAborted/ChunkLimit",
            "the sender wrote chunks out of order" => "StreamAborted/OutOfOrder",
            "the sender exceeded the length it may write" => "StreamAborted/LengthLimit",
            "the stream is already closed" => "StreamAborted/AlreadyClosed",
            "the stream is already being read" => "StreamAborted/AlreadyReading",
            _ => "StreamAborted/Other",
        },
        tinybus::Error::ConnectionClosed => "ConnectionClosed",
        tinybus::Error::Timeout { .. } => "Timeout",
        tinybus::Error::UnknownStream { .. } => "UnknownStream",
        tinybus::Error::StreamTooLarge { .. } => "StreamTooLarge",
        tinybus::Error::TooManyStreams { .. } => "TooManyStreams",
        tinybus::Error::Transport(_) => "Transport",
        tinybus::Error::Protocol(_) => "Protocol",
        tinybus::Error::MethodFailed { .. } => "MethodFailed",
        _ => "Other",
    }
}

#[cfg(feature = "test-stream-diagnostics")]
#[derive(Debug)]
struct DiagnosticStreamFailure {
    category: &'static str,
}

#[cfg(feature = "test-stream-diagnostics")]
impl fmt::Display for DiagnosticStreamFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("storage blob stream failed")
    }
}

#[cfg(feature = "test-stream-diagnostics")]
impl std::error::Error for DiagnosticStreamFailure {}

/// Build the normal generic stream error while retaining only its SDK category.
///
/// This helper exists only when the `test-stream-diagnostics` feature is enabled.
#[cfg(feature = "test-stream-diagnostics")]
#[doc(hidden)]
#[must_use]
pub fn diagnostic_io_error(error: &tinybus::Error) -> io::Error {
    io::Error::other(DiagnosticStreamFailure {
        category: sdk_error_category(error),
    })
}

/// Return the retained SDK category for a diagnostic stream error, if present.
///
/// This helper exists only when the `test-stream-diagnostics` feature is enabled.
#[cfg(feature = "test-stream-diagnostics")]
#[doc(hidden)]
#[must_use]
pub fn stream_error_category(error: &io::Error) -> Option<&'static str> {
    error
        .get_ref()?
        .downcast_ref::<DiagnosticStreamFailure>()
        .map(|failure| failure.category)
}

#[cfg(all(test, feature = "test-stream-diagnostics"))]
#[path = "test_support_tests.rs"]
mod tests;
