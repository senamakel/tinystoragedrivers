use super::*;
use tinybus::{Connection, StreamDescriptor, broker::Broker, transport::memory::MemoryBus};
use tokio::io::AsyncReadExt;

pub(super) fn stream_error_category(error: &tinybus::Error) -> &'static str {
    match error {
        tinybus::Error::StreamAborted { .. } => "StreamAborted",
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

#[test]
fn stream_error_diagnostic_reports_only_the_sdk_category() {
    let error = tinybus::Error::StreamAborted {
        reason: "private stream detail".to_owned(),
    };
    assert_eq!(stream_error_category(&error), "StreamAborted");
    assert_ne!(stream_error_category(&error), "private stream detail");
}

/// Two connected peers on an in-process broker: `(sender, receiver)`.
async fn peers() -> (Connection, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let sender = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let receiver = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (sender, receiver)
}

#[tokio::test(flavor = "multi_thread")]
async fn bytes_round_trip_through_a_stream_in_order() {
    let (sender, receiver) = peers().await;
    let to = receiver.unique_name().unwrap();
    let body: Vec<u8> = (0..1_300_000_u32).map(|at| (at % 253) as u8).collect();
    let writer = sender
        .open_stream(&to, StreamDescriptor::with_len(body.len() as u64))
        .await
        .unwrap();
    let mut reader = blob_reader(receiver.accept_stream(&writer.stream_ref()).unwrap());
    let mut source: BlobReader = Box::pin(std::io::Cursor::new(body.clone()));
    let len = body.len() as u64;
    let (pumped, collected) = tokio::join!(pump(writer, &mut source, len), async {
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        out
    });
    pumped.unwrap();
    assert_eq!(collected, body);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_stream_is_an_error_on_the_reader_not_a_clean_end() {
    let (sender, receiver) = peers().await;
    let to = receiver.unique_name().unwrap();
    let mut writer = sender
        .open_stream(&to, StreamDescriptor::with_len(10))
        .await
        .unwrap();
    let mut reader = blob_reader(receiver.accept_stream(&writer.stream_ref()).unwrap());
    writer.write_chunk(b"abc").await.unwrap();
    writer.abort().await.unwrap();
    let mut out = Vec::new();
    let error = reader.read_to_end(&mut out).await.unwrap_err();
    assert_eq!(error.to_string(), "storage blob stream failed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_short_source_aborts_the_stream_and_reports_short() {
    let (sender, receiver) = peers().await;
    let to = receiver.unique_name().unwrap();
    let writer = sender
        .open_stream(&to, StreamDescriptor::with_len(100))
        .await
        .unwrap();
    let mut reader = blob_reader(receiver.accept_stream(&writer.stream_ref()).unwrap());
    let mut source: BlobReader = Box::pin(std::io::Cursor::new(vec![1_u8; 10]));
    let result = pump(writer, &mut source, 100).await;
    assert!(matches!(result, Err(PumpError::Short)));
    let mut out = Vec::new();
    assert!(reader.read_to_end(&mut out).await.is_err());
}

/// A source that fails after its first read.
struct Failing(bool);

impl AsyncRead for Failing {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.0 {
            return Poll::Ready(Err(io::Error::other("source failed")));
        }
        self.0 = true;
        buf.put_slice(&[1, 2, 3]);
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_source_is_reported_as_the_sources_fault() {
    let (sender, receiver) = peers().await;
    let to = receiver.unique_name().unwrap();
    let writer = sender
        .open_stream(&to, StreamDescriptor::with_len(100))
        .await
        .unwrap();
    let _reader = receiver.accept_stream(&writer.stream_ref()).unwrap();
    let mut source: BlobReader = Box::pin(Failing(false));
    let result = pump(writer, &mut source, 100).await;
    assert!(matches!(result, Err(PumpError::Source)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receiver_that_walks_away_fails_the_writer_on_the_bus() {
    let (sender, receiver) = peers().await;
    let to = receiver.unique_name().unwrap();
    let writer = sender
        .open_stream(&to, StreamDescriptor::with_len(8 * 1024 * 1024))
        .await
        .unwrap();
    // Taking the reader and dropping it closes the window.
    drop(receiver.accept_stream(&writer.stream_ref()).unwrap());
    let mut source: BlobReader = Box::pin(std::io::Cursor::new(vec![0_u8; 8 * 1024 * 1024]));
    let result = pump(writer, &mut source, 8 * 1024 * 1024).await;
    assert!(matches!(result, Err(PumpError::Bus(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_zero_length_stream_closes_without_chunks() {
    let (sender, receiver) = peers().await;
    let to = receiver.unique_name().unwrap();
    let writer = sender
        .open_stream(&to, StreamDescriptor::with_len(0))
        .await
        .unwrap();
    let mut reader = blob_reader(receiver.accept_stream(&writer.stream_ref()).unwrap());
    let mut source: BlobReader = Box::pin(std::io::Cursor::new(Vec::new()));
    pump(writer, &mut source, 0).await.unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out.len(), 0);
}
