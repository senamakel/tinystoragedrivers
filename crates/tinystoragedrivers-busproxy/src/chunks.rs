//! Moving blob bytes between a `BlobReader` and a `TinyBus` stream.
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tinybus::{MAX_CHUNK_LEN, StreamReader, StreamWriter};
use tinystoragedrivers_core::BlobReader;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::sync::mpsc;

/// How many received chunks wait for the consumer. Small on purpose: the
/// stream's own window is the buffer, this only decouples the two tasks.
const READ_AHEAD: usize = 2;

#[cfg(test)]
fn stream_error_category(error: &tinybus::Error) -> &'static str {
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

/// An `AsyncRead` over the chunks a spawned task pulls from a stream.
struct ChunkReader {
    rx: mpsc::Receiver<io::Result<Vec<u8>>>,
    current: Vec<u8>,
    at: usize,
}

impl AsyncRead for ChunkReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.at < self.current.len() {
                let take = buf.remaining().min(self.current.len() - self.at);
                let (from, to) = (self.at, self.at + take);
                buf.put_slice(&self.current[from..to]);
                self.at = to;
                return Poll::Ready(Ok(()));
            }
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    self.current = chunk;
                    self.at = 0;
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                // The pump finished cleanly: end of stream.
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// A `BlobReader` over a received stream. A stream that aborts or goes idle
/// surfaces as an I/O error on the reader, never as a short clean end.
#[must_use]
pub(crate) fn blob_reader(mut stream: StreamReader) -> BlobReader {
    let (tx, rx) = mpsc::channel(READ_AHEAD);
    tokio::spawn(async move {
        loop {
            match stream.next_chunk().await {
                Ok(Some(chunk)) => {
                    if tx.send(Ok(chunk)).await.is_err() {
                        // The consumer is gone; dropping the reader aborts the
                        // sender's next write.
                        return;
                    }
                }
                Ok(None) => return,
                Err(error) => {
                    #[cfg(test)]
                    eprintln!(
                        "storage stream reader error category={}",
                        stream_error_category(&error)
                    );
                    #[cfg(not(test))]
                    let _ = error;
                    let _ = tx
                        .send(Err(io::Error::other("storage blob stream failed")))
                        .await;
                    return;
                }
            }
        }
    });
    Box::pin(ChunkReader {
        rx,
        current: Vec::new(),
        at: 0,
    })
}

/// Why [`pump`] stopped.
#[derive(Debug)]
pub(crate) enum PumpError {
    /// The source ended before `len` bytes.
    Short,
    /// The source failed.
    Source,
    /// The bus refused a chunk.
    Bus(tinybus::Error),
}

/// Write exactly `len` bytes from `reader` into `writer` in stream-sized chunks
/// and close it. On failure the stream is aborted so the receiver stops
/// waiting.
///
/// # Errors
/// [`PumpError`] naming which side failed.
pub(crate) async fn pump(
    mut writer: StreamWriter,
    reader: &mut BlobReader,
    len: u64,
) -> Result<(), PumpError> {
    let mut remaining = len;
    let mut buf = vec![0_u8; MAX_CHUNK_LEN.min(usize::try_from(len).unwrap_or(MAX_CHUNK_LEN))];
    let result = async {
        while remaining > 0 {
            let want = usize::try_from(remaining).map_or(buf.len(), |rest| rest.min(buf.len()));
            reader.read_exact(&mut buf[..want]).await.map_err(|error| {
                if error.kind() == io::ErrorKind::UnexpectedEof {
                    PumpError::Short
                } else {
                    PumpError::Source
                }
            })?;
            writer
                .write_chunk(&buf[..want])
                .await
                .map_err(PumpError::Bus)?;
            remaining -= want as u64;
        }
        Ok(())
    }
    .await;
    // `write_chunk` borrows the writer mutably above, so close it here.
    match result {
        Ok(()) => writer.finish().await.map(|_| ()).map_err(PumpError::Bus),
        Err(error) => {
            let _ = writer.abort().await;
            Err(error)
        }
    }
}

#[cfg(test)]
#[path = "chunks_tests.rs"]
mod tests;
