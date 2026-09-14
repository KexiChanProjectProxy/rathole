//! zstd encoder that keeps compression off the async runtime.
//!
//! High zstd levels spend tens of milliseconds per block and allocate tens of MiB per
//! context. Running that inside `poll_write` stalls a tokio worker, and with it every
//! other connection scheduled there. This encoder batches plaintext and hands each batch
//! to tokio's blocking pool; the async side only copies bytes and writes finished output.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::{io::AsyncWrite, runtime::Handle, task::JoinHandle};
use zstd::stream::raw::{Encoder, InBuffer, Operation, OutBuffer};

/// Plaintext accumulated before a batch is compressed without an explicit flush.
/// Matches zstd's maximum block size.
const BATCH_SIZE: usize = 128 * 1024;
/// Spare output space offered to each zstd call. At least `ZSTD_CStreamOutSize()`,
/// so every call makes progress.
const OUTPUT_CHUNK: usize = BATCH_SIZE + 1024;

#[derive(Clone, Copy)]
enum Step {
    Compress,
    Flush,
    Finish,
}

/// Everything the blocking pool needs, moved there and back as one unit.
struct Job {
    encoder: Encoder<'static>,
    input: Vec<u8>,
    output: Vec<u8>,
    #[cfg(test)]
    threads: Vec<std::thread::ThreadId>,
}

impl Job {
    fn run(mut self: Box<Self>, step: Step) -> io::Result<Box<Self>> {
        #[cfg(test)]
        self.threads.push(std::thread::current().id());

        let Job {
            encoder,
            input,
            output,
            ..
        } = &mut *self;
        output.clear();

        let mut pending = &input[..];
        while !pending.is_empty() {
            let mut in_buffer = InBuffer::around(pending);
            write_into_spare(output, |out| encoder.run(&mut in_buffer, out))?;
            pending = &pending[in_buffer.pos()..];
        }
        input.clear();

        match step {
            Step::Compress => {}
            Step::Flush => while write_into_spare(output, |out| encoder.flush(out))? > 0 {},
            Step::Finish => while write_into_spare(output, |out| encoder.finish(out, true))? > 0 {},
        }
        Ok(self)
    }
}

/// Runs one zstd call against `OUTPUT_CHUNK` bytes of spare space appended to `output`.
fn write_into_spare(
    output: &mut Vec<u8>,
    op: impl FnOnce(&mut OutBuffer<'_, [u8]>) -> io::Result<usize>,
) -> io::Result<usize> {
    let start = output.len();
    output.resize(start + OUTPUT_CHUNK, 0);
    let mut out = OutBuffer::around(&mut output[start..]);
    let result = op(&mut out);
    let written = out.pos();
    output.truncate(start + written);
    result
}

enum State {
    /// No compression in flight. `output` may still hold bytes not yet written.
    Idle(Box<Job>),
    Busy(JoinHandle<io::Result<Box<Job>>>),
    /// A compression job failed; the zstd context is gone.
    Failed,
}

pub struct OffloadedZstdEncoder<W> {
    writer: W,
    state: State,
    /// Bytes of the current job output already written to `writer`.
    written: usize,
    /// Plaintext was handed to zstd since the last flush.
    dirty: bool,
    finishing: bool,
}

impl<W> OffloadedZstdEncoder<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(writer: W, level: i32) -> io::Result<Self> {
        Ok(Self::from_encoder(writer, Encoder::new(level)?))
    }

    pub fn with_dict(writer: W, level: i32, dictionary: &[u8]) -> io::Result<Self> {
        Ok(Self::from_encoder(
            writer,
            Encoder::with_dictionary(level, dictionary)?,
        ))
    }

    fn from_encoder(writer: W, encoder: Encoder<'static>) -> Self {
        Self {
            writer,
            state: State::Idle(Box::new(Job {
                encoder,
                input: Vec::new(),
                output: Vec::new(),
                #[cfg(test)]
                threads: Vec::new(),
            })),
            written: 0,
            dirty: false,
            finishing: false,
        }
    }

    /// Waits for the in-flight job, then writes all of its output to `writer`.
    /// On `Ready(Ok)`, the state is `Idle` with an empty output buffer.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                State::Busy(handle) => {
                    let joined = ready!(Pin::new(handle).poll(cx));
                    match joined.map_err(io::Error::other).and_then(|job| job) {
                        Ok(job) => {
                            self.state = State::Idle(job);
                            self.written = 0;
                        }
                        Err(error) => {
                            self.state = State::Failed;
                            return Poll::Ready(Err(error));
                        }
                    }
                }
                State::Idle(job) => {
                    while self.written < job.output.len() {
                        let n =
                            ready!(Pin::new(&mut self.writer)
                                .poll_write(cx, &job.output[self.written..]))?;
                        if n == 0 {
                            return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                        }
                        self.written += n;
                    }
                    job.output.clear();
                    self.written = 0;
                    return Poll::Ready(Ok(()));
                }
                State::Failed => {
                    return Poll::Ready(Err(io::Error::other("zstd encoder failed earlier")));
                }
            }
        }
    }

    /// Moves the idle job onto the blocking pool. Call only right after `poll_drain`
    /// returned `Ready(Ok)`.
    fn dispatch(&mut self, step: Step) {
        if let State::Idle(job) = std::mem::replace(&mut self.state, State::Failed) {
            self.state = State::Busy(tokio::task::spawn_blocking(move || job.run(step)));
        }
    }
}

impl<W> AsyncWrite for OffloadedZstdEncoder<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.finishing {
            return Poll::Ready(Err(io::Error::other("write after shutdown")));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        ready!(this.poll_drain(cx))?;
        let State::Idle(job) = &mut this.state else {
            return Poll::Ready(Err(io::Error::other("zstd encoder is not idle")));
        };
        let len = buf.len().min(BATCH_SIZE - job.input.len());
        job.input.extend_from_slice(&buf[..len]);
        this.dirty = true;
        if job.input.len() == BATCH_SIZE {
            this.dispatch(Step::Compress);
        }
        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_drain(cx))?;
        if this.dirty && !this.finishing {
            this.dirty = false;
            this.dispatch(Step::Flush);
            ready!(this.poll_drain(cx))?;
        }
        Pin::new(&mut this.writer).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.finishing {
            ready!(this.poll_drain(cx))?;
            this.finishing = true;
            this.dirty = false;
            this.dispatch(Step::Finish);
        }
        ready!(this.poll_drain(cx))?;
        Pin::new(&mut this.writer).poll_shutdown(cx)
    }
}

impl<W> Drop for OffloadedZstdEncoder<W> {
    fn drop(&mut self) {
        // Freeing a high-level context unmaps tens of MiB; do that on the blocking pool too.
        // A `Busy` job is detached and dropped where it runs.
        if let State::Idle(job) = std::mem::replace(&mut self.state, State::Failed) {
            match Handle::try_current() {
                Ok(handle) => drop(handle.spawn_blocking(move || drop(job))),
                Err(_) => drop(job),
            }
        }
    }
}

#[cfg(test)]
impl<W> OffloadedZstdEncoder<W> {
    fn compression_threads(&self) -> &[std::thread::ThreadId] {
        match &self.state {
            State::Idle(job) => &job.threads,
            State::Busy(_) | State::Failed => &[],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OffloadedZstdEncoder, BATCH_SIZE};
    use async_compression::tokio::bufread::ZstdDecoder;
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

    fn payload(len: usize) -> Vec<u8> {
        b"{\"service\":\"tunnel\",\"status\":200,\"payload\":\"rathole offload\"}\n"
            .iter()
            .copied()
            .cycle()
            .take(len)
            .collect()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compresses_on_blocking_pool_not_runtime_thread() -> io::Result<()> {
        // Given
        let runtime_thread = std::thread::current().id();
        let mut encoder = OffloadedZstdEncoder::new(tokio::io::sink(), 19)?;

        // When
        encoder.write_all(&payload(3 * BATCH_SIZE + 17)).await?;
        encoder.flush().await?;

        // Then
        let threads = encoder.compression_threads();
        assert!(threads.len() >= 4, "expected batched jobs, got {threads:?}");
        assert!(threads.iter().all(|thread| *thread != runtime_thread));
        Ok(())
    }

    #[tokio::test]
    async fn roundtrips_multi_batch_stream_with_interleaved_flushes() -> io::Result<()> {
        // Given
        let (left, right) = tokio::io::duplex(8192);
        let mut encoder = OffloadedZstdEncoder::new(left, 3)?;
        let mut decoder = ZstdDecoder::new(BufReader::new(right));
        let data = payload(5 * BATCH_SIZE / 2 + 333);
        let mut received = Vec::new();

        // When
        let (sent, read) = tokio::join!(
            async {
                for (index, chunk) in data.chunks(7919).enumerate() {
                    encoder.write_all(chunk).await?;
                    if index % 5 == 0 {
                        encoder.flush().await?;
                    }
                }
                encoder.shutdown().await
            },
            decoder.read_to_end(&mut received),
        );
        sent?;
        read?;

        // Then
        assert_eq!(received, data);
        Ok(())
    }

    #[tokio::test]
    async fn flush_delivers_partial_batch_before_shutdown() -> io::Result<()> {
        // Given
        let (left, right) = tokio::io::duplex(8192);
        let mut encoder = OffloadedZstdEncoder::with_dict(left, 19, b"offload dictionary")?;
        let mut decoder = ZstdDecoder::with_dict(BufReader::new(right), b"offload dictionary")?;
        let message = b"data: first server-sent event\n\n";
        let mut received = vec![0; message.len()];

        // When
        encoder.write_all(message).await?;
        encoder.flush().await?;
        decoder.read_exact(&mut received).await?;

        // Then
        assert_eq!(&received, message);
        Ok(())
    }

    #[tokio::test]
    async fn empty_stream_shuts_down_to_valid_frame() -> io::Result<()> {
        // Given
        let (left, right) = tokio::io::duplex(8192);
        let mut encoder = OffloadedZstdEncoder::new(left, 3)?;
        let mut decoder = ZstdDecoder::new(BufReader::new(right));
        let mut received = Vec::new();

        // When
        encoder.shutdown().await?;
        decoder.read_to_end(&mut received).await?;

        // Then
        assert!(received.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn write_after_shutdown_fails() -> io::Result<()> {
        // Given
        let mut encoder = OffloadedZstdEncoder::new(tokio::io::sink(), 3)?;
        encoder.shutdown().await?;

        // When
        let result = encoder.write_all(b"late").await;

        // Then
        assert!(result.is_err());
        Ok(())
    }
}
