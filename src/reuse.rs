//! Back-to-back TCP sessions over one data channel.
//!
//! A plain data channel forwards exactly one visitor: closing the connection is the only
//! way to say "this visitor is done", so every visitor pays for a fresh connection and
//! transport handshake. A reusable channel frames the forwarded bytes instead. When a
//! session ends the connection is still open and sits on a frame boundary, ready for the
//! next visitor.
//!
//! Inside a session each direction is a sequence of frames:
//!
//! ```text
//! frame := kind:u8  len:u16le  payload[len]
//! ```
//!
//! * `DATA` carries `len` payload bytes.
//! * `FIN` ends the direction gracefully, like a TCP half-close. `len` is 0.
//! * `RST` aborts the session. `len` is 0.
//!
//! A direction carries any number of `DATA` frames and then exactly one `FIN` or `RST`.
//! The session is over once both directions have ended. Whatever follows on the wire
//! belongs to the next session, which the server opens with a fresh `DataChannelCmd`.

use bytes::{Buf, BufMut, BytesMut};
use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;
use tokio::time::{self, Duration};

const FRAME_DATA: u8 = 0;
const FRAME_FIN: u8 = 1;
const FRAME_RST: u8 = 2;

const HEADER_LEN: usize = 3;
const MAX_PAYLOAD: usize = u16::MAX as usize;
const READ_BUF_SIZE: usize = 16 * 1024;

/// How long `SessionHandle::finish` may spend bringing a channel back to a frame
/// boundary before the channel is given up.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(5);
/// Payload bytes `SessionHandle::finish` is willing to throw away while waiting for the
/// peer to end its direction. Past that, a new connection is cheaper than draining.
const SETTLE_DRAIN_LIMIT: usize = 4 * 1024 * 1024;

/// Bytes read from the connection but not consumed yet. It outlives sessions because a
/// single read may return the tail of one session together with the head of the next.
struct ReadBuffer {
    buf: Box<[u8]>,
    pos: usize,
    end: usize,
}

impl ReadBuffer {
    fn new() -> Self {
        Self {
            buf: vec![0; READ_BUF_SIZE].into_boxed_slice(),
            pos: 0,
            end: 0,
        }
    }

    fn len(&self) -> usize {
        self.end - self.pos
    }

    fn is_empty(&self) -> bool {
        self.pos == self.end
    }

    fn chunk(&self) -> &[u8] {
        &self.buf[self.pos..self.end]
    }

    fn consume(&mut self, n: usize) {
        self.pos += n;
    }

    /// Reads more bytes from `io` behind the unconsumed ones. `Ok(0)` means EOF.
    fn poll_fill<S: AsyncRead + Unpin>(
        &mut self,
        cx: &mut Context<'_>,
        io: &mut S,
    ) -> Poll<io::Result<usize>> {
        if self.is_empty() {
            self.pos = 0;
            self.end = 0;
        } else if self.end == self.buf.len() {
            self.buf.copy_within(self.pos..self.end, 0);
            self.end -= self.pos;
            self.pos = 0;
        }
        debug_assert!(self.end < self.buf.len());

        let mut read_buf = ReadBuf::new(&mut self.buf[self.end..]);
        ready!(Pin::new(io).poll_read(cx, &mut read_buf))?;
        let n = read_buf.filled().len();
        self.end += n;
        Poll::Ready(Ok(n))
    }
}

/// A data channel that can carry several TCP sessions, one after another.
///
/// Between sessions it reads and writes the connection as is, which is how the
/// `DataChannelCmd` opening the next session travels.
pub struct ReusableChannel<S> {
    io: S,
    read_buf: ReadBuffer,
    write_buf: BytesMut,
}

impl<S> ReusableChannel<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(io: S) -> Self {
        Self {
            io,
            read_buf: ReadBuffer::new(),
            write_buf: BytesMut::new(),
        }
    }

    /// Starts a session. Drop the `Session` when forwarding is done, then call
    /// `SessionHandle::finish` to get the channel back.
    pub fn start_session(self) -> (Session<S>, SessionHandle<S>) {
        let (give_back, returned) = oneshot::channel();
        let session = Session {
            state: Some(SessionState {
                channel: self,
                rx: Direction::Open,
                tx: Direction::Open,
                remaining: 0,
                broken: false,
            }),
            give_back: Some(give_back),
        };
        (session, SessionHandle { returned })
    }

    /// Checks an idle channel without waiting. The peer sends nothing between sessions,
    /// so anything readable is either EOF, an error, or a protocol violation.
    pub fn is_idle_healthy(&mut self) -> bool {
        if !self.read_buf.is_empty() {
            return false;
        }
        let mut cx = Context::from_waker(Waker::noop());
        self.read_buf.poll_fill(&mut cx, &mut self.io).is_pending()
    }
}

impl<S> AsyncRead for ReusableChannel<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.read_buf.is_empty() {
            let n = this.read_buf.len().min(buf.remaining());
            buf.put_slice(&this.read_buf.chunk()[..n]);
            this.read_buf.consume(n);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.io).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for ReusableChannel<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Direction {
    Open,
    Fin,
    Rst,
}

struct SessionState<S> {
    channel: ReusableChannel<S>,
    rx: Direction,
    tx: Direction,
    /// Payload bytes of the current inbound `DATA` frame not delivered yet.
    remaining: usize,
    /// The connection failed or lost framing. It can't be reused.
    broken: bool,
}

impl<S> SessionState<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn fail<T>(&mut self, error: impl Into<io::Error>) -> Poll<io::Result<T>> {
        self.broken = true;
        Poll::Ready(Err(error.into()))
    }

    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            match self.rx {
                Direction::Open => {}
                Direction::Fin => return Poll::Ready(Ok(())),
                Direction::Rst => return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into())),
            }
            if self.broken {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }

            if self.remaining == 0 {
                while self.channel.read_buf.len() < HEADER_LEN {
                    let channel = &mut self.channel;
                    match ready!(channel.read_buf.poll_fill(cx, &mut channel.io)) {
                        Ok(0) => return self.fail(io::ErrorKind::UnexpectedEof),
                        Ok(_) => {}
                        Err(e) => return self.fail(e),
                    }
                }
                let header = self.channel.read_buf.chunk();
                let (kind, len) = (header[0], u16::from_le_bytes([header[1], header[2]]));
                self.channel.read_buf.consume(HEADER_LEN);
                match kind {
                    FRAME_DATA => self.remaining = usize::from(len),
                    FRAME_FIN => self.rx = Direction::Fin,
                    FRAME_RST => self.rx = Direction::Rst,
                    _ => {
                        return self.fail(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unknown data channel frame kind {kind}"),
                        ))
                    }
                }
                continue;
            }

            if !self.channel.read_buf.is_empty() {
                let n = self
                    .remaining
                    .min(self.channel.read_buf.len())
                    .min(buf.remaining());
                buf.put_slice(&self.channel.read_buf.chunk()[..n]);
                self.channel.read_buf.consume(n);
                self.remaining -= n;
                return Poll::Ready(Ok(()));
            }

            // Nothing buffered: let the payload go straight into the caller's buffer,
            // stopping at the frame boundary.
            let max = self.remaining.min(buf.remaining());
            let n = {
                let mut limited = ReadBuf::new(buf.initialize_unfilled_to(max));
                if let Err(e) = ready!(Pin::new(&mut self.channel.io).poll_read(cx, &mut limited)) {
                    return self.fail(e);
                }
                limited.filled().len()
            };
            if n == 0 {
                return self.fail(io::ErrorKind::UnexpectedEof);
            }
            buf.advance(n);
            self.remaining -= n;
            return Poll::Ready(Ok(()));
        }
    }

    /// Writes out everything queued in `write_buf`.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while !self.channel.write_buf.is_empty() {
            let channel = &mut self.channel;
            match ready!(Pin::new(&mut channel.io).poll_write(cx, &channel.write_buf)) {
                Ok(0) => return self.fail(io::ErrorKind::WriteZero),
                Ok(n) => self.channel.write_buf.advance(n),
                Err(e) => return self.fail(e),
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if self.tx != Direction::Open {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // Header and payload leave in one write, so a frame never costs an extra packet
        // or TLS record. At most one frame is queued at a time.
        ready!(self.poll_drain(cx))?;
        let n = buf.len().min(MAX_PAYLOAD);
        let write_buf = &mut self.channel.write_buf;
        write_buf.reserve(HEADER_LEN + n);
        write_buf.put_u8(FRAME_DATA);
        write_buf.put_u16_le(n as u16);
        write_buf.put_slice(&buf[..n]);

        // Whatever doesn't go out now goes out with the next write or flush.
        if let Poll::Ready(Err(e)) = self.poll_drain(cx) {
            return Poll::Ready(Err(e));
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_drain(cx))?;
        match ready!(Pin::new(&mut self.channel.io).poll_flush(cx)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => self.fail(e),
        }
    }

    fn end_tx(&mut self, how: Direction) {
        let kind = match how {
            Direction::Fin => FRAME_FIN,
            _ => FRAME_RST,
        };
        self.channel.write_buf.put_u8(kind);
        self.channel.write_buf.put_u16_le(0);
        self.tx = how;
    }

    /// Ends the outbound direction. The connection itself stays open.
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.tx == Direction::Open {
            self.end_tx(Direction::Fin);
        }
        self.poll_flush(cx)
    }

    /// Brings the connection to the frame boundary that ends this session.
    async fn settle(&mut self) -> io::Result<()> {
        if self.broken {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if self.tx == Direction::Open {
            // Forwarding stopped before the half-close, so it was cut short.
            self.end_tx(Direction::Rst);
        }
        poll_fn(|cx| self.poll_flush(cx)).await?;

        if self.rx != Direction::Open {
            return Ok(());
        }
        let mut scratch = vec![0u8; READ_BUF_SIZE];
        let mut drained = 0;
        loop {
            let mut buf = ReadBuf::new(&mut scratch);
            match poll_fn(|cx| self.poll_read(cx, &mut buf)).await {
                Ok(()) if self.rx == Direction::Fin => return Ok(()),
                Ok(()) => {
                    drained += buf.filled().len();
                    if drained > SETTLE_DRAIN_LIMIT {
                        return Err(io::Error::other("peer keeps sending after the session"));
                    }
                }
                Err(_) if self.rx == Direction::Rst => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}

/// One visitor's byte stream on a `ReusableChannel`.
///
/// `shutdown` half-closes the session, not the connection. Dropping the session hands
/// the channel to its `SessionHandle`.
pub struct Session<S> {
    state: Option<SessionState<S>>,
    give_back: Option<oneshot::Sender<SessionState<S>>>,
}

impl<S> Session<S> {
    fn state(&mut self) -> &mut SessionState<S> {
        self.state
            .as_mut()
            .expect("session state is only taken on drop")
    }
}

impl<S> Drop for Session<S> {
    fn drop(&mut self) {
        if let (Some(state), Some(give_back)) = (self.state.take(), self.give_back.take()) {
            // A send failure means nobody wants the channel back. Dropping closes it.
            let _ = give_back.send(state);
        }
    }
}

impl<S> AsyncRead for Session<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut().state().poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for Session<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().state().poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().state().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().state().poll_shutdown(cx)
    }
}

/// Recovers the channel once its `Session` is dropped.
pub struct SessionHandle<S> {
    returned: oneshot::Receiver<SessionState<S>>,
}

impl<S> SessionHandle<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Waits for the session to be dropped, then ends it on the wire: sends `RST` if
    /// forwarding stopped before the half-close, and discards inbound frames until the
    /// peer ends its direction too.
    ///
    /// Returns `None` when the channel can't be reused: the connection failed, or the
    /// peer didn't end the session in time.
    pub async fn finish(self) -> Option<ReusableChannel<S>> {
        let mut state = self.returned.await.ok()?;
        match time::timeout(SETTLE_TIMEOUT, state.settle()).await {
            Ok(Ok(())) => Some(state.channel),
            Ok(Err(_)) | Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{copy_bidirectional, duplex, AsyncReadExt, AsyncWriteExt, DuplexStream};

    fn pair() -> (ReusableChannel<DuplexStream>, ReusableChannel<DuplexStream>) {
        let (a, b) = duplex(64 * 1024);
        (ReusableChannel::new(a), ReusableChannel::new(b))
    }

    /// One request/response exchange with a half-close in each direction.
    async fn exchange(
        left: ReusableChannel<DuplexStream>,
        right: ReusableChannel<DuplexStream>,
        request: &[u8],
        response: &[u8],
    ) -> (ReusableChannel<DuplexStream>, ReusableChannel<DuplexStream>) {
        let (mut left_session, left_handle) = left.start_session();
        let (mut right_session, right_handle) = right.start_session();

        let left_side = async {
            left_session.write_all(request).await.unwrap();
            left_session.shutdown().await.unwrap();
            let mut received = Vec::new();
            left_session.read_to_end(&mut received).await.unwrap();
            drop(left_session);
            (received, left_handle.finish().await.unwrap())
        };
        let right_side = async {
            let mut received = Vec::new();
            right_session.read_to_end(&mut received).await.unwrap();
            right_session.write_all(response).await.unwrap();
            right_session.shutdown().await.unwrap();
            drop(right_session);
            (received, right_handle.finish().await.unwrap())
        };
        let ((got_response, left), (got_request, right)) = tokio::join!(left_side, right_side);

        assert_eq!(got_request, request);
        assert_eq!(got_response, response);
        (left, right)
    }

    #[tokio::test]
    async fn carries_sessions_back_to_back() {
        // Given
        let (left, right) = pair();
        let big = vec![0xA5; 3 * MAX_PAYLOAD + 17];

        // When
        let (left, right) = exchange(left, right, b"first request", b"first response").await;
        let (left, right) = exchange(left, right, &big, b"").await;
        let (mut left, mut right) = exchange(left, right, b"", &big).await;

        // Then
        assert!(left.is_idle_healthy());
        assert!(right.is_idle_healthy());
    }

    #[tokio::test]
    async fn passes_raw_bytes_between_sessions() {
        // Given
        let (left, right) = pair();
        let (mut left, mut right) = exchange(left, right, b"ping", b"pong").await;

        // When
        left.write_all(b"next-cmd").await.unwrap();
        let mut cmd = [0; 8];
        right.read_exact(&mut cmd).await.unwrap();

        // Then
        assert_eq!(&cmd, b"next-cmd");
        exchange(left, right, b"ping again", b"pong again").await;
    }

    #[tokio::test]
    async fn keeps_bytes_of_the_next_session_read_along_with_fin() {
        // Given: the peer ends the session and opens the next one before we read anything,
        // so a single read returns the FIN together with the next command.
        let (left, right) = pair();
        let (mut left_session, left_handle) = left.start_session();
        let (mut right_session, right_handle) = right.start_session();
        right_session.shutdown().await.unwrap();
        left_session.shutdown().await.unwrap();
        drop(left_session);
        let mut left = left_handle.finish().await.unwrap();
        left.write_all(b"next-cmd").await.unwrap();

        // When
        let mut eof = Vec::new();
        right_session.read_to_end(&mut eof).await.unwrap();
        drop(right_session);
        let mut right = right_handle.finish().await.unwrap();
        let mut cmd = [0; 8];
        right.read_exact(&mut cmd).await.unwrap();

        // Then
        assert!(eof.is_empty());
        assert_eq!(&cmd, b"next-cmd");
    }

    #[tokio::test]
    async fn aborted_session_resets_the_peer_and_keeps_the_channel() {
        // Given
        let (left, right) = pair();
        let (mut left_session, left_handle) = left.start_session();
        let (mut right_session, right_handle) = right.start_session();
        left_session.write_all(b"partial").await.unwrap();
        left_session.flush().await.unwrap();

        // When: the left side stops without a half-close.
        drop(left_session);
        let left_side = left_handle.finish();
        let right_side = async {
            let mut received = Vec::new();
            let error = right_session.read_to_end(&mut received).await.unwrap_err();
            drop(right_session);
            (received, error, right_handle.finish().await)
        };
        let (left, (received, error, right)) = tokio::join!(left_side, right_side);

        // Then
        assert_eq!(received, b"partial");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        exchange(left.unwrap(), right.unwrap(), b"after", b"abort").await;
    }

    #[tokio::test]
    async fn drains_a_peer_that_was_still_sending_when_we_aborted() {
        // Given
        let (left, right) = pair();
        let (left_session, left_handle) = left.start_session();
        let (mut right_session, right_handle) = right.start_session();
        right_session.write_all(&[7; 4096]).await.unwrap();
        right_session.shutdown().await.unwrap();

        // When
        drop(left_session);
        let left = left_handle.finish().await;
        drop(right_session);
        let right = right_handle.finish().await;

        // Then
        exchange(left.unwrap(), right.unwrap(), b"still", b"aligned").await;
    }

    #[tokio::test]
    async fn gives_up_the_channel_when_the_connection_closes() {
        // Given
        let (left, right) = pair();
        let (mut left_session, left_handle) = left.start_session();

        // When
        drop(right);
        let mut received = Vec::new();
        let error = left_session.read_to_end(&mut received).await.unwrap_err();
        drop(left_session);

        // Then
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(left_handle.finish().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_the_channel_when_the_peer_never_ends_the_session() {
        // Given
        let (left, _right) = pair();
        let (left_session, left_handle) = left.start_session();

        // When
        drop(left_session);

        // Then
        assert!(left_handle.finish().await.is_none());
    }

    #[tokio::test]
    async fn idle_channel_is_unhealthy_once_the_peer_is_gone() {
        // Given
        let (mut left, right) = pair();
        assert!(left.is_idle_healthy());

        // When
        drop(right);

        // Then
        assert!(!left.is_idle_healthy());
    }

    #[tokio::test]
    async fn forwards_with_copy_bidirectional() {
        // Given: visitor <-> left channel <-> right channel <-> echo service
        let (left, right) = pair();
        let (mut left_session, left_handle) = left.start_session();
        let (mut right_session, right_handle) = right.start_session();
        let (mut visitor, mut visitor_side) = duplex(1024);
        let (mut service_side, mut service) = duplex(1024);
        let payload = vec![0x5A; 200_000];

        // When
        let left_side = async {
            let _ = copy_bidirectional(&mut left_session, &mut visitor_side).await;
            drop(left_session);
            left_handle.finish().await
        };
        let right_side = async {
            let _ = copy_bidirectional(&mut right_session, &mut service_side).await;
            drop(right_session);
            right_handle.finish().await
        };
        let echo = async {
            let (mut rd, mut wr) = tokio::io::split(&mut service);
            tokio::io::copy(&mut rd, &mut wr).await.unwrap();
            wr.shutdown().await.unwrap();
        };
        let visit = async {
            let (mut rd, mut wr) = tokio::io::split(&mut visitor);
            let mut echoed = Vec::new();
            let (written, read) = tokio::join!(
                async {
                    wr.write_all(&payload).await?;
                    wr.shutdown().await
                },
                rd.read_to_end(&mut echoed),
            );
            written.unwrap();
            read.unwrap();
            echoed
        };
        let (left, right, (), echoed) = tokio::join!(left_side, right_side, echo, visit);

        // Then
        assert_eq!(echoed, payload);
        exchange(left.unwrap(), right.unwrap(), b"next", b"visitor").await;
    }

    #[cfg(feature = "compression-zstd")]
    #[tokio::test]
    async fn carries_zstd_sessions_back_to_back() {
        use crate::compression::ZstdStream;

        // Given
        let (mut left, mut right) = pair();
        let payload = b"compressible tunnel payload ".repeat(512);

        for _ in 0..2 {
            let (left_session, left_handle) = left.start_session();
            let (right_session, right_handle) = right.start_session();
            let mut sender = ZstdStream::new(left_session).unwrap();
            let mut receiver = ZstdStream::new(right_session).unwrap();

            // When
            let mut received = Vec::new();
            let (sent, read) = tokio::join!(
                async {
                    sender.write_all(&payload).await?;
                    sender.shutdown().await
                },
                receiver.read_to_end(&mut received),
            );
            sent.unwrap();
            read.unwrap();
            receiver.shutdown().await.unwrap();
            drop(sender);
            drop(receiver);

            // Then
            assert_eq!(received, payload);
            left = left_handle.finish().await.unwrap();
            right = right_handle.finish().await.unwrap();
        }
    }
}
